//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/migrations"
)

const method = marketplace.MethodUpsertOrder

func TestMigrationsAreRepeatableAndConcurrent(t *testing.T) {
	st, _ := setup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- st.Migrate(context.Background(), migrations.FS)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := queryInt(t, `SELECT count(*) FROM schema_migrations`); n != 2 {
		t.Fatalf("schema_migrations rows = %d, want 2", n)
	}
}

func TestUpsertIsIdempotentForUnchangedContent(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	fo := freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"sku":"A"}`)

	if n, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{fo}); err != nil || n != 1 {
		t.Fatalf("first upsert n=%d err=%v", n, err)
	}
	if n, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{fo}); err != nil || n != 0 {
		t.Fatalf("redelivery must be a no-op: n=%d err=%v", n, err)
	}
	changed := freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"sku":"B"}`)
	if n, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{changed}); err != nil || n != 1 {
		t.Fatalf("changed content n=%d err=%v", n, err)
	}
	o, err := st.GetOrderByExternalID(ctx, domain.MarketplaceWBLike, "ERP-1")
	if err != nil {
		t.Fatal(err)
	}
	if o.Version != 2 || o.ContentHash != changed.ContentHash {
		t.Fatalf("version=%d hash=%s", o.Version, o.ContentHash)
	}
}

func TestExpiredLeaseIsReclaimedAndStaleCompletionIsFenced(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, "ERP-1", domain.MarketplaceOzonLike, `{"a":1}`)}); err != nil {
		t.Fatal(err)
	}

	claimedA, err := st.ClaimOrders(ctx, "worker-a", 10, 5, 100*time.Millisecond)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("A claim: n=%d err=%v", len(claimedA), err)
	}
	if again, _ := st.ClaimOrders(ctx, "worker-b", 10, 5, time.Minute); len(again) != 0 {
		t.Fatal("a live lease must not be claimable")
	}

	time.Sleep(200 * time.Millisecond)
	claimedB, err := st.ClaimOrders(ctx, "worker-b", 10, 5, time.Minute)
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("B reclaim: n=%d err=%v", len(claimedB), err)
	}
	if claimedB[0].Attempts != 2 {
		t.Fatalf("reclaim must count an attempt, got %d", claimedB[0].Attempts)
	}

	// A finishes its (slow) remote call after losing the lease.
	if _, err := st.CompleteOrder(ctx, claimedA[0], method, "ref-A"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale completion must be fenced, got %v", err)
	}
	ref, found, err := st.LedgerRef(ctx, claimedB[0].ID, method, claimedB[0].ContentHash)
	if err != nil || !found || ref != "ref-A" {
		t.Fatalf("ledger must keep A's remote write for B: ref=%q found=%v err=%v", ref, found, err)
	}
	status, err := st.CompleteOrder(ctx, claimedB[0], method, ref)
	if err != nil || status != domain.OrderStatusSynced {
		t.Fatalf("B completion: status=%s err=%v", status, err)
	}
	if n := queryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'order.synced'`); n != 1 {
		t.Fatalf("order.synced events = %d, want 1", n)
	}
}

func TestContentChangeDuringSyncRequeuesOrder(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"v":1}`)}); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimOrders(ctx, "w", 10, 5, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}

	v2 := freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"v":2}`)
	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{v2}); err != nil {
		t.Fatal(err)
	}
	if other, _ := st.ClaimOrders(ctx, "w2", 10, 5, time.Minute); len(other) != 0 {
		t.Fatal("an order mid-sync must stay leased after a content change")
	}

	status, err := st.CompleteOrder(ctx, claimed[0], method, "ref-v1")
	if err != nil || status != domain.OrderStatusPending {
		t.Fatalf("stale content completion should re-queue: status=%s err=%v", status, err)
	}
	next, err := st.ClaimOrders(ctx, "w", 10, 5, time.Minute)
	if err != nil || len(next) != 1 || next[0].ContentHash != v2.ContentHash {
		t.Fatalf("v2 must be claimable: n=%d err=%v", len(next), err)
	}
	if n := queryInt(t, `SELECT count(*) FROM outbox_events`); n != 0 {
		t.Fatalf("no synced event may be emitted for stale content, got %d", n)
	}
}

func TestFailuresDeadLetterAfterMaxAttempts(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"v":1}`)}); err != nil {
		t.Fatal(err)
	}
	const maxAttempts = 2
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		claimed, err := st.ClaimOrders(ctx, "w", 10, maxAttempts, time.Minute)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("attempt %d claim: n=%d err=%v", attempt, len(claimed), err)
		}
		status, err := st.FailOrder(ctx, claimed[0], "boom", maxAttempts, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := domain.OrderStatusFailed
		if attempt == maxAttempts {
			want = domain.OrderStatusDead
		}
		if status != want {
			t.Fatalf("attempt %d: status %s, want %s", attempt, status, want)
		}
	}
	if left, _ := st.ClaimOrders(ctx, "w", 10, maxAttempts, time.Minute); len(left) != 0 {
		t.Fatal("dead orders must not be claimable")
	}
	if n := queryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'order.dead_lettered'`); n != 1 {
		t.Fatalf("dead-letter events = %d", n)
	}

	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{"v":2}`)}); err != nil {
		t.Fatal(err)
	}
	revived, err := st.ClaimOrders(ctx, "w", 10, maxAttempts, time.Minute)
	if err != nil || len(revived) != 1 || revived[0].Attempts != 1 {
		t.Fatalf("content change must revive a dead order with fresh attempts: %+v err=%v", revived, err)
	}
}

func TestLeaseExpiredOnFinalAttemptIsDeadLettered(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, "ERP-1", domain.MarketplaceWBLike, `{}`)}); err != nil {
		t.Fatal(err)
	}
	if c, err := st.ClaimOrders(ctx, "crashy", 1, 1, 50*time.Millisecond); err != nil || len(c) != 1 {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if c, _ := st.ClaimOrders(ctx, "w", 1, 1, time.Minute); len(c) != 0 {
		t.Fatal("exhausted order must not be reclaimed")
	}
	n, err := st.DeadLetterExpiredLeases(ctx, 1)
	if err != nil || n != 1 {
		t.Fatalf("dead-lettered %d err=%v", n, err)
	}
	if n := queryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'order.dead_lettered'`); n != 1 {
		t.Fatalf("dead-letter events = %d", n)
	}
}

func TestConcurrentTriggersConvergeOnOneQueuedJob(t *testing.T) {
	st, _ := setup(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[uuid.UUID]bool{}
	created := 0
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, isNew, err := st.EnsureQueuedJob(context.Background(), "api")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			ids[job.ID] = true
			if isNew {
				created++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ids) != 1 || created != 1 {
		t.Fatalf("jobs=%d created=%d, want 1/1", len(ids), created)
	}
}

func TestExpiredJobLeaseIsClosed(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	if _, _, err := st.EnsureQueuedJob(ctx, "api"); err != nil {
		t.Fatal(err)
	}
	job, ok, err := st.ClaimNextJob(ctx, "crashy", 0, 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim job: ok=%v err=%v", ok, err)
	}
	time.Sleep(100 * time.Millisecond)
	if n, err := st.FailExpiredJobs(ctx); err != nil || n != 1 {
		t.Fatalf("closed %d err=%v", n, err)
	}
	if err := st.FinishJob(ctx, job.ID, "crashy", domain.JobStatusCompleted, ""); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("finishing a reaped job must fail, got %v", err)
	}
}

func TestIdempotencyKeyLifecycle(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()
	begin := func(hash string) store.IdempotencyState {
		t.Helper()
		res, err := st.BeginIdempotent(ctx, "scope", "k", hash, time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return res.State
	}

	if s := begin("h1"); s != store.IdempotencyNew {
		t.Fatalf("first: %v", s)
	}
	if s := begin("h1"); s != store.IdempotencyInProgress {
		t.Fatalf("concurrent: %v", s)
	}
	if err := st.AbortIdempotent(ctx, "scope", "k"); err != nil {
		t.Fatal(err)
	}
	if s := begin("h1"); s != store.IdempotencyNew {
		t.Fatalf("after abort: %v", s)
	}
	if err := st.CompleteIdempotent(ctx, "scope", "k", "job-1"); err != nil {
		t.Fatal(err)
	}
	res, err := st.BeginIdempotent(ctx, "scope", "k", "h1", time.Hour, time.Hour)
	if err != nil || res.State != store.IdempotencyReplay || res.ResultRef != "job-1" {
		t.Fatalf("replay: %+v err=%v", res, err)
	}
	if s := begin("h2"); s != store.IdempotencyMismatch {
		t.Fatalf("different body: %v", s)
	}

	// An abandoned reservation (crashed request) is taken over after staleAfter.
	if _, err := st.BeginIdempotent(ctx, "scope", "stale", "h", time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	res, err = st.BeginIdempotent(ctx, "scope", "stale", "h", time.Hour, 10*time.Millisecond)
	if err != nil || res.State != store.IdempotencyNew {
		t.Fatalf("stale takeover: %+v err=%v", res, err)
	}
}

func TestConcurrentRelaysDeliverEachEventOnce(t *testing.T) {
	st, _ := setup(t)
	ctx := context.Background()

	for _, id := range orderIDs(150, "ERP") {
		if _, err := st.UpsertFreshOrders(ctx, []domain.FreshOrder{freshOrder(t, id, domain.MarketplaceWBLike, `{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := st.ClaimOrders(ctx, "w", 1000, 5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range claimed {
		if _, err := st.CompleteOrder(ctx, o, method, "ref"); err != nil {
			t.Fatal(err)
		}
	}

	pub := newRecordingPublisher()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := st.DrainOutbox(ctx, 10, pub.Publish)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	unique, dup := pub.stats()
	if unique != 150 || dup != 0 {
		t.Fatalf("unique=%d duplicates=%d, want 150/0", unique, dup)
	}
}
