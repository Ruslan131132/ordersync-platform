//go:build integration

package integration

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/lock"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/mockerp"
	"github.com/ordersync/platform/internal/mockmarket"
	"github.com/ordersync/platform/internal/outbox"
	"github.com/ordersync/platform/internal/retry"
	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/internal/syncer"
)

// TestTwoWorkersSyncEveryOrderExactlyOnce runs the production wiring for two
// replicas against a marketplace that fails 30% of requests, half of them
// after applying the write. Every order must reach the marketplace exactly
// once and produce exactly one published order.synced event.
func TestTwoWorkersSyncEveryOrderExactlyOnce(t *testing.T) {
	st, rdb := setup(t)
	const orders = 300

	mock := mockmarket.NewServer(mockmarket.Config{
		RPS: map[domain.Marketplace]float64{
			domain.MarketplaceWBLike:   400,
			domain.MarketplaceOzonLike: 400,
		},
		FailRate: 0.3,
		Latency:  2 * time.Millisecond,
	}, nil)
	srv := httptest.NewServer(mock.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	publisher := newRecordingPublisher()
	var wg sync.WaitGroup
	for _, id := range []string{"worker-a", "worker-b"} {
		runReplica(ctx, &wg, st, rdb, srv.URL, id, publisher)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	if _, err := st.UpsertFreshOrders(ctx, mockerp.NewGenerator().Generate(orders)); err != nil {
		t.Fatal(err)
	}

	eventually(t, 60*time.Second, "all orders synced", func() bool {
		return queryInt(t, `SELECT count(*) FROM orders WHERE status = 'synced'`) == orders
	})
	eventually(t, 10*time.Second, "outbox drained", func() bool {
		return queryInt(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})

	if ms := mock.Stats(); ms.UniqueWrites != orders || ms.DuplicateWrites != 0 {
		t.Fatalf("marketplace side effects: %+v (want %d unique, 0 duplicates)", ms, orders)
	}
	if n := queryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'order.synced'`); n != orders {
		t.Fatalf("order.synced events = %d, want %d", n, orders)
	}
	if n := queryInt(t, `SELECT count(DISTINCT aggregate_id) FROM outbox_events WHERE event_type = 'order.synced'`); n != orders {
		t.Fatalf("distinct orders with synced event = %d, want %d", n, orders)
	}
	unique, dup := publisher.stats()
	if dup != 0 {
		t.Fatalf("relays published %d duplicate deliveries", dup)
	}
	if total := queryInt(t, `SELECT count(*) FROM outbox_events`); unique != total {
		t.Fatalf("published %d of %d events", unique, total)
	}
	if n := queryInt(t, `SELECT count(DISTINCT worker_id) FROM sync_jobs WHERE orders_claimed > 0`); n < 1 {
		t.Fatal("no worker processed a job")
	}
}

func runReplica(ctx context.Context, wg *sync.WaitGroup, st *store.Store, rdb *redis.Client, baseURL, id string, pub outbox.Publisher) {
	m := metrics.New(prometheus.NewRegistry())
	log := discardLogger()
	policy := retry.Policy{Attempts: 3, Base: 5 * time.Millisecond, Max: 50 * time.Millisecond}
	client := func(mp domain.Marketplace) *marketplace.Client {
		return marketplace.NewClient(marketplace.ClientConfig{
			Marketplace: mp, BaseURL: baseURL, Token: "itest-token-" + string(mp),
			Limiter: limiter.NewRedisLimiter(rdb, "itest:rl:", 200, 200),
			Retry:   policy, Metrics: m,
		})
	}
	worker := syncer.NewWorker(syncer.WorkerConfig{
		ID: id, PollInterval: 50 * time.Millisecond, MaxConcurrency: 8,
		BatchMin: 5, BatchSoftLimit: 20, BatchMax: 50, MaxBatchesPerJob: 20,
		OrderLease: 30 * time.Second, JobLease: 10 * time.Second, OrderTimeout: 5 * time.Second,
		MaxAttempts: 10, RetryBase: 10 * time.Millisecond, RetryMax: 100 * time.Millisecond,
		ShutdownGrace: time.Second,
	}, st, marketplace.NewRouter(client(domain.MarketplaceWBLike), client(domain.MarketplaceOzonLike)), log, m)
	scheduler := syncer.NewScheduler(syncer.SchedulerConfig{
		PollInterval: 50 * time.Millisecond, HousekeepingInterval: time.Second,
		MaxAttempts: 10, OutboxRetention: time.Hour,
	}, st, log, m)
	leader := lock.NewLeader(lock.NewRedisLocker(rdb, "itest:lock:", 2*time.Second), "scheduler", id, log, m)
	relay := outbox.NewRelay(st, pub, log, m, 20*time.Millisecond, 25)

	for _, run := range []func(context.Context) error{
		worker.Run,
		func(ctx context.Context) error { return leader.Run(ctx, scheduler.Run) },
		relay.Run,
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = run(ctx)
		}()
	}
}
