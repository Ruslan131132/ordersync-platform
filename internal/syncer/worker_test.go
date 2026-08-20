package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/metrics"
)

// fakeStore is an in-memory Store with just enough behaviour to exercise the
// worker's decision logic; SQL semantics are covered by integration tests.
type fakeStore struct {
	mu        sync.Mutex
	pending   []domain.ClaimedOrder
	ledger    map[uuid.UUID]string
	completed map[uuid.UUID]string
	failed    map[uuid.UUID]string
	released  map[uuid.UUID]bool
	batches   []domain.BatchResult
	finished  domain.JobStatus
	leaseLost map[uuid.UUID]bool
	maxAtt    int
}

func newFakeStore(n int) *fakeStore {
	s := &fakeStore{
		ledger:    map[uuid.UUID]string{},
		completed: map[uuid.UUID]string{},
		failed:    map[uuid.UUID]string{},
		released:  map[uuid.UUID]bool{},
		leaseLost: map[uuid.UUID]bool{},
	}
	for i := range n {
		s.pending = append(s.pending, domain.ClaimedOrder{
			Order: domain.Order{
				ID: uuid.New(), ExternalID: "ERP-" + string(rune('A'+i%26)),
				Marketplace: domain.MarketplaceWBLike, ContentHash: "h", Attempts: 1,
				PendingSince: time.Now(),
			},
			LeaseToken: uuid.New(),
		})
	}
	return s
}

func (s *fakeStore) CountClaimable(context.Context, int, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), nil
}

func (s *fakeStore) ClaimNextJob(context.Context, string, int, time.Duration) (domain.SyncJob, bool, error) {
	return domain.SyncJob{ID: uuid.New()}, true, nil
}

func (s *fakeStore) ClaimOrders(_ context.Context, _ string, limit, _ int, _ time.Duration) ([]domain.ClaimedOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.pending))
	out := s.pending[:n]
	s.pending = s.pending[n:]
	return out, nil
}

func (s *fakeStore) LedgerRef(_ context.Context, id uuid.UUID, _, _ string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.ledger[id]
	return ref, ok, nil
}

func (s *fakeStore) CompleteOrder(ctx context.Context, o domain.ClaimedOrder, _, ref string) (domain.OrderStatus, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLost[o.ID] {
		return "", domain.ErrLeaseLost
	}
	s.completed[o.ID] = ref
	return domain.OrderStatusSynced, nil
}

func (s *fakeStore) FailOrder(ctx context.Context, o domain.ClaimedOrder, reason string, maxAttempts int, _ time.Duration) (domain.OrderStatus, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[o.ID] = reason
	s.maxAtt = maxAttempts
	if o.Attempts >= maxAttempts {
		return domain.OrderStatusDead, nil
	}
	return domain.OrderStatusFailed, nil
}

func (s *fakeStore) ReleaseOrder(_ context.Context, o domain.ClaimedOrder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released[o.ID] = true
	return nil
}

func (s *fakeStore) RecordBatch(_ context.Context, _ uuid.UUID, _ string, r domain.BatchResult, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, r)
	return nil
}

func (s *fakeStore) FinishJob(_ context.Context, _ uuid.UUID, _ string, status domain.JobStatus, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = status
	return nil
}

type fakeMarket struct {
	calls    atomic.Int32
	inFlight atomic.Int32
	peak     atomic.Int32
	delay    time.Duration
	err      error
	block    chan struct{}
}

func (m *fakeMarket) UpsertOrder(ctx context.Context, _ domain.Marketplace, req marketplace.Upsert) (string, error) {
	m.calls.Add(1)
	n := m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	for {
		p := m.peak.Load()
		if n <= p || m.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if m.block != nil {
		select {
		case <-m.block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	time.Sleep(m.delay)
	if m.err != nil {
		return "", m.err
	}
	return "ref-" + req.ExternalID, nil
}

func testConfig() WorkerConfig {
	return WorkerConfig{
		ID: "w1", PollInterval: 10 * time.Millisecond, MaxConcurrency: 4,
		BatchMin: 2, BatchSoftLimit: 5, BatchMax: 10, MaxBatchesPerJob: 100,
		OrderLease: time.Minute, JobLease: time.Minute, OrderTimeout: time.Second,
		MaxAttempts: 3, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	}
}

func newTestWorker(cfg WorkerConfig, st Store, m Marketplace) *Worker {
	return NewWorker(cfg, st, m, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New(prometheus.NewRegistry()))
}

func TestProcessJobDrainsWithAdaptiveBatchesAndBoundedConcurrency(t *testing.T) {
	t.Parallel()
	st := newFakeStore(37)
	market := &fakeMarket{delay: 2 * time.Millisecond}
	w := newTestWorker(testConfig(), st, market)

	ctx := context.Background()
	if err := w.ProcessJob(ctx, ctx, domain.SyncJob{ID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	if len(st.completed) != 37 || market.calls.Load() != 37 {
		t.Fatalf("completed=%d calls=%d", len(st.completed), market.calls.Load())
	}
	if st.finished != domain.JobStatusCompleted {
		t.Fatalf("job status %s", st.finished)
	}
	if peak := market.peak.Load(); peak > 4 {
		t.Fatalf("concurrency limit exceeded: peak %d", peak)
	}
	first, last := st.batches[0].Size, st.batches[len(st.batches)-1].Size
	if first != 10 || last >= first {
		t.Fatalf("batches should start at max for a backlog and shrink as it drains: %+v", st.batches)
	}
}

func TestLedgerHitSkipsRemoteCall(t *testing.T) {
	t.Parallel()
	st := newFakeStore(1)
	o := st.pending[0]
	st.ledger[o.ID] = "ref-from-ledger"
	market := &fakeMarket{}

	got := newTestWorker(testConfig(), st, market).syncOrder(context.Background(), o)
	if got != outcomeSynced || market.calls.Load() != 0 {
		t.Fatalf("outcome=%s calls=%d", got, market.calls.Load())
	}
	if st.completed[o.ID] != "ref-from-ledger" {
		t.Fatalf("completed with %q", st.completed[o.ID])
	}
}

func TestFailureSchedulesRetryThenDeadLetters(t *testing.T) {
	t.Parallel()
	st := newFakeStore(2)
	st.pending[1].Attempts = 3
	w := newTestWorker(testConfig(), st, &fakeMarket{err: errors.New("503")})

	if got := w.syncOrder(context.Background(), st.pending[0]); got != outcomeFailed {
		t.Fatalf("first attempt outcome %s", got)
	}
	if got := w.syncOrder(context.Background(), st.pending[1]); got != outcomeDead {
		t.Fatalf("final attempt outcome %s", got)
	}
	if st.maxAtt != 3 {
		t.Fatalf("max attempts not passed through: %d", st.maxAtt)
	}
}

func TestLeaseLostIsNotCountedAsSuccess(t *testing.T) {
	t.Parallel()
	st := newFakeStore(1)
	o := st.pending[0]
	st.leaseLost[o.ID] = true

	if got := newTestWorker(testConfig(), st, &fakeMarket{}).syncOrder(context.Background(), o); got != outcomeLost {
		t.Fatalf("outcome %s", got)
	}
}

func TestShutdownReleasesInFlightOrdersAfterGrace(t *testing.T) {
	t.Parallel()
	st := newFakeStore(3)
	market := &fakeMarket{block: make(chan struct{})}
	w := newTestWorker(testConfig(), st, market)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for market.inFlight.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after shutdown grace")
	}
	if len(st.released) != 3 {
		t.Fatalf("expected 3 released leases, got %d", len(st.released))
	}
	if st.finished != domain.JobStatusCancelled {
		t.Fatalf("job status %s, want cancelled", st.finished)
	}
}

func TestIdempotencyKeyIsStablePerContent(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	a := IdempotencyKey(id, marketplace.MethodUpsertOrder, "h1")
	if a != IdempotencyKey(id, marketplace.MethodUpsertOrder, "h1") {
		t.Fatal("key must be deterministic")
	}
	if a == IdempotencyKey(id, marketplace.MethodUpsertOrder, "h2") {
		t.Fatal("content change must produce a new key")
	}
}

func TestSlowRemoteCallDoesNotEatPersistBudget(t *testing.T) {
	t.Parallel()
	st := newFakeStore(3)
	cfg := testConfig()
	cfg.PersistTimeout = 20 * time.Millisecond
	w := newTestWorker(cfg, st, &fakeMarket{delay: 60 * time.Millisecond})

	if err := w.ProcessJob(context.Background(), context.Background(), domain.SyncJob{ID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	if len(st.completed) != 3 {
		t.Fatalf("completed %d/3 orders; persist deadline must start after the remote call", len(st.completed))
	}
}
