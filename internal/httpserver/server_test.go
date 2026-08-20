package httpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/store"
)

type fakeStore struct {
	mu       sync.Mutex
	upserted []domain.FreshOrder
	jobs     map[uuid.UUID]domain.SyncJob
	idem     map[string]store.IdempotencyResult
	hashes   map[string]string
	enqueue  error
	aborted  []string
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: map[uuid.UUID]domain.SyncJob{}, idem: map[string]store.IdempotencyResult{}, hashes: map[string]string{}}
}

func (f *fakeStore) UpsertFreshOrders(_ context.Context, fresh []domain.FreshOrder) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserted = append(f.upserted, fresh...)
	return len(fresh), nil
}

func (f *fakeStore) EnsureQueuedJob(context.Context, string) (domain.SyncJob, bool, error) {
	if f.enqueue != nil {
		return domain.SyncJob{}, false, f.enqueue
	}
	j := domain.SyncJob{ID: uuid.New(), Status: domain.JobStatusQueued}
	f.mu.Lock()
	f.jobs[j.ID] = j
	f.mu.Unlock()
	return j, true, nil
}

func (f *fakeStore) GetJob(_ context.Context, id uuid.UUID) (domain.SyncJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return domain.SyncJob{}, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) ListRecentJobs(context.Context, int) ([]domain.SyncJob, error) { return nil, nil }

func (f *fakeStore) GetOrder(context.Context, uuid.UUID) (domain.Order, error) {
	return domain.Order{}, store.ErrNotFound
}

func (f *fakeStore) GetOrderByExternalID(context.Context, domain.Marketplace, string) (domain.Order, error) {
	return domain.Order{}, errors.New("db down")
}

func (f *fakeStore) Stats(context.Context, int) (domain.Stats, error) { return domain.Stats{}, nil }

func (f *fakeStore) BeginIdempotent(_ context.Context, _, key, hash string, _, _ time.Duration) (store.IdempotencyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if prev, ok := f.hashes[key]; ok && prev != hash {
		return store.IdempotencyResult{State: store.IdempotencyMismatch}, nil
	}
	if res, ok := f.idem[key]; ok {
		return res, nil
	}
	f.hashes[key] = hash
	f.idem[key] = store.IdempotencyResult{State: store.IdempotencyInProgress}
	return store.IdempotencyResult{State: store.IdempotencyNew}, nil
}

func (f *fakeStore) CompleteIdempotent(_ context.Context, _, key, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.idem[key] = store.IdempotencyResult{State: store.IdempotencyReplay, ResultRef: ref}
	return nil
}

func (f *fakeStore) AbortIdempotent(_ context.Context, _, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.idem, key)
	delete(f.hashes, key)
	f.aborted = append(f.aborted, key)
	return nil
}

type fakeGen struct{}

func (fakeGen) Generate(n int) []domain.FreshOrder { return make([]domain.FreshOrder, n) }

func newTestServer(st Store) http.Handler {
	return New(Config{APIKey: "secret", IdempotencyTTL: time.Hour, MaxAttempts: 3}, st, fakeGen{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New(prometheus.NewRegistry())).Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-API-Key", "secret")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAuthRequiresHeaderKey(t *testing.T) {
	t.Parallel()
	h := newTestServer(newFakeStore())
	for name, hdr := range map[string]map[string]string{
		"missing": {"X-API-Key": ""},
		"wrong":   {"X-API-Key": "nope"},
	} {
		if rec := do(t, h, http.MethodGet, "/v1/stats", "", hdr); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s key: got %d", name, rec.Code)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/stats?api_key=secret", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("query-string key must be rejected, got %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/healthz", "", map[string]string{"X-API-Key": ""}); rec.Code != http.StatusOK {
		t.Fatalf("healthz must be public, got %d", rec.Code)
	}
}

func TestPprofIsOptInAndAuthenticated(t *testing.T) {
	t.Parallel()
	if rec := do(t, newTestServer(newFakeStore()), http.MethodGet, "/debug/pprof/", "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("pprof must be disabled by default, got %d", rec.Code)
	}
	h := Pprof("secret")
	if rec := do(t, h, http.MethodGet, "/heap", "", map[string]string{"X-API-Key": ""}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("pprof without key: got %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/heap", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("pprof heap with key: got %d", rec.Code)
	}
}

func TestIngestValidatesOrders(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	h := newTestServer(st)

	bad := `{"orders":[{"external_id":"A","marketplace":"amazon","payload":{}}]}`
	if rec := do(t, h, http.MethodPost, "/v1/erp/orders", bad, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown marketplace: got %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/v1/erp/orders", `{"orders":[],"extra":1}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: got %d", rec.Code)
	}
	good := `{"orders":[{"external_id":"A","marketplace":"wb_like","payload":{"sku":"X"}}]}`
	if rec := do(t, h, http.MethodPost, "/v1/erp/orders", good, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("valid ingest: got %d %s", rec.Code, rec.Body)
	}
	if len(st.upserted) != 1 || st.upserted[0].ContentHash == "" {
		t.Fatalf("upserted=%+v", st.upserted)
	}
}

func TestSeedChunksLargeRequests(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	rec := do(t, newTestServer(st), http.MethodPost, "/v1/demo/seed", `{"count":2500}`, nil)
	if rec.Code != http.StatusAccepted || len(st.upserted) != 2500 {
		t.Fatalf("code=%d upserted=%d", rec.Code, len(st.upserted))
	}
}

func TestTriggerIdempotency(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	h := newTestServer(st)
	key := map[string]string{"Idempotency-Key": "k1"}

	first := do(t, h, http.MethodPost, "/v1/sync/trigger", `{}`, key)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first: %d", first.Code)
	}
	replay := do(t, h, http.MethodPost, "/v1/sync/trigger", `{}`, key)
	if replay.Code != http.StatusOK || replay.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d", replay.Code)
	}
	if first.Body.String() != replay.Body.String() {
		t.Fatalf("replay returned a different job:\n%s\n%s", first.Body, replay.Body)
	}
	if rec := do(t, h, http.MethodPost, "/v1/sync/trigger", `{"other":true}`, key); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("mismatched body: %d", rec.Code)
	}
}

func TestTriggerAbortsKeyWhenEnqueueFails(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.enqueue = errors.New("db down")
	rec := do(t, newTestServer(st), http.MethodPost, "/v1/sync/trigger", `{}`, map[string]string{"Idempotency-Key": "k1"})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "db down") {
		t.Fatal("internal error details leaked to client")
	}
	if len(st.aborted) != 1 {
		t.Fatal("failed request must release its idempotency key")
	}
}

func TestLookupErrors(t *testing.T) {
	t.Parallel()
	h := newTestServer(newFakeStore())
	if rec := do(t, h, http.MethodGet, "/v1/orders/"+uuid.NewString(), "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("missing order: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/v1/orders/not-a-uuid", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/v1/orders?marketplace=wb_like&external_id=A", "", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("store failure: %d", rec.Code)
	}
}
