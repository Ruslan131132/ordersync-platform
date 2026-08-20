package marketplace_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/retry"
)

type countingLimiter struct{ calls atomic.Int32 }

func (l *countingLimiter) Wait(context.Context, string) error {
	l.calls.Add(1)
	return nil
}

func newClient(t *testing.T, url string, lim limiter.Limiter) *marketplace.Client {
	t.Helper()
	return marketplace.NewClient(marketplace.ClientConfig{
		Marketplace: domain.MarketplaceWBLike,
		BaseURL:     url,
		Token:       "demo-token-1234",
		Limiter:     lim,
		Retry:       retry.Policy{Attempts: 3, Base: time.Millisecond, Max: 5 * time.Millisecond},
		Metrics:     metrics.New(prometheus.NewRegistry()),
	})
}

func upsert() marketplace.Upsert {
	return marketplace.Upsert{ExternalID: "ERP-1", Payload: json.RawMessage(`{"sku":"A"}`), IdempotencyKey: "key-1"}
}

func TestUpsertRetriesRateLimitAndServerErrors(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(marketplace.HeaderIdempotencyKey) != "key-1" {
			t.Errorf("missing idempotency key")
		}
		if r.Header.Get("Authorization") != "Bearer demo-token-1234" {
			t.Errorf("missing bearer token")
		}
		switch calls.Add(1) {
		case 1:
			w.Header().Set(marketplace.HeaderRetryAfterMillis, "5")
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			_ = json.NewEncoder(w).Encode(marketplace.UpsertOrderResponse{RemoteRef: "ref-1"})
		}
	}))
	t.Cleanup(srv.Close)

	lim := &countingLimiter{}
	ref, err := newClient(t, srv.URL, lim).UpsertOrder(context.Background(), upsert())
	if err != nil {
		t.Fatal(err)
	}
	if ref != "ref-1" || calls.Load() != 3 {
		t.Fatalf("ref=%q calls=%d", ref, calls.Load())
	}
	if lim.calls.Load() != 3 {
		t.Fatalf("every attempt must take a rate-limit token, got %d", lim.calls.Load())
	}
}

func TestUpsertDoesNotRetryClientErrors(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	_, err := newClient(t, srv.URL, &countingLimiter{}).UpsertOrder(context.Background(), upsert())
	var se *marketplace.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 StatusError, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("4xx must not be retried, calls=%d", calls.Load())
	}
}

func TestUpsertGivesUpAfterAttempts(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	if _, err := newClient(t, srv.URL, &countingLimiter{}).UpsertOrder(context.Background(), upsert()); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want 3", calls.Load())
	}
}

func TestRouterRejectsUnknownMarketplace(t *testing.T) {
	t.Parallel()
	r := marketplace.NewRouter(newClient(t, "http://unused", &countingLimiter{}))
	if _, err := r.UpsertOrder(context.Background(), domain.MarketplaceOzonLike, upsert()); err == nil {
		t.Fatal("expected error for unconfigured marketplace")
	}
}
