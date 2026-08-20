package mockmarket

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/marketplace"
)

func post(t *testing.T, h http.Handler, key string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(marketplace.UpsertOrderRequest{ExternalID: "ERP-1", Payload: json.RawMessage(`{"a":1}`)})
	req := httptest.NewRequest(http.MethodPost, "/wb_like/v1/orders", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer t")
	if key != "" {
		req.Header.Set(marketplace.HeaderIdempotencyKey, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func newServer(rand func() float64, failRate, rps float64) *Server {
	return NewServer(Config{
		RPS:      map[domain.Marketplace]float64{domain.MarketplaceWBLike: rps},
		FailRate: failRate,
		Rand:     rand,
	}, nil)
}

func TestReplayReturnsSameRefWithoutSecondWrite(t *testing.T) {
	t.Parallel()
	s := newServer(func() float64 { return 0.99 }, 0, 1000)
	h := s.Handler()

	first := post(t, h, "k1")
	second := post(t, h, "k1")
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("codes %d/%d", first.Code, second.Code)
	}
	if second.Header().Get(marketplace.HeaderReplayed) != "true" {
		t.Fatal("second response must be marked as replay")
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("replay changed result: %s vs %s", first.Body, second.Body)
	}
	if st := s.Stats(); st.UniqueWrites != 1 || st.DuplicateWrites != 0 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestGhostWriteIsDedupedOnRetry(t *testing.T) {
	t.Parallel()
	rolls := []float64{0.75, 0.99} // fail after applying, then succeed
	s := newServer(func() float64 { r := rolls[0]; rolls = rolls[1:]; return r }, 0.8, 1000)
	h := s.Handler()

	if rec := post(t, h, "k1"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected ghost-write 503, got %d", rec.Code)
	}
	if rec := post(t, h, "k1"); rec.Code != http.StatusOK || rec.Header().Get(marketplace.HeaderReplayed) != "true" {
		t.Fatalf("retry should replay the applied write, got %d", rec.Code)
	}
	if st := s.Stats(); st.UniqueWrites != 1 || st.DuplicateWrites != 0 {
		t.Fatalf("ghost write duplicated: %+v", st)
	}
}

func TestRateLimitReturns429WithHint(t *testing.T) {
	t.Parallel()
	s := newServer(func() float64 { return 0.99 }, 0, 1)
	h := s.Handler()
	for i := range 3 {
		if rec := post(t, h, "k"+string(rune('a'+i))); rec.Code != http.StatusOK {
			t.Fatalf("burst request %d got %d", i, rec.Code)
		}
	}
	rec := post(t, h, "kz")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get(marketplace.HeaderRetryAfterMillis) == "" {
		t.Fatalf("expected 429 with Retry-After-Ms, got %d", rec.Code)
	}
}

func TestRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()
	if rec := post(t, newServer(nil, 0, 10).Handler(), ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d", rec.Code)
	}
}
