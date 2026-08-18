// Package mockmarket is a deliberately unreliable marketplace simulator for
// local development and tests. It is not modelled on any real vendor API.
package mockmarket

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/metrics"
)

type Config struct {
	// RPS is the per-token, per-method limit for each marketplace.
	RPS map[domain.Marketplace]float64
	// FailRate is the probability of a 503. Half of failures happen after the
	// write was applied ("ghost writes"), which is what idempotency keys guard.
	FailRate float64
	Latency  time.Duration
	// Rand is injectable for deterministic tests.
	Rand func() float64
}

type Server struct {
	cfg      Config
	limiters map[domain.Marketplace]*limiter.TokenBucket
	metrics  *metrics.Metrics

	mu      sync.Mutex
	results map[string]string // idempotency key -> remote ref
	applied map[string]int    // marketplace/external_id/payload hash -> times applied
}

func NewServer(cfg Config, m *metrics.Metrics) *Server {
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64 //nolint:gosec // simulation only
	}
	s := &Server{
		cfg:      cfg,
		limiters: make(map[domain.Marketplace]*limiter.TokenBucket),
		metrics:  m,
		results:  make(map[string]string),
		applied:  make(map[string]int),
	}
	for mp, rps := range cfg.RPS {
		// Server burst is twice the client's so jitter between replicas
		// sharing the Redis bucket rarely trips a 429.
		s.limiters[mp] = limiter.NewTokenBucket(rps, int(rps*2)+1)
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{marketplace}/v1/orders", s.handleUpsert)
	mux.HandleFunc("GET /stats", s.handleStats)
	return mux
}

type Stats struct {
	UniqueWrites    int `json:"unique_writes"`
	DuplicateWrites int `json:"duplicate_writes"`
	IdempotencyKeys int `json:"idempotency_keys"`
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{UniqueWrites: len(s.applied), IdempotencyKeys: len(s.results)}
	for _, n := range s.applied {
		st.DuplicateWrites += n - 1
	}
	return st
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Stats())
}

func (s *Server) handleUpsert(w http.ResponseWriter, r *http.Request) {
	mp := domain.Marketplace(r.PathValue("marketplace"))
	bucket, ok := s.limiters[mp]
	if !ok {
		s.reply(w, mp, "not_found", http.StatusNotFound, map[string]string{"error": "unknown marketplace"})
		return
	}
	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || token == "" {
		s.reply(w, mp, "unauthorized", http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return
	}
	key := r.Header.Get(marketplace.HeaderIdempotencyKey)
	if key == "" {
		s.reply(w, mp, "bad_request", http.StatusBadRequest, map[string]string{"error": "Idempotency-Key is required"})
		return
	}
	var req marketplace.UpsertOrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.ExternalID == "" {
		s.reply(w, mp, "bad_request", http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}

	if allowed, wait := bucket.Allow(token + ":" + marketplace.MethodUpsertOrder); !allowed {
		w.Header().Set(marketplace.HeaderRetryAfterMillis, strconv.FormatInt(max(wait.Milliseconds(), 1), 10))
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		s.reply(w, mp, "rate_limited", http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		return
	}

	select {
	case <-r.Context().Done():
		return
	case <-time.After(s.cfg.Latency):
	}

	roll := s.cfg.Rand()
	if roll < s.cfg.FailRate/2 {
		s.reply(w, mp, "failed_before_write", http.StatusServiceUnavailable, map[string]string{"error": "temporarily unavailable"})
		return
	}

	ref, replayed := s.apply(mp, key, req)
	if roll < s.cfg.FailRate {
		s.reply(w, mp, "failed_after_write", http.StatusServiceUnavailable, map[string]string{"error": "gateway timeout"})
		return
	}
	if replayed {
		w.Header().Set(marketplace.HeaderReplayed, "true")
		s.reply(w, mp, "replayed", http.StatusOK, marketplace.UpsertOrderResponse{RemoteRef: ref})
		return
	}
	s.reply(w, mp, "applied", http.StatusOK, marketplace.UpsertOrderResponse{RemoteRef: ref})
}

func (s *Server) apply(mp domain.Marketplace, key string, req marketplace.UpsertOrderRequest) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref, ok := s.results[key]; ok {
		return ref, true
	}
	sum := sha256.Sum256(req.Payload)
	s.applied[fmt.Sprintf("%s/%s/%s", mp, req.ExternalID, hex.EncodeToString(sum[:8]))]++
	ref := fmt.Sprintf("%s-%s", mp, uuid.NewString())
	s.results[key] = ref
	return ref, false
}

func (s *Server) reply(w http.ResponseWriter, mp domain.Marketplace, outcome string, code int, body any) {
	if s.metrics != nil {
		label := string(mp)
		if !mp.Valid() {
			label = "unknown"
		}
		s.metrics.MockMarketCalls.WithLabelValues(label, outcome).Inc()
	}
	writeJSON(w, code, body)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
