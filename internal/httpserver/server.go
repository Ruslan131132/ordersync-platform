package httpserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/store"
)

// Store is the persistence surface used by the API; *store.Store implements it.
type Store interface {
	UpsertFreshOrders(ctx context.Context, fresh []domain.FreshOrder) (int, error)
	EnsureQueuedJob(ctx context.Context, source string) (domain.SyncJob, bool, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.SyncJob, error)
	ListRecentJobs(ctx context.Context, limit int) ([]domain.SyncJob, error)
	GetOrder(ctx context.Context, id uuid.UUID) (domain.Order, error)
	GetOrderByExternalID(ctx context.Context, mp domain.Marketplace, externalID string) (domain.Order, error)
	Stats(ctx context.Context, maxAttempts int) (domain.Stats, error)
	BeginIdempotent(ctx context.Context, scope, key, requestHash string, ttl, staleAfter time.Duration) (store.IdempotencyResult, error)
	CompleteIdempotent(ctx context.Context, scope, key, resultRef string) error
	AbortIdempotent(ctx context.Context, scope, key string) error
}

// Generator produces demo ERP orders.
type Generator interface {
	Generate(n int) []domain.FreshOrder
}

type Pinger func(ctx context.Context) error

type Config struct {
	APIKey         string
	EnablePprof    bool
	IdempotencyTTL time.Duration
	MaxAttempts    int
	// MockMarket, when set, is mounted at /mock/marketplaces (dev only).
	MockMarket http.Handler
}

type Server struct {
	cfg     Config
	store   Store
	gen     Generator
	ready   []Pinger
	log     *slog.Logger
	metrics *metrics.Metrics
	handler http.Handler
}

const (
	maxIngestOrders = 1000
	maxSeedOrders   = 10_000
	maxBodyBytes    = 2 << 20
	triggerScope    = "sync.trigger"
)

func New(cfg Config, st Store, gen Generator, log *slog.Logger, m *metrics.Metrics, ready ...Pinger) *Server {
	s := &Server{cfg: cfg, store: st, gen: gen, ready: ready, log: log, metrics: m}
	s.handler = s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s.recoverer)
	r.Use(s.observe)

	r.Get("/healthz", s.handleHealth)
	r.Get("/readyz", s.handleReady)
	r.Handle("/metrics", promhttp.Handler())

	r.Group(func(r chi.Router) {
		r.Use(RequireAPIKey(s.cfg.APIKey))
		r.Use(middleware.Timeout(30 * time.Second))
		r.Post("/v1/erp/orders", s.handleIngest)
		r.Post("/v1/demo/seed", s.handleSeed)
		r.Post("/v1/sync/trigger", s.handleTrigger)
		r.Get("/v1/jobs", s.handleListJobs)
		r.Get("/v1/jobs/{id}", s.handleGetJob)
		r.Get("/v1/orders", s.handleFindOrder)
		r.Get("/v1/orders/{id}", s.handleGetOrder)
		r.Get("/v1/stats", s.handleStats)
	})

	if s.cfg.EnablePprof {
		r.Mount("/debug/pprof", Pprof(s.cfg.APIKey))
	}

	if s.cfg.MockMarket != nil {
		r.Mount("/mock/marketplaces", http.StripPrefix("/mock/marketplaces", s.cfg.MockMarket))
	}
	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for _, ping := range s.ready {
		if err := ping(ctx); err != nil {
			s.log.Warn("readiness check failed", "err", err)
			writeError(w, http.StatusServiceUnavailable, "dependency not ready")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type ingestRequest struct {
	Orders []struct {
		ExternalID  string             `json:"external_id"`
		Marketplace domain.Marketplace `json:"marketplace"`
		Payload     json.RawMessage    `json:"payload"`
	} `json:"orders"`
}

type ingestResponse struct {
	Received int `json:"received"`
	Changed  int `json:"changed"`
}

// handleIngest is the push half of "poll or receive": the ERP delivers order
// changes. Redelivery is harmless because unchanged content is a no-op.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if !s.decode(w, r, &req) {
		return
	}
	if len(req.Orders) == 0 || len(req.Orders) > maxIngestOrders {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("orders must contain 1..%d items", maxIngestOrders))
		return
	}
	fresh := make([]domain.FreshOrder, 0, len(req.Orders))
	for i, in := range req.Orders {
		fo, err := domain.NewFreshOrder(in.ExternalID, in.Marketplace, in.Payload)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("orders[%d]: %v", i, err))
			return
		}
		fresh = append(fresh, fo)
	}
	s.ingest(w, r, fresh)
}

func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if req.Count < 1 || req.Count > maxSeedOrders {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("count must be 1..%d", maxSeedOrders))
		return
	}
	s.ingest(w, r, s.gen.Generate(req.Count))
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request, fresh []domain.FreshOrder) {
	changed := 0
	for start := 0; start < len(fresh); start += maxIngestOrders {
		n, err := s.store.UpsertFreshOrders(r.Context(), fresh[start:min(start+maxIngestOrders, len(fresh))])
		if err != nil {
			s.internalError(w, r, "ingest orders", err)
			return
		}
		changed += n
	}
	writeJSON(w, http.StatusAccepted, ingestResponse{Received: len(fresh), Changed: changed})
}

// handleTrigger queues a sync job. Without a key, concurrent triggers already
// converge on the single queued job; with Idempotency-Key, a retried request
// returns the original job even after it has run.
func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		s.enqueue(w, r)
		return
	}
	if len(key) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key too long")
		return
	}
	sum := sha256.Sum256(body)
	res, err := s.store.BeginIdempotent(r.Context(), triggerScope, key, hex.EncodeToString(sum[:]),
		s.cfg.IdempotencyTTL, time.Minute)
	if err != nil {
		s.internalError(w, r, "begin idempotent trigger", err)
		return
	}
	switch res.State {
	case store.IdempotencyMismatch:
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key reused with a different request")
	case store.IdempotencyInProgress:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "a request with this Idempotency-Key is in progress")
	case store.IdempotencyReplay:
		s.replayTrigger(w, r, res.ResultRef)
	case store.IdempotencyNew:
		job, _, err := s.store.EnsureQueuedJob(r.Context(), "api")
		if err != nil {
			abortCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
			defer cancel()
			if abortErr := s.store.AbortIdempotent(abortCtx, triggerScope, key); abortErr != nil {
				s.log.Error("abort idempotency key failed", "err", abortErr)
			}
			s.internalError(w, r, "enqueue job", err)
			return
		}
		if err := s.store.CompleteIdempotent(r.Context(), triggerScope, key, job.ID.String()); err != nil {
			s.internalError(w, r, "complete idempotent trigger", err)
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	default:
		s.internalError(w, r, "trigger", fmt.Errorf("unexpected idempotency state %d", res.State))
	}
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	job, _, err := s.store.EnsureQueuedJob(r.Context(), "api")
	if err != nil {
		s.internalError(w, r, "enqueue job", err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) replayTrigger(w http.ResponseWriter, r *http.Request, ref string) {
	id, err := uuid.Parse(ref)
	if err != nil {
		s.internalError(w, r, "replay trigger", fmt.Errorf("stored result %q: %w", ref, err))
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		s.internalError(w, r, "replay trigger", err)
		return
	}
	w.Header().Set("Idempotent-Replayed", "true")
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if s.lookupFailed(w, r, err, "job") {
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListRecentJobs(r.Context(), 50)
	if err != nil {
		s.internalError(w, r, "list jobs", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}
	order, err := s.store.GetOrder(r.Context(), id)
	if s.lookupFailed(w, r, err, "order") {
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *Server) handleFindOrder(w http.ResponseWriter, r *http.Request) {
	mp := domain.Marketplace(r.URL.Query().Get("marketplace"))
	externalID := r.URL.Query().Get("external_id")
	if !mp.Valid() || externalID == "" {
		writeError(w, http.StatusBadRequest, "marketplace and external_id query parameters are required")
		return
	}
	order, err := s.store.GetOrderByExternalID(r.Context(), mp, externalID)
	if s.lookupFailed(w, r, err, "order") {
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context(), s.cfg.MaxAttempts)
	if err != nil {
		s.internalError(w, r, "stats", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func (s *Server) lookupFailed(w http.ResponseWriter, r *http.Request, err error, what string) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, what+" not found")
	default:
		s.internalError(w, r, "get "+what, err)
	}
	return true
}

// internalError logs the cause and returns a generic message, so database
// errors never leak to clients.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	s.log.ErrorContext(r.Context(), op+" failed", "err", err, "request_id", middleware.GetReqID(r.Context()))
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
