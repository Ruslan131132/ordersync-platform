package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/ordersync/platform/internal/config"
	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/internal/sync"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	cfg    config.Config
	store  *store.Store
	engine *sync.Engine
	log    *slog.Logger
	mux    http.Handler
}

func New(cfg config.Config, st *store.Store, engine *sync.Engine, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, store: st, engine: engine, log: log}
	s.mux = s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Get("/healthz", s.handleHealth)
	r.Get("/readyz", s.handleReady)
	r.Handle("/metrics", promhttp.Handler())

	r.Group(func(r chi.Router) {
		r.Use(s.apiKeyAuth)
		r.Post("/v1/sync/trigger", s.handleTrigger)
		r.Post("/v1/demo/seed", s.handleSeed)
		r.Get("/v1/jobs/{id}", s.handleGetJob)
		r.Get("/v1/jobs", s.handleListJobs)
		r.Get("/v1/orders/{id}", s.handleGetOrder)
	})

	if s.cfg.EnablePprof {
		r.Route("/debug/pprof", func(r chi.Router) {
			r.Use(s.apiKeyAuth)
			r.HandleFunc("/", pprof.Index)
			r.HandleFunc("/cmdline", pprof.Cmdline)
			r.HandleFunc("/profile", pprof.Profile)
			r.HandleFunc("/symbol", pprof.Symbol)
			r.HandleFunc("/trace", pprof.Trace)
			r.Handle("/heap", pprof.Handler("heap"))
			r.Handle("/goroutine", pprof.Handler("goroutine"))
			r.Handle("/allocs", pprof.Handler("allocs"))
			r.Handle("/block", pprof.Handler("block"))
			r.Handle("/mutex", pprof.Handler("mutex"))
		})
	}

	return r
}

func (s *Server) apiKeyAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.APIKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		key := r.Header.Get("X-API-Key")
		if key == "" {
			key = r.URL.Query().Get("api_key")
		}
		if key != s.cfg.APIKey {
			writeErr(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "postgres not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type triggerRequest struct {
	Seed int `json:"seed"`
}

func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	var req triggerRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" {
		reserved, existing, err := s.store.ReserveIdempotency(r.Context(), "sync.trigger", idemKey, 24*time.Hour)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !reserved {
			if existing == "" {
				writeErr(w, http.StatusConflict, "request in progress")
				return
			}
			id, err := uuid.Parse(existing)
			if err != nil {
				writeErr(w, http.StatusConflict, "duplicate in progress")
				return
			}
			job, err := s.store.GetJob(r.Context(), id)
			if err != nil {
				writeErr(w, http.StatusConflict, "duplicate")
				return
			}
			writeJSON(w, http.StatusOK, job)
			return
		}
	}

	var (
		job domain.SyncJob
		err error
	)
	if req.Seed > 0 {
		job, err = s.engine.SeedAndEnqueue(r.Context(), req.Seed)
	} else {
		job, err = s.engine.EnqueueJob(r.Context(), "api")
	}
	if err != nil {
		s.log.Error("trigger failed", "err", err)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if idemKey != "" {
		_ = s.store.CompleteIdempotency(r.Context(), "sync.trigger", idemKey, job.ID.String())
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Count < 1 {
		writeErr(w, http.StatusBadRequest, "count required")
		return
	}
	job, err := s.engine.SeedAndEnqueue(r.Context(), req.Count)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListRecentJobs(r.Context(), 20)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid order id")
		return
	}
	order, err := s.store.GetOrder(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "order not found")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
