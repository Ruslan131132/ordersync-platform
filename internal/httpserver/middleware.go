package httpserver

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// RequireAPIKey accepts the key only via header (query strings end up in
// access logs) and compares in constant time. An empty key rejects everything.
func RequireAPIKey(key string) func(http.Handler) http.Handler {
	want := []byte(key)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(r.Header.Get("X-API-Key"))
			if len(want) == 0 || subtle.ConstantTimeCompare(got, want) != 1 {
				writeError(w, http.StatusUnauthorized, "invalid or missing X-API-Key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				s.log.ErrorContext(r.Context(), "panic in handler", "panic", rec,
					"stack", string(debug.Stack()), "request_id", middleware.GetReqID(r.Context()))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// observe records metrics labelled by route pattern (bounded cardinality)
// and writes an access log line.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := "unmatched"
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			route = rc.RoutePattern()
		}
		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		elapsed := time.Since(start)
		s.metrics.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(status)).Inc()
		s.metrics.HTTPSeconds.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())

		if route == "/metrics" || route == "/healthz" || route == "/readyz" || strings.HasPrefix(route, "/mock/") {
			return
		}
		s.log.InfoContext(r.Context(), "http request",
			"method", r.Method, "route", route, "status", status,
			"duration_ms", elapsed.Milliseconds(), "request_id", middleware.GetReqID(r.Context()))
	})
}
