package httpserver

import (
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
)

// Pprof returns the profiling endpoints behind API-key auth. It must be
// mounted at /debug/pprof because pprof.Index derives profile names from the
// full request path.
func Pprof(apiKey string) http.Handler {
	r := chi.NewRouter()
	r.Use(RequireAPIKey(apiKey))
	r.HandleFunc("/", pprof.Index)
	r.HandleFunc("/cmdline", pprof.Cmdline)
	r.HandleFunc("/profile", pprof.Profile)
	r.HandleFunc("/symbol", pprof.Symbol)
	r.HandleFunc("/trace", pprof.Trace)
	r.Get("/{profile}", func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler(chi.URLParam(r, "profile")).ServeHTTP(w, r)
	})
	return r
}
