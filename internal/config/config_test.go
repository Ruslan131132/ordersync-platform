package config

import (
	"strings"
	"testing"
	"time"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func baseEnv() map[string]string {
	return map[string]string{
		"API_KEY":      "k",
		"POSTGRES_DSN": "postgres://x",
		"WORKER_ID":    "w1",
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := LoadFrom(envOf(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OrderLease != 2*time.Minute || cfg.EnablePprof {
		t.Fatalf("unexpected defaults: lease=%s pprof=%v", cfg.OrderLease, cfg.EnablePprof)
	}
}

func TestLoadRequiresSecrets(t *testing.T) {
	t.Parallel()
	_, err := LoadFrom(envOf(map[string]string{"WORKER_ID": "w1"}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"API_KEY", "POSTGRES_DSN"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadReportsInvalidValues(t *testing.T) {
	t.Parallel()
	env := baseEnv()
	env["MAX_CONCURRENCY"] = "many"
	env["ORDER_LEASE"] = "soon"
	env["ENABLE_PPROF"] = "maybe"
	_, err := LoadFrom(envOf(env))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"MAX_CONCURRENCY", "ORDER_LEASE", "ENABLE_PPROF"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadValidatesRelations(t *testing.T) {
	t.Parallel()
	env := baseEnv()
	env["BATCH_MIN"] = "50"
	env["BATCH_SOFT_LIMIT"] = "10"
	env["ORDER_TIMEOUT"] = "5m"
	_, err := LoadFrom(envOf(env))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "batch bounds") || !strings.Contains(err.Error(), "ORDER_TIMEOUT") {
		t.Fatalf("unexpected error: %v", err)
	}
}
