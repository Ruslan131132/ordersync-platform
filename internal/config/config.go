package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName string
	Env         string
	HTTPAddr    string
	APIKey      string

	PostgresDSN string
	RedisAddr   string
	RedisDB     int

	WorkerID           string
	PollInterval       time.Duration
	MaxConcurrency     int
	LockTTL            time.Duration
	OutboxPollInterval time.Duration
	OutboxBatchSize    int

	// Adaptive batching knobs.
	BatchMin       int
	BatchMax       int
	BatchSoftLimit int

	// Mock marketplace rate limits (tokens per second).
	WBLikeRPS   float64
	OzonLikeRPS float64
	MockFailPct float64

	EnablePprof bool
}

func Load() (Config, error) {
	cfg := Config{
		ServiceName:        getenv("SERVICE_NAME", "ordersync"),
		Env:                getenv("APP_ENV", "development"),
		HTTPAddr:           getenv("HTTP_ADDR", ":8080"),
		APIKey:             getenv("API_KEY", "dev-local-key"),
		PostgresDSN:        getenv("POSTGRES_DSN", "postgres://ordersync:ordersync@localhost:5433/ordersync?sslmode=disable"),
		RedisAddr:          getenv("REDIS_ADDR", "localhost:6380"),
		RedisDB:            getenvInt("REDIS_DB", 0),
		WorkerID:           getenv("WORKER_ID", hostnameOr("worker")),
		PollInterval:       getenvDuration("POLL_INTERVAL", 2*time.Second),
		MaxConcurrency:     getenvInt("MAX_CONCURRENCY", 8),
		LockTTL:            getenvDuration("LOCK_TTL", 30*time.Second),
		OutboxPollInterval: getenvDuration("OUTBOX_POLL_INTERVAL", time.Second),
		OutboxBatchSize:    getenvInt("OUTBOX_BATCH_SIZE", 50),
		BatchMin:           getenvInt("BATCH_MIN", 5),
		BatchMax:           getenvInt("BATCH_MAX", 100),
		BatchSoftLimit:     getenvInt("BATCH_SOFT_LIMIT", 40),
		WBLikeRPS:          getenvFloat("WB_LIKE_RPS", 5),
		OzonLikeRPS:        getenvFloat("OZON_LIKE_RPS", 8),
		MockFailPct:        getenvFloat("MOCK_FAIL_PCT", 0.08),
		EnablePprof:        getenvBool("ENABLE_PPROF", true),
	}

	if cfg.PostgresDSN == "" {
		return Config{}, fmt.Errorf("POSTGRES_DSN is required")
	}
	if cfg.MaxConcurrency < 1 {
		return Config{}, fmt.Errorf("MAX_CONCURRENCY must be >= 1")
	}
	if cfg.BatchMin < 1 || cfg.BatchMax < cfg.BatchMin {
		return Config{}, fmt.Errorf("invalid batch bounds: min=%d max=%d", cfg.BatchMin, cfg.BatchMax)
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func getenvBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func hostnameOr(fallback string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return fallback
	}
	return h
}
