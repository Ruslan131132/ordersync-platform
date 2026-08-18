package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName string
	Env         string
	LogLevel    slog.Level

	HTTPAddr    string
	MetricsAddr string
	APIKey      string
	EnablePprof bool

	PostgresDSN      string
	PostgresMaxConns int32
	RedisAddr        string
	RedisDB          int

	WorkerID         string
	PollInterval     time.Duration
	MaxConcurrency   int
	BatchMin         int
	BatchSoftLimit   int
	BatchMax         int
	MaxBatchesPerJob int
	OrderLease       time.Duration
	JobLease         time.Duration
	OrderTimeout     time.Duration
	MaxAttempts      int
	RetryBase        time.Duration
	RetryMax         time.Duration
	LeaderTTL        time.Duration
	ShutdownGrace    time.Duration

	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxRetention    time.Duration
	IdempotencyTTL     time.Duration

	MarketBaseURL string
	WBLikeToken   string
	OzonLikeToken string
	WBLikeRPS     float64
	OzonLikeRPS   float64

	MockMarketEnabled bool
	MockFailRate      float64
	MockLatency       time.Duration
}

// Load reads configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.Getenv)
}

// LoadFrom reads configuration through getenv and reports every invalid
// value at once instead of silently falling back to defaults.
func LoadFrom(getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}
	cfg := Config{
		ServiceName: p.str("SERVICE_NAME", "ordersync"),
		Env:         p.str("APP_ENV", "development"),
		LogLevel:    p.level("LOG_LEVEL", slog.LevelInfo),

		HTTPAddr:    p.str("HTTP_ADDR", ":8080"),
		MetricsAddr: p.str("METRICS_ADDR", ":8081"),
		APIKey:      p.str("API_KEY", ""),
		EnablePprof: p.bool("ENABLE_PPROF", false),

		PostgresDSN:      p.str("POSTGRES_DSN", ""),
		PostgresMaxConns: int32(p.int("POSTGRES_MAX_CONNS", 20)),
		RedisAddr:        p.str("REDIS_ADDR", "localhost:6380"),
		RedisDB:          p.int("REDIS_DB", 0),

		WorkerID:         p.str("WORKER_ID", hostname()),
		PollInterval:     p.duration("POLL_INTERVAL", time.Second),
		MaxConcurrency:   p.int("MAX_CONCURRENCY", 16),
		BatchMin:         p.int("BATCH_MIN", 5),
		BatchSoftLimit:   p.int("BATCH_SOFT_LIMIT", 40),
		BatchMax:         p.int("BATCH_MAX", 200),
		MaxBatchesPerJob: p.int("MAX_BATCHES_PER_JOB", 50),
		OrderLease:       p.duration("ORDER_LEASE", 2*time.Minute),
		JobLease:         p.duration("JOB_LEASE", time.Minute),
		OrderTimeout:     p.duration("ORDER_TIMEOUT", 30*time.Second),
		MaxAttempts:      p.int("MAX_ATTEMPTS", 5),
		RetryBase:        p.duration("RETRY_BASE", 2*time.Second),
		RetryMax:         p.duration("RETRY_MAX", 5*time.Minute),
		LeaderTTL:        p.duration("LEADER_TTL", 10*time.Second),
		ShutdownGrace:    p.duration("SHUTDOWN_GRACE", 20*time.Second),

		OutboxPollInterval: p.duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    p.int("OUTBOX_BATCH_SIZE", 200),
		OutboxRetention:    p.duration("OUTBOX_RETENTION", 72*time.Hour),
		IdempotencyTTL:     p.duration("IDEMPOTENCY_TTL", 24*time.Hour),

		MarketBaseURL: p.str("MARKET_BASE_URL", "http://localhost:8080/mock/marketplaces"),
		WBLikeToken:   p.str("WB_LIKE_TOKEN", ""),
		OzonLikeToken: p.str("OZON_LIKE_TOKEN", ""),
		WBLikeRPS:     p.float("WB_LIKE_RPS", 20),
		OzonLikeRPS:   p.float("OZON_LIKE_RPS", 30),

		MockMarketEnabled: p.bool("MOCK_MARKET_ENABLED", false),
		MockFailRate:      p.float("MOCK_FAIL_RATE", 0.05),
		MockLatency:       p.duration("MOCK_LATENCY", 25*time.Millisecond),
	}
	if err := errors.Join(p.errs...); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	check(c.APIKey != "", "API_KEY is required")
	check(c.PostgresDSN != "", "POSTGRES_DSN is required")
	check(c.PostgresMaxConns >= 2, "POSTGRES_MAX_CONNS must be >= 2")
	check(c.WorkerID != "", "WORKER_ID is required")
	check(c.MaxConcurrency >= 1, "MAX_CONCURRENCY must be >= 1")
	check(c.BatchMin >= 1 && c.BatchMin <= c.BatchSoftLimit && c.BatchSoftLimit <= c.BatchMax,
		"batch bounds must satisfy 1 <= BATCH_MIN <= BATCH_SOFT_LIMIT <= BATCH_MAX (got %d/%d/%d)",
		c.BatchMin, c.BatchSoftLimit, c.BatchMax)
	check(c.MaxBatchesPerJob >= 1, "MAX_BATCHES_PER_JOB must be >= 1")
	check(c.MaxAttempts >= 1, "MAX_ATTEMPTS must be >= 1")
	check(c.OrderTimeout > 0 && c.OrderTimeout < c.OrderLease,
		"ORDER_TIMEOUT must be positive and shorter than ORDER_LEASE")
	check(c.JobLease >= time.Second, "JOB_LEASE must be >= 1s")
	check(c.LeaderTTL >= time.Second, "LEADER_TTL must be >= 1s")
	check(c.PollInterval > 0 && c.OutboxPollInterval > 0, "poll intervals must be positive")
	check(c.OutboxBatchSize >= 1, "OUTBOX_BATCH_SIZE must be >= 1")
	check(c.RetryBase > 0 && c.RetryBase <= c.RetryMax, "RETRY_BASE must be positive and <= RETRY_MAX")
	check(c.WBLikeRPS > 0 && c.OzonLikeRPS > 0, "marketplace RPS limits must be positive")
	check(c.MockFailRate >= 0 && c.MockFailRate < 1, "MOCK_FAIL_RATE must be in [0, 1)")
	return errors.Join(errs...)
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) raw(key string) (string, bool) {
	v := strings.TrimSpace(p.getenv(key))
	return v, v != ""
}

func (p *parser) str(key, fallback string) string {
	if v, ok := p.raw(key); ok {
		return v
	}
	return fallback
}

func (p *parser) int(key string, fallback int) int {
	v, ok := p.raw(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: invalid integer %q", key, v))
		return fallback
	}
	return n
}

func (p *parser) float(key string, fallback float64) float64 {
	v, ok := p.raw(key)
	if !ok {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: invalid number %q", key, v))
		return fallback
	}
	return n
}

func (p *parser) bool(key string, fallback bool) bool {
	v, ok := p.raw(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: invalid boolean %q", key, v))
		return fallback
	}
	return b
}

func (p *parser) duration(key string, fallback time.Duration) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: invalid duration %q", key, v))
		return fallback
	}
	return d
}

func (p *parser) level(key string, fallback slog.Level) slog.Level {
	v, ok := p.raw(key)
	if !ok {
		return fallback
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: invalid log level %q", key, v))
		return fallback
	}
	return lvl
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}
