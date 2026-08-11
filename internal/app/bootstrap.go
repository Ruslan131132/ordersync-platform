package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ordersync/platform/internal/config"
	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/lock"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/mockerp"
	"github.com/ordersync/platform/internal/mockmarket"
	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/internal/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type Runtime struct {
	Cfg     config.Config
	Log     *slog.Logger
	Store   *store.Store
	Redis   *redis.Client
	Locker  *lock.RedisLocker
	ERP     *mockerp.ERP
	Market  *mockmarket.Registry
	Engine  *sync.Engine
	Metrics *metrics.Metrics
}

func Bootstrap(ctx context.Context, role string) (*Runtime, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if role != "" && os.Getenv("WORKER_ID") == "" {
		cfg.WorkerID = fmt.Sprintf("%s-%s", role, cfg.WorkerID)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}

	migPath := getenv("MIGRATIONS_PATH", "migrations/001_init.sql")
	if abs, err := filepath.Abs(migPath); err == nil {
		migPath = abs
	}
	if err := st.Migrate(ctx, migPath); err != nil {
		st.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		st.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	m := metrics.New(prometheus.DefaultRegisterer)
	erp := mockerp.New()
	wb := mockmarket.New(domain.MarketplaceWBLike, "token-wb-demo", cfg.WBLikeRPS, cfg.MockFailPct, m)
	oz := mockmarket.New(domain.MarketplaceOzonLike, "token-ozon-demo", cfg.OzonLikeRPS, cfg.MockFailPct, m)
	market := mockmarket.NewRegistry(wb, oz)
	locker := lock.NewRedisLocker(rdb, cfg.ServiceName+":lock:", cfg.LockTTL)
	engine := sync.New(cfg, st, erp, market, locker, log, m)

	return &Runtime{
		Cfg:     cfg,
		Log:     log,
		Store:   st,
		Redis:   rdb,
		Locker:  locker,
		ERP:     erp,
		Market:  market,
		Engine:  engine,
		Metrics: m,
	}, nil
}

func (r *Runtime) Close() {
	if r.Redis != nil {
		_ = r.Redis.Close()
	}
	if r.Store != nil {
		r.Store.Close()
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
