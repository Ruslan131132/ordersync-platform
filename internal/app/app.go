// Package app wires shared infrastructure for the api and worker binaries.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/ordersync/platform/internal/config"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/migrations"
)

type Deps struct {
	Cfg     config.Config
	Log     *slog.Logger
	Store   *store.Store
	Redis   *redis.Client
	Metrics *metrics.Metrics
}

// Init connects to Postgres and Redis and applies migrations. Both binaries
// migrate on start; an advisory lock and a version table make that safe.
func Init(ctx context.Context, component string) (*Deps, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", cfg.ServiceName, "component", component)

	st, err := store.New(ctx, cfg.PostgresDSN, cfg.PostgresMaxConns)
	if err != nil {
		return nil, err
	}
	if err := st.Migrate(ctx, migrations.FS); err != nil {
		st.Close()
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(ctx).Err(); err != nil {
		st.Close()
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Deps{
		Cfg:     cfg,
		Log:     log,
		Store:   st,
		Redis:   rdb,
		Metrics: metrics.New(prometheus.DefaultRegisterer),
	}, nil
}

func (d *Deps) Close() {
	_ = d.Redis.Close()
	d.Store.Close()
}
