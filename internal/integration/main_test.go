//go:build integration

// Package integration exercises the system against real Postgres and Redis
// started by testcontainers. Run with: go test -tags=integration ./internal/integration
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/ordersync/platform/internal/store"
	"github.com/ordersync/platform/migrations"
)

var (
	pgDSN     string
	redisAddr string
)

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ordersync"),
		postgres.WithUsername("ordersync"),
		postgres.WithPassword("ordersync"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return 0, fmt.Errorf("start postgres: %w", err)
	}
	defer func() { _ = pg.Terminate(context.Background()) }()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return 0, fmt.Errorf("start redis: %w", err)
	}
	defer func() { _ = rc.Terminate(context.Background()) }()

	if pgDSN, err = pg.ConnectionString(ctx, "sslmode=disable"); err != nil {
		return 0, err
	}
	if redisAddr, err = rc.Endpoint(ctx, ""); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// setup returns a migrated store and a Redis client with all state wiped.
func setup(t *testing.T) (*store.Store, *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.New(ctx, pgDSN, 30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx, migrations.FS); err != nil {
		t.Fatal(err)
	}
	if err := truncateAll(ctx); err != nil {
		t.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return st, rdb
}
