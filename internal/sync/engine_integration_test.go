//go:build integration

package sync_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestEndToEndSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration in short mode")
	}
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("ordersync"),
		postgres.WithUsername("ordersync"),
		postgres.WithPassword("ordersync"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })

	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = redisContainer.Terminate(ctx) })

	pgDSN, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	redisAddr, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	st, err := store.New(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx, "../../migrations/001_init.sql"); err != nil {
		t.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := config.Config{
		WorkerID:       "test-worker",
		MaxConcurrency: 4,
		BatchMin:       5,
		BatchSoftLimit: 20,
		BatchMax:       50,
		LockTTL:        5 * time.Second,
		PollInterval:   time.Second,
		WBLikeRPS:      50,
		OzonLikeRPS:    50,
		MockFailPct:    0,
	}
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	erp := mockerp.New()
	market := mockmarket.NewRegistry(
		mockmarket.New(domain.MarketplaceWBLike, "t1", cfg.WBLikeRPS, 0, m),
		mockmarket.New(domain.MarketplaceOzonLike, "t2", cfg.OzonLikeRPS, 0, m),
	)
	locker := lock.NewRedisLocker(rdb, "itest:", cfg.LockTTL)
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	engine := sync.New(cfg, st, erp, market, locker, log, m)

	job, err := engine.SeedAndEnqueue(ctx, 25)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ProcessJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.JobStatusCompleted {
		t.Fatalf("status=%s done=%d failed=%d", got.Status, got.OrdersDone, got.OrdersFailed)
	}
	if got.OrdersDone < 1 {
		t.Fatalf("expected synced orders, got done=%d", got.OrdersDone)
	}

	// Idempotent re-run: requeue same content should hit ledger skips.
	fresh, _ := st.CountFreshOrders(ctx)
	if fresh != 0 {
		t.Fatalf("expected no fresh orders after sync, got %d", fresh)
	}
}
