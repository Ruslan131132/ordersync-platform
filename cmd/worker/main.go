// Command worker syncs orders to marketplaces. Run any number of replicas:
// orders are claimed with Postgres leases, the scheduler runs on a Redis-
// elected leader, and rate limits are shared through Redis.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/ordersync/platform/internal/app"
	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/httpserver"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/lock"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/outbox"
	"github.com/ordersync/platform/internal/retry"
	"github.com/ordersync/platform/internal/syncer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := app.Init(ctx, "worker")
	if err != nil {
		return err
	}
	defer deps.Close()
	cfg, log, m := deps.Cfg, deps.Log, deps.Metrics
	log = log.With("worker_id", cfg.WorkerID)

	httpClient := &http.Client{
		Timeout: min(10*time.Second, cfg.OrderTimeout),
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.MaxConcurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	callPolicy := retry.Policy{Attempts: 3, Base: 100 * time.Millisecond, Max: 2 * time.Second}
	newClient := func(mp domain.Marketplace, token string, rps float64) *marketplace.Client {
		return marketplace.NewClient(marketplace.ClientConfig{
			Marketplace: mp,
			BaseURL:     cfg.MarketBaseURL,
			Token:       token,
			HTTPClient:  httpClient,
			Limiter:     limiter.NewRedisLimiter(deps.Redis, cfg.ServiceName+":ratelimit:", rps, max(int(rps), 1)),
			Retry:       callPolicy,
			Metrics:     m,
		})
	}
	router := marketplace.NewRouter(
		newClient(domain.MarketplaceWBLike, cfg.WBLikeToken, cfg.WBLikeRPS),
		newClient(domain.MarketplaceOzonLike, cfg.OzonLikeToken, cfg.OzonLikeRPS),
	)

	worker := syncer.NewWorker(syncer.WorkerConfig{
		ID:               cfg.WorkerID,
		PollInterval:     cfg.PollInterval,
		MaxConcurrency:   cfg.MaxConcurrency,
		BatchMin:         cfg.BatchMin,
		BatchSoftLimit:   cfg.BatchSoftLimit,
		BatchMax:         cfg.BatchMax,
		MaxBatchesPerJob: cfg.MaxBatchesPerJob,
		OrderLease:       cfg.OrderLease,
		JobLease:         cfg.JobLease,
		OrderTimeout:     cfg.OrderTimeout,
		MaxAttempts:      cfg.MaxAttempts,
		RetryBase:        cfg.RetryBase,
		RetryMax:         cfg.RetryMax,
		ShutdownGrace:    cfg.ShutdownGrace,
	}, deps.Store, router, log, m)

	scheduler := syncer.NewScheduler(syncer.SchedulerConfig{
		PollInterval:    cfg.PollInterval,
		MaxAttempts:     cfg.MaxAttempts,
		OutboxRetention: cfg.OutboxRetention,
	}, deps.Store, log, m)
	locker := lock.NewRedisLocker(deps.Redis, cfg.ServiceName+":lock:", cfg.LeaderTTL)
	leader := lock.NewLeader(locker, "scheduler", cfg.WorkerID, log, m)

	relay := outbox.NewRelay(deps.Store, outbox.LogPublisher{Log: log}, log, m,
		cfg.OutboxPollInterval, cfg.OutboxBatchSize)

	mux := chi.NewRouter()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	if cfg.EnablePprof {
		mux.Mount("/debug/pprof", httpserver.Pprof(cfg.APIKey))
	}
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("worker metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics server: %w", err)
		}
		return nil
	})
	g.Go(func() error { return worker.Run(gctx) })
	g.Go(func() error { return leader.Run(gctx, scheduler.Run) })
	g.Go(func() error { return relay.Run(gctx) })
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return metricsSrv.Shutdown(shutdownCtx)
	})

	log.Info("worker started", "max_concurrency", cfg.MaxConcurrency)
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("worker stopped")
	return nil
}
