// Command api serves the HTTP API: ERP ingest, sync triggers, inspection,
// metrics, and (in development) the mock marketplace.
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

	"github.com/ordersync/platform/internal/app"
	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/httpserver"
	"github.com/ordersync/platform/internal/mockerp"
	"github.com/ordersync/platform/internal/mockmarket"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := app.Init(ctx, "api")
	if err != nil {
		return err
	}
	defer deps.Close()
	cfg, log := deps.Cfg, deps.Log

	var mock http.Handler
	if cfg.MockMarketEnabled {
		mock = mockmarket.NewServer(mockmarket.Config{
			RPS: map[domain.Marketplace]float64{
				domain.MarketplaceWBLike:   cfg.WBLikeRPS,
				domain.MarketplaceOzonLike: cfg.OzonLikeRPS,
			},
			FailRate: cfg.MockFailRate,
			Latency:  cfg.MockLatency,
		}, deps.Metrics).Handler()
		log.Warn("mock marketplace enabled; development only", "path", "/mock/marketplaces")
	}

	api := httpserver.New(httpserver.Config{
		APIKey:         cfg.APIKey,
		EnablePprof:    cfg.EnablePprof,
		IdempotencyTTL: cfg.IdempotencyTTL,
		MaxAttempts:    cfg.MaxAttempts,
		MockMarket:     mock,
	}, deps.Store, mockerp.NewGenerator(), log, deps.Metrics,
		deps.Store.Ping,
		func(ctx context.Context) error { return deps.Redis.Ping(ctx).Err() },
	)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	log.Info("api stopped")
	return nil
}
