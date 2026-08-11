package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ordersync/platform/internal/app"
	"github.com/ordersync/platform/internal/outbox"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rt, err := app.Bootstrap(ctx, "worker")
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	consumer := outbox.NewConsumer(rt.Store, rt.Log, rt.Metrics, rt.Cfg.OutboxPollInterval, rt.Cfg.OutboxBatchSize)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	metricsAddr := getenv("WORKER_METRICS_ADDR", ":8081")
	httpServer := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rt.Log.Info("worker metrics listening", "addr", metricsAddr)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	g.Go(func() error {
		rt.Log.Info("worker loop starting", "worker_id", rt.Cfg.WorkerID)
		return rt.Engine.RunWorkerLoop(gctx)
	})
	g.Go(func() error {
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		return nil
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		rt.Log.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
	rt.Log.Info("worker stopped")
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
