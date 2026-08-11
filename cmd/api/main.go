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
	"github.com/ordersync/platform/internal/httpserver"
	"github.com/ordersync/platform/internal/outbox"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rt, err := app.Bootstrap(ctx, "api")
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	srv := httpserver.New(rt.Cfg, rt.Store, rt.Engine, rt.Log)
	httpServer := &http.Server{
		Addr:              rt.Cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumer := outbox.NewConsumer(rt.Store, rt.Log, rt.Metrics, rt.Cfg.OutboxPollInterval, rt.Cfg.OutboxBatchSize)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rt.Log.Info("api listening", "addr", rt.Cfg.HTTPAddr)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	g.Go(func() error {
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		rt.Log.Error("api stopped with error", "err", err)
		os.Exit(1)
	}
	rt.Log.Info("api stopped")
}
