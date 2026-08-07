package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ordersync/platform/internal/config"
	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/lock"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/mockerp"
	"github.com/ordersync/platform/internal/mockmarket"
	"github.com/ordersync/platform/internal/store"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Engine struct {
	cfg    config.Config
	store  *store.Store
	erp    *mockerp.ERP
	market *mockmarket.Registry
	locker *lock.RedisLocker
	log    *slog.Logger
	metrics *metrics.Metrics
}

func New(
	cfg config.Config,
	st *store.Store,
	erp *mockerp.ERP,
	market *mockmarket.Registry,
	locker *lock.RedisLocker,
	log *slog.Logger,
	m *metrics.Metrics,
) *Engine {
	return &Engine{cfg: cfg, store: st, erp: erp, market: market, locker: locker, log: log, metrics: m}
}

// IngestFromERP pulls fresh orders from the mock ERP into Postgres.
func (e *Engine) IngestFromERP(ctx context.Context, maxPull int) (int, error) {
	fresh, err := e.erp.PollFresh(ctx, maxPull)
	if err != nil {
		return 0, fmt.Errorf("poll erp: %w", err)
	}
	if len(fresh) == 0 {
		return 0, nil
	}
	n, err := e.store.UpsertFreshOrders(ctx, fresh)
	if err != nil {
		return 0, err
	}
	e.log.Info("ingested erp orders", "pulled", len(fresh), "upserted", n)
	return len(fresh), nil
}

// EnqueueJob creates a sync job sized by current fresh volume.
func (e *Engine) EnqueueJob(ctx context.Context, source string) (domain.SyncJob, error) {
	volume, err := e.store.CountFreshOrders(ctx)
	if err != nil {
		return domain.SyncJob{}, err
	}
	batch := AdaptiveBatchSize(volume, e.cfg.BatchMin, e.cfg.BatchSoftLimit, e.cfg.BatchMax)
	job, err := e.store.CreateSyncJob(ctx, source, volume, batch)
	if err != nil {
		return domain.SyncJob{}, err
	}
	e.metrics.BatchSize.Observe(float64(batch))
	e.log.Info("enqueued sync job", "job_id", job.ID, "fresh_volume", volume, "batch_size", batch)
	return job, nil
}

// SeedAndEnqueue is a demo helper: seed ERP, ingest, enqueue.
func (e *Engine) SeedAndEnqueue(ctx context.Context, n int) (domain.SyncJob, error) {
	e.erp.Seed(n)
	if _, err := e.IngestFromERP(ctx, n+10); err != nil {
		return domain.SyncJob{}, err
	}
	return e.EnqueueJob(ctx, "api")
}

// RunWorkerLoop claims jobs under a distributed lock and processes them.
func (e *Engine) RunWorkerLoop(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				e.log.Error("worker tick failed", "err", err)
			}
		}
	}
}

func (e *Engine) tick(ctx context.Context) error {
	// Opportunistically drain ERP buffer so volume-based batching stays meaningful.
	if _, err := e.IngestFromERP(ctx, e.cfg.BatchMax*2); err != nil {
		e.log.Warn("erp ingest failed", "err", err)
	}

	handle, waited, err := e.locker.TryWithWait(ctx, "sync-worker", e.cfg.WorkerID, 200*time.Millisecond)
	e.metrics.LockWait.Observe(waited.Seconds())
	if errors.Is(err, lock.ErrNotAcquired) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acquire worker lock: %w", err)
	}
	defer func() {
		if relErr := handle.Release(context.Background()); relErr != nil {
			e.log.Warn("lock release failed", "err", relErr)
		}
	}()

	job, ok, err := e.store.ClaimNextJob(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return e.ProcessJob(ctx, job)
}

func (e *Engine) ProcessJob(ctx context.Context, job domain.SyncJob) error {
	start := time.Now()
	e.log.Info("processing job", "job_id", job.ID, "batch_size", job.BatchSize)

	orders, err := e.store.ClaimOrders(ctx, job.BatchSize)
	if err != nil {
		_ = e.store.FinishJob(ctx, job.ID, domain.JobStatusFailed, err.Error())
		e.metrics.JobsProcessed.WithLabelValues("failed").Inc()
		return err
	}
	_ = e.store.UpdateJobProgress(ctx, job.ID, len(orders), 0, 0)

	sem := semaphore.NewWeighted(int64(e.cfg.MaxConcurrency))
	g, gctx := errgroup.WithContext(ctx)

	var (
		mu     sync.Mutex
		done   int
		failed int
	)

	for _, order := range orders {
		order := order
		if err := sem.Acquire(gctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer sem.Release(1)
			if err := e.syncOrder(gctx, order); err != nil {
				e.log.Warn("order sync failed", "order_id", order.ID, "err", err)
				_ = e.store.MarkOrderFailed(gctx, order.ID)
				e.metrics.OrdersProcessed.WithLabelValues(string(order.Marketplace), "failed").Inc()
				mu.Lock()
				failed++
				mu.Unlock()
				return nil // do not cancel sibling orders
			}
			e.metrics.OrdersProcessed.WithLabelValues(string(order.Marketplace), "synced").Inc()
			e.metrics.SyncLatency.WithLabelValues(string(order.Marketplace)).Observe(time.Since(start).Seconds())
			mu.Lock()
			done++
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	_ = e.store.UpdateJobProgress(ctx, job.ID, len(orders), done, failed)
	status := domain.JobStatusCompleted
	errMsg := ""
	if failed > 0 && done == 0 {
		status = domain.JobStatusFailed
		errMsg = "all orders failed"
	}
	if err := e.store.FinishJob(ctx, job.ID, status, errMsg); err != nil {
		return err
	}

	payload := map[string]any{
		"job_id": job.ID.String(),
		"done":   done,
		"failed": failed,
		"batch":  job.BatchSize,
	}
	_ = e.store.InsertOutbox(ctx, nil, "sync_job", job.ID.String(), domain.EventJobCompleted, payload)

	e.metrics.JobsProcessed.WithLabelValues(string(status)).Inc()
	e.log.Info("job finished", "job_id", job.ID, "done", done, "failed", failed, "took", time.Since(start))
	return nil
}

func (e *Engine) syncOrder(ctx context.Context, order domain.Order) error {
	const method = "orders.upsert"

	exists, remoteRef, err := e.store.HasLedgerEntry(ctx, order.ID, order.Marketplace, method, order.ContentHash)
	if err != nil {
		return err
	}
	if exists {
		e.log.Debug("idempotent skip", "order_id", order.ID, "remote_ref", remoteRef)
		if err := e.store.MarkOrderSynced(ctx, order.ID); err != nil {
			return err
		}
		return e.emitOrderEvent(ctx, order, domain.EventOrderSynced, remoteRef)
	}

	client, err := e.market.For(order.Marketplace)
	if err != nil {
		return err
	}
	res, err := client.UpsertOrder(ctx, order.ExternalID, order.Payload)
	if err != nil {
		_ = e.emitOrderEvent(ctx, order, domain.EventOrderFailed, err.Error())
		return err
	}
	if err := e.store.InsertLedger(ctx, order.ID, order.Marketplace, method, order.ContentHash, res.RemoteRef); err != nil {
		return err
	}
	if err := e.store.MarkOrderSynced(ctx, order.ID); err != nil {
		return err
	}
	return e.emitOrderEvent(ctx, order, domain.EventOrderSynced, res.RemoteRef)
}

func (e *Engine) emitOrderEvent(ctx context.Context, order domain.Order, eventType, ref string) error {
	payload := map[string]any{
		"order_id":    order.ID.String(),
		"external_id": order.ExternalID,
		"marketplace": order.Marketplace,
		"ref":         ref,
	}
	return e.store.InsertOutbox(ctx, nil, "order", order.ID.String(), eventType, payload)
}

// ProcessJobByID is used by tests / manual triggers.
func (e *Engine) ProcessJobByID(ctx context.Context, id uuid.UUID) error {
	job, err := e.store.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if job.Status != domain.JobStatusQueued && job.Status != domain.JobStatusRunning {
		return fmt.Errorf("job %s not processable in status %s", id, job.Status)
	}
	return e.ProcessJob(ctx, job)
}
