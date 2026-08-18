package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/marketplace"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/retry"
)

// Store is the persistence the worker needs; *store.Store implements it.
type Store interface {
	CountClaimable(ctx context.Context, maxAttempts, limit int) (int, error)
	ClaimNextJob(ctx context.Context, workerID string, freshVolume int, lease time.Duration) (domain.SyncJob, bool, error)
	ClaimOrders(ctx context.Context, workerID string, limit, maxAttempts int, lease time.Duration) ([]domain.ClaimedOrder, error)
	LedgerRef(ctx context.Context, orderID uuid.UUID, method, contentHash string) (string, bool, error)
	CompleteOrder(ctx context.Context, o domain.ClaimedOrder, method, remoteRef string) (domain.OrderStatus, error)
	FailOrder(ctx context.Context, o domain.ClaimedOrder, reason string, maxAttempts int, retryIn time.Duration) (domain.OrderStatus, error)
	ReleaseOrder(ctx context.Context, o domain.ClaimedOrder) error
	RecordBatch(ctx context.Context, jobID uuid.UUID, workerID string, r domain.BatchResult, lease time.Duration) error
	FinishJob(ctx context.Context, jobID uuid.UUID, workerID string, status domain.JobStatus, errMsg string) error
}

// Marketplace performs the remote side effect.
type Marketplace interface {
	UpsertOrder(ctx context.Context, mp domain.Marketplace, req marketplace.Upsert) (string, error)
}

type WorkerConfig struct {
	ID               string
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
	ShutdownGrace    time.Duration
	// PersistTimeout bounds each outcome write. It starts after the remote
	// call, so rate-limit waits never eat into it. Defaults to 5s.
	PersistTimeout time.Duration
}

type Worker struct {
	cfg     WorkerConfig
	store   Store
	market  Marketplace
	log     *slog.Logger
	metrics *metrics.Metrics
}

func NewWorker(cfg WorkerConfig, st Store, market Marketplace, log *slog.Logger, m *metrics.Metrics) *Worker {
	if cfg.PersistTimeout <= 0 {
		cfg.PersistTimeout = 5 * time.Second
	}
	return &Worker{cfg: cfg, store: st, market: market, log: log, metrics: m}
}

type outcome string

const (
	outcomeSynced   outcome = "synced"
	outcomeRequeued outcome = "requeued"
	outcomeFailed   outcome = "failed"
	outcomeDead     outcome = "dead"
	outcomeLost     outcome = "lease_lost"
	outcomeReleased outcome = "released"
)

// Run claims and processes jobs until ctx is cancelled. Cancellation stops new
// claims immediately; in-flight orders get ShutdownGrace to finish before
// their calls are cancelled and their leases released.
func (w *Worker) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	stop := context.AfterFunc(ctx, func() {
		time.AfterFunc(w.cfg.ShutdownGrace, cancelWork)
	})
	defer stop()

	for {
		if ctx.Err() != nil {
			return nil
		}
		processed, err := w.runOnce(ctx, workCtx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("worker iteration failed", "err", err)
		}
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

func (w *Worker) runOnce(ctx, workCtx context.Context) (bool, error) {
	volume, err := w.store.CountClaimable(ctx, w.cfg.MaxAttempts, w.volumeCap())
	if err != nil {
		return false, err
	}
	job, ok, err := w.store.ClaimNextJob(ctx, w.cfg.ID, volume, w.cfg.JobLease)
	if err != nil || !ok {
		return false, err
	}
	return true, w.ProcessJob(ctx, workCtx, job)
}

// ProcessJob drains claimable orders in adaptive batches. ctx gates claiming
// new batches; workCtx bounds in-flight work.
func (w *Worker) ProcessJob(ctx, workCtx context.Context, job domain.SyncJob) error {
	log := w.log.With("job_id", job.ID)
	log.Info("job started", "fresh_volume", job.FreshVolume)
	start := time.Now()

	status, errMsg := domain.JobStatusCompleted, ""
	var totals domain.BatchResult
	for batch := 0; batch < w.cfg.MaxBatchesPerJob; batch++ {
		if ctx.Err() != nil {
			status, errMsg = domain.JobStatusCancelled, "worker shutting down"
			break
		}
		res, err := w.runBatch(ctx, workCtx)
		if err != nil {
			status, errMsg = domain.JobStatusFailed, err.Error()
			if ctx.Err() != nil {
				status, errMsg = domain.JobStatusCancelled, "worker shutting down"
			}
			break
		}
		if res.Size == 0 {
			break
		}
		totals.Size += res.Size
		totals.Done += res.Done
		totals.Failed += res.Failed
		recCtx, cancelRec := context.WithTimeout(context.WithoutCancel(workCtx), w.cfg.PersistTimeout)
		err = w.store.RecordBatch(recCtx, job.ID, w.cfg.ID, res, w.cfg.JobLease)
		cancelRec()
		if err != nil {
			if errors.Is(err, domain.ErrLeaseLost) {
				log.Warn("job lease lost; stopping")
				w.metrics.JobsFinished.WithLabelValues("lease_lost").Inc()
				return nil
			}
			status, errMsg = domain.JobStatusFailed, err.Error()
			break
		}
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.PersistTimeout)
	defer cancel()
	if err := w.store.FinishJob(finishCtx, job.ID, w.cfg.ID, status, errMsg); err != nil {
		return fmt.Errorf("finish job %s: %w", job.ID, err)
	}
	w.metrics.JobsFinished.WithLabelValues(string(status)).Inc()
	log.Info("job finished", "status", status, "claimed", totals.Size, "done", totals.Done,
		"failed", totals.Failed, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

func (w *Worker) runBatch(ctx, workCtx context.Context) (domain.BatchResult, error) {
	volume, err := w.store.CountClaimable(ctx, w.cfg.MaxAttempts, w.volumeCap())
	if err != nil {
		return domain.BatchResult{}, err
	}
	w.metrics.FreshOrders.Set(float64(volume))
	if volume == 0 {
		return domain.BatchResult{}, nil
	}
	size := AdaptiveBatchSize(volume, w.cfg.BatchMin, w.cfg.BatchSoftLimit, w.cfg.BatchMax)
	orders, err := w.store.ClaimOrders(ctx, w.cfg.ID, size, w.cfg.MaxAttempts, w.cfg.OrderLease)
	if err != nil {
		return domain.BatchResult{}, err
	}
	if len(orders) == 0 {
		return domain.BatchResult{}, nil
	}
	w.metrics.BatchSize.Observe(float64(len(orders)))

	var done, failed atomic.Int64
	var g errgroup.Group
	g.SetLimit(w.cfg.MaxConcurrency)
	for _, o := range orders {
		g.Go(func() error {
			switch w.syncOrder(workCtx, o) {
			case outcomeSynced, outcomeRequeued:
				done.Add(1)
			case outcomeFailed, outcomeDead:
				failed.Add(1)
			case outcomeLost, outcomeReleased:
			}
			return nil
		})
	}
	_ = g.Wait()
	return domain.BatchResult{Size: len(orders), Done: int(done.Load()), Failed: int(failed.Load())}, nil
}

// syncOrder never returns an error: every path ends in a persisted state
// (synced, re-queued, failed with backoff, dead, or released).
func (w *Worker) syncOrder(workCtx context.Context, o domain.ClaimedOrder) outcome {
	start := time.Now()
	log := w.log.With("order_id", o.ID, "external_id", o.ExternalID, "marketplace", o.Marketplace, "attempt", o.Attempts)

	callCtx, cancel := context.WithTimeout(workCtx, w.cfg.OrderTimeout)
	defer cancel()
	ref, err := w.pushOrder(callCtx, o)

	// Persisting the outcome must survive both the call deadline and
	// shutdown; otherwise the order stays leased until the lease expires.
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(workCtx), w.cfg.PersistTimeout)
	defer cancelPersist()
	var oc outcome
	switch {
	case err == nil:
		oc = w.complete(persistCtx, log, o, ref)
	case workCtx.Err() != nil:
		if relErr := w.store.ReleaseOrder(persistCtx, o); relErr != nil {
			log.Error("release order failed", "err", relErr)
		}
		oc = outcomeReleased
	default:
		oc = w.fail(persistCtx, log, o, err)
	}

	w.metrics.OrdersProcessed.WithLabelValues(string(o.Marketplace), string(oc)).Inc()
	w.metrics.OrderSyncSeconds.WithLabelValues(string(o.Marketplace)).Observe(time.Since(start).Seconds())
	if oc == outcomeSynced {
		w.metrics.OrderLagSeconds.WithLabelValues(string(o.Marketplace)).Observe(time.Since(o.PendingSince).Seconds())
	}
	return oc
}

func (w *Worker) pushOrder(ctx context.Context, o domain.ClaimedOrder) (string, error) {
	ref, found, err := w.store.LedgerRef(ctx, o.ID, marketplace.MethodUpsertOrder, o.ContentHash)
	if err != nil {
		return "", err
	}
	if found {
		return ref, nil
	}
	return w.market.UpsertOrder(ctx, o.Marketplace, marketplace.Upsert{
		ExternalID:     o.ExternalID,
		Payload:        o.Payload,
		IdempotencyKey: IdempotencyKey(o.ID, marketplace.MethodUpsertOrder, o.ContentHash),
	})
}

func (w *Worker) complete(ctx context.Context, log *slog.Logger, o domain.ClaimedOrder, ref string) outcome {
	status, err := w.store.CompleteOrder(ctx, o, marketplace.MethodUpsertOrder, ref)
	switch {
	case errors.Is(err, domain.ErrLeaseLost):
		log.Warn("lease lost before completion; ledger kept for the new owner")
		return outcomeLost
	case err != nil:
		// The remote write succeeded but was not recorded. The lease will
		// expire and the retry replays the same idempotency key.
		log.Error("persist completion failed", "err", err)
		return outcomeFailed
	case status == domain.OrderStatusPending:
		log.Info("content changed during sync; re-queued")
		return outcomeRequeued
	default:
		return outcomeSynced
	}
}

func (w *Worker) fail(ctx context.Context, log *slog.Logger, o domain.ClaimedOrder, cause error) outcome {
	retryIn := retry.Backoff(w.cfg.RetryBase, w.cfg.RetryMax, o.Attempts) + w.cfg.RetryBase/2
	status, err := w.store.FailOrder(ctx, o, cause.Error(), w.cfg.MaxAttempts, retryIn)
	switch {
	case errors.Is(err, domain.ErrLeaseLost):
		return outcomeLost
	case err != nil:
		log.Error("persist failure failed", "err", err, "cause", cause)
		return outcomeFailed
	case status == domain.OrderStatusDead:
		log.Error("order dead-lettered", "err", cause)
		return outcomeDead
	default:
		log.Warn("order sync failed; will retry", "err", cause, "retry_in", retryIn.Round(time.Millisecond))
		return outcomeFailed
	}
}

func (w *Worker) volumeCap() int { return w.cfg.BatchSoftLimit * 8 }

// IdempotencyKey is stable for (order, method, content), so every retry of
// the same change reuses it and a content change gets a fresh one.
func IdempotencyKey(orderID uuid.UUID, method, contentHash string) string {
	sum := sha256.Sum256([]byte(orderID.String() + "|" + method + "|" + contentHash))
	return hex.EncodeToString(sum[:16])
}
