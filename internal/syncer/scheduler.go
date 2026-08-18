package syncer

import (
	"context"
	"log/slog"
	"time"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/metrics"
)

// SchedulerStore is what the leader needs for polling and housekeeping.
type SchedulerStore interface {
	CountClaimable(ctx context.Context, maxAttempts, limit int) (int, error)
	EnsureQueuedJob(ctx context.Context, source string) (domain.SyncJob, bool, error)
	FailExpiredJobs(ctx context.Context) (int, error)
	DeadLetterExpiredLeases(ctx context.Context, maxAttempts int) (int, error)
	DeleteExpiredIdempotencyKeys(ctx context.Context) (int, error)
	PurgePublishedOutbox(ctx context.Context, olderThan time.Duration) (int, error)
	OutboxBacklog(ctx context.Context) (int, error)
}

type SchedulerConfig struct {
	PollInterval         time.Duration
	HousekeepingInterval time.Duration
	MaxAttempts          int
	OutboxRetention      time.Duration
}

// Scheduler runs on the elected leader only. It turns claimable volume into
// queued jobs (the polling half of the "poll or receive" model) and performs
// recovery that must not run concurrently on every replica.
type Scheduler struct {
	cfg     SchedulerConfig
	store   SchedulerStore
	log     *slog.Logger
	metrics *metrics.Metrics
}

func NewScheduler(cfg SchedulerConfig, st SchedulerStore, log *slog.Logger, m *metrics.Metrics) *Scheduler {
	if cfg.HousekeepingInterval <= 0 {
		cfg.HousekeepingInterval = 30 * time.Second
	}
	return &Scheduler{cfg: cfg, store: st, log: log, metrics: m}
}

func (s *Scheduler) Run(ctx context.Context) error {
	poll := time.NewTicker(s.cfg.PollInterval)
	defer poll.Stop()
	housekeeping := time.NewTicker(s.cfg.HousekeepingInterval)
	defer housekeeping.Stop()

	s.housekeep(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
			s.schedule(ctx)
		case <-housekeeping.C:
			s.housekeep(ctx)
		}
	}
}

func (s *Scheduler) schedule(ctx context.Context) {
	volume, err := s.store.CountClaimable(ctx, s.cfg.MaxAttempts, 1)
	if err != nil {
		s.logErr(ctx, "count claimable", err)
		return
	}
	if volume == 0 {
		return
	}
	job, created, err := s.store.EnsureQueuedJob(ctx, "scheduler")
	if err != nil {
		s.logErr(ctx, "ensure queued job", err)
		return
	}
	if created {
		s.log.Debug("scheduled sync job", "job_id", job.ID)
	}
}

func (s *Scheduler) housekeep(ctx context.Context) {
	if n, err := s.store.FailExpiredJobs(ctx); err != nil {
		s.logErr(ctx, "fail expired jobs", err)
	} else if n > 0 {
		s.log.Warn("closed jobs with expired leases", "count", n)
	}
	if n, err := s.store.DeadLetterExpiredLeases(ctx, s.cfg.MaxAttempts); err != nil {
		s.logErr(ctx, "dead-letter expired leases", err)
	} else if n > 0 {
		s.log.Warn("dead-lettered orders whose final lease expired", "count", n)
	}
	if _, err := s.store.DeleteExpiredIdempotencyKeys(ctx); err != nil {
		s.logErr(ctx, "delete expired idempotency keys", err)
	}
	if _, err := s.store.PurgePublishedOutbox(ctx, s.cfg.OutboxRetention); err != nil {
		s.logErr(ctx, "purge outbox", err)
	}
	if n, err := s.store.OutboxBacklog(ctx); err != nil {
		s.logErr(ctx, "outbox backlog", err)
	} else {
		s.metrics.OutboxBacklog.Set(float64(n))
	}
}

func (s *Scheduler) logErr(ctx context.Context, op string, err error) {
	if ctx.Err() != nil {
		return
	}
	s.log.Error("scheduler "+op+" failed", "err", err)
}
