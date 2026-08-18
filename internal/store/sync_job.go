package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ordersync/platform/internal/domain"
)

const jobColumns = `j.id, j.status, j.trigger_source, COALESCE(j.worker_id, ''), j.fresh_volume, j.batches,
	j.last_batch_size, j.orders_claimed, j.orders_done, j.orders_failed, COALESCE(j.error_message, ''),
	j.started_at, j.finished_at, j.created_at`

func scanJob(row pgx.Row) (domain.SyncJob, error) {
	var j domain.SyncJob
	err := row.Scan(&j.ID, &j.Status, &j.TriggerSource, &j.WorkerID, &j.FreshVolume, &j.Batches, &j.LastBatchSize,
		&j.OrdersClaimed, &j.OrdersDone, &j.OrdersFailed, &j.ErrorMessage, &j.StartedAt, &j.FinishedAt, &j.CreatedAt)
	return j, err
}

// EnsureQueuedJob returns the single queued job, creating it if none exists.
// The partial unique index makes concurrent triggers converge on one row.
func (s *Store) EnsureQueuedJob(ctx context.Context, source string) (domain.SyncJob, bool, error) {
	for range 3 {
		job, err := scanJob(s.pool.QueryRow(ctx, `
			INSERT INTO sync_jobs AS j (status, trigger_source)
			VALUES ('queued', $1)
			ON CONFLICT (status) WHERE status = 'queued' DO NOTHING
			RETURNING `+jobColumns, source))
		if err == nil {
			return job, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.SyncJob{}, false, fmt.Errorf("create sync job: %w", err)
		}
		job, err = scanJob(s.pool.QueryRow(ctx,
			`SELECT `+jobColumns+` FROM sync_jobs j WHERE j.status = 'queued'`))
		if err == nil {
			return job, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.SyncJob{}, false, fmt.Errorf("load queued job: %w", err)
		}
		// The queued job was claimed between our insert and select; retry.
	}
	return domain.SyncJob{}, false, errors.New("ensure queued job: too much contention")
}

func (s *Store) ClaimNextJob(ctx context.Context, workerID string, freshVolume int, lease time.Duration) (domain.SyncJob, bool, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id FROM sync_jobs
			WHERE status = 'queued'
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE sync_jobs j
		SET status       = 'running',
		    worker_id    = $1,
		    fresh_volume = $2,
		    started_at   = now(),
		    lease_until  = now() + $3 * interval '1 millisecond'
		FROM next
		WHERE j.id = next.id
		RETURNING `+jobColumns, workerID, freshVolume, millis(lease)))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SyncJob{}, false, nil
	}
	if err != nil {
		return domain.SyncJob{}, false, fmt.Errorf("claim next job: %w", err)
	}
	return job, true, nil
}

// RecordBatch accumulates batch counters and doubles as the job heartbeat.
func (s *Store) RecordBatch(ctx context.Context, jobID uuid.UUID, workerID string, r domain.BatchResult, lease time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE sync_jobs
		SET batches         = batches + 1,
		    last_batch_size = $3,
		    orders_claimed  = orders_claimed + $3,
		    orders_done     = orders_done + $4,
		    orders_failed   = orders_failed + $5,
		    lease_until     = now() + $6 * interval '1 millisecond'
		WHERE id = $1 AND worker_id = $2 AND status = 'running'`,
		jobID, workerID, r.Size, r.Done, r.Failed, millis(lease))
	if err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrLeaseLost
	}
	return nil
}

func (s *Store) FinishJob(ctx context.Context, jobID uuid.UUID, workerID string, status domain.JobStatus, errMsg string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		job, err := scanJob(tx.QueryRow(ctx, `
			UPDATE sync_jobs j
			SET status        = $3,
			    error_message = NULLIF($4, ''),
			    finished_at   = now(),
			    lease_until   = NULL
			WHERE id = $1 AND worker_id = $2 AND status = 'running'
			RETURNING `+jobColumns, jobID, workerID, status, truncate(errMsg, 500)))
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return fmt.Errorf("finish job: %w", err)
		}
		return insertOutbox(ctx, tx, domain.AggregateSyncJob, job.ID.String(), domain.EventJobFinished, map[string]any{
			"job_id":         job.ID,
			"status":         job.Status,
			"worker_id":      job.WorkerID,
			"batches":        job.Batches,
			"orders_claimed": job.OrdersClaimed,
			"orders_done":    job.OrdersDone,
			"orders_failed":  job.OrdersFailed,
		})
	})
}

// FailExpiredJobs closes jobs whose worker stopped heartbeating. Their orders
// are recovered independently through order leases.
func (s *Store) FailExpiredJobs(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE sync_jobs
		SET status = 'failed', error_message = 'worker lease expired', finished_at = now(), lease_until = NULL
		WHERE status = 'running' AND lease_until < now()`)
	if err != nil {
		return 0, fmt.Errorf("fail expired jobs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.SyncJob, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM sync_jobs j WHERE j.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SyncJob{}, ErrNotFound
	}
	if err != nil {
		return domain.SyncJob{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListRecentJobs(ctx context.Context, limit int) ([]domain.SyncJob, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM sync_jobs j ORDER BY j.created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []domain.SyncJob{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}
