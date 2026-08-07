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

func (s *Store) CreateSyncJob(ctx context.Context, source string, freshVolume, batchSize int) (domain.SyncJob, error) {
	var j domain.SyncJob
	err := s.pool.QueryRow(ctx, `
		INSERT INTO sync_jobs (status, trigger_source, fresh_volume, batch_size)
		VALUES ('queued', $1, $2, $3)
		RETURNING id, status, trigger_source, fresh_volume, batch_size, orders_queued, orders_done, orders_failed,
		          COALESCE(error_message, ''), started_at, finished_at, created_at
	`, source, freshVolume, batchSize).Scan(
		&j.ID, &j.Status, &j.TriggerSource, &j.FreshVolume, &j.BatchSize,
		&j.OrdersQueued, &j.OrdersDone, &j.OrdersFailed, &j.ErrorMessage,
		&j.StartedAt, &j.FinishedAt, &j.CreatedAt,
	)
	if err != nil {
		return domain.SyncJob{}, fmt.Errorf("create sync job: %w", err)
	}
	return j, nil
}

func (s *Store) ClaimNextJob(ctx context.Context) (domain.SyncJob, bool, error) {
	var j domain.SyncJob
	err := s.pool.QueryRow(ctx, `
		WITH cte AS (
			SELECT id FROM sync_jobs
			WHERE status = 'queued'
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE sync_jobs sj
		SET status = 'running', started_at = now()
		FROM cte
		WHERE sj.id = cte.id
		RETURNING sj.id, sj.status, sj.trigger_source, sj.fresh_volume, sj.batch_size,
		          sj.orders_queued, sj.orders_done, sj.orders_failed,
		          COALESCE(sj.error_message, ''), sj.started_at, sj.finished_at, sj.created_at
	`).Scan(
		&j.ID, &j.Status, &j.TriggerSource, &j.FreshVolume, &j.BatchSize,
		&j.OrdersQueued, &j.OrdersDone, &j.OrdersFailed, &j.ErrorMessage,
		&j.StartedAt, &j.FinishedAt, &j.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.SyncJob{}, false, nil
		}
		return domain.SyncJob{}, false, fmt.Errorf("claim next job: %w", err)
	}
	return j, true, nil
}

func (s *Store) UpdateJobProgress(ctx context.Context, id uuid.UUID, queued, done, failed int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE sync_jobs
		SET orders_queued = $2, orders_done = $3, orders_failed = $4
		WHERE id = $1
	`, id, queued, done, failed)
	if err != nil {
		return fmt.Errorf("update job progress: %w", err)
	}
	return nil
}

func (s *Store) FinishJob(ctx context.Context, id uuid.UUID, status domain.JobStatus, errMsg string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE sync_jobs
		SET status = $2, error_message = NULLIF($3, ''), finished_at = $4
		WHERE id = $1
	`, id, status, errMsg, now)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.SyncJob, error) {
	var j domain.SyncJob
	err := s.pool.QueryRow(ctx, `
		SELECT id, status, trigger_source, fresh_volume, batch_size, orders_queued, orders_done, orders_failed,
		       COALESCE(error_message, ''), started_at, finished_at, created_at
		FROM sync_jobs WHERE id = $1
	`, id).Scan(
		&j.ID, &j.Status, &j.TriggerSource, &j.FreshVolume, &j.BatchSize,
		&j.OrdersQueued, &j.OrdersDone, &j.OrdersFailed, &j.ErrorMessage,
		&j.StartedAt, &j.FinishedAt, &j.CreatedAt,
	)
	if err != nil {
		return domain.SyncJob{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

func (s *Store) ListRecentJobs(ctx context.Context, limit int) ([]domain.SyncJob, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, status, trigger_source, fresh_volume, batch_size, orders_queued, orders_done, orders_failed,
		       COALESCE(error_message, ''), started_at, finished_at, created_at
		FROM sync_jobs
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var out []domain.SyncJob
	for rows.Next() {
		var j domain.SyncJob
		if err := rows.Scan(
			&j.ID, &j.Status, &j.TriggerSource, &j.FreshVolume, &j.BatchSize,
			&j.OrdersQueued, &j.OrdersDone, &j.OrdersFailed, &j.ErrorMessage,
			&j.StartedAt, &j.FinishedAt, &j.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
