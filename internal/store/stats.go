package store

import (
	"context"
	"fmt"

	"github.com/ordersync/platform/internal/domain"
)

func (s *Store) OutboxBacklog(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox backlog: %w", err)
	}
	return n, nil
}

// Stats is an operator snapshot; full scans are acceptable at demo scale.
func (s *Store) Stats(ctx context.Context, maxAttempts int) (domain.Stats, error) {
	st := domain.Stats{
		Orders: map[domain.OrderStatus]int{},
		Jobs:   map[domain.JobStatus]int{},
	}

	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM orders GROUP BY status`)
	if err != nil {
		return st, fmt.Errorf("order stats: %w", err)
	}
	for rows.Next() {
		var status domain.OrderStatus
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return st, fmt.Errorf("scan order stats: %w", err)
		}
		st.Orders[status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("order stats: %w", err)
	}

	rows, err = s.pool.Query(ctx, `SELECT status, count(*) FROM sync_jobs GROUP BY status`)
	if err != nil {
		return st, fmt.Errorf("job stats: %w", err)
	}
	for rows.Next() {
		var status domain.JobStatus
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return st, fmt.Errorf("scan job stats: %w", err)
		}
		st.Jobs[status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("job stats: %w", err)
	}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE published_at IS NULL),
		       count(*) FILTER (WHERE published_at IS NOT NULL)
		FROM outbox_events`).Scan(&st.OutboxBacklog, &st.OutboxPublished); err != nil {
		return st, fmt.Errorf("outbox stats: %w", err)
	}

	st.Claimable, err = s.CountClaimable(ctx, maxAttempts, 1_000_000)
	return st, err
}
