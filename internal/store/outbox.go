package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ordersync/platform/internal/domain"
)

func insertOutbox(ctx context.Context, db execer, aggregateType, aggregateID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4)`, aggregateType, aggregateID, eventType, body); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// DrainOutbox locks up to limit unpublished events, hands them to publish and
// marks them published in the same transaction. SKIP LOCKED lets several
// consumers drain concurrently without delivering the same row twice; a crash
// after publish but before commit re-delivers (at-least-once), so downstream
// consumers dedupe by event id.
func (s *Store) DrainOutbox(ctx context.Context, limit int, publish func(context.Context, []domain.OutboxEvent) error) (int, error) {
	var n int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
			FROM outbox_events
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return fmt.Errorf("select outbox: %w", err)
		}
		events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.OutboxEvent, error) {
			var e domain.OutboxEvent
			err := row.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.CreatedAt)
			return e, err
		})
		if err != nil {
			return fmt.Errorf("scan outbox: %w", err)
		}
		if len(events) == 0 {
			return nil
		}
		if err := publish(ctx, events); err != nil {
			return fmt.Errorf("publish outbox batch: %w", err)
		}
		ids := make([]int64, len(events))
		for i, e := range events {
			ids[i] = e.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return fmt.Errorf("mark outbox published: %w", err)
		}
		n = len(events)
		return nil
	})
	return n, err
}

func (s *Store) PurgePublishedOutbox(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM outbox_events
		WHERE published_at IS NOT NULL AND published_at < now() - $1 * interval '1 millisecond'`,
		millis(olderThan))
	if err != nil {
		return 0, fmt.Errorf("purge outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
