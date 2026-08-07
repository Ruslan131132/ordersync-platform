package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ordersync/platform/internal/domain"
)

func (s *Store) UpsertFreshOrders(ctx context.Context, fresh []domain.FreshOrder) (int, error) {
	if len(fresh) == 0 {
		return 0, nil
	}
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		for _, fo := range fresh {
			_, err := tx.Exec(ctx, `
				INSERT INTO orders (external_id, marketplace, status, payload, content_hash)
				VALUES ($1, $2, 'pending', $3, $4)
				ON CONFLICT (marketplace, external_id) DO UPDATE
				SET payload = EXCLUDED.payload,
				    content_hash = EXCLUDED.content_hash,
				    status = CASE
				        WHEN orders.content_hash IS DISTINCT FROM EXCLUDED.content_hash THEN 'pending'
				        ELSE orders.status
				    END,
				    version = CASE
				        WHEN orders.content_hash IS DISTINCT FROM EXCLUDED.content_hash THEN orders.version + 1
				        ELSE orders.version
				    END,
				    updated_at = now()
			`, fo.ExternalID, fo.Marketplace, fo.Payload, fo.ContentHash)
			if err != nil {
				return fmt.Errorf("upsert order %s: %w", fo.ExternalID, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(fresh), nil
}

func (s *Store) CountFreshOrders(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'failed')`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count fresh orders: %w", err)
	}
	return n, nil
}

func (s *Store) ClaimOrders(ctx context.Context, limit int) ([]domain.Order, error) {
	rows, err := s.pool.Query(ctx, `
		WITH cte AS (
			SELECT id FROM orders
			WHERE status IN ('pending', 'failed')
			ORDER BY updated_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE orders o
		SET status = 'syncing', updated_at = now()
		FROM cte
		WHERE o.id = cte.id
		RETURNING o.id, o.external_id, o.marketplace, o.status, o.payload, o.content_hash, o.version, o.created_at, o.updated_at
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim orders: %w", err)
	}
	defer rows.Close()

	var out []domain.Order
	for rows.Next() {
		var o domain.Order
		if err := rows.Scan(
			&o.ID, &o.ExternalID, &o.Marketplace, &o.Status,
			&o.Payload, &o.ContentHash, &o.Version, &o.CreatedAt, &o.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed order: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *Store) GetOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	var o domain.Order
	err := s.pool.QueryRow(ctx, `
		SELECT id, external_id, marketplace, status, payload, content_hash, version, created_at, updated_at
		FROM orders WHERE id = $1
	`, id).Scan(
		&o.ID, &o.ExternalID, &o.Marketplace, &o.Status,
		&o.Payload, &o.ContentHash, &o.Version, &o.CreatedAt, &o.UpdatedAt,
	)
	if err != nil {
		return domain.Order{}, fmt.Errorf("get order: %w", err)
	}
	return o, nil
}

func (s *Store) MarkOrderSynced(ctx context.Context, orderID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE orders SET status = 'synced', updated_at = now() WHERE id = $1
	`, orderID)
	if err != nil {
		return fmt.Errorf("mark order synced: %w", err)
	}
	return nil
}

func (s *Store) MarkOrderFailed(ctx context.Context, orderID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE orders SET status = 'failed', updated_at = now() WHERE id = $1
	`, orderID)
	if err != nil {
		return fmt.Errorf("mark order failed: %w", err)
	}
	return nil
}

func (s *Store) HasLedgerEntry(ctx context.Context, orderID uuid.UUID, marketplace domain.Marketplace, method, hash string) (bool, string, error) {
	var remoteRef string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(remote_ref, '') FROM sync_ledger
		WHERE order_id = $1 AND marketplace = $2 AND method = $3 AND content_hash = $4
	`, orderID, marketplace, method, hash).Scan(&remoteRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("ledger lookup: %w", err)
	}
	return true, remoteRef, nil
}

func (s *Store) InsertLedger(ctx context.Context, orderID uuid.UUID, marketplace domain.Marketplace, method, hash, remoteRef string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sync_ledger (order_id, marketplace, method, content_hash, remote_ref)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (order_id, marketplace, method, content_hash) DO NOTHING
	`, orderID, marketplace, method, hash, remoteRef)
	if err != nil {
		return fmt.Errorf("insert ledger: %w", err)
	}
	return nil
}
