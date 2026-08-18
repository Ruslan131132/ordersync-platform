package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ordersync/platform/internal/domain"
)

var ErrNotFound = errors.New("not found")

const orderColumns = `o.id, o.external_id, o.marketplace, o.status, o.payload, o.content_hash, o.version,
	o.attempts, COALESCE(o.last_error, ''), o.pending_since, o.created_at, o.updated_at`

func scanOrder(row pgx.Row, extra ...any) (domain.Order, error) {
	var o domain.Order
	dest := append([]any{
		&o.ID, &o.ExternalID, &o.Marketplace, &o.Status, &o.Payload, &o.ContentHash, &o.Version,
		&o.Attempts, &o.LastError, &o.PendingSince, &o.CreatedAt, &o.UpdatedAt,
	}, extra...)
	err := row.Scan(dest...)
	return o, err
}

// claimablePredicate selects orders that are due: new, retry-eligible, or
// held by a lease that expired (crashed or stalled worker).
const claimablePredicate = `
	(o.status IN ('pending', 'failed') AND o.next_attempt_at <= now())
	OR (o.status = 'syncing' AND o.lease_until < now() AND o.attempts < $1)`

// UpsertFreshOrders ingests ERP orders. Unchanged content is a no-op, which
// makes ERP redelivery idempotent. Changed content resets retries and makes
// the order pending again; an order mid-sync stays leased and is re-queued on
// completion because its content hash no longer matches.
func (s *Store) UpsertFreshOrders(ctx context.Context, fresh []domain.FreshOrder) (int, error) {
	if len(fresh) == 0 {
		return 0, nil
	}
	changed := 0
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		for _, fo := range fresh {
			batch.Queue(`
				INSERT INTO orders AS o (external_id, marketplace, status, payload, content_hash)
				VALUES ($1, $2, 'pending', $3, $4)
				ON CONFLICT (marketplace, external_id) DO UPDATE
				SET payload         = EXCLUDED.payload,
				    content_hash    = EXCLUDED.content_hash,
				    version         = o.version + 1,
				    status          = CASE WHEN o.status = 'syncing' THEN o.status ELSE 'pending' END,
				    attempts        = 0,
				    next_attempt_at = now(),
				    pending_since   = now(),
				    last_error      = NULL,
				    updated_at      = now()
				WHERE o.content_hash IS DISTINCT FROM EXCLUDED.content_hash`,
				fo.ExternalID, fo.Marketplace, fo.Payload, fo.ContentHash)
		}
		results := tx.SendBatch(ctx, batch)
		for _, fo := range fresh {
			tag, err := results.Exec()
			if err != nil {
				_ = results.Close()
				return fmt.Errorf("upsert order %s: %w", fo.ExternalID, err)
			}
			changed += int(tag.RowsAffected())
		}
		return results.Close()
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

// CountClaimable returns min(claimable, limit); the cap keeps the query cheap
// because batch sizing saturates long before large volumes.
func (s *Store) CountClaimable(ctx context.Context, maxAttempts, limit int) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT 1 FROM orders o WHERE `+claimablePredicate+` LIMIT $2
		) t`, maxAttempts, limit).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count claimable orders: %w", err)
	}
	return n, nil
}

// ClaimOrders leases up to limit due orders to workerID. SKIP LOCKED lets
// concurrent workers claim disjoint sets without blocking each other.
func (s *Store) ClaimOrders(ctx context.Context, workerID string, limit, maxAttempts int, lease time.Duration) ([]domain.ClaimedOrder, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT o.id FROM orders o
			WHERE `+claimablePredicate+`
			ORDER BY o.next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE orders o
		SET status      = 'syncing',
		    attempts    = o.attempts + 1,
		    lease_owner = $3,
		    lease_token = gen_random_uuid(),
		    lease_until = now() + $4 * interval '1 millisecond',
		    updated_at  = now()
		FROM due
		WHERE o.id = due.id
		RETURNING `+orderColumns+`, o.lease_token`,
		maxAttempts, limit, workerID, millis(lease))
	if err != nil {
		return nil, fmt.Errorf("claim orders: %w", err)
	}
	defer rows.Close()

	var out []domain.ClaimedOrder
	for rows.Next() {
		var token uuid.UUID
		o, err := scanOrder(rows, &token)
		if err != nil {
			return nil, fmt.Errorf("scan claimed order: %w", err)
		}
		out = append(out, domain.ClaimedOrder{Order: o, LeaseToken: token})
	}
	return out, rows.Err()
}

func (s *Store) LedgerRef(ctx context.Context, orderID uuid.UUID, method, contentHash string) (string, bool, error) {
	var ref string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(remote_ref, '') FROM sync_ledger
		WHERE order_id = $1 AND method = $2 AND content_hash = $3`,
		orderID, method, contentHash).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger lookup: %w", err)
	}
	return ref, true, nil
}

// CompleteOrder records a successful marketplace side effect. The ledger row
// is a fact about the remote system and is committed even if the lease was
// lost, so the next lease holder skips the remote call. Status and outbox are
// fenced by the lease token and committed atomically with the ledger.
func (s *Store) CompleteOrder(ctx context.Context, o domain.ClaimedOrder, method, remoteRef string) (domain.OrderStatus, error) {
	var status domain.OrderStatus
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync_ledger (order_id, marketplace, method, content_hash, remote_ref)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (order_id, marketplace, method, content_hash) DO NOTHING`,
			o.ID, o.Marketplace, method, o.ContentHash, remoteRef); err != nil {
			return fmt.Errorf("insert ledger: %w", err)
		}
		err := tx.QueryRow(ctx, `
			UPDATE orders
			SET status          = CASE WHEN content_hash = $3 THEN 'synced' ELSE 'pending' END,
			    attempts        = 0,
			    next_attempt_at = now(),
			    last_error      = NULL,
			    lease_owner     = NULL,
			    lease_token     = NULL,
			    lease_until     = NULL,
			    updated_at      = now()
			WHERE id = $1 AND lease_token = $2
			RETURNING status`, o.ID, o.LeaseToken, o.ContentHash).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			status = ""
			return nil
		}
		if err != nil {
			return fmt.Errorf("complete order: %w", err)
		}
		if status != domain.OrderStatusSynced {
			return nil
		}
		return insertOutbox(ctx, tx, domain.AggregateOrder, o.ID.String(), domain.EventOrderSynced, map[string]any{
			"order_id":     o.ID,
			"external_id":  o.ExternalID,
			"marketplace":  o.Marketplace,
			"version":      o.Version,
			"content_hash": o.ContentHash,
			"remote_ref":   remoteRef,
		})
	})
	if err != nil {
		return "", err
	}
	if status == "" {
		return "", domain.ErrLeaseLost
	}
	return status, nil
}

// FailOrder records a failed attempt. Once attempts reach maxAttempts the
// order is dead-lettered and an event is emitted in the same transaction.
func (s *Store) FailOrder(ctx context.Context, o domain.ClaimedOrder, reason string, maxAttempts int, retryIn time.Duration) (domain.OrderStatus, error) {
	var status domain.OrderStatus
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE orders
			SET status          = CASE WHEN attempts >= $3 THEN 'dead' ELSE 'failed' END,
			    next_attempt_at = now() + $4 * interval '1 millisecond',
			    last_error      = $5,
			    lease_owner     = NULL,
			    lease_token     = NULL,
			    lease_until     = NULL,
			    updated_at      = now()
			WHERE id = $1 AND lease_token = $2
			RETURNING status`, o.ID, o.LeaseToken, maxAttempts, millis(retryIn), truncate(reason, 500)).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("fail order: %w", err)
		}
		if status != domain.OrderStatusDead {
			return nil
		}
		return insertOutbox(ctx, tx, domain.AggregateOrder, o.ID.String(), domain.EventOrderDeadLettered, map[string]any{
			"order_id":    o.ID,
			"external_id": o.ExternalID,
			"marketplace": o.Marketplace,
			"attempts":    o.Attempts,
			"last_error":  truncate(reason, 500),
		})
	})
	if err != nil {
		return "", err
	}
	if status == "" {
		return "", domain.ErrLeaseLost
	}
	return status, nil
}

// ReleaseOrder hands a lease back without consuming an attempt; used when a
// worker shuts down mid-call so the order does not wait for lease expiry.
func (s *Store) ReleaseOrder(ctx context.Context, o domain.ClaimedOrder) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE orders
		SET status          = CASE WHEN attempts > 1 THEN 'failed' ELSE 'pending' END,
		    attempts        = GREATEST(attempts - 1, 0),
		    next_attempt_at = now(),
		    lease_owner     = NULL,
		    lease_token     = NULL,
		    lease_until     = NULL,
		    updated_at      = now()
		WHERE id = $1 AND lease_token = $2`, o.ID, o.LeaseToken)
	if err != nil {
		return fmt.Errorf("release order: %w", err)
	}
	return nil
}

// DeadLetterExpiredLeases handles orders whose lease expired on their final
// attempt (e.g. a worker crashed every time); they are no longer claimable.
func (s *Store) DeadLetterExpiredLeases(ctx context.Context, maxAttempts int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH dead AS (
			UPDATE orders
			SET status      = 'dead',
			    last_error  = 'lease expired on final attempt',
			    lease_owner = NULL,
			    lease_token = NULL,
			    lease_until = NULL,
			    updated_at  = now()
			WHERE status = 'syncing' AND lease_until < now() AND attempts >= $1
			RETURNING id, external_id, marketplace, attempts
		)
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		SELECT 'order', id::text, 'order.dead_lettered',
		       jsonb_build_object('order_id', id, 'external_id', external_id,
		                          'marketplace', marketplace, 'attempts', attempts,
		                          'last_error', 'lease expired on final attempt')
		FROM dead`, maxAttempts)
	if err != nil {
		return 0, fmt.Errorf("dead-letter expired leases: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) GetOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	o, err := scanOrder(s.pool.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders o WHERE o.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, ErrNotFound
	}
	if err != nil {
		return domain.Order{}, fmt.Errorf("get order: %w", err)
	}
	return o, nil
}

func (s *Store) GetOrderByExternalID(ctx context.Context, mp domain.Marketplace, externalID string) (domain.Order, error) {
	o, err := scanOrder(s.pool.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders o WHERE o.marketplace = $1 AND o.external_id = $2`, mp, externalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, ErrNotFound
	}
	if err != nil {
		return domain.Order{}, fmt.Errorf("get order by external id: %w", err)
	}
	return o, nil
}

// truncate cuts to at most n bytes without splitting a UTF-8 sequence,
// which Postgres would reject.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
