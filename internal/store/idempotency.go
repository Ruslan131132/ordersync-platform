package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ReserveIdempotency inserts a key if absent. Returns reserved=true on first insert.
// If the key already exists with a result_ref, existingRef is returned for replay.
func (s *Store) ReserveIdempotency(ctx context.Context, scope, key string, ttl time.Duration) (reserved bool, existingRef string, err error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_keys (key, scope, expires_at)
		VALUES ($1, $2, now() + make_interval(secs => $3))
		ON CONFLICT (scope, key) DO NOTHING
	`, key, scope, int(ttl.Seconds()))
	if err != nil {
		return false, "", fmt.Errorf("reserve idempotency: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, "", nil
	}
	var ref *string
	err = s.pool.QueryRow(ctx, `
		SELECT result_ref FROM idempotency_keys WHERE scope = $1 AND key = $2
	`, scope, key).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("lookup idempotency: %w", err)
	}
	if ref != nil {
		return false, *ref, nil
	}
	return false, "", nil
}

func (s *Store) CompleteIdempotency(ctx context.Context, scope, key, resultRef string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE idempotency_keys SET result_ref = $3 WHERE scope = $1 AND key = $2
	`, scope, key, resultRef)
	if err != nil {
		return fmt.Errorf("complete idempotency: %w", err)
	}
	return nil
}
