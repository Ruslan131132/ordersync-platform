package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type IdempotencyState int

const (
	// IdempotencyNew means the caller owns the key and must Complete or Abort it.
	IdempotencyNew IdempotencyState = iota
	// IdempotencyReplay means a previous request finished; ResultRef identifies its result.
	IdempotencyReplay
	// IdempotencyInProgress means another request holds the key right now.
	IdempotencyInProgress
	// IdempotencyMismatch means the key was reused with a different request body.
	IdempotencyMismatch
)

type IdempotencyResult struct {
	State     IdempotencyState
	ResultRef string
}

// BeginIdempotent reserves (scope, key) for a request identified by
// requestHash. Expired keys, and reservations abandoned for longer than
// staleAfter (a crashed request), can be taken over.
func (s *Store) BeginIdempotent(ctx context.Context, scope, key, requestHash string, ttl, staleAfter time.Duration) (IdempotencyResult, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_hash, expires_at)
		VALUES ($1, $2, $3, now() + $4 * interval '1 millisecond')
		ON CONFLICT (scope, key) DO NOTHING`, scope, key, requestHash, millis(ttl))
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("reserve idempotency key: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return IdempotencyResult{State: IdempotencyNew}, nil
	}

	tag, err = s.pool.Exec(ctx, `
		UPDATE idempotency_keys
		SET request_hash = $3, result_ref = NULL, created_at = now(),
		    expires_at = now() + $4 * interval '1 millisecond'
		WHERE scope = $1 AND key = $2
		  AND (expires_at < now()
		       OR (result_ref IS NULL AND request_hash = $3 AND created_at < now() - $5 * interval '1 millisecond'))`,
		scope, key, requestHash, millis(ttl), millis(staleAfter))
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("take over idempotency key: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return IdempotencyResult{State: IdempotencyNew}, nil
	}

	var storedHash string
	var ref *string
	err = s.pool.QueryRow(ctx, `
		SELECT request_hash, result_ref FROM idempotency_keys WHERE scope = $1 AND key = $2`,
		scope, key).Scan(&storedHash, &ref)
	if errors.Is(err, pgx.ErrNoRows) {
		// Deleted concurrently by Abort or cleanup; the client may retry.
		return IdempotencyResult{State: IdempotencyInProgress}, nil
	}
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("load idempotency key: %w", err)
	}
	switch {
	case storedHash != requestHash:
		return IdempotencyResult{State: IdempotencyMismatch}, nil
	case ref != nil:
		return IdempotencyResult{State: IdempotencyReplay, ResultRef: *ref}, nil
	default:
		return IdempotencyResult{State: IdempotencyInProgress}, nil
	}
}

func (s *Store) CompleteIdempotent(ctx context.Context, scope, key, resultRef string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE idempotency_keys SET result_ref = $3 WHERE scope = $1 AND key = $2`,
		scope, key, resultRef); err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	return nil
}

// AbortIdempotent frees a reservation whose request failed, so a retry with
// the same key is processed instead of being stuck "in progress".
func (s *Store) AbortIdempotent(ctx context.Context, scope, key string) error {
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND result_ref IS NULL`,
		scope, key); err != nil {
		return fmt.Errorf("abort idempotency key: %w", err)
	}
	return nil
}

func (s *Store) DeleteExpiredIdempotencyKeys(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
