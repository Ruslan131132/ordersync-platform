package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Do retries fn with exponential backoff and full jitter.
func Do(ctx context.Context, attempts int, base, max time.Duration, fn func(attempt int) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = fn(attempt)
		if last == nil {
			return nil
		}
		if attempt == attempts || !IsRetryable(last) {
			return last
		}
		delay := jitter(base, max, attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func IsRetryable(err error) bool {
	var r interface{ Retryable() bool }
	if errors.As(err, &r) {
		return r.Retryable()
	}
	return true
}

type TemporaryError struct {
	Err error
}

func (e TemporaryError) Error() string { return e.Err.Error() }
func (e TemporaryError) Unwrap() error { return e.Err }
func (e TemporaryError) Retryable() bool {
	return true
}

func jitter(base, max time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) {
		exp = float64(max)
	}
	if exp <= 0 {
		return 0
	}
	return time.Duration(rand.Float64() * exp) //nolint:gosec // jitter only
}
