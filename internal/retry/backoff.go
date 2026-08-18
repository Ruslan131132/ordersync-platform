package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Policy configures exponential backoff with full jitter.
type Policy struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
	// OnRetry is called before sleeping between attempts.
	OnRetry func(attempt int, err error, delay time.Duration)
}

type retryableError struct {
	err   error
	after time.Duration
}

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

// Retryable marks err as transient. Unmarked errors are never retried, so a
// bug or a 4xx response fails fast instead of burning the rate limit.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err}
}

// RetryableAfter marks err as transient with a server-provided minimum delay.
func RetryableAfter(err error, after time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err, after: after}
}

func IsRetryable(err error) bool {
	var r *retryableError
	return errors.As(err, &r)
}

// Do runs fn until it succeeds, returns a non-retryable error, attempts are
// exhausted, or ctx is done.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context, attempt int) error) error {
	attempts := max(p.Attempts, 1)
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(ctxErr, err)
		}
		err = fn(ctx, attempt)
		if err == nil || attempt == attempts || !IsRetryable(err) {
			return err
		}
		delay := Backoff(p.Base, p.Max, attempt)
		var r *retryableError
		if errors.As(err, &r) && r.after > delay {
			delay = r.after
		}
		if p.OnRetry != nil {
			p.OnRetry(attempt, err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
	return err
}

// Backoff returns a full-jitter delay in [0, min(max, base*2^(attempt-1))).
func Backoff(base, maxDelay time.Duration, attempt int) time.Duration {
	if base <= 0 || attempt < 1 {
		return 0
	}
	ceiling := base
	for i := 1; i < attempt && ceiling < maxDelay; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, maxDelay)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceiling))) //nolint:gosec // jitter does not need crypto randomness
}
