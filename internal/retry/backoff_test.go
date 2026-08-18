package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoSucceedsAfterRetries(t *testing.T) {
	t.Parallel()
	calls := 0
	retries := 0
	err := Do(context.Background(), Policy{
		Attempts: 4, Base: time.Millisecond, Max: 5 * time.Millisecond,
		OnRetry: func(int, error, time.Duration) { retries++ },
	}, func(context.Context, int) error {
		calls++
		if calls < 3 {
			return Retryable(errors.New("flaky"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || retries != 2 {
		t.Fatalf("calls=%d retries=%d", calls, retries)
	}
}

func TestDoDoesNotRetryUnmarkedErrors(t *testing.T) {
	t.Parallel()
	calls := 0
	permanent := errors.New("bad request")
	err := Do(context.Background(), Policy{Attempts: 5, Base: time.Millisecond, Max: time.Millisecond},
		func(context.Context, int) error {
			calls++
			return permanent
		})
	if !errors.Is(err, permanent) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDoHonoursRetryAfter(t *testing.T) {
	t.Parallel()
	var delays []time.Duration
	_ = Do(context.Background(), Policy{
		Attempts: 2, Base: time.Microsecond, Max: time.Microsecond,
		OnRetry: func(_ int, _ error, d time.Duration) { delays = append(delays, d) },
	}, func(context.Context, int) error {
		return RetryableAfter(errors.New("429"), 20*time.Millisecond)
	})
	if len(delays) != 1 || delays[0] != 20*time.Millisecond {
		t.Fatalf("delays=%v", delays)
	}
}

func TestDoStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, Policy{Attempts: 5, Base: time.Second, Max: time.Second},
		func(context.Context, int) error { return Retryable(errors.New("x")) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestBackoffIsBounded(t *testing.T) {
	t.Parallel()
	for attempt := 1; attempt <= 40; attempt++ {
		if d := Backoff(time.Second, 10*time.Second, attempt); d < 0 || d >= 10*time.Second {
			t.Fatalf("attempt %d: delay %s out of bounds", attempt, d)
		}
	}
}
