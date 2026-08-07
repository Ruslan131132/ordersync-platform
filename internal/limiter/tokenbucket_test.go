package limiter

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucketAllowsBurstThenLimits(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(10, 3)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := tb.Wait(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("burst should be immediate, took %s", time.Since(start))
	}

	start = time.Now()
	if err := tb.Wait(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("expected rate limit wait, took %s", time.Since(start))
	}
}

func TestTokenBucketCancel(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(0.1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	_ = tb.Wait(ctx, "x") // take the only token
	cancel()
	err := tb.Wait(ctx, "x")
	if err == nil {
		t.Fatal("expected context error")
	}
}
