package limiter

import (
	"context"
	"sync"
	"time"
)

// TokenBucket is an in-process token-bucket limiter keyed by method/token.
// For multi-instance fairness Redis would hold the shared state; here each
// worker process owns its local allowance, matching the mock's per-token model.
type TokenBucket struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewTokenBucket(rps float64, burst int) *TokenBucket {
	if rps <= 0 {
		rps = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &TokenBucket{
		buckets: make(map[string]*bucket),
		rate:    rps,
		burst:   float64(burst),
	}
}

func (t *TokenBucket) Wait(ctx context.Context, key string) error {
	for {
		wait, ok := t.tryTake(key)
		if ok {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (t *TokenBucket) tryTake(key string) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	b, ok := t.buckets[key]
	if !ok {
		t.buckets[key] = &bucket{tokens: t.burst - 1, last: now}
		return 0, true
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(t.burst, b.tokens+elapsed*t.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	need := (1 - b.tokens) / t.rate
	return time.Duration(need * float64(time.Second)), false
}
