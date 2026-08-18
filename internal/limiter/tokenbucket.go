package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

// Limiter blocks until a request under key may proceed.
type Limiter interface {
	Wait(ctx context.Context, key string) error
}

// TokenBucket is an in-process limiter keyed by an arbitrary string. The mock
// marketplace uses it server-side; workers use RedisLimiter so replicas share
// one budget per token.
type TokenBucket struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewTokenBucket(rps float64, burst int) *TokenBucket {
	return &TokenBucket{
		buckets: make(map[string]*bucket),
		rate:    math.Max(rps, 0.001),
		burst:   float64(max(burst, 1)),
		now:     time.Now,
	}
}

// Allow takes a token if available; otherwise it reports how long until one is.
func (t *TokenBucket) Allow(key string) (bool, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	b, ok := t.buckets[key]
	if !ok {
		b = &bucket{tokens: t.burst, last: now}
		t.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(t.burst, b.tokens+elapsed*t.rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / t.rate * float64(time.Second))
}

func (t *TokenBucket) Wait(ctx context.Context, key string) error {
	for {
		ok, wait := t.Allow(key)
		if ok {
			return nil
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
