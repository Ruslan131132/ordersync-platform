package limiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestTokenBucketBurstThenRefill(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{t: time.Unix(0, 0)}
	tb := NewTokenBucket(10, 3)
	tb.now = clock.now

	for i := range 3 {
		if ok, _ := tb.Allow("k"); !ok {
			t.Fatalf("burst token %d denied", i)
		}
	}
	ok, wait := tb.Allow("k")
	if ok || wait != 100*time.Millisecond {
		t.Fatalf("expected denial with 100ms wait, got ok=%v wait=%s", ok, wait)
	}
	clock.advance(100 * time.Millisecond)
	if ok, _ := tb.Allow("k"); !ok {
		t.Fatal("token should refill after 100ms at 10 rps")
	}
}

func TestTokenBucketKeysAreIndependent(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{t: time.Unix(0, 0)}
	tb := NewTokenBucket(1, 1)
	tb.now = clock.now
	if ok, _ := tb.Allow("wb:orders.upsert"); !ok {
		t.Fatal("first key denied")
	}
	if ok, _ := tb.Allow("ozon:orders.upsert"); !ok {
		t.Fatal("second key should have its own bucket")
	}
}

func TestTokenBucketWaitHonoursContext(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(0.01, 1)
	ctx, cancel := context.WithCancel(context.Background())
	if err := tb.Wait(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := tb.Wait(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func BenchmarkTokenBucketAllow(b *testing.B) {
	tb := NewTokenBucket(1e9, 1e6)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow("bench")
		}
	})
}
