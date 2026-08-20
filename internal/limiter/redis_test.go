package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisLimiterSharesBudgetAcrossInstances(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	workerA := NewRedisLimiter(rdb, "rl:", 10, 2)
	workerB := NewRedisLimiter(rdb, "rl:", 10, 2)
	workerA.now, workerB.now = clock.now, clock.now
	ctx := context.Background()

	for _, l := range []*RedisLimiter{workerA, workerB} {
		wait, err := l.reserve(ctx, "wb:token:orders.upsert")
		if err != nil {
			t.Fatal(err)
		}
		if wait != 0 {
			t.Fatalf("burst token denied, wait=%s", wait)
		}
	}
	wait, err := workerA.reserve(ctx, "wb:token:orders.upsert")
	if err != nil {
		t.Fatal(err)
	}
	if wait != 100*time.Millisecond {
		t.Fatalf("replicas must share the bucket: expected 100ms wait, got %s", wait)
	}

	clock.advance(100 * time.Millisecond)
	wait, err = workerB.reserve(ctx, "wb:token:orders.upsert")
	if err != nil {
		t.Fatal(err)
	}
	if wait != 0 {
		t.Fatalf("token should refill, wait=%s", wait)
	}
}
