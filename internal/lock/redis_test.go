package lock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ordersync/platform/internal/lock"
	"github.com/redis/go-redis/v9"
)

func TestRedisLockExclusive(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	locker := lock.NewRedisLocker(rdb, "test:", time.Second)
	ctx := context.Background()

	h1, err := locker.Acquire(ctx, "job", "w1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = locker.Acquire(ctx, "job", "w2")
	if !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	h2, err := locker.Acquire(ctx, "job", "w2")
	if err != nil {
		t.Fatal(err)
	}
	_ = h2.Release(ctx)
}
