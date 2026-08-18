package lock_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ordersync/platform/internal/lock"
)

func newLocker(t *testing.T, ttl time.Duration) (*lock.RedisLocker, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return lock.NewRedisLocker(rdb, "test:", ttl), mr
}

func TestLockIsExclusiveAndOwnerScoped(t *testing.T) {
	t.Parallel()
	locker, _ := newLocker(t, time.Second)
	ctx := context.Background()

	h1, err := locker.Acquire(ctx, "job", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := locker.Acquire(ctx, "job", "w2"); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	h2, err := locker.Acquire(ctx, "job", "w2")
	if err != nil {
		t.Fatal(err)
	}
	// A stale handle must not release someone else's lock.
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := locker.Acquire(ctx, "job", "w3"); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatal("stale release removed the current owner's lock")
	}
	_ = h2.Release(ctx)
}

func TestExtendFailsAfterExpiry(t *testing.T) {
	t.Parallel()
	locker, mr := newLocker(t, time.Second)
	ctx := context.Background()

	h, err := locker.Acquire(ctx, "job", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Extend(ctx); err != nil {
		t.Fatalf("extend while owned: %v", err)
	}
	mr.FastForward(2 * time.Second)
	if _, err := locker.Acquire(ctx, "job", "w2"); err != nil {
		t.Fatalf("lock should be free after TTL: %v", err)
	}
	if err := h.Extend(ctx); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired after takeover, got %v", err)
	}
}

func TestLeaderFailover(t *testing.T) {
	t.Parallel()
	locker, _ := newLocker(t, 300*time.Millisecond)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	var active atomic.Int32
	var maxActive atomic.Int32
	var elected atomic.Int32
	lead := func(ctx context.Context) error {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		elected.Add(1)
		<-ctx.Done()
		return nil
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() {
		_ = lock.NewLeader(locker, "leader", "a", log, nil).Run(ctxA, lead)
		close(doneA)
	}()
	waitFor(t, func() bool { return elected.Load() == 1 })

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go func() { _ = lock.NewLeader(locker, "leader", "b", log, nil).Run(ctxB, lead) }()

	time.Sleep(250 * time.Millisecond)
	if elected.Load() != 1 {
		t.Fatal("second candidate must not lead while the first holds the lock")
	}

	cancelA()
	<-doneA
	waitFor(t, func() bool { return elected.Load() == 2 })
	if maxActive.Load() != 1 {
		t.Fatalf("leaders overlapped: max active %d", maxActive.Load())
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
