package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNotAcquired = errors.New("lock not acquired")

// RedisLocker implements a simple SET NX PX distributed lock with Lua unlock.
// Chosen over Postgres advisory locks because:
//  1. Lock TTL + worker identity fit Redis naturally for crash recovery.
//  2. Keeps OLTP Postgres free of lock churn under multi-worker contention.
//  3. Matches common marketplace-integration ops patterns (Redis as coordination).
type RedisLocker struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewRedisLocker(client *redis.Client, prefix string, ttl time.Duration) *RedisLocker {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &RedisLocker{client: client, prefix: prefix, ttl: ttl}
}

type Handle struct {
	key   string
	token string
	l     *RedisLocker
}

func (l *RedisLocker) Acquire(ctx context.Context, name, token string) (*Handle, error) {
	key := l.prefix + name
	ok, err := l.client.SetNX(ctx, key, token, l.ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lock acquire %s: %w", name, err)
	}
	if !ok {
		return nil, ErrNotAcquired
	}
	return &Handle{key: key, token: token, l: l}, nil
}

func (l *RedisLocker) TryWithWait(ctx context.Context, name, token string, wait time.Duration) (*Handle, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(wait)
	for {
		h, err := l.Acquire(ctx, name, token)
		if err == nil {
			return h, time.Since(start), nil
		}
		if !errors.Is(err, ErrNotAcquired) {
			return nil, time.Since(start), err
		}
		if time.Now().After(deadline) {
			return nil, time.Since(start), ErrNotAcquired
		}
		select {
		case <-ctx.Done():
			return nil, time.Since(start), ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (h *Handle) Release(ctx context.Context) error {
	_, err := unlockScript.Run(ctx, h.l.client, []string{h.key}, h.token).Result()
	if err != nil {
		return fmt.Errorf("redis lock release: %w", err)
	}
	return nil
}

func (h *Handle) Extend(ctx context.Context) error {
	ok, err := h.l.client.Get(ctx, h.key).Result()
	if err != nil {
		return fmt.Errorf("redis lock extend get: %w", err)
	}
	if ok != h.token {
		return ErrNotAcquired
	}
	if err := h.l.client.PExpire(ctx, h.key, h.l.ttl).Err(); err != nil {
		return fmt.Errorf("redis lock extend: %w", err)
	}
	return nil
}
