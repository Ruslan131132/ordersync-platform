package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNotAcquired = errors.New("lock not acquired")

// RedisLocker is a single-instance Redis lease lock (SET NX PX + owner token).
// It is used for leader election, where a brief overlap after a network
// partition is harmless because all leader duties are idempotent SQL.
// Per-order exclusivity does not rely on it: Postgres leases fence that.
type RedisLocker struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewRedisLocker(client *redis.Client, prefix string, ttl time.Duration) *RedisLocker {
	return &RedisLocker{client: client, prefix: prefix, ttl: ttl}
}

func (l *RedisLocker) TTL() time.Duration { return l.ttl }

type Handle struct {
	key   string
	token string
	l     *RedisLocker
}

func (l *RedisLocker) Acquire(ctx context.Context, name, token string) (*Handle, error) {
	key := l.prefix + name
	ok, err := l.client.SetNX(ctx, key, token, l.ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire lock %s: %w", name, err)
	}
	if !ok {
		return nil, ErrNotAcquired
	}
	return &Handle{key: key, token: token, l: l}, nil
}

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

var extendScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// Release deletes the lock only if this handle still owns it.
func (h *Handle) Release(ctx context.Context) error {
	if err := releaseScript.Run(ctx, h.l.client, []string{h.key}, h.token).Err(); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	return nil
}

// Extend renews the TTL atomically; ErrNotAcquired means ownership was lost.
func (h *Handle) Extend(ctx context.Context) error {
	n, err := extendScript.Run(ctx, h.l.client, []string{h.key}, h.token, h.l.ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("extend lock: %w", err)
	}
	if n == 0 {
		return ErrNotAcquired
	}
	return nil
}
