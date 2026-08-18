package limiter

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// The bucket state lives in one hash so every replica draws from the same
// budget. The caller supplies the clock; replicas are assumed to be roughly
// NTP-synced, and the mock server tolerates small overshoot with 429s.
var tokenBucketScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local state = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
if now > ts then
  tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
  ts = now
end
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait = math.ceil((1 - tokens) * 1000 / rate)
end
redis.call("HSET", KEYS[1], "tokens", tostring(tokens), "ts", tostring(ts))
redis.call("PEXPIRE", KEYS[1], math.ceil(burst * 1000 / rate) + 1000)
return wait
`)

// RedisLimiter is a token bucket shared by all processes using the same Redis.
type RedisLimiter struct {
	client *redis.Client
	prefix string
	rate   float64
	burst  int
	now    func() time.Time
}

func NewRedisLimiter(client *redis.Client, prefix string, rps float64, burst int) *RedisLimiter {
	return &RedisLimiter{
		client: client,
		prefix: prefix,
		rate:   math.Max(rps, 0.001),
		burst:  max(burst, 1),
		now:    time.Now,
	}
}

func (l *RedisLimiter) Wait(ctx context.Context, key string) error {
	for {
		wait, err := l.reserve(ctx, key)
		if err != nil {
			return err
		}
		if wait == 0 {
			return nil
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func (l *RedisLimiter) reserve(ctx context.Context, key string) (time.Duration, error) {
	ms, err := tokenBucketScript.Run(ctx, l.client, []string{l.prefix + key},
		l.rate, l.burst, l.now().UnixMilli()).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis rate limiter %s: %w", key, err)
	}
	return time.Duration(ms) * time.Millisecond, nil
}
