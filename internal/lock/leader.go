package lock

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Observer receives leadership telemetry; nil-safe via noopObserver.
type Observer interface {
	LeadershipChanged(leader bool)
	AcquireAttempt(d time.Duration, err error)
}

type noopObserver struct{}

func (noopObserver) LeadershipChanged(bool)              {}
func (noopObserver) AcquireAttempt(time.Duration, error) {}

// Leader runs a function only while this process holds the named lock.
type Leader struct {
	locker   *RedisLocker
	name     string
	id       string
	log      *slog.Logger
	observer Observer
}

func NewLeader(locker *RedisLocker, name, id string, log *slog.Logger, obs Observer) *Leader {
	if obs == nil {
		obs = noopObserver{}
	}
	return &Leader{locker: locker, name: name, id: id, log: log, observer: obs}
}

// Run campaigns until ctx is done. While elected, lead runs with a context
// that is cancelled as soon as renewal fails, so a deposed leader stops
// promptly instead of acting on stale ownership.
func (l *Leader) Run(ctx context.Context, lead func(ctx context.Context) error) error {
	interval := l.locker.TTL() / 3
	for {
		start := time.Now()
		h, err := l.locker.Acquire(ctx, l.name, l.id)
		l.observer.AcquireAttempt(time.Since(start), err)
		switch {
		case err == nil:
			l.serve(ctx, h, interval, lead)
		case !errors.Is(err, ErrNotAcquired) && ctx.Err() == nil:
			l.log.Warn("leader campaign failed", "lock", l.name, "err", err)
		}
		if !sleep(ctx, interval) {
			return nil
		}
	}
}

func (l *Leader) serve(ctx context.Context, h *Handle, interval time.Duration, lead func(context.Context) error) {
	l.log.Info("acquired leadership", "lock", l.name, "id", l.id)
	l.observer.LeadershipChanged(true)
	defer l.observer.LeadershipChanged(false)

	leadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- lead(leadCtx) }()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil && leadCtx.Err() == nil {
				l.log.Error("leader duties failed", "lock", l.name, "err", err)
			}
			l.release(h)
			return
		case <-ticker.C:
			if err := h.Extend(ctx); err != nil {
				l.log.Warn("lost leadership", "lock", l.name, "err", err)
				cancel()
				<-done
				return
			}
		}
	}
}

func (l *Leader) release(h *Handle) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.Release(ctx); err != nil {
		l.log.Warn("leader release failed", "lock", l.name, "err", err)
	}
	l.log.Info("released leadership", "lock", l.name, "id", l.id)
}

// sleep reports false if ctx ended before d elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
