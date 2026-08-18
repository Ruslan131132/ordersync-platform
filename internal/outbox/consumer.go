package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/metrics"
)

// Store drains unpublished events transactionally.
type Store interface {
	DrainOutbox(ctx context.Context, limit int, publish func(context.Context, []domain.OutboxEvent) error) (int, error)
}

// Publisher delivers a batch to the outside world. In production this would
// produce to Kafka (keyed by aggregate_id for per-order ordering); returning
// an error leaves the batch unpublished for the next poll.
type Publisher interface {
	Publish(ctx context.Context, events []domain.OutboxEvent) error
}

// LogPublisher stands in for a message broker.
type LogPublisher struct {
	Log *slog.Logger
}

func (p LogPublisher) Publish(ctx context.Context, events []domain.OutboxEvent) error {
	for _, e := range events {
		p.Log.DebugContext(ctx, "event published",
			"event_id", e.ID, "event_type", e.EventType, "aggregate_id", e.AggregateID)
	}
	return nil
}

// Relay polls the outbox. Several relays may run (one per worker replica);
// row locks with SKIP LOCKED keep them from publishing the same event.
type Relay struct {
	store     Store
	publisher Publisher
	log       *slog.Logger
	metrics   *metrics.Metrics
	interval  time.Duration
	batch     int
}

func NewRelay(st Store, pub Publisher, log *slog.Logger, m *metrics.Metrics, interval time.Duration, batch int) *Relay {
	return &Relay{store: st, publisher: pub, log: log, metrics: m, interval: interval, batch: batch}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		// Keep draining while full batches come back, so a burst is not
		// throttled to one batch per tick.
		for {
			n, err := r.DrainOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.log.Error("outbox drain failed", "err", err)
				}
				break
			}
			if n < r.batch {
				break
			}
		}
	}
}

func (r *Relay) DrainOnce(ctx context.Context) (int, error) {
	var published []domain.OutboxEvent
	n, err := r.store.DrainOutbox(ctx, r.batch, func(ctx context.Context, events []domain.OutboxEvent) error {
		if err := r.publisher.Publish(ctx, events); err != nil {
			return err
		}
		published = events
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, e := range published {
		r.metrics.OutboxPublished.WithLabelValues(e.EventType).Inc()
	}
	return n, nil
}
