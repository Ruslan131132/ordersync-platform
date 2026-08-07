package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/store"
)

// Consumer polls the transactional outbox and "publishes" events in-process.
// In production this would write to Kafka / Redis Streams / SNS; the outbox
// table remains the source of truth so publishing can be retried safely.
type Consumer struct {
	store    *store.Store
	log      *slog.Logger
	metrics  *metrics.Metrics
	interval time.Duration
	batch    int
	handler  func(ctx context.Context, eventType string, payload []byte) error
}

func NewConsumer(st *store.Store, log *slog.Logger, m *metrics.Metrics, interval time.Duration, batch int) *Consumer {
	c := &Consumer{
		store:    st,
		log:      log,
		metrics:  m,
		interval: interval,
		batch:    batch,
	}
	c.handler = c.defaultHandler
	return c
}

func (c *Consumer) SetHandler(h func(ctx context.Context, eventType string, payload []byte) error) {
	c.handler = h
}

func (c *Consumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.drain(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("outbox drain failed", "err", err)
			}
		}
	}
}

func (c *Consumer) drain(ctx context.Context) error {
	events, err := c.store.FetchUnpublishedOutbox(ctx, c.batch)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(events))
	for _, ev := range events {
		if err := c.handler(ctx, ev.EventType, ev.Payload); err != nil {
			return err
		}
		ids = append(ids, ev.ID)
		c.metrics.OutboxPublished.Inc()
	}
	return c.store.MarkOutboxPublished(ctx, ids)
}

func (c *Consumer) defaultHandler(_ context.Context, eventType string, payload []byte) error {
	c.log.Info("outbox published", "event_type", eventType, "payload_bytes", len(payload))
	return nil
}
