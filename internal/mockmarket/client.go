package mockmarket

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/retry"
)

type Client struct {
	name     domain.Marketplace
	token    string
	limiter  *limiter.TokenBucket
	failPct  float64
	rng      *rand.Rand
	mu       sync.Mutex
	metrics  *metrics.Metrics
	latency  time.Duration
}

func New(name domain.Marketplace, token string, rps float64, failPct float64, m *metrics.Metrics) *Client {
	burst := int(rps*2) + 1
	return &Client{
		name:    name,
		token:   token,
		limiter: limiter.NewTokenBucket(rps, burst),
		failPct: failPct,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
		metrics: m,
		latency: 20 * time.Millisecond,
	}
}

type UpsertResult struct {
	RemoteRef string
}

func (c *Client) UpsertOrder(ctx context.Context, externalID string, payload []byte) (UpsertResult, error) {
	method := "orders.upsert"
	key := fmt.Sprintf("%s:%s:%s", c.name, c.token, method)

	var result UpsertResult
	err := retry.Do(ctx, 4, 50*time.Millisecond, 800*time.Millisecond, func(attempt int) error {
		if attempt > 1 && c.metrics != nil {
			c.metrics.Retries.WithLabelValues("marketplace_api").Inc()
		}
		if err := c.limiter.Wait(ctx, key); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.latency):
		}

		c.mu.Lock()
		fail := c.rng.Float64() < c.failPct
		c.mu.Unlock()
		if fail {
			if c.metrics != nil {
				c.metrics.APICalls.WithLabelValues(string(c.name), method, "error").Inc()
			}
			return retry.TemporaryError{Err: fmt.Errorf("%s temporary failure for %s", c.name, externalID)}
		}
		if c.metrics != nil {
			c.metrics.APICalls.WithLabelValues(string(c.name), method, "ok").Inc()
		}
		result = UpsertResult{RemoteRef: fmt.Sprintf("%s-%s", c.name, externalID)}
		return nil
	})
	return result, err
}

// Registry routes to marketplace-specific clients.
type Registry struct {
	clients map[domain.Marketplace]*Client
}

func NewRegistry(wb, ozon *Client) *Registry {
	return &Registry{clients: map[domain.Marketplace]*Client{
		domain.MarketplaceWBLike:   wb,
		domain.MarketplaceOzonLike: ozon,
	}}
}

func (r *Registry) For(mp domain.Marketplace) (*Client, error) {
	c, ok := r.clients[mp]
	if !ok {
		return nil, fmt.Errorf("unknown marketplace %q", mp)
	}
	return c, nil
}
