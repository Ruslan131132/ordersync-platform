// Package marketplace is the HTTP client for marketplace-like order APIs.
// The wire contract is intentionally generic; it does not mirror any real
// vendor API.
package marketplace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ordersync/platform/internal/domain"
	"github.com/ordersync/platform/internal/limiter"
	"github.com/ordersync/platform/internal/metrics"
	"github.com/ordersync/platform/internal/retry"
)

const (
	MethodUpsertOrder = "orders.upsert"

	HeaderIdempotencyKey   = "Idempotency-Key"
	HeaderRetryAfterMillis = "Retry-After-Ms"
	HeaderReplayed         = "Idempotent-Replayed"
)

type UpsertOrderRequest struct {
	ExternalID string          `json:"external_id"`
	Payload    json.RawMessage `json:"payload"`
}

type UpsertOrderResponse struct {
	RemoteRef string `json:"remote_ref"`
}

type Upsert struct {
	ExternalID     string
	Payload        json.RawMessage
	IdempotencyKey string
}

type Client struct {
	marketplace domain.Marketplace
	baseURL     string
	token       string
	http        *http.Client
	limiter     limiter.Limiter
	policy      retry.Policy
	metrics     *metrics.Metrics
}

type ClientConfig struct {
	Marketplace domain.Marketplace
	BaseURL     string
	Token       string
	HTTPClient  *http.Client
	Limiter     limiter.Limiter
	Retry       retry.Policy
	Metrics     *metrics.Metrics
}

func NewClient(cfg ClientConfig) *Client {
	c := &Client{
		marketplace: cfg.Marketplace,
		baseURL:     cfg.BaseURL,
		token:       cfg.Token,
		http:        cfg.HTTPClient,
		limiter:     cfg.Limiter,
		policy:      cfg.Retry,
		metrics:     cfg.Metrics,
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 10 * time.Second}
	}
	onRetry := c.policy.OnRetry
	c.policy.OnRetry = func(attempt int, err error, delay time.Duration) {
		c.metrics.Retries.WithLabelValues("marketplace_api", retryReason(err)).Inc()
		if onRetry != nil {
			onRetry(attempt, err, delay)
		}
	}
	return c
}

// StatusError is a non-2xx response from the marketplace.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("marketplace responded %d: %s", e.Code, e.Body)
}

// UpsertOrder creates or updates an order. The idempotency key makes retries
// safe even when a previous attempt was applied but its response was lost.
func (c *Client) UpsertOrder(ctx context.Context, req Upsert) (string, error) {
	body, err := json.Marshal(UpsertOrderRequest{ExternalID: req.ExternalID, Payload: req.Payload})
	if err != nil {
		return "", fmt.Errorf("marshal upsert: %w", err)
	}
	url := fmt.Sprintf("%s/%s/v1/orders", c.baseURL, c.marketplace)
	limitKey := fmt.Sprintf("%s:%s:%s", c.marketplace, tokenID(c.token), MethodUpsertOrder)

	var out UpsertOrderResponse
	err = retry.Do(ctx, c.policy, func(ctx context.Context, _ int) error {
		waitStart := time.Now()
		if err := c.limiter.Wait(ctx, limitKey); err != nil {
			return err
		}
		c.metrics.RateLimitSeconds.WithLabelValues(string(c.marketplace), MethodUpsertOrder).
			Observe(time.Since(waitStart).Seconds())
		return c.do(ctx, url, req.IdempotencyKey, body, &out)
	})
	if err != nil {
		return "", fmt.Errorf("%s %s %s: %w", c.marketplace, MethodUpsertOrder, req.ExternalID, err)
	}
	return out.RemoteRef, nil
}

func (c *Client) do(ctx context.Context, url, idempotencyKey string, body []byte, out any) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set(HeaderIdempotencyKey, idempotencyKey)

	start := time.Now()
	resp, err := c.http.Do(httpReq)
	c.metrics.APICallSeconds.WithLabelValues(string(c.marketplace), MethodUpsertOrder).
		Observe(time.Since(start).Seconds())
	if err != nil {
		c.observe("network_error")
		if ctx.Err() != nil {
			return err
		}
		return retry.Retryable(fmt.Errorf("transport: %w", err))
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if resp.Header.Get(HeaderReplayed) == "true" {
			c.observe("replayed")
		} else {
			c.observe("ok")
		}
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		return nil
	case resp.StatusCode == http.StatusTooManyRequests:
		c.observe("rate_limited")
		return retry.RetryableAfter(&StatusError{Code: resp.StatusCode, Body: string(payload)}, retryAfter(resp.Header))
	case resp.StatusCode >= 500:
		c.observe("server_error")
		return retry.Retryable(&StatusError{Code: resp.StatusCode, Body: string(payload)})
	default:
		c.observe("client_error")
		return &StatusError{Code: resp.StatusCode, Body: string(payload)}
	}
}

func (c *Client) observe(result string) {
	c.metrics.APICalls.WithLabelValues(string(c.marketplace), MethodUpsertOrder, result).Inc()
}

func retryAfter(h http.Header) time.Duration {
	if ms, err := strconv.Atoi(h.Get(HeaderRetryAfterMillis)); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	if s, err := strconv.Atoi(h.Get("Retry-After")); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

func retryReason(err error) string {
	var se *StatusError
	if errors.As(err, &se) {
		if se.Code == http.StatusTooManyRequests {
			return "rate_limited"
		}
		return "server_error"
	}
	return "network_error"
}

// tokenID keeps tokens out of Redis keys and metrics while staying stable.
func tokenID(token string) string {
	if len(token) <= 4 {
		return "tok"
	}
	return "tok-" + token[len(token)-4:]
}

// Router dispatches to the client for each marketplace.
type Router struct {
	clients map[domain.Marketplace]*Client
}

func NewRouter(clients ...*Client) *Router {
	r := &Router{clients: make(map[domain.Marketplace]*Client, len(clients))}
	for _, c := range clients {
		r.clients[c.marketplace] = c
	}
	return r
}

func (r *Router) UpsertOrder(ctx context.Context, mp domain.Marketplace, req Upsert) (string, error) {
	c, ok := r.clients[mp]
	if !ok {
		return "", fmt.Errorf("no client configured for marketplace %q", mp)
	}
	return c.UpsertOrder(ctx, req)
}
