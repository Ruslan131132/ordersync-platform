// Command loadgen seeds N orders through the public API, triggers a sync and
// waits until every one of them is terminal (synced or dead-lettered). It
// prints wall-clock throughput and the mock marketplace's duplicate-write
// count, which must stay at zero for the idempotency guarantees to hold.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type options struct {
	addr     string
	apiKey   string
	orders   int
	chunk    int
	timeout  time.Duration
	interval time.Duration
}

type apiStats struct {
	Orders          map[string]int `json:"orders"`
	OutboxBacklog   int            `json:"outbox_backlog"`
	OutboxPublished int            `json:"outbox_published"`
}

func (s apiStats) terminal() int { return s.Orders["synced"] + s.Orders["dead"] }

type mockStats struct {
	UniqueWrites    int `json:"unique_writes"`
	DuplicateWrites int `json:"duplicate_writes"`
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", "http://localhost:8080", "API base URL")
	flag.StringVar(&o.apiKey, "api-key", envOr("API_KEY", "dev-local-key"), "API key (env API_KEY)")
	flag.IntVar(&o.orders, "orders", 2000, "number of orders to seed")
	flag.IntVar(&o.chunk, "chunk", 1000, "orders per seed request")
	flag.DurationVar(&o.timeout, "timeout", 15*time.Minute, "give up after this long")
	flag.DurationVar(&o.interval, "poll", 250*time.Millisecond, "stats poll interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	if o.orders < 1 || o.chunk < 1 {
		return errors.New("-orders and -chunk must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	c := &client{base: o.addr, key: o.apiKey, http: &http.Client{Timeout: 30 * time.Second}}

	var before apiStats
	if err := c.get(ctx, "/v1/stats", &before); err != nil {
		return err
	}
	var mockBefore mockStats
	if err := c.get(ctx, "/mock/marketplaces/stats", &mockBefore); err != nil {
		return fmt.Errorf("mock marketplace stats (is MOCK_MARKET_ENABLED=true?): %w", err)
	}

	start := time.Now()
	for left := o.orders; left > 0; left -= o.chunk {
		n := min(left, o.chunk)
		if err := c.post(ctx, "/v1/demo/seed", map[string]int{"count": n}); err != nil {
			return err
		}
	}
	seeded := time.Since(start)
	if err := c.post(ctx, "/v1/sync/trigger", map[string]any{}); err != nil {
		return err
	}
	fmt.Printf("seeded %d orders in %s; waiting for sync...\n", o.orders, seeded.Round(time.Millisecond))

	var now apiStats
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	lastReport := time.Now()
	for {
		if err := c.get(ctx, "/v1/stats", &now); err != nil {
			return err
		}
		done := now.terminal() - before.terminal()
		if done >= o.orders {
			break
		}
		if time.Since(lastReport) >= 5*time.Second {
			fmt.Printf("  %d/%d terminal (%.0f orders/s)\n", done, o.orders, float64(done)/time.Since(start).Seconds())
			lastReport = time.Now()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("stopped with %d/%d orders terminal: %w", done, o.orders, ctx.Err())
		case <-ticker.C:
		}
	}
	elapsed := time.Since(start)

	var mockAfter mockStats
	if err := c.get(ctx, "/mock/marketplaces/stats", &mockAfter); err != nil {
		return err
	}
	synced := now.Orders["synced"] - before.Orders["synced"]
	dead := now.Orders["dead"] - before.Orders["dead"]
	fmt.Printf(`
orders          %d
elapsed         %s (seed %s)
throughput      %.1f orders/s
synced          %d
dead-lettered   %d
remote writes   %d unique, %d duplicate
outbox backlog  %d
`,
		o.orders, elapsed.Round(time.Millisecond), seeded.Round(time.Millisecond),
		float64(o.orders)/elapsed.Seconds(), synced, dead,
		mockAfter.UniqueWrites-mockBefore.UniqueWrites, mockAfter.DuplicateWrites-mockBefore.DuplicateWrites,
		now.OutboxBacklog)
	if mockAfter.DuplicateWrites != mockBefore.DuplicateWrites {
		return errors.New("marketplace observed duplicate writes")
	}
	return nil
}

type client struct {
	base string
	key  string
	http *http.Client
}

func (c *client) get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

func (c *client) post(ctx context.Context, path string, body any) error {
	return c.do(ctx, http.MethodPost, path, body, nil)
}

func (c *client) do(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
