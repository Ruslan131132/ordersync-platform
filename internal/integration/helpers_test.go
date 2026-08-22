//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ordersync/platform/internal/domain"
)

func truncateAll(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `TRUNCATE orders, sync_jobs, sync_ledger, idempotency_keys, outbox_events RESTART IDENTITY CASCADE`)
	return err
}

func queryInt(t *testing.T, sql string, args ...any) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func freshOrder(t *testing.T, id string, mp domain.Marketplace, payload string) domain.FreshOrder {
	t.Helper()
	fo, err := domain.NewFreshOrder(id, mp, json.RawMessage(payload))
	if err != nil {
		t.Fatal(err)
	}
	return fo
}

func eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
}

// recordingPublisher captures every delivered event id to detect duplicates
// across concurrent relays.
type recordingPublisher struct {
	mu   sync.Mutex
	seen map[int64]int
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{seen: map[int64]int{}}
}

func (p *recordingPublisher) Publish(_ context.Context, events []domain.OutboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		p.seen[e.ID]++
	}
	return nil
}

func (p *recordingPublisher) stats() (unique, duplicates int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range p.seen {
		unique++
		duplicates += n - 1
	}
	return unique, duplicates
}

func orderIDs(n int, prefix string) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return ids
}
