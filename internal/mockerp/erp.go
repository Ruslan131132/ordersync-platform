package mockerp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ordersync/platform/internal/domain"
)

// ERP simulates an internal source of truth that accumulates "fresh" order volume.
type ERP struct {
	mu      sync.Mutex
	pending []domain.FreshOrder
	seq     atomic.Int64
	rng     *rand.Rand
}

func New() *ERP {
	return &ERP{rng: rand.New(rand.NewSource(time.Now().UnixNano()))} //nolint:gosec
}

// Seed generates n fresh orders across mock marketplaces.
func (e *ERP) Seed(n int) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := 0; i < n; i++ {
		e.pending = append(e.pending, e.makeOrder())
	}
	return len(e.pending)
}

// PollFresh returns up to limit fresh orders and removes them from the ERP buffer.
// In a real ERP this would be a CDC/feed cursor; here it models "volume of fresh data".
func (e *ERP) PollFresh(_ context.Context, limit int) ([]domain.FreshOrder, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if limit <= 0 || len(e.pending) == 0 {
		return nil, nil
	}
	if limit > len(e.pending) {
		limit = len(e.pending)
	}
	out := make([]domain.FreshOrder, limit)
	copy(out, e.pending[:limit])
	e.pending = e.pending[limit:]
	return out, nil
}

func (e *ERP) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

func (e *ERP) makeOrder() domain.FreshOrder {
	n := e.seq.Add(1)
	mp := domain.MarketplaceWBLike
	if n%2 == 0 {
		mp = domain.MarketplaceOzonLike
	}
	payload := map[string]any{
		"sku":       fmt.Sprintf("SKU-%04d", n%500),
		"qty":       1 + e.rng.Intn(3),
		"price":     1000 + e.rng.Intn(9000),
		"currency":  "RUB",
		"customer":  fmt.Sprintf("cust-%d", n%200),
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return domain.FreshOrder{
		ExternalID:  fmt.Sprintf("ERP-%d", n),
		Marketplace: mp,
		Payload:     b,
		ContentHash: hex.EncodeToString(sum[:]),
	}
}
