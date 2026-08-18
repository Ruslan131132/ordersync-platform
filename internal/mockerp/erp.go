// Package mockerp generates ERP-like order changes for demos and load tests.
package mockerp

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ordersync/platform/internal/domain"
)

type Generator struct {
	runID string
	seq   atomic.Int64
}

// NewGenerator prefixes IDs with a per-process run id so restarts create new
// orders instead of silently rewriting earlier ones.
func NewGenerator() *Generator {
	return &Generator{runID: strconv.FormatInt(time.Now().UnixNano()%1e9, 36)}
}

func (g *Generator) Generate(n int) []domain.FreshOrder {
	out := make([]domain.FreshOrder, 0, n)
	for range n {
		seq := g.seq.Add(1)
		mp := domain.MarketplaceWBLike
		if seq%2 == 0 {
			mp = domain.MarketplaceOzonLike
		}
		payload, _ := json.Marshal(map[string]any{
			"sku":        fmt.Sprintf("SKU-%04d", seq%500),
			"qty":        1 + rand.IntN(3),
			"price":      1000 + rand.IntN(9000),
			"warehouse":  fmt.Sprintf("WH-%d", seq%4),
			"currency":   "RUB",
			"created_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
		fo, err := domain.NewFreshOrder(fmt.Sprintf("ERP-%s-%d", g.runID, seq), mp, payload)
		if err != nil {
			panic(fmt.Sprintf("mockerp produced invalid order: %v", err))
		}
		out = append(out, fo)
	}
	return out
}
