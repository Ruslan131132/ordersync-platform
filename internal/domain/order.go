package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Marketplace string

const (
	MarketplaceWBLike   Marketplace = "wb_like"
	MarketplaceOzonLike Marketplace = "ozon_like"
)

func (m Marketplace) Valid() bool {
	switch m {
	case MarketplaceWBLike, MarketplaceOzonLike:
		return true
	default:
		return false
	}
}

type OrderStatus string

const (
	OrderStatusPending OrderStatus = "pending"
	OrderStatusSyncing OrderStatus = "syncing"
	OrderStatusSynced  OrderStatus = "synced"
	OrderStatusFailed  OrderStatus = "failed"
	// OrderStatusDead means retries are exhausted; only a content change revives the order.
	OrderStatusDead OrderStatus = "dead"
)

// ErrLeaseLost is returned when a worker tries to complete work whose lease
// was taken over by another worker.
var ErrLeaseLost = errors.New("lease lost")

type Order struct {
	ID           uuid.UUID       `json:"id"`
	ExternalID   string          `json:"external_id"`
	Marketplace  Marketplace     `json:"marketplace"`
	Status       OrderStatus     `json:"status"`
	Payload      json.RawMessage `json:"payload"`
	ContentHash  string          `json:"content_hash"`
	Version      int64           `json:"version"`
	Attempts     int             `json:"attempts"`
	LastError    string          `json:"last_error,omitempty"`
	PendingSince time.Time       `json:"pending_since"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// ClaimedOrder is an order leased to one worker. Completion is accepted only
// while LeaseToken still matches the row.
type ClaimedOrder struct {
	Order
	LeaseToken uuid.UUID
}

type FreshOrder struct {
	ExternalID  string
	Marketplace Marketplace
	Payload     json.RawMessage
	ContentHash string
}

const maxExternalIDLen = 128

// NewFreshOrder validates input and derives the content hash from the
// compacted payload, so formatting differences do not trigger re-syncs.
func NewFreshOrder(externalID string, mp Marketplace, payload json.RawMessage) (FreshOrder, error) {
	if externalID == "" || len(externalID) > maxExternalIDLen {
		return FreshOrder{}, fmt.Errorf("external_id must be 1..%d chars", maxExternalIDLen)
	}
	if !mp.Valid() {
		return FreshOrder{}, fmt.Errorf("unknown marketplace %q", mp)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, payload); err != nil {
		return FreshOrder{}, fmt.Errorf("payload must be valid JSON: %w", err)
	}
	if compact.Len() == 0 || compact.Bytes()[0] != '{' {
		return FreshOrder{}, errors.New("payload must be a JSON object")
	}
	sum := sha256.Sum256(compact.Bytes())
	return FreshOrder{
		ExternalID:  externalID,
		Marketplace: mp,
		Payload:     compact.Bytes(),
		ContentHash: hex.EncodeToString(sum[:]),
	}, nil
}
