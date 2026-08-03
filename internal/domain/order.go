package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Marketplace string

const (
	MarketplaceWBLike   Marketplace = "wb_like"
	MarketplaceOzonLike Marketplace = "ozon_like"
)

type OrderStatus string

const (
	OrderStatusPending OrderStatus = "pending"
	OrderStatusSyncing OrderStatus = "syncing"
	OrderStatusSynced  OrderStatus = "synced"
	OrderStatusFailed  OrderStatus = "failed"
)

type Order struct {
	ID          uuid.UUID       `json:"id"`
	ExternalID  string          `json:"external_id"`
	Marketplace Marketplace     `json:"marketplace"`
	Status      OrderStatus     `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	ContentHash string          `json:"content_hash"`
	Version     int64           `json:"version"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type FreshOrder struct {
	ExternalID  string
	Marketplace Marketplace
	Payload     json.RawMessage
	ContentHash string
}
