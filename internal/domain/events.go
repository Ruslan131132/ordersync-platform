package domain

import (
	"encoding/json"
	"time"
)

const (
	AggregateOrder   = "order"
	AggregateSyncJob = "sync_job"

	EventOrderSynced       = "order.synced"
	EventOrderDeadLettered = "order.dead_lettered"
	EventJobFinished       = "sync_job.finished"
)

type OutboxEvent struct {
	ID            int64           `json:"id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}
