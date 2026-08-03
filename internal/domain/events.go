package domain

import (
	"encoding/json"
	"time"
)

const (
	EventOrderSynced  = "order.synced"
	EventOrderFailed  = "order.failed"
	EventJobCompleted = "sync_job.completed"
)

type OutboxEvent struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	CreatedAt     time.Time
	PublishedAt   *time.Time
}
