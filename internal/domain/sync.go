package domain

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

type SyncJob struct {
	ID            uuid.UUID  `json:"id"`
	Status        JobStatus  `json:"status"`
	TriggerSource string     `json:"trigger_source"`
	FreshVolume   int        `json:"fresh_volume"`
	BatchSize     int        `json:"batch_size"`
	OrdersQueued  int        `json:"orders_queued"`
	OrdersDone    int        `json:"orders_done"`
	OrdersFailed  int        `json:"orders_failed"`
	ErrorMessage  string     `json:"error_message,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type SyncLedgerEntry struct {
	ID          uuid.UUID
	OrderID     uuid.UUID
	Marketplace Marketplace
	Method      string
	ContentHash string
	RemoteRef   string
	CreatedAt   time.Time
}
