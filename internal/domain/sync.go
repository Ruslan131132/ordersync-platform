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

// SyncJob is one drain run: a worker claims it and processes adaptive batches
// until no claimable orders remain or the per-job batch cap is reached.
type SyncJob struct {
	ID            uuid.UUID  `json:"id"`
	Status        JobStatus  `json:"status"`
	TriggerSource string     `json:"trigger_source"`
	WorkerID      string     `json:"worker_id,omitempty"`
	FreshVolume   int        `json:"fresh_volume"`
	Batches       int        `json:"batches"`
	LastBatchSize int        `json:"last_batch_size"`
	OrdersClaimed int        `json:"orders_claimed"`
	OrdersDone    int        `json:"orders_done"`
	OrdersFailed  int        `json:"orders_failed"`
	ErrorMessage  string     `json:"error_message,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type BatchResult struct {
	Size   int
	Done   int
	Failed int
}

type Stats struct {
	Orders          map[OrderStatus]int `json:"orders"`
	Claimable       int                 `json:"claimable"`
	Jobs            map[JobStatus]int   `json:"jobs"`
	OutboxBacklog   int                 `json:"outbox_backlog"`
	OutboxPublished int                 `json:"outbox_published"`
}
