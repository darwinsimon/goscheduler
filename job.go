package goscheduler

import "time"

// All job statuses
const (
	JobStatusInactive = 0
	JobStatusActive   = 1
)

// Job represents a scheduled job that will run in specific time
type Job struct {
	Channel string                 `json:"channel"`
	ID      string                 `json:"id"`
	Args    map[string]interface{} `json:"args"`
	RunAt   time.Time              `json:"run_at"`
	Status  int                    `json:"status"`
}

// JobBatch represents a batch of jobs with same scheduled time
type JobBatch struct {
	RunAt time.Time `json:"run_at"`
	Jobs  []*Job
}
