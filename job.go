package goscheduler

import "time"

// Job represents a scheduled job that will run in specific time
type Job struct {
	Channel string                 `json:"channel"`
	ID      string                 `json:"id"`
	Args    map[string]interface{} `json:"args"`
	RunAt   time.Time              `json:"run_at"`
}

// JobBatch represents a batch of jobs with same scheduled time
type JobBatch struct {
	RunAt time.Time `json:"run_at"`
	Jobs  []*Job
}
