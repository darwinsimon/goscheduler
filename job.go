package goscheduler

// Job represents a scheduled job that will run in specific time
type Job struct {
	Channel string            `json:"channel"`
	ID      string            `json:"id"`
	Args    map[string]string `json:"args"`
	StartAt int64             `json:"start_at"`
}
