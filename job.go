package goscheduler

type Job struct {
	Name    string            `json:"name"`
	ID      string            `json:"id"`
	Args    map[string]string `json:"args"`
	StartAt int64             `json:"start_at"`

	CreatedAt int64 `json:"created_at"`
}
