package job

import "time"

type Job struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
}
