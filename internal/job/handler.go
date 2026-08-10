package job

import (
	"encoding/json"
	"net/http"
	"time"
)

type CreateJobRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

func CreateJobHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var request CreateJobRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if request.Type == "" {
		http.Error(w, `{"error":"type is required"}`, http.StatusBadRequest)
		return
	}

	newJob := Job{
		ID:        "job-001",
		Type:      request.Type,
		Payload:   request.Payload,
		Status:    "queued",
		CreatedAt: time.Now(),
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newJob)
}
