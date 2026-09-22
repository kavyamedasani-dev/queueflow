package job

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

var store = NewStore()

type CreateJobRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type UpdateStatusRequest struct {
	Status string `json:"status"`
}

func JobsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		createJob(w, r)

	case http.MethodGet:
		listJobs(w)

	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func createJob(w http.ResponseWriter, r *http.Request) {
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
		ID:         uuid.NewString(),
		Type:       request.Type,
		Payload:    request.Payload,
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	store.Save(newJob)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newJob)
}

func listJobs(w http.ResponseWriter) {
	jobs := store.List()

	json.NewEncoder(w).Encode(jobs)
}

func GetJobHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/jobs/")

	if strings.HasSuffix(path, "/status") {
		updateJobStatus(w, r, path)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	id := path

	if id == "" {
		http.Error(w, `{"error":"job id is required"}`, http.StatusBadRequest)
		return
	}

	existingJob, exists := store.Get(id)
	if !exists {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(existingJob)
}

func updateJobStatus(w http.ResponseWriter, r *http.Request, path string) {
	if r.Method != http.MethodPatch {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimSuffix(path, "/status")

	if id == "" {
		http.Error(w, `{"error":"job id is required"}`, http.StatusBadRequest)
		return
	}

	var request UpdateStatusRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if request.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}

	updatedJob, exists := store.UpdateStatus(id, request.Status)
	if !exists {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(updatedJob)
}
