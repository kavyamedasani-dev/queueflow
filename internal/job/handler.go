package job

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

var store *Store

type CreateJobRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type UpdateStatusRequest struct {
	Status string `json:"status"`
}

func SetStore(s *Store) {
	store = s
}

// writeJSONError sends errors consistently as JSON.
func writeJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func JobsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		createJob(w, r)

	case http.MethodGet:
		listJobs(w)

	default:
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

func createJob(w http.ResponseWriter, r *http.Request) {
	var request CreateJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	err := decoder.Decode(&request)
	if err != nil {
		writeJSONError(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	request.Type = strings.TrimSpace(request.Type)

	if request.Type == "" {
		writeJSONError(
			w,
			"type is required",
			http.StatusBadRequest,
		)
		return
	}

	if len(request.Type) > 100 {
		writeJSONError(
			w,
			"type must be 100 characters or fewer",
			http.StatusBadRequest,
		)
		return
	}

	if request.Payload == nil {
		request.Payload = map[string]any{}
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

	err = store.Save(newJob)
	if err != nil {
		writeJSONError(
			w,
			"failed to save job",
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(newJob)
}

func listJobs(w http.ResponseWriter) {
	jobs := store.List()

	_ = json.NewEncoder(w).Encode(jobs)
}

func GetJobHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/jobs/")

	if strings.HasSuffix(path, "/status") {
		updateJobStatus(w, r, path)
		return
	}

	if r.Method != http.MethodGet {
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	id := strings.TrimSpace(path)

	if id == "" {
		writeJSONError(
			w,
			"job id is required",
			http.StatusBadRequest,
		)
		return
	}

	// Validate that the supplied ID is a valid UUID.
	if _, err := uuid.Parse(id); err != nil {
		writeJSONError(
			w,
			"invalid job id",
			http.StatusBadRequest,
		)
		return
	}

	existingJob, exists := store.Get(id)
	if !exists {
		writeJSONError(
			w,
			"job not found",
			http.StatusNotFound,
		)
		return
	}

	_ = json.NewEncoder(w).Encode(existingJob)
}

func updateJobStatus(w http.ResponseWriter, r *http.Request, path string) {
	if r.Method != http.MethodPatch {
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	id := strings.TrimSpace(
		strings.TrimSuffix(path, "/status"),
	)

	if id == "" {
		writeJSONError(
			w,
			"job id is required",
			http.StatusBadRequest,
		)
		return
	}

	if _, err := uuid.Parse(id); err != nil {
		writeJSONError(
			w,
			"invalid job id",
			http.StatusBadRequest,
		)
		return
	}

	var request UpdateStatusRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	err := decoder.Decode(&request)
	if err != nil {
		writeJSONError(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	request.Status = strings.ToLower(
		strings.TrimSpace(request.Status),
	)

	if request.Status == "" {
		writeJSONError(
			w,
			"status is required",
			http.StatusBadRequest,
		)
		return
	}

	if !isValidStatus(request.Status) {
		writeJSONError(
			w,
			"invalid status",
			http.StatusBadRequest,
		)
		return
	}

	updatedJob, exists := store.UpdateStatus(
		id,
		request.Status,
	)

	if !exists {
		writeJSONError(
			w,
			"job not found or status update failed",
			http.StatusNotFound,
		)
		return
	}

	_ = json.NewEncoder(w).Encode(updatedJob)
}

func isValidStatus(status string) bool {
	switch status {
	case "queued",
		"processing",
		"completed",
		"failed":
		return true

	default:
		return false
	}
}
