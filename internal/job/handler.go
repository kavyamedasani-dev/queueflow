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
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
	ScheduledAt *time.Time     `json:"scheduled_at,omitempty"`
}

type UpdateStatusRequest struct {
	Status string `json:"status"`
}

func SetStore(s *Store) {
	store = s
}

// writeJSONError sends errors consistently as JSON.
func writeJSONError(
	w http.ResponseWriter,
	message string,
	status int,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(
		map[string]string{
			"error": message,
		},
	)
}

// JobsHandler handles:
//
// POST /jobs
// GET  /jobs
//
// GET /jobs also supports optional query parameters:
//
// ?status=queued
// ?status=processing
// ?status=completed
// ?status=failed
// ?status=cancelled
// ?type=email
//
// Filters can also be combined:
//
// ?status=completed&type=email
func JobsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		createJob(w, r)

	case http.MethodGet:
		listJobs(w, r)

	default:
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

func createJob(
	w http.ResponseWriter,
	r *http.Request,
) {
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
		ID:          uuid.NewString(),
		Type:        request.Type,
		Payload:     request.Payload,
		Status:      "queued",
		Retries:     0,
		MaxRetries:  3,
		CreatedAt:   time.Now(),
		ScheduledAt: request.ScheduledAt,
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

// listJobs returns all jobs or jobs matching optional
// status and type query parameters.
func listJobs(
	w http.ResponseWriter,
	r *http.Request,
) {
	status := strings.ToLower(
		strings.TrimSpace(
			r.URL.Query().Get("status"),
		),
	)

	jobType := strings.TrimSpace(
		r.URL.Query().Get("type"),
	)

	if status != "" && !isValidStatus(status) {
		writeJSONError(
			w,
			"invalid status filter",
			http.StatusBadRequest,
		)
		return
	}

	if len(jobType) > 100 {
		writeJSONError(
			w,
			"type filter must be 100 characters or fewer",
			http.StatusBadRequest,
		)
		return
	}

	filter := JobFilter{
		Status: status,
		Type:   jobType,
	}

	jobs := store.ListFiltered(filter)

	_ = json.NewEncoder(w).Encode(jobs)
}

// GetJobHandler handles:
//
// GET    /jobs/{id}
// DELETE /jobs/{id}
// PATCH  /jobs/{id}/status
func GetJobHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(
		r.URL.Path,
		"/jobs/",
	)

	if strings.HasSuffix(path, "/status") {
		updateJobStatus(w, r, path)
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

	if _, err := uuid.Parse(id); err != nil {
		writeJSONError(
			w,
			"invalid job id",
			http.StatusBadRequest,
		)
		return
	}

	switch r.Method {
	case http.MethodGet:
		getJob(w, id)

	case http.MethodDelete:
		cancelJob(w, id)

	default:
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

func getJob(
	w http.ResponseWriter,
	id string,
) {
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

// cancelJob cancels a job only if it is still queued.
//
// Processing, completed, failed, and already-cancelled
// jobs cannot be cancelled.
func cancelJob(
	w http.ResponseWriter,
	id string,
) {
	existingJob, exists := store.Get(id)

	if !exists {
		writeJSONError(
			w,
			"job not found",
			http.StatusNotFound,
		)
		return
	}

	if existingJob.Status != "queued" {
		writeJSONError(
			w,
			"only queued jobs can be cancelled",
			http.StatusConflict,
		)
		return
	}

	cancelledJob, cancelled := store.CancelJob(id)

	if !cancelled {
		// The job may have been claimed by a worker
		// between the Get call and the cancellation attempt.
		writeJSONError(
			w,
			"job could not be cancelled",
			http.StatusConflict,
		)
		return
	}

	_ = json.NewEncoder(w).Encode(cancelledJob)
}

func updateJobStatus(
	w http.ResponseWriter,
	r *http.Request,
	path string,
) {
	if r.Method != http.MethodPatch {
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	id := strings.TrimSpace(
		strings.TrimSuffix(
			path,
			"/status",
		),
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

// StatsHandler handles:
//
// GET /stats
func StatsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if r.Method != http.MethodGet {
		writeJSONError(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	stats, err := store.Stats()
	if err != nil {
		writeJSONError(
			w,
			"failed to retrieve queue stats",
			http.StatusInternalServerError,
		)
		return
	}

	_ = json.NewEncoder(w).Encode(stats)
}

func isValidStatus(status string) bool {
	switch status {
	case "queued",
		"processing",
		"completed",
		"failed",
		"cancelled":
		return true

	default:
		return false
	}
}
