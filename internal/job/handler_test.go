package job

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func setupHandlerTestStore(t *testing.T) *Store {
	t.Helper()

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for API integration tests")
	}

	testStore, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	SetStore(testStore)

	return testStore
}

func deleteTestJob(t *testing.T, testStore *Store, id string) {
	t.Helper()

	_, err := testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		id,
	)

	if err != nil {
		t.Fatalf("failed to clean up test job: %v", err)
	}
}

// ----------------------------------------------------
// Test 1: POST /jobs
// ----------------------------------------------------

func TestCreateJobHandler(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	requestBody := map[string]any{
		"type": "api_test",
		"payload": map[string]any{
			"message": "testing POST jobs",
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to create request body: %v", err)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewReader(body),
	)

	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusCreated,
			response.Code,
		)
	}

	var createdJob Job

	err = json.NewDecoder(response.Body).Decode(&createdJob)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	defer deleteTestJob(t, testStore, createdJob.ID)

	if createdJob.ID == "" {
		t.Error("expected created job to have an ID")
	}

	if createdJob.Type != "api_test" {
		t.Errorf(
			"expected type api_test, got %s",
			createdJob.Type,
		)
	}

	if createdJob.Status != "queued" {
		t.Errorf(
			"expected status queued, got %s",
			createdJob.Status,
		)
	}

	savedJob, exists := testStore.Get(createdJob.ID)
	if !exists {
		t.Fatal("expected created job to be stored in PostgreSQL")
	}

	if savedJob.ID != createdJob.ID {
		t.Errorf(
			"expected stored job ID %s, got %s",
			createdJob.ID,
			savedJob.ID,
		)
	}
}

// ----------------------------------------------------
// Test 2: GET /jobs/{id}
// ----------------------------------------------------

func TestGetJobHandler(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	requestBody := map[string]any{
		"type": "get_api_test",
		"payload": map[string]any{
			"message": "testing GET job",
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to create request body: %v", err)
	}

	createRequest := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewReader(body),
	)

	createResponse := httptest.NewRecorder()

	JobsHandler(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf(
			"expected create status %d, got %d",
			http.StatusCreated,
			createResponse.Code,
		)
	}

	var createdJob Job

	err = json.NewDecoder(createResponse.Body).Decode(&createdJob)
	if err != nil {
		t.Fatalf("failed to decode created job: %v", err)
	}

	defer deleteTestJob(t, testStore, createdJob.ID)

	getRequest := httptest.NewRequest(
		http.MethodGet,
		"/jobs/"+createdJob.ID,
		nil,
	)

	getResponse := httptest.NewRecorder()

	GetJobHandler(getResponse, getRequest)

	if getResponse.Code != http.StatusOK {
		t.Fatalf(
			"expected GET status %d, got %d",
			http.StatusOK,
			getResponse.Code,
		)
	}

	var returnedJob Job

	err = json.NewDecoder(getResponse.Body).Decode(&returnedJob)
	if err != nil {
		t.Fatalf("failed to decode GET response: %v", err)
	}

	if returnedJob.ID != createdJob.ID {
		t.Errorf(
			"expected job ID %s, got %s",
			createdJob.ID,
			returnedJob.ID,
		)
	}

	if returnedJob.Type != "get_api_test" {
		t.Errorf(
			"expected type get_api_test, got %s",
			returnedJob.Type,
		)
	}
}

// ----------------------------------------------------
// Test 3: GET /jobs
// ----------------------------------------------------

func TestListJobsHandler(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	requestBody := map[string]any{
		"type": "list_api_test",
		"payload": map[string]any{
			"message": "testing GET jobs",
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to create request body: %v", err)
	}

	createRequest := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewReader(body),
	)

	createResponse := httptest.NewRecorder()

	JobsHandler(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf(
			"expected create status %d, got %d",
			http.StatusCreated,
			createResponse.Code,
		)
	}

	var createdJob Job

	err = json.NewDecoder(createResponse.Body).Decode(&createdJob)
	if err != nil {
		t.Fatalf("failed to decode created job: %v", err)
	}

	defer deleteTestJob(t, testStore, createdJob.ID)

	listRequest := httptest.NewRequest(
		http.MethodGet,
		"/jobs",
		nil,
	)

	listResponse := httptest.NewRecorder()

	JobsHandler(listResponse, listRequest)

	if listResponse.Code != http.StatusOK {
		t.Fatalf(
			"expected list status %d, got %d",
			http.StatusOK,
			listResponse.Code,
		)
	}

	var jobs []Job

	err = json.NewDecoder(listResponse.Body).Decode(&jobs)
	if err != nil {
		t.Fatalf("failed to decode jobs list: %v", err)
	}

	found := false

	for _, listedJob := range jobs {
		if listedJob.ID == createdJob.ID {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("expected created job to appear in GET /jobs response")
	}
}

// ----------------------------------------------------
// Test 4: PATCH /jobs/{id}/status
// ----------------------------------------------------

func TestUpdateJobStatusHandler(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	requestBody := map[string]any{
		"type": "status_api_test",
		"payload": map[string]any{
			"message": "testing PATCH job status",
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to create request body: %v", err)
	}

	createRequest := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewReader(body),
	)

	createResponse := httptest.NewRecorder()

	JobsHandler(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf(
			"expected create status %d, got %d",
			http.StatusCreated,
			createResponse.Code,
		)
	}

	var createdJob Job

	err = json.NewDecoder(createResponse.Body).Decode(&createdJob)
	if err != nil {
		t.Fatalf("failed to decode created job: %v", err)
	}

	defer deleteTestJob(t, testStore, createdJob.ID)

	statusBody := map[string]string{
		"status": "completed",
	}

	statusJSON, err := json.Marshal(statusBody)
	if err != nil {
		t.Fatalf("failed to create status request: %v", err)
	}

	patchRequest := httptest.NewRequest(
		http.MethodPatch,
		"/jobs/"+createdJob.ID+"/status",
		bytes.NewReader(statusJSON),
	)

	patchRequest.Header.Set("Content-Type", "application/json")

	patchResponse := httptest.NewRecorder()

	GetJobHandler(patchResponse, patchRequest)

	if patchResponse.Code != http.StatusOK {
		t.Fatalf(
			"expected PATCH status %d, got %d",
			http.StatusOK,
			patchResponse.Code,
		)
	}

	var updatedJob Job

	err = json.NewDecoder(patchResponse.Body).Decode(&updatedJob)
	if err != nil {
		t.Fatalf("failed to decode PATCH response: %v", err)
	}

	if updatedJob.Status != "completed" {
		t.Errorf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	savedJob, exists := testStore.Get(createdJob.ID)
	if !exists {
		t.Fatal("expected updated job to exist in PostgreSQL")
	}

	if savedJob.Status != "completed" {
		t.Errorf(
			"expected persisted status completed, got %s",
			savedJob.Status,
		)
	}
}

// ----------------------------------------------------
// Validation tests
// ----------------------------------------------------

func TestCreateJobMissingType(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"payload":{"message":"hello"}}`),
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestCreateJobWhitespaceType(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"type":"   ","payload":{}}`),
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestCreateJobInvalidJSON(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"type":`),
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestGetJobInvalidUUID(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs/not-a-valid-uuid",
		nil,
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestUpdateJobInvalidStatus(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodPatch,
		"/jobs/11111111-1111-1111-1111-111111111111/status",
		strings.NewReader(`{"status":"banana"}`),
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestJobsHandlerMethodNotAllowed(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodDelete,
		"/jobs",
		nil,
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusMethodNotAllowed,
			response.Code,
		)
	}
}

// ----------------------------------------------------
// Test 5: GET /stats
// ----------------------------------------------------

func TestStatsHandler(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	// Get the expected statistics directly from the store.
	expectedStats, err := testStore.Stats()
	if err != nil {
		t.Fatalf(
			"failed to retrieve expected queue stats: %v",
			err,
		)
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/stats",
		nil,
	)

	response := httptest.NewRecorder()

	StatsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	var returnedStats QueueStats

	err = json.NewDecoder(response.Body).Decode(&returnedStats)
	if err != nil {
		t.Fatalf(
			"failed to decode stats response: %v",
			err,
		)
	}

	if returnedStats != expectedStats {
		t.Errorf(
			"expected stats %+v, got %+v",
			expectedStats,
			returnedStats,
		)
	}

	if returnedStats.Total !=
		returnedStats.Queued+
			returnedStats.Processing+
			returnedStats.Completed+
			returnedStats.Failed {

		t.Errorf(
			"stats total does not match status counts: %+v",
			returnedStats,
		)
	}
}

// ----------------------------------------------------
// Test 6: /stats rejects unsupported methods
// ----------------------------------------------------

func TestStatsHandlerMethodNotAllowed(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodPost,
		"/stats",
		nil,
	)

	response := httptest.NewRecorder()

	StatsHandler(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusMethodNotAllowed,
			response.Code,
		)
	}
}

// ----------------------------------------------------
// Cancellation API tests
// ----------------------------------------------------

// A queued job should be cancelled through
// DELETE /jobs/{id}.
func TestDeleteJobCancelsQueuedJob(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
		Type:       "cancel_handler_test",
		Payload:    map[string]any{"message": "cancel me"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	defer func() {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			testJob.ID,
		)
	}()

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save cancellation handler test job: %v",
			err,
		)
	}

	request := httptest.NewRequest(
		http.MethodDelete,
		"/jobs/"+testJob.ID,
		nil,
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusOK,
			response.Code,
			response.Body.String(),
		)
	}

	var returnedJob Job

	if err := json.NewDecoder(response.Body).Decode(&returnedJob); err != nil {
		t.Fatalf(
			"failed to decode cancellation response: %v",
			err,
		)
	}

	if returnedJob.ID != testJob.ID {
		t.Errorf(
			"expected job ID %s, got %s",
			testJob.ID,
			returnedJob.ID,
		)
	}

	if returnedJob.Status != "cancelled" {
		t.Errorf(
			"expected returned status cancelled, got %s",
			returnedJob.Status,
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected cancelled job to remain in database")
	}

	if savedJob.Status != "cancelled" {
		t.Errorf(
			"expected persisted status cancelled, got %s",
			savedJob.Status,
		)
	}
}

// A processing job cannot be cancelled.
func TestDeleteJobRejectsProcessingJob(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2",
		Type:       "processing_cancel_test",
		Payload:    map[string]any{"message": "already processing"},
		Status:     "processing",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	defer func() {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			testJob.ID,
		)
	}()

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save processing cancellation test job: %v",
			err,
		)
	}

	request := httptest.NewRequest(
		http.MethodDelete,
		"/jobs/"+testJob.ID,
		nil,
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusConflict,
			response.Code,
			response.Body.String(),
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected processing job to remain in database")
	}

	if savedJob.Status != "processing" {
		t.Errorf(
			"expected status to remain processing, got %s",
			savedJob.Status,
		)
	}
}

// DELETE with an invalid UUID should return 400.
func TestDeleteJobInvalidID(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodDelete,
		"/jobs/not-a-valid-uuid",
		nil,
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

// DELETE for a valid UUID that does not exist
// should return 404.
func TestDeleteJobNotFound(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	missingID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb9"

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		missingID,
	)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/jobs/"+missingID,
		nil,
	)

	response := httptest.NewRecorder()

	GetJobHandler(response, request)

	if response.Code != http.StatusNotFound {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusNotFound,
			response.Code,
		)
	}
}

// ----------------------------------------------------
// Job filtering API tests
// ----------------------------------------------------

// GET /jobs?status=completed should return only
// jobs whose status is completed.
func TestListJobsFilterByStatus(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	testJobs := []Job{
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc1",
			Type:       "email",
			Payload:    map[string]any{"message": "completed email"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc2",
			Type:       "report",
			Payload:    map[string]any{"message": "failed report"},
			Status:     "failed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(time.Millisecond),
		},
	}

	cleanupHandlerFilterJobs(t, testStore, testJobs)

	for _, testJob := range testJobs {
		if err := testStore.Save(testJob); err != nil {
			t.Fatalf("failed to save test job: %v", err)
		}
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs?status=completed",
		nil,
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusOK,
			response.Code,
			response.Body.String(),
		)
	}

	var jobs []Job

	if err := json.NewDecoder(response.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	foundTestJob := false

	for _, currentJob := range jobs {
		if currentJob.Status != "completed" {
			t.Fatalf(
				"expected only completed jobs, got %s",
				currentJob.Status,
			)
		}

		if currentJob.ID == testJobs[0].ID {
			foundTestJob = true
		}
	}

	if !foundTestJob {
		t.Fatal("expected completed filtering test job")
	}
}

// GET /jobs?type=email should return only email jobs.
func TestListJobsFilterByType(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	testJobs := []Job{
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc3",
			Type:       "email",
			Payload:    map[string]any{"message": "email job"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc4",
			Type:       "report",
			Payload:    map[string]any{"message": "report job"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(time.Millisecond),
		},
	}

	cleanupHandlerFilterJobs(t, testStore, testJobs)

	for _, testJob := range testJobs {
		if err := testStore.Save(testJob); err != nil {
			t.Fatalf("failed to save test job: %v", err)
		}
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs?type=email",
		nil,
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusOK,
			response.Code,
			response.Body.String(),
		)
	}

	var jobs []Job

	if err := json.NewDecoder(response.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	foundTestJob := false

	for _, currentJob := range jobs {
		if currentJob.Type != "email" {
			t.Fatalf(
				"expected only email jobs, got %s",
				currentJob.Type,
			)
		}

		if currentJob.ID == testJobs[0].ID {
			foundTestJob = true
		}
	}

	if !foundTestJob {
		t.Fatal("expected email filtering test job")
	}
}

// GET /jobs?status=completed&type=email should apply
// both filters at the same time.
func TestListJobsFilterByStatusAndType(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	testJobs := []Job{
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc5",
			Type:       "email",
			Payload:    map[string]any{"message": "completed email"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc6",
			Type:       "email",
			Payload:    map[string]any{"message": "failed email"},
			Status:     "failed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(time.Millisecond),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc7",
			Type:       "report",
			Payload:    map[string]any{"message": "completed report"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(2 * time.Millisecond),
		},
	}

	cleanupHandlerFilterJobs(t, testStore, testJobs)

	for _, testJob := range testJobs {
		if err := testStore.Save(testJob); err != nil {
			t.Fatalf("failed to save test job: %v", err)
		}
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs?status=completed&type=email",
		nil,
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusOK,
			response.Code,
			response.Body.String(),
		)
	}

	var jobs []Job

	if err := json.NewDecoder(response.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	foundTestJob := false

	for _, currentJob := range jobs {
		if currentJob.Status != "completed" {
			t.Fatalf(
				"expected completed status, got %s",
				currentJob.Status,
			)
		}

		if currentJob.Type != "email" {
			t.Fatalf(
				"expected email type, got %s",
				currentJob.Type,
			)
		}

		if currentJob.ID == testJobs[0].ID {
			foundTestJob = true
		}
	}

	if !foundTestJob {
		t.Fatal(
			"expected completed email filtering test job",
		)
	}
}

// An unsupported status filter should return 400.
func TestListJobsRejectsInvalidStatusFilter(t *testing.T) {
	testStore := setupHandlerTestStore(t)
	defer testStore.Close()

	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs?status=banana",
		nil,
	)

	response := httptest.NewRecorder()

	JobsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusBadRequest,
			response.Code,
			response.Body.String(),
		)
	}
}

// cleanupHandlerFilterJobs removes filtering-test records
// before and after each test.
func cleanupHandlerFilterJobs(
	t *testing.T,
	testStore *Store,
	jobs []Job,
) {
	t.Helper()

	for _, testJob := range jobs {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			testJob.ID,
		)
	}

	t.Cleanup(func() {
		for _, testJob := range jobs {
			_, _ = testStore.db.Exec(
				context.Background(),
				"DELETE FROM jobs WHERE id = $1",
				testJob.ID,
			)
		}
	})
}
