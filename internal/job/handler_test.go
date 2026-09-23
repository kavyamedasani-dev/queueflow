package job

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

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

// Test 1:
// POST /jobs
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

// Test 2:
// GET /jobs/{id}
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

// Test 3:
// GET /jobs
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

// Test 4:
// PATCH /jobs/{id}/status
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

	// Verify PostgreSQL also contains the updated status.
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
