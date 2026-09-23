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

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		createdJob.ID,
	)
}

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

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		createdJob.ID,
	)
}
