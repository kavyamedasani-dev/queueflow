package job

import (
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

// setupWorkerTestStore creates a PostgreSQL store for worker integration tests.
func setupWorkerTestStore(t *testing.T) (*Store, func()) {
	t.Helper()

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for worker integration tests")
	}

	testStore, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	cleanup := func() {
		testStore.Close()
	}

	return testStore, cleanup
}

// Test that a normal queued job is processed successfully.
func TestWorkerCompletesQueuedJob(t *testing.T) {
	testStore, cleanup := setupWorkerTestStore(t)
	defer cleanup()

	SetStore(testStore)

	job := Job{
		ID:         "44444444-4444-4444-4444-444444444444",
		Type:       "send_email",
		Payload:    map[string]any{"to": "worker-test@example.com"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	// Remove an old copy in case a previous test run left one behind.
	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save worker test job: %v", err)
	}

	StartWorker()

	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"completed",
		5*time.Second,
	)

	updatedJob, exists := testStore.Get(job.ID)
	if !exists {
		t.Fatal("expected worker test job to exist")
	}

	if updatedJob.Status != "completed" {
		t.Errorf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 0 {
		t.Errorf(
			"expected 0 retries, got %d",
			updatedJob.Retries,
		)
	}

	// Clean up test data.
	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)
}

// Test that a failing job is retried three times and then marked failed.
func TestWorkerRetriesAndFailsJob(t *testing.T) {
	testStore, cleanup := setupWorkerTestStore(t)
	defer cleanup()

	SetStore(testStore)

	job := Job{
		ID:         "55555555-5555-5555-5555-555555555555",
		Type:       "fail_job",
		Payload:    map[string]any{"reason": "test retry logic"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	// Remove an old copy in case a previous test run left one behind.
	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save failing worker test job: %v", err)
	}

	StartWorker()

	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"failed",
		15*time.Second,
	)

	updatedJob, exists := testStore.Get(job.ID)
	if !exists {
		t.Fatal("expected failing worker test job to exist")
	}

	if updatedJob.Status != "failed" {
		t.Errorf(
			"expected status failed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 3 {
		t.Errorf(
			"expected 3 retries, got %d",
			updatedJob.Retries,
		)
	}

	if updatedJob.MaxRetries != 3 {
		t.Errorf(
			"expected max retries 3, got %d",
			updatedJob.MaxRetries,
		)
	}

	// Clean up test data.
	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)
}

// waitForJobStatus waits until the worker changes a job to the expected status.
func waitForJobStatus(
	t *testing.T,
	testStore *Store,
	jobID string,
	expectedStatus string,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		currentJob, exists := testStore.Get(jobID)

		if exists && currentJob.Status == expectedStatus {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	currentJob, exists := testStore.Get(jobID)

	if !exists {
		t.Fatalf(
			"job %s was not found while waiting for status %s",
			jobID,
			expectedStatus,
		)
	}

	t.Fatalf(
		"timed out waiting for job %s to reach status %s; current status is %s",
		jobID,
		expectedStatus,
		currentJob.Status,
	)
}
