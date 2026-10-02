package job

import (
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func setupWorkerTestStore(t *testing.T) *Store {
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

	SetStore(testStore)

	return testStore
}

// Test 1:
// Normal queued job should become completed.
func TestWorkerCompletesQueuedJob(t *testing.T) {
	testStore := setupWorkerTestStore(t)
	defer testStore.Close()

	job := Job{
		ID:         "44444444-4444-4444-4444-444444444444",
		Type:       "send_email",
		Payload:    map[string]any{"to": "worker-test@example.com"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save test job: %v", err)
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
		t.Fatal("expected job to exist")
	}

	if updatedJob.Status != "completed" {
		t.Fatalf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 0 {
		t.Fatalf(
			"expected 0 retries, got %d",
			updatedJob.Retries,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)
}

// Test 2:
// Failing job should retry 3 times and then become failed.
func TestWorkerRetriesAndFailsJob(t *testing.T) {
	testStore := setupWorkerTestStore(t)
	defer testStore.Close()

	job := Job{
		ID:         "55555555-5555-5555-5555-555555555555",
		Type:       "fail_job",
		Payload:    map[string]any{"reason": "test retry logic"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save failing test job: %v", err)
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
		t.Fatal("expected failing job to exist")
	}

	if updatedJob.Status != "failed" {
		t.Fatalf(
			"expected status failed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 3 {
		t.Fatalf(
			"expected 3 retries, got %d",
			updatedJob.Retries,
		)
	}

	if updatedJob.MaxRetries != 3 {
		t.Fatalf(
			"expected max retries 3, got %d",
			updatedJob.MaxRetries,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)
}

// Test 3:
// A scheduled job should stay queued until its scheduled time,
// then be processed and completed.
func TestWorkerWaitsForScheduledJob(t *testing.T) {
	testStore := setupWorkerTestStore(t)
	defer testStore.Close()

	scheduledAt := time.Now().Add(3 * time.Second)

	job := Job{
		ID:          "66666666-6666-6666-6666-666666666666",
		Type:        "send_email",
		Payload:     map[string]any{"to": "scheduled@example.com"},
		Status:      "queued",
		Retries:     0,
		MaxRetries:  3,
		CreatedAt:   time.Now(),
		ScheduledAt: &scheduledAt,
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save scheduled test job: %v", err)
	}

	StartWorker()

	// The scheduled time has not arrived yet.
	time.Sleep(1 * time.Second)

	currentJob, exists := testStore.Get(job.ID)
	if !exists {
		t.Fatal("expected scheduled job to exist")
	}

	if currentJob.Status != "queued" {
		t.Fatalf(
			"expected scheduled job to remain queued before scheduled time, got %s",
			currentJob.Status,
		)
	}

	// Now wait for the scheduled time and processing to finish.
	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"completed",
		8*time.Second,
	)

	completedJob, exists := testStore.Get(job.ID)
	if !exists {
		t.Fatal("expected scheduled job to exist after processing")
	}

	if completedJob.Status != "completed" {
		t.Fatalf(
			"expected scheduled job to complete, got %s",
			completedJob.Status,
		)
	}

	if completedJob.Retries != 0 {
		t.Fatalf(
			"expected scheduled job to complete without retries, got %d",
			completedJob.Retries,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		job.ID,
	)
}

// Helper:
// Wait until a job reaches the expected status.
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
