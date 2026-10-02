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

func deleteWorkerTestJob(t *testing.T, testStore *Store, id string) {
	t.Helper()

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		id,
	)
}

// Test 1:
// A normal queued job should be claimed and completed.
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

	deleteTestJob(t, testStore, job.ID)
	defer deleteTestJob(t, testStore, job.ID)

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
}

// Test 2:
// A failing job should retry three times and then fail permanently.
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

	deleteTestJob(t, testStore, job.ID)
	defer deleteTestJob(t, testStore, job.ID)

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
}

// Test 3:
// A scheduled job must remain queued until its scheduled time.
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

	deleteTestJob(t, testStore, job.ID)
	defer deleteTestJob(t, testStore, job.ID)

	if err := testStore.Save(job); err != nil {
		t.Fatalf("failed to save scheduled test job: %v", err)
	}

	StartWorker()

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

	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"completed",
		8*time.Second,
	)
}

// Test 4:
// Multiple workers should be able to process multiple jobs concurrently.
//
// Each job simulates two seconds of work. With only one worker,
// three jobs would require roughly six seconds. With three workers,
// they should complete at approximately the same time.
func TestMultipleWorkersProcessJobsConcurrently(t *testing.T) {
	testStore := setupWorkerTestStore(t)
	defer testStore.Close()

	jobs := []Job{
		{
			ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
			Type:       "send_email",
			Payload:    map[string]any{"to": "worker1@example.com"},
			Status:     "queued",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2",
			Type:       "send_email",
			Payload:    map[string]any{"to": "worker2@example.com"},
			Status:     "queued",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(time.Millisecond),
		},
		{
			ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3",
			Type:       "send_email",
			Payload:    map[string]any{"to": "worker3@example.com"},
			Status:     "queued",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now().Add(2 * time.Millisecond),
		},
	}

	for _, job := range jobs {
		deleteTestJob(t, testStore, job.ID)
	}

	defer func() {
		for _, job := range jobs {
			deleteTestJob(t, testStore, job.ID)
		}
	}()

	for _, job := range jobs {
		if err := testStore.Save(job); err != nil {
			t.Fatalf(
				"failed to save concurrent worker test job %s: %v",
				job.ID,
				err,
			)
		}
	}

	startedAt := time.Now()

	StartWorker()

	for _, job := range jobs {
		waitForJobStatus(
			t,
			testStore,
			job.ID,
			"completed",
			5*time.Second,
		)
	}

	elapsed := time.Since(startedAt)

	// One worker would take about six seconds because each job
	// sleeps for two seconds. Three workers should finish well
	// below that threshold.
	if elapsed >= 5*time.Second {
		t.Fatalf(
			"expected concurrent processing to finish in under 5 seconds, took %s",
			elapsed,
		)
	}

	for _, job := range jobs {
		completedJob, exists := testStore.Get(job.ID)
		if !exists {
			t.Fatalf(
				"expected job %s to exist",
				job.ID,
			)
		}

		if completedJob.Status != "completed" {
			t.Fatalf(
				"expected job %s to be completed, got %s",
				job.ID,
				completedJob.Status,
			)
		}

		if completedJob.Retries != 0 {
			t.Fatalf(
				"expected job %s to have 0 retries, got %d",
				job.ID,
				completedJob.Retries,
			)
		}
	}
}

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
