package job

import (
	"context"
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

func deleteWorkerTestJob(
	t *testing.T,
	testStore *Store,
	id string,
) {
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

	job := Job{
		ID:         "44444444-4444-4444-4444-444444444444",
		Type:       "send_email",
		Payload:    map[string]any{"to": "worker-test@example.com"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	deleteWorkerTestJob(t, testStore, job.ID)

	if err := testStore.Save(job); err != nil {
		testStore.Close()
		t.Fatalf("failed to save test job: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	workerGroup := StartWorkers(ctx)

	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"completed",
		5*time.Second,
	)

	updatedJob, exists := testStore.Get(job.ID)
	if !exists {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatal("expected job to exist")
	}

	if updatedJob.Status != "completed" {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 0 {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected 0 retries, got %d",
			updatedJob.Retries,
		)
	}

	cancel()
	workerGroup.Wait()

	deleteWorkerTestJob(t, testStore, job.ID)

	testStore.Close()
}

// Test 2:
// A failing job should retry three times and then fail permanently.
func TestWorkerRetriesAndFailsJob(t *testing.T) {
	testStore := setupWorkerTestStore(t)

	job := Job{
		ID:         "55555555-5555-5555-5555-555555555555",
		Type:       "fail_job",
		Payload:    map[string]any{"reason": "test retry logic"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	deleteWorkerTestJob(t, testStore, job.ID)

	if err := testStore.Save(job); err != nil {
		testStore.Close()
		t.Fatalf("failed to save failing test job: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	workerGroup := StartWorkers(ctx)

	waitForJobStatus(
		t,
		testStore,
		job.ID,
		"failed",
		15*time.Second,
	)

	updatedJob, exists := testStore.Get(job.ID)
	if !exists {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatal("expected failing job to exist")
	}

	if updatedJob.Status != "failed" {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected status failed, got %s",
			updatedJob.Status,
		)
	}

	if updatedJob.Retries != 3 {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected 3 retries, got %d",
			updatedJob.Retries,
		)
	}

	if updatedJob.MaxRetries != 3 {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected max retries 3, got %d",
			updatedJob.MaxRetries,
		)
	}

	cancel()
	workerGroup.Wait()

	deleteWorkerTestJob(t, testStore, job.ID)

	testStore.Close()
}

// Test 3:
// A scheduled job must remain queued until its scheduled time.
func TestWorkerWaitsForScheduledJob(t *testing.T) {
	testStore := setupWorkerTestStore(t)

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

	deleteWorkerTestJob(t, testStore, job.ID)

	if err := testStore.Save(job); err != nil {
		testStore.Close()
		t.Fatalf("failed to save scheduled test job: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	workerGroup := StartWorkers(ctx)

	// The job should remain queued before scheduled_at.
	time.Sleep(1 * time.Second)

	currentJob, exists := testStore.Get(job.ID)
	if !exists {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatal("expected scheduled job to exist")
	}

	if currentJob.Status != "queued" {
		cancel()
		workerGroup.Wait()
		testStore.Close()

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

	cancel()
	workerGroup.Wait()

	deleteWorkerTestJob(t, testStore, job.ID)

	testStore.Close()
}

// Test 4:
// Three workers should process three jobs concurrently.
func TestMultipleWorkersProcessJobsConcurrently(t *testing.T) {
	testStore := setupWorkerTestStore(t)

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

	for _, currentJob := range jobs {
		deleteWorkerTestJob(
			t,
			testStore,
			currentJob.ID,
		)
	}

	for _, currentJob := range jobs {
		if err := testStore.Save(currentJob); err != nil {
			testStore.Close()

			t.Fatalf(
				"failed to save concurrent worker test job %s: %v",
				currentJob.ID,
				err,
			)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	startedAt := time.Now()

	workerGroup := StartWorkers(ctx)

	for _, currentJob := range jobs {
		waitForJobStatus(
			t,
			testStore,
			currentJob.ID,
			"completed",
			5*time.Second,
		)
	}

	elapsed := time.Since(startedAt)

	if elapsed >= 5*time.Second {
		cancel()
		workerGroup.Wait()
		testStore.Close()

		t.Fatalf(
			"expected concurrent processing to finish in under 5 seconds, took %s",
			elapsed,
		)
	}

	for _, currentJob := range jobs {
		completedJob, exists := testStore.Get(
			currentJob.ID,
		)

		if !exists {
			cancel()
			workerGroup.Wait()
			testStore.Close()

			t.Fatalf(
				"expected job %s to exist",
				currentJob.ID,
			)
		}

		if completedJob.Status != "completed" {
			cancel()
			workerGroup.Wait()
			testStore.Close()

			t.Fatalf(
				"expected job %s to be completed, got %s",
				currentJob.ID,
				completedJob.Status,
			)
		}

		if completedJob.Retries != 0 {
			cancel()
			workerGroup.Wait()
			testStore.Close()

			t.Fatalf(
				"expected job %s to have 0 retries, got %d",
				currentJob.ID,
				completedJob.Retries,
			)
		}
	}

	cancel()

	workerGroup.Wait()

	for _, currentJob := range jobs {
		deleteWorkerTestJob(
			t,
			testStore,
			currentJob.ID,
		)
	}

	testStore.Close()
}

// Test 5:
// Cancelling the worker context should stop all workers.
func TestWorkersStopAfterContextCancellation(t *testing.T) {
	testStore := setupWorkerTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())

	workerGroup := StartWorkers(ctx)

	// Allow workers to start.
	time.Sleep(500 * time.Millisecond)

	cancel()

	done := make(chan struct{})

	go func() {
		workerGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success: all workers stopped.

	case <-time.After(3 * time.Second):
		testStore.Close()

		t.Fatal(
			"workers did not stop after context cancellation",
		)
	}

	testStore.Close()
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

		if exists &&
			currentJob.Status == expectedStatus {

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
