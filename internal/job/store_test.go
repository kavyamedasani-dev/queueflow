package job

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func setupStoreTest(t *testing.T) *Store {
	t.Helper()

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for integration test")
	}

	testStore, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	return testStore
}

// Test 1:
// Save a job to PostgreSQL and verify that it can be retrieved.
func TestStoreSaveAndGet(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "11111111-1111-1111-1111-111111111111",
		Type:       "test_job",
		Payload:    map[string]any{"message": "hello"},
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

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf("failed to save job: %v", err)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected saved job to exist")
	}

	if savedJob.ID != testJob.ID {
		t.Errorf(
			"expected ID %s, got %s",
			testJob.ID,
			savedJob.ID,
		)
	}

	if savedJob.Type != testJob.Type {
		t.Errorf(
			"expected type %s, got %s",
			testJob.Type,
			savedJob.Type,
		)
	}

	if savedJob.Status != "queued" {
		t.Errorf(
			"expected status queued, got %s",
			savedJob.Status,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 2:
// Update a job status and verify PostgreSQL persisted it.
func TestStoreUpdateStatus(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "22222222-2222-2222-2222-222222222222",
		Type:       "status_test",
		Payload:    map[string]any{"message": "testing status"},
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

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf("failed to save test job: %v", err)
	}

	updatedJob, exists := testStore.UpdateStatus(
		testJob.ID,
		"completed",
	)

	if !exists {
		t.Fatal("expected status update to succeed")
	}

	if updatedJob.Status != "completed" {
		t.Fatalf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected updated job to exist")
	}

	if savedJob.Status != "completed" {
		t.Fatalf(
			"expected persisted status completed, got %s",
			savedJob.Status,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 3:
// Increment the retry counter and verify PostgreSQL persisted it.
func TestStoreIncrementRetry(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "33333333-3333-3333-3333-333333333333",
		Type:       "retry_test",
		Payload:    map[string]any{"message": "testing retry"},
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

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf("failed to save test job: %v", err)
	}

	updatedJob, exists := testStore.IncrementRetry(testJob.ID)
	if !exists {
		t.Fatal("expected retry increment to succeed")
	}

	if updatedJob.Retries != 1 {
		t.Fatalf(
			"expected retries to be 1, got %d",
			updatedJob.Retries,
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected retry test job to exist")
	}

	if savedJob.Retries != 1 {
		t.Fatalf(
			"expected persisted retries to be 1, got %d",
			savedJob.Retries,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 4:
// A job scheduled for the future must not be claimed yet.
func TestClaimNextJobSkipsFutureScheduledJob(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	scheduledAt := time.Now().Add(10 * time.Minute)

	testJob := Job{
		ID:          "77777777-7777-7777-7777-777777777777",
		Type:        "scheduled_test",
		Payload:     map[string]any{"message": "not ready"},
		Status:      "queued",
		Retries:     0,
		MaxRetries:  3,
		CreatedAt:   time.Now(),
		ScheduledAt: &scheduledAt,
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf("failed to save scheduled job: %v", err)
	}

	claimedJob, claimed := testStore.ClaimNextJob()

	if claimed && claimedJob.ID == testJob.ID {
		t.Fatal("future scheduled job should not have been claimed")
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected scheduled job to exist")
	}

	if savedJob.Status != "queued" {
		t.Fatalf(
			"expected future scheduled job to remain queued, got %s",
			savedJob.Status,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 5:
// Two concurrent claim attempts must not successfully claim
// the same job twice.
func TestClaimNextJobPreventsDuplicateClaim(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "88888888-8888-8888-8888-888888888888",
		Type:       "concurrent_test",
		Payload:    map[string]any{"message": "claim me once"},
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

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf("failed to save concurrent test job: %v", err)
	}

	type claimResult struct {
		job     Job
		claimed bool
	}

	results := make(chan claimResult, 2)

	var wg sync.WaitGroup

	wg.Add(2)

	claim := func() {
		defer wg.Done()

		claimedJob, claimed := testStore.ClaimNextJob()

		results <- claimResult{
			job:     claimedJob,
			claimed: claimed,
		}
	}

	// Simulate two workers attempting to claim work
	// at approximately the same time.
	go claim()
	go claim()

	wg.Wait()
	close(results)

	claimCount := 0

	for result := range results {
		if result.claimed && result.job.ID == testJob.ID {
			claimCount++
		}
	}

	if claimCount != 1 {
		t.Fatalf(
			"expected job to be claimed exactly once, got %d claims",
			claimCount,
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected claimed job to exist")
	}

	if savedJob.Status != "processing" {
		t.Fatalf(
			"expected claimed job status processing, got %s",
			savedJob.Status,
		)
	}

	_, _ = testStore.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}
