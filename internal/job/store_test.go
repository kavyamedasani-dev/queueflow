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
// A future scheduled job must not be claimed yet.
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
// Two concurrent claim attempts must not successfully
// claim the same job twice.
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

	go claim()
	go claim()

	wg.Wait()
	close(results)

	claimCount := 0

	for result := range results {
		if result.claimed &&
			result.job.ID == testJob.ID {

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

// Test 6:
// Stats should accurately count jobs by status,
// including cancelled jobs.
func TestStoreStats(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJobs := []Job{
		{
			ID:         "99999999-9999-9999-9999-999999999991",
			Type:       "stats_test",
			Payload:    map[string]any{"number": 1},
			Status:     "queued",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "99999999-9999-9999-9999-999999999992",
			Type:       "stats_test",
			Payload:    map[string]any{"number": 2},
			Status:     "processing",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "99999999-9999-9999-9999-999999999993",
			Type:       "stats_test",
			Payload:    map[string]any{"number": 3},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "99999999-9999-9999-9999-999999999994",
			Type:       "stats_test",
			Payload:    map[string]any{"number": 4},
			Status:     "failed",
			Retries:    3,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "99999999-9999-9999-9999-999999999995",
			Type:       "stats_test",
			Payload:    map[string]any{"number": 5},
			Status:     "cancelled",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		},
	}

	for _, currentJob := range testJobs {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			currentJob.ID,
		)
	}

	// Capture the existing database counts first.
	before, err := testStore.Stats()
	if err != nil {
		t.Fatalf(
			"failed to retrieve initial queue stats: %v",
			err,
		)
	}

	for _, currentJob := range testJobs {
		if err := testStore.Save(currentJob); err != nil {
			t.Fatalf(
				"failed to save stats test job %s: %v",
				currentJob.ID,
				err,
			)
		}
	}

	after, err := testStore.Stats()
	if err != nil {
		t.Fatalf(
			"failed to retrieve queue stats: %v",
			err,
		)
	}

	if after.Queued != before.Queued+1 {
		t.Fatalf(
			"expected queued count to increase by 1, before=%d after=%d",
			before.Queued,
			after.Queued,
		)
	}

	if after.Processing != before.Processing+1 {
		t.Fatalf(
			"expected processing count to increase by 1, before=%d after=%d",
			before.Processing,
			after.Processing,
		)
	}

	if after.Completed != before.Completed+1 {
		t.Fatalf(
			"expected completed count to increase by 1, before=%d after=%d",
			before.Completed,
			after.Completed,
		)
	}

	if after.Failed != before.Failed+1 {
		t.Fatalf(
			"expected failed count to increase by 1, before=%d after=%d",
			before.Failed,
			after.Failed,
		)
	}

	if after.Cancelled != before.Cancelled+1 {
		t.Fatalf(
			"expected cancelled count to increase by 1, before=%d after=%d",
			before.Cancelled,
			after.Cancelled,
		)
	}

	if after.Total != before.Total+5 {
		t.Fatalf(
			"expected total count to increase by 5, before=%d after=%d",
			before.Total,
			after.Total,
		)
	}

	for _, currentJob := range testJobs {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			currentJob.ID,
		)
	}
}

// Test 7:
// A queued job should be successfully cancelled.
func TestStoreCancelQueuedJob(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
		Type:       "cancel_test",
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
			"failed to save cancellation test job: %v",
			err,
		)
	}

	cancelledJob, cancelled := testStore.CancelJob(testJob.ID)

	if !cancelled {
		t.Fatal("expected queued job cancellation to succeed")
	}

	if cancelledJob.Status != "cancelled" {
		t.Fatalf(
			"expected status cancelled, got %s",
			cancelledJob.Status,
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected cancelled job to exist")
	}

	if savedJob.Status != "cancelled" {
		t.Fatalf(
			"expected persisted status cancelled, got %s",
			savedJob.Status,
		)
	}
}

// Test 8:
// A processing job must not be cancelled.
func TestStoreCannotCancelProcessingJob(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2",
		Type:       "cancel_processing_test",
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
			"failed to save processing test job: %v",
			err,
		)
	}

	_, cancelled := testStore.CancelJob(testJob.ID)

	if cancelled {
		t.Fatal("expected processing job cancellation to fail")
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected processing job to exist")
	}

	if savedJob.Status != "processing" {
		t.Fatalf(
			"expected job to remain processing, got %s",
			savedJob.Status,
		)
	}
}

// Test 9:
// Once cancelled, a job must not be claimed by a worker.
func TestCancelledJobCannotBeClaimed(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3",
		Type:       "cancel_claim_test",
		Payload:    map[string]any{"message": "do not process"},
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
			"failed to save claim cancellation test job: %v",
			err,
		)
	}

	_, cancelled := testStore.CancelJob(testJob.ID)
	if !cancelled {
		t.Fatal("expected job cancellation to succeed")
	}

	claimedJob, claimed := testStore.ClaimNextJob()

	if claimed && claimedJob.ID == testJob.ID {
		t.Fatal("cancelled job must not be claimed")
	}

	savedJob, exists := testStore.Get(testJob.ID)
	if !exists {
		t.Fatal("expected cancelled job to exist")
	}

	if savedJob.Status != "cancelled" {
		t.Fatalf(
			"expected cancelled job to remain cancelled, got %s",
			savedJob.Status,
		)
	}
}

// Test 10:
// Cancelling the same job twice must not succeed twice.
func TestStoreCannotCancelJobTwice(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	testJob := Job{
		ID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4",
		Type:       "double_cancel_test",
		Payload:    map[string]any{"message": "cancel once"},
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
			"failed to save double cancellation test job: %v",
			err,
		)
	}

	_, cancelled := testStore.CancelJob(testJob.ID)
	if !cancelled {
		t.Fatal("expected first cancellation to succeed")
	}

	_, cancelledAgain := testStore.CancelJob(testJob.ID)

	if cancelledAgain {
		t.Fatal("expected second cancellation to fail")
	}
}
