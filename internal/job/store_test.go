package job

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

// setupStoreTest creates a PostgreSQL-backed Store
// for integration testing.
func setupStoreTest(t *testing.T) *Store {
	t.Helper()

	_ = godotenv.Overload("../../.env.test")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for integration test")
	}

	testStore, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf(
			"failed to connect to database: %v",
			err,
		)
	}

	return testStore
}

// deleteTestJobs removes jobs by ID so tests can be
// safely run multiple times.
func deleteTestJobs(
	t *testing.T,
	testStore *Store,
	ids ...string,
) {
	t.Helper()

	for _, id := range ids {
		_, _ = testStore.db.Exec(
			t.Context(),
			"DELETE FROM jobs WHERE id = $1",
			id,
		)
	}
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save job: %v",
			err,
		)
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save test job: %v",
			err,
		)
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save test job: %v",
			err,
		)
	}

	updatedJob, exists := testStore.IncrementRetry(
		testJob.ID,
	)

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
}

// Test 4:
// A future scheduled job must not be claimed yet.
func TestClaimNextJobSkipsFutureScheduledJob(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	scheduledAt := time.Now().Add(
		10 * time.Minute,
	)

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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save scheduled job: %v",
			err,
		)
	}

	claimedJob, claimed, err := testStore.ClaimNextJob()
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}

	if claimed && claimedJob.ID == testJob.ID {
		t.Fatal(
			"future scheduled job should not have been claimed",
		)
	}

	savedJob, exists := testStore.Get(testJob.ID)

	if !exists {
		t.Fatal(
			"expected scheduled job to exist",
		)
	}

	if savedJob.Status != "queued" {
		t.Fatalf(
			"expected future scheduled job to remain queued, got %s",
			savedJob.Status,
		)
	}
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save concurrent test job: %v",
			err,
		)
	}

	type claimResult struct {
		job     Job
		claimed bool
	}

	results := make(
		chan claimResult,
		2,
	)

	var wg sync.WaitGroup

	wg.Add(2)

	claim := func() {
		defer wg.Done()

		claimedJob, claimed, err := testStore.ClaimNextJob()
		if err != nil {
			t.Fatalf("failed to claim job: %v", err)
		}
		if err != nil {
			t.Fatalf("failed to claim job: %v", err)
		}

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

	savedJob, exists := testStore.Get(
		testJob.ID,
	)

	if !exists {
		t.Fatal(
			"expected claimed job to exist",
		)
	}

	if savedJob.Status != "processing" {
		t.Fatalf(
			"expected claimed job status processing, got %s",
			savedJob.Status,
		)
	}
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

	ids := make([]string, 0, len(testJobs))

	for _, currentJob := range testJobs {
		ids = append(
			ids,
			currentJob.ID,
		)
	}

	deleteTestJobs(
		t,
		testStore,
		ids...,
	)

	defer deleteTestJobs(
		t,
		testStore,
		ids...,
	)

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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save cancellation test job: %v",
			err,
		)
	}

	cancelledJob, cancelled :=
		testStore.CancelJob(testJob.ID)

	if !cancelled {
		t.Fatal(
			"expected queued job cancellation to succeed",
		)
	}

	if cancelledJob.Status != "cancelled" {
		t.Fatalf(
			"expected status cancelled, got %s",
			cancelledJob.Status,
		)
	}

	savedJob, exists := testStore.Get(
		testJob.ID,
	)

	if !exists {
		t.Fatal(
			"expected cancelled job to exist",
		)
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save processing test job: %v",
			err,
		)
	}

	_, cancelled :=
		testStore.CancelJob(testJob.ID)

	if cancelled {
		t.Fatal(
			"expected processing job cancellation to fail",
		)
	}

	savedJob, exists := testStore.Get(
		testJob.ID,
	)

	if !exists {
		t.Fatal(
			"expected processing job to exist",
		)
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save claim cancellation test job: %v",
			err,
		)
	}

	_, cancelled :=
		testStore.CancelJob(testJob.ID)

	if !cancelled {
		t.Fatal(
			"expected job cancellation to succeed",
		)
	}

	claimedJob, claimed, err := testStore.ClaimNextJob()
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}

	if claimed &&
		claimedJob.ID == testJob.ID {

		t.Fatal(
			"cancelled job must not be claimed",
		)
	}

	savedJob, exists :=
		testStore.Get(testJob.ID)

	if !exists {
		t.Fatal(
			"expected cancelled job to exist",
		)
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

	deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		testJob.ID,
	)

	if err := testStore.Save(testJob); err != nil {
		t.Fatalf(
			"failed to save double cancellation test job: %v",
			err,
		)
	}

	_, cancelled :=
		testStore.CancelJob(testJob.ID)

	if !cancelled {
		t.Fatal(
			"expected first cancellation to succeed",
		)
	}

	_, cancelledAgain :=
		testStore.CancelJob(testJob.ID)

	if cancelledAgain {
		t.Fatal(
			"expected second cancellation to fail",
		)
	}
}

// Test 11:
// ListFiltered should correctly filter jobs by status,
// type, both status and type, or no filters.
func TestStoreListFiltered(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	baseTime := time.Now()

	testJobs := []Job{
		{
			ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
			Type:       "email",
			Payload:    map[string]any{"message": "queued email"},
			Status:     "queued",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime,
		},
		{
			ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2",
			Type:       "email",
			Payload:    map[string]any{"message": "completed email"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(time.Millisecond),
		},
		{
			ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb3",
			Type:       "report",
			Payload:    map[string]any{"message": "failed report"},
			Status:     "failed",
			Retries:    3,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(2 * time.Millisecond),
		},
		{
			ID:         "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb4",
			Type:       "report",
			Payload:    map[string]any{"message": "completed report"},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(3 * time.Millisecond),
		},
	}

	ids := make([]string, 0, len(testJobs))

	for _, currentJob := range testJobs {
		ids = append(
			ids,
			currentJob.ID,
		)
	}

	deleteTestJobs(
		t,
		testStore,
		ids...,
	)

	defer deleteTestJobs(
		t,
		testStore,
		ids...,
	)

	for _, currentJob := range testJobs {
		if err := testStore.Save(currentJob); err != nil {
			t.Fatalf(
				"failed to save filtering test job %s: %v",
				currentJob.ID,
				err,
			)
		}
	}

	t.Run("filter by status", func(t *testing.T) {
		jobs := testStore.ListFiltered(
			JobFilter{
				Status: "completed",
			},
		)

		foundEmail := false
		foundReport := false

		for _, currentJob := range jobs {
			if currentJob.Status != "completed" {
				t.Fatalf(
					"expected only completed jobs, got status %s",
					currentJob.Status,
				)
			}

			if currentJob.ID == testJobs[1].ID {
				foundEmail = true
			}

			if currentJob.ID == testJobs[3].ID {
				foundReport = true
			}
		}

		if !foundEmail || !foundReport {
			t.Fatal(
				"expected both filtering test completed jobs to be returned",
			)
		}
	})

	t.Run("filter by type", func(t *testing.T) {
		jobs := testStore.ListFiltered(
			JobFilter{
				Type: "email",
			},
		)

		foundQueued := false
		foundCompleted := false

		for _, currentJob := range jobs {
			if currentJob.Type != "email" {
				t.Fatalf(
					"expected only email jobs, got type %s",
					currentJob.Type,
				)
			}

			if currentJob.ID == testJobs[0].ID {
				foundQueued = true
			}

			if currentJob.ID == testJobs[1].ID {
				foundCompleted = true
			}
		}

		if !foundQueued || !foundCompleted {
			t.Fatal(
				"expected both filtering test email jobs to be returned",
			)
		}
	})

	t.Run(
		"filter by status and type",
		func(t *testing.T) {
			jobs := testStore.ListFiltered(
				JobFilter{
					Status: "completed",
					Type:   "report",
				},
			)

			found := false

			for _, currentJob := range jobs {
				if currentJob.Status != "completed" {
					t.Fatalf(
						"expected completed status, got %s",
						currentJob.Status,
					)
				}

				if currentJob.Type != "report" {
					t.Fatalf(
						"expected report type, got %s",
						currentJob.Type,
					)
				}

				if currentJob.ID == testJobs[3].ID {
					found = true
				}
			}

			if !found {
				t.Fatal(
					"expected completed report filtering test job",
				)
			}
		},
	)

	t.Run("no filters", func(t *testing.T) {
		jobs := testStore.ListFiltered(
			JobFilter{},
		)

		found := make(map[string]bool)

		for _, currentJob := range jobs {
			for _, testJob := range testJobs {
				if currentJob.ID == testJob.ID {
					found[currentJob.ID] = true
				}
			}
		}

		for _, testJob := range testJobs {
			if !found[testJob.ID] {
				t.Fatalf(
					"expected job %s when no filters are applied",
					testJob.ID,
				)
			}
		}
	})
}

// Test 12:
// Verify limit, offset, limit+offset, and filtering
// combined with pagination.
func TestStoreListFilteredPagination(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	baseTime := time.Now()

	jobs := []Job{
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc1",
			Type:       "pagination_email",
			Payload:    map[string]any{"number": 1},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime,
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc2",
			Type:       "pagination_email",
			Payload:    map[string]any{"number": 2},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(time.Second),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc3",
			Type:       "pagination_report",
			Payload:    map[string]any{"number": 3},
			Status:     "failed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(2 * time.Second),
		},
		{
			ID:         "cccccccc-cccc-cccc-cccc-ccccccccccc4",
			Type:       "pagination_email",
			Payload:    map[string]any{"number": 4},
			Status:     "completed",
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  baseTime.Add(3 * time.Second),
		},
	}

	ids := make([]string, 0, len(jobs))

	for _, currentJob := range jobs {
		ids = append(
			ids,
			currentJob.ID,
		)
	}

	deleteTestJobs(
		t,
		testStore,
		ids...,
	)

	defer deleteTestJobs(
		t,
		testStore,
		ids...,
	)

	for _, currentJob := range jobs {
		if err := testStore.Save(currentJob); err != nil {
			t.Fatalf(
				"failed to save pagination test job %s: %v",
				currentJob.ID,
				err,
			)
		}
	}

	t.Run("limit only", func(t *testing.T) {
		result := testStore.ListFiltered(
			JobFilter{
				Type:  "pagination_email",
				Limit: 2,
			},
		)

		if len(result) != 2 {
			t.Fatalf(
				"expected 2 jobs, got %d",
				len(result),
			)
		}

		if result[0].ID != jobs[0].ID {
			t.Errorf(
				"expected first job %s, got %s",
				jobs[0].ID,
				result[0].ID,
			)
		}

		if result[1].ID != jobs[1].ID {
			t.Errorf(
				"expected second job %s, got %s",
				jobs[1].ID,
				result[1].ID,
			)
		}
	})

	t.Run("offset only", func(t *testing.T) {
		result := testStore.ListFiltered(
			JobFilter{
				Type:   "pagination_email",
				Offset: 2,
			},
		)

		if len(result) != 1 {
			t.Fatalf(
				"expected 1 job after offset, got %d",
				len(result),
			)
		}

		if result[0].ID != jobs[3].ID {
			t.Errorf(
				"expected job %s, got %s",
				jobs[3].ID,
				result[0].ID,
			)
		}
	})

	t.Run(
		"limit and offset",
		func(t *testing.T) {
			result := testStore.ListFiltered(
				JobFilter{
					Type:   "pagination_email",
					Limit:  1,
					Offset: 1,
				},
			)

			if len(result) != 1 {
				t.Fatalf(
					"expected 1 job, got %d",
					len(result),
				)
			}

			if result[0].ID != jobs[1].ID {
				t.Errorf(
					"expected job %s, got %s",
					jobs[1].ID,
					result[0].ID,
				)
			}
		},
	)

	t.Run(
		"filter and pagination together",
		func(t *testing.T) {
			result := testStore.ListFiltered(
				JobFilter{
					Status: "completed",
					Type:   "pagination_email",
					Limit:  1,
					Offset: 1,
				},
			)

			if len(result) != 1 {
				t.Fatalf(
					"expected 1 job, got %d",
					len(result),
				)
			}

			if result[0].ID != jobs[1].ID {
				t.Errorf(
					"expected job %s, got %s",
					jobs[1].ID,
					result[0].ID,
				)
			}
		},
	)
}

// Test 13:
//
// Higher-priority queued jobs should be claimed before
// normal- and low-priority jobs, regardless of creation order.
func TestClaimNextJobRespectsPriority(t *testing.T) {
	testStore := setupStoreTest(t)
	defer testStore.Close()

	baseTime := time.Now()

	lowJob := Job{
		ID:         "dddddddd-dddd-dddd-dddd-ddddddddddd1",
		Type:       "priority_test",
		Payload:    map[string]any{"priority": "low"},
		Status:     "queued",
		Priority:   "low",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  baseTime,
	}

	normalJob := Job{
		ID:         "dddddddd-dddd-dddd-dddd-ddddddddddd2",
		Type:       "priority_test",
		Payload:    map[string]any{"priority": "normal"},
		Status:     "queued",
		Priority:   "normal",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  baseTime.Add(time.Second),
	}

	highJob := Job{
		ID:         "dddddddd-dddd-dddd-dddd-ddddddddddd3",
		Type:       "priority_test",
		Payload:    map[string]any{"priority": "high"},
		Status:     "queued",
		Priority:   "high",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  baseTime.Add(2 * time.Second),
	}

	deleteTestJobs(
		t,
		testStore,
		lowJob.ID,
		normalJob.ID,
		highJob.ID,
	)

	defer deleteTestJobs(
		t,
		testStore,
		lowJob.ID,
		normalJob.ID,
		highJob.ID,
	)

	if err := testStore.Save(lowJob); err != nil {
		t.Fatalf(
			"failed to save low-priority job: %v",
			err,
		)
	}

	if err := testStore.Save(normalJob); err != nil {
		t.Fatalf(
			"failed to save normal-priority job: %v",
			err,
		)
	}

	if err := testStore.Save(highJob); err != nil {
		t.Fatalf(
			"failed to save high-priority job: %v",
			err,
		)
	}

	firstClaimed, claimed, err := testStore.ClaimNextJob()
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	if !claimed {
		t.Fatal("expected a job to be claimed")
	}

	if firstClaimed.ID != highJob.ID {
		t.Fatalf(
			"expected high-priority job %s first, got %s",
			highJob.ID,
			firstClaimed.ID,
		)
	}

	if firstClaimed.Priority != "high" {
		t.Fatalf(
			"expected first priority high, got %s",
			firstClaimed.Priority,
		)
	}

	secondClaimed, claimed, err := testStore.ClaimNextJob()
	if err != nil {
		t.Fatalf("failed to claim second job: %v", err)
	}
	if !claimed {
		t.Fatal("expected second job to be claimed")
	}

	if secondClaimed.ID != normalJob.ID {
		t.Fatalf(
			"expected normal-priority job %s second, got %s",
			normalJob.ID,
			secondClaimed.ID,
		)
	}

	if secondClaimed.Priority != "normal" {
		t.Fatalf(
			"expected second priority normal, got %s",
			secondClaimed.Priority,
		)
	}

	thirdClaimed, claimed, err := testStore.ClaimNextJob()
	if err != nil {
		t.Fatalf("failed to claim third job: %v", err)
	}
	if !claimed {
		t.Fatal("expected third job to be claimed")
	}

	if thirdClaimed.ID != lowJob.ID {
		t.Fatalf(
			"expected low-priority job %s third, got %s",
			lowJob.ID,
			thirdClaimed.ID,
		)
	}

	if thirdClaimed.Priority != "low" {
		t.Fatalf(
			"expected third priority low, got %s",
			thirdClaimed.Priority,
		)
	}
}
