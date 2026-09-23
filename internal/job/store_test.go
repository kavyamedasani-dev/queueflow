package job

import (
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

// Test 1:
// Save a job to PostgreSQL and verify that it can be retrieved.
func TestStoreSaveAndGet(t *testing.T) {
	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for integration test")
	}

	store, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer store.Close()

	testJob := Job{
		ID:         "11111111-1111-1111-1111-111111111111",
		Type:       "test_job",
		Payload:    map[string]any{"message": "hello"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	// Remove an old copy of this test job if it exists.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	err = store.Save(testJob)
	if err != nil {
		t.Fatalf("failed to save job: %v", err)
	}

	savedJob, exists := store.Get(testJob.ID)
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

	// Clean up test data.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 2:
// Update a job's status and verify that the change
// is persisted in PostgreSQL.
func TestStoreUpdateStatus(t *testing.T) {
	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for integration test")
	}

	store, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer store.Close()

	testJob := Job{
		ID:         "22222222-2222-2222-2222-222222222222",
		Type:       "status_test",
		Payload:    map[string]any{"message": "testing status"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	// Remove an old copy of this test job if it exists.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	err = store.Save(testJob)
	if err != nil {
		t.Fatalf("failed to save test job: %v", err)
	}

	// Change the job from queued to completed.
	updatedJob, exists := store.UpdateStatus(
		testJob.ID,
		"completed",
	)

	if !exists {
		t.Fatal("expected status update to succeed")
	}

	if updatedJob.Status != "completed" {
		t.Errorf(
			"expected status completed, got %s",
			updatedJob.Status,
		)
	}

	// Read it again to verify PostgreSQL stored the change.
	savedJob, exists := store.Get(testJob.ID)
	if !exists {
		t.Fatal("expected updated job to exist")
	}

	if savedJob.Status != "completed" {
		t.Errorf(
			"expected persisted status completed, got %s",
			savedJob.Status,
		)
	}

	// Clean up test data.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}

// Test 3:
// Increment a job's retry counter and verify that
// the new value is persisted in PostgreSQL.
func TestStoreIncrementRetry(t *testing.T) {
	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for integration test")
	}

	store, err := NewStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer store.Close()

	testJob := Job{
		ID:         "33333333-3333-3333-3333-333333333333",
		Type:       "retry_test",
		Payload:    map[string]any{"message": "testing retry"},
		Status:     "queued",
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	// Remove an old copy of this test job if it exists.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)

	err = store.Save(testJob)
	if err != nil {
		t.Fatalf("failed to save test job: %v", err)
	}

	// Increase retries from 0 to 1.
	updatedJob, exists := store.IncrementRetry(testJob.ID)
	if !exists {
		t.Fatal("expected retry increment to succeed")
	}

	if updatedJob.Retries != 1 {
		t.Errorf(
			"expected retries to be 1, got %d",
			updatedJob.Retries,
		)
	}

	// Read the job again to verify PostgreSQL stored it.
	savedJob, exists := store.Get(testJob.ID)
	if !exists {
		t.Fatal("expected retry test job to exist")
	}

	if savedJob.Retries != 1 {
		t.Errorf(
			"expected persisted retries to be 1, got %d",
			savedJob.Retries,
		)
	}

	// Clean up test data.
	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}
