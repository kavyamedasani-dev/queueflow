package job

import (
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

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
		t.Errorf("expected ID %s, got %s", testJob.ID, savedJob.ID)
	}

	if savedJob.Type != testJob.Type {
		t.Errorf("expected type %s, got %s", testJob.Type, savedJob.Type)
	}

	if savedJob.Status != "queued" {
		t.Errorf("expected status queued, got %s", savedJob.Status)
	}

	_, _ = store.db.Exec(
		t.Context(),
		"DELETE FROM jobs WHERE id = $1",
		testJob.ID,
	)
}
