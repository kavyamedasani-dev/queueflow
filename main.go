package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/kavyamedasani-dev/queueflow/internal/job"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]string{
		"status": "ok",
	}

	json.NewEncoder(w).Encode(response)
}

func main() {
	// Load environment variables from .env.
	// If the file does not exist, QueueFlow can still use
	// environment variables configured by the operating system.
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	store, err := job.NewStore(databaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer store.Close()

	job.SetStore(store)

	log.Println("Connected to PostgreSQL")

	job.StartWorker()

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", job.JobsHandler)
	http.HandleFunc("/jobs/", job.GetJobHandler)

	log.Println("QueueFlow server starting on http://localhost:8080")

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
