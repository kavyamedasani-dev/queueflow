package main

import (
	"encoding/json"
	"log"
	"net/http"

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
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", job.CreateJobHandler)
	http.HandleFunc("/jobs/", job.GetJobHandler)

	log.Println("QueueFlow server starting on http://localhost:8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
