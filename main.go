package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/kavyamedasani-dev/queueflow/internal/job"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]string{
		"status": "ok",
	}

	_ = json.NewEncoder(w).Encode(response)
}

func main() {
	// Load environment variables from .env.
	err := godotenv.Load()
	if err != nil {
		log.Println(
			"No .env file found, using system environment variables",
		)
	}

	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		log.Fatal(
			"DATABASE_URL environment variable is required",
		)
	}

	store, err := job.NewStore(databaseURL)
	if err != nil {
		log.Fatalf(
			"Failed to connect to PostgreSQL: %v",
			err,
		)
	}

	defer func() {
		log.Println("Closing PostgreSQL connection")
		store.Close()
	}()

	job.SetStore(store)

	log.Println("Connected to PostgreSQL")

	// Create a context used to control the worker lifecycle.
	workerContext, cancelWorkers := context.WithCancel(
		context.Background(),
	)

	// Start all QueueFlow workers.
	workerGroup := job.StartWorkers(workerContext)

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/health",
		healthHandler,
	)

	mux.HandleFunc(
		"/jobs",
		job.JobsHandler,
	)

	mux.HandleFunc(
		"/jobs/",
		job.GetJobHandler,
	)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Listen for Ctrl+C or a Docker/OS termination signal.
	shutdownSignal := make(chan os.Signal, 1)

	signal.Notify(
		shutdownSignal,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {
		log.Println(
			"QueueFlow server starting on http://localhost:8080",
		)

		err := server.ListenAndServe()

		if err != nil &&
			!errors.Is(err, http.ErrServerClosed) {

			log.Printf(
				"HTTP server error: %v",
				err,
			)
		}
	}()

	// Wait here until QueueFlow receives a shutdown signal.
	sig := <-shutdownSignal

	log.Printf(
		"Shutdown signal received: %v",
		sig,
	)

	log.Println(
		"QueueFlow graceful shutdown started",
	)

	// Stop workers from claiming NEW jobs.
	//
	// A worker that already claimed a job is allowed
	// to finish processing that job.
	cancelWorkers()

	// Stop accepting new HTTP requests and allow
	// active requests up to 10 seconds to finish.
	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	defer cancelShutdown()

	err = server.Shutdown(shutdownContext)
	if err != nil {
		log.Printf(
			"HTTP server shutdown error: %v",
			err,
		)
	}

	log.Println("HTTP server stopped")

	// Wait until all workers have completely exited.
	log.Println(
		"Waiting for workers to finish",
	)

	workerGroup.Wait()

	log.Println(
		"All workers stopped",
	)

	log.Println(
		"QueueFlow shutdown complete",
	)
}
