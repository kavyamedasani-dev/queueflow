package job

import (
	"log"
	"time"
)

const workerCount = 3

// StartWorker starts multiple worker goroutines.
// Each worker independently asks PostgreSQL for the next available job.
func StartWorker() {
	for workerID := 1; workerID <= workerCount; workerID++ {
		go runWorker(workerID)
	}

	log.Printf(
		"Started %d QueueFlow workers",
		workerCount,
	)
}

// runWorker continuously looks for jobs that are ready to process.
func runWorker(workerID int) {
	for {
		// ClaimNextJob performs an atomic database claim.
		// FOR UPDATE SKIP LOCKED prevents two workers
		// from claiming the same queued job.
		currentJob, exists := store.ClaimNextJob()

		if !exists {
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf(
			"Worker %d claimed job %s",
			workerID,
			currentJob.ID,
		)

		// Simulate work being performed.
		time.Sleep(2 * time.Second)

		// fail_job intentionally fails so retry behavior
		// can be tested.
		if currentJob.Type == "fail_job" {
			handleJobFailure(workerID, currentJob)
			continue
		}

		_, updated := store.UpdateStatus(
			currentJob.ID,
			"completed",
		)

		if !updated {
			log.Printf(
				"Worker %d failed to mark job %s as completed",
				workerID,
				currentJob.ID,
			)
			continue
		}

		log.Printf(
			"Worker %d completed job %s",
			workerID,
			currentJob.ID,
		)
	}
}

func handleJobFailure(workerID int, currentJob Job) {
	if currentJob.Retries < currentJob.MaxRetries {
		updatedJob, exists := store.IncrementRetry(
			currentJob.ID,
		)

		if !exists {
			log.Printf(
				"Worker %d failed to increment retry for job %s",
				workerID,
				currentJob.ID,
			)
			return
		}

		_, updated := store.UpdateStatus(
			currentJob.ID,
			"queued",
		)

		if !updated {
			log.Printf(
				"Worker %d failed to requeue job %s",
				workerID,
				currentJob.ID,
			)
			return
		}

		log.Printf(
			"Worker %d: job %s failed. Retrying %d/%d",
			workerID,
			currentJob.ID,
			updatedJob.Retries,
			updatedJob.MaxRetries,
		)

		return
	}

	_, updated := store.UpdateStatus(
		currentJob.ID,
		"failed",
	)

	if !updated {
		log.Printf(
			"Worker %d failed to mark job %s as permanently failed",
			workerID,
			currentJob.ID,
		)
		return
	}

	log.Printf(
		"Worker %d: job %s failed permanently after %d retries",
		workerID,
		currentJob.ID,
		currentJob.MaxRetries,
	)
}
