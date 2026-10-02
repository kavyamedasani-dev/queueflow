package job

import (
	"log"
	"time"
)

func StartWorker() {
	go func() {
		for {
			// Atomically claim one job that is ready to run.
			// PostgreSQL ensures multiple workers cannot claim
			// the same queued job.
			currentJob, exists := store.ClaimNextJob()

			if !exists {
				// No job is currently ready.
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf(
				"Worker claimed job %s",
				currentJob.ID,
			)

			// Simulate the worker doing some work.
			time.Sleep(2 * time.Second)

			// This job type intentionally fails so
			// we can test retry behavior.
			if currentJob.Type == "fail_job" {
				handleJobFailure(currentJob)
				continue
			}

			_, updated := store.UpdateStatus(
				currentJob.ID,
				"completed",
			)

			if !updated {
				log.Printf(
					"Worker failed to mark job %s as completed",
					currentJob.ID,
				)
				continue
			}

			log.Printf(
				"Worker completed job %s",
				currentJob.ID,
			)
		}
	}()
}

func handleJobFailure(currentJob Job) {
	if currentJob.Retries < currentJob.MaxRetries {
		updatedJob, exists := store.IncrementRetry(
			currentJob.ID,
		)

		if !exists {
			log.Printf(
				"Failed to increment retry for job %s",
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
				"Failed to requeue job %s",
				currentJob.ID,
			)
			return
		}

		log.Printf(
			"Job %s failed. Retrying %d/%d",
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
			"Failed to mark job %s as permanently failed",
			currentJob.ID,
		)
		return
	}

	log.Printf(
		"Job %s failed permanently after %d retries",
		currentJob.ID,
		currentJob.MaxRetries,
	)
}
