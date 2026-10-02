package job

import (
	"log"
	"time"
)

func StartWorker() {
	go func() {
		for {
			jobs := store.List()

			for _, currentJob := range jobs {
				// Worker only processes queued jobs.
				if currentJob.Status != "queued" {
					continue
				}

				// If the job has a scheduled time and that time
				// has not arrived yet, leave it queued.
				if currentJob.ScheduledAt != nil &&
					time.Now().Before(*currentJob.ScheduledAt) {

					continue
				}

				log.Printf(
					"Worker picked up job %s",
					currentJob.ID,
				)

				store.UpdateStatus(
					currentJob.ID,
					"processing",
				)

				// Simulate the worker doing some work.
				time.Sleep(2 * time.Second)

				// This job type intentionally fails so
				// we can test retry behavior.
				if currentJob.Type == "fail_job" {
					handleJobFailure(currentJob)
					continue
				}

				store.UpdateStatus(
					currentJob.ID,
					"completed",
				)

				log.Printf(
					"Worker completed job %s",
					currentJob.ID,
				)
			}

			// Check the queue once every second.
			time.Sleep(1 * time.Second)
		}
	}()
}

func handleJobFailure(currentJob Job) {
	if currentJob.Retries < currentJob.MaxRetries {
		updatedJob, exists := store.IncrementRetry(
			currentJob.ID,
		)

		if !exists {
			return
		}

		store.UpdateStatus(
			currentJob.ID,
			"queued",
		)

		log.Printf(
			"Job %s failed. Retrying %d/%d",
			currentJob.ID,
			updatedJob.Retries,
			updatedJob.MaxRetries,
		)

		return
	}

	store.UpdateStatus(
		currentJob.ID,
		"failed",
	)

	log.Printf(
		"Job %s failed permanently after %d retries",
		currentJob.ID,
		currentJob.MaxRetries,
	)
}
