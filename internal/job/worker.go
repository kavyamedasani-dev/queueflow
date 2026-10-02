package job

import (
	"context"
	"log"
	"sync"
	"time"
)

const workerCount = 3

// StartWorkers starts multiple worker goroutines.
//
// The returned WaitGroup allows main.go to wait until
// every worker has completely stopped.
func StartWorkers(
	ctx context.Context,
) *sync.WaitGroup {

	var wg sync.WaitGroup

	wg.Add(workerCount)

	for workerID := 1; workerID <= workerCount; workerID++ {
		go runWorker(
			ctx,
			workerID,
			&wg,
		)
	}

	log.Printf(
		"Started %d QueueFlow workers",
		workerCount,
	)

	return &wg
}

// runWorker continuously claims and processes jobs.
//
// When ctx is cancelled, the worker stops claiming
// new jobs. If it is already processing a job,
// that job is allowed to finish first.
func runWorker(
	ctx context.Context,
	workerID int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		// Check whether QueueFlow is shutting down
		// before claiming another job.
		select {
		case <-ctx.Done():
			log.Printf(
				"Worker %d stopped",
				workerID,
			)
			return

		default:
		}

		currentJob, exists := store.ClaimNextJob()

		if !exists {
			// Instead of blindly sleeping for one second,
			// wait for either:
			//
			// 1. the next polling interval, or
			// 2. a shutdown signal.
			select {
			case <-ctx.Done():
				log.Printf(
					"Worker %d stopped",
					workerID,
				)
				return

			case <-time.After(1 * time.Second):
				continue
			}
		}

		log.Printf(
			"Worker %d claimed job %s",
			workerID,
			currentJob.ID,
		)

		// Simulate work being performed.
		//
		// Once a job has been claimed, we intentionally
		// allow it to finish even if shutdown begins.
		time.Sleep(2 * time.Second)

		if currentJob.Type == "fail_job" {
			handleJobFailure(
				workerID,
				currentJob,
			)
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

func handleJobFailure(
	workerID int,
	currentJob Job,
) {
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
