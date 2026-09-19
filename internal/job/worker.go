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
				if currentJob.Status != "queued" {
					continue
				}

				log.Printf("Worker picked up job %s", currentJob.ID)

				store.UpdateStatus(currentJob.ID, "processing")

				time.Sleep(2 * time.Second)

				store.UpdateStatus(currentJob.ID, "completed")

				log.Printf("Worker completed job %s", currentJob.ID)
			}

			time.Sleep(1 * time.Second)
		}
	}()
}
