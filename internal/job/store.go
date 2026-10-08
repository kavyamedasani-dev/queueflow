package job

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueueStats struct {
	Queued     int `json:"queued"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
	Total      int `json:"total"`
}

// JobFilter contains optional filters and pagination
// settings used when listing jobs.
type JobFilter struct {
	Status string
	Type   string
	Limit  int
	Offset int
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(databaseURL string) (*Store, error) {
	db, err := pgxpool.New(
		context.Background(),
		databaseURL,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to create database pool: %w",
			err,
		)
	}

	if err := db.Ping(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf(
			"unable to connect to database: %w",
			err,
		)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) Save(job Job) error {
	if job.Priority == "" {
		job.Priority = "normal"
	}

	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		context.Background(),
		`
		INSERT INTO jobs
		(
			id,
			type,
			payload,
			status,
			priority,
			retries,
			max_retries,
			created_at,
			scheduled_at
		)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`,
		job.ID,
		job.Type,
		payload,
		job.Status,
		job.Priority,
		job.Retries,
		job.MaxRetries,
		job.CreatedAt,
		job.ScheduledAt,
	)

	return err
}

func (s *Store) Get(id string) (Job, bool) {
	var job Job
	var payload []byte

	err := s.db.QueryRow(
		context.Background(),
		`
		SELECT
			id,
			type,
			payload,
			status,
			priority,
			retries,
			max_retries,
			created_at,
			scheduled_at
		FROM jobs
		WHERE id = $1
		`,
		id,
	).Scan(
		&job.ID,
		&job.Type,
		&payload,
		&job.Status,
		&job.Priority,
		&job.Retries,
		&job.MaxRetries,
		&job.CreatedAt,
		&job.ScheduledAt,
	)

	if err != nil {
		return Job{}, false
	}

	if len(payload) > 0 {
		if err := json.Unmarshal(
			payload,
			&job.Payload,
		); err != nil {
			return Job{}, false
		}
	}

	return job, true
}

// List returns every job without filtering or pagination.
func (s *Store) List() []Job {
	return s.ListFiltered(JobFilter{})
}

// ListFiltered returns jobs matching the supplied filters
// and optional pagination settings.
//
// Limit <= 0 means no limit.
// Offset < 0 is treated as zero.
func (s *Store) ListFiltered(filter JobFilter) []Job {
	if filter.Limit < 0 {
		filter.Limit = 0
	}

	if filter.Offset < 0 {
		filter.Offset = 0
	}

	rows, err := s.db.Query(
		context.Background(),
		`
		SELECT
			id,
			type,
			payload,
			status,
			priority,
			retries,
			max_retries,
			created_at,
			scheduled_at
		FROM jobs
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR type = $2)
		ORDER BY created_at ASC
		LIMIT NULLIF($3, 0)
		OFFSET $4
		`,
		filter.Status,
		filter.Type,
		filter.Limit,
		filter.Offset,
	)

	if err != nil {
		return []Job{}
	}

	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		var job Job
		var payload []byte

		err := rows.Scan(
			&job.ID,
			&job.Type,
			&payload,
			&job.Status,
			&job.Priority,
			&job.Retries,
			&job.MaxRetries,
			&job.CreatedAt,
			&job.ScheduledAt,
		)

		if err != nil {
			continue
		}

		if len(payload) > 0 {
			if err := json.Unmarshal(
				payload,
				&job.Payload,
			); err != nil {
				continue
			}
		}

		jobs = append(jobs, job)
	}

	return jobs
}

func (s *Store) UpdateStatus(id string, status string) (Job, bool) {
	result, err := s.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET
			status = $1,
			processing_started_at = CASE
				WHEN $3::text = 'processing'
					THEN COALESCE(processing_started_at, NOW())
				ELSE NULL
			END
		WHERE id = $2
		`,
		status,
		id,
		status,
	)

	if err != nil {
		log.Printf("UpdateStatus database error: %v", err)
		return Job{}, false
	}

	if result.RowsAffected() == 0 {
		log.Printf("UpdateStatus: job %s not found", id)
		return Job{}, false
	}

	return s.Get(id)
}

func (s *Store) IncrementRetry(
	id string,
) (Job, bool) {
	result, err := s.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET retries = retries + 1
		WHERE id = $1
		`,
		id,
	)

	if err != nil {
		log.Printf("IncrementRetry database error: %v", err)
		return Job{}, false
	}

	if result.RowsAffected() == 0 {
		return Job{}, false
	}

	return s.Get(id)
}

// CancelJob safely cancels a job only while it is queued.
func (s *Store) CancelJob(id string) (Job, bool) {
	result, err := s.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET status = 'cancelled'
		WHERE id = $1
		  AND status = 'queued'
		`,
		id,
	)

	if err != nil {
		return Job{}, false
	}

	if result.RowsAffected() == 0 {
		return Job{}, false
	}

	return s.Get(id)
}

// ClaimNextJob atomically finds one job that is ready
// to run and changes its status from queued to processing.
//
// Jobs are claimed by priority:
// high -> normal -> low
//
// Jobs with the same priority are processed in
// creation order.
//
// FOR UPDATE SKIP LOCKED allows multiple workers to
// safely request jobs at the same time without
// processing the same job.
func (s *Store) ClaimNextJob() (Job, bool, error) {
	ctx := context.Background()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Job{}, false, fmt.Errorf(
			"failed to begin job claim transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var job Job
	var payload []byte

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			type,
			payload,
			status,
			priority,
			retries,
			max_retries,
			created_at,
			scheduled_at
		FROM jobs
		WHERE status = 'queued'
		  AND (
				scheduled_at IS NULL
				OR scheduled_at <= NOW()
		  )
		ORDER BY
			CASE priority
				WHEN 'high' THEN 1
				WHEN 'normal' THEN 2
				WHEN 'low' THEN 3
				ELSE 4
			END ASC,
			created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&job.ID,
		&job.Type,
		&payload,
		&job.Status,
		&job.Priority,
		&job.Retries,
		&job.MaxRetries,
		&job.CreatedAt,
		&job.ScheduledAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return Job{}, false, nil
		}

		return Job{}, false, fmt.Errorf(
			"failed to select next queued job: %w",
			err,
		)
	}

	if len(payload) > 0 {
		if err := json.Unmarshal(
			payload,
			&job.Payload,
		); err != nil {
			return Job{}, false, fmt.Errorf(
				"failed to decode payload for job %s: %w",
				job.ID,
				err,
			)
		}
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE jobs
		SET
			status = 'processing',
			processing_started_at = NOW()
		WHERE id = $1
		`,
		job.ID,
	)

	if err != nil {
		return Job{}, false, fmt.Errorf(
			"failed to mark job %s as processing: %w",
			job.ID,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, fmt.Errorf(
			"failed to commit claim for job %s: %w",
			job.ID,
			err,
		)
	}

	job.Status = "processing"

	return job, true, nil
}

// RecoverProcessingJobs returns unfinished processing jobs
// to the queue after an unexpected application shutdown.
//
// IMPORTANT:
// Call only at application startup, before starting workers,
// and only when no other QueueFlow instance is processing
// jobs in the same database.
func (s *Store) RecoverProcessingJobs() (int64, error) {
	result, err := s.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET
			status = 'queued',
			processing_started_at = NULL
		WHERE status = 'processing'
		`,
	)

	if err != nil {
		return 0, fmt.Errorf(
			"failed to recover processing jobs: %w",
			err,
		)
	}

	return result.RowsAffected(), nil
}

// Stats returns the current number of jobs in each
// QueueFlow status and the total number of jobs.
func (s *Store) Stats() (QueueStats, error) {
	var stats QueueStats

	err := s.db.QueryRow(
		context.Background(),
		`
		SELECT
			COUNT(*) FILTER (
				WHERE status = 'queued'
			),
			COUNT(*) FILTER (
				WHERE status = 'processing'
			),
			COUNT(*) FILTER (
				WHERE status = 'completed'
			),
			COUNT(*) FILTER (
				WHERE status = 'failed'
			),
			COUNT(*) FILTER (
				WHERE status = 'cancelled'
			),
			COUNT(*)
		FROM jobs
		`,
	).Scan(
		&stats.Queued,
		&stats.Processing,
		&stats.Completed,
		&stats.Failed,
		&stats.Cancelled,
		&stats.Total,
	)

	if err != nil {
		return QueueStats{}, fmt.Errorf(
			"failed to retrieve queue stats: %w",
			err,
		)
	}

	return stats, nil
}
