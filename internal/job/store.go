package job

import (
	"context"
	"encoding/json"
	"fmt"

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
				retries,
				max_retries,
				created_at,
				scheduled_at
			)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8)
		`,
		job.ID,
		job.Type,
		payload,
		job.Status,
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

func (s *Store) List() []Job {
	rows, err := s.db.Query(
		context.Background(),
		`
		SELECT
			id,
			type,
			payload,
			status,
			retries,
			max_retries,
			created_at,
			scheduled_at
		FROM jobs
		ORDER BY created_at ASC
		`,
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

func (s *Store) UpdateStatus(
	id string,
	status string,
) (Job, bool) {

	result, err := s.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET status = $1
		WHERE id = $2
		`,
		status,
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
		return Job{}, false
	}

	if result.RowsAffected() == 0 {
		return Job{}, false
	}

	return s.Get(id)
}

// CancelJob safely cancels a job only while it is queued.
//
// The status condition is included directly in the UPDATE so
// cancellation remains safe if a worker attempts to claim the
// same job at approximately the same time.
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
// FOR UPDATE SKIP LOCKED allows multiple workers to
// safely request jobs at the same time without
// processing the same job.
func (s *Store) ClaimNextJob() (Job, bool) {
	ctx := context.Background()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Job{}, false
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
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&job.ID,
		&job.Type,
		&payload,
		&job.Status,
		&job.Retries,
		&job.MaxRetries,
		&job.CreatedAt,
		&job.ScheduledAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return Job{}, false
		}

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

	_, err = tx.Exec(
		ctx,
		`
		UPDATE jobs
		SET status = 'processing'
		WHERE id = $1
		`,
		job.ID,
	)

	if err != nil {
		return Job{}, false
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false
	}

	job.Status = "processing"

	return job, true
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
