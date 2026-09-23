package job

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func NewStore(databaseURL string) (*Store, error) {
	db, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to create database pool: %w", err)
	}

	if err := db.Ping(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to connect to database: %w", err)
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
			(id, type, payload, status, retries, max_retries, created_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7)
		`,
		job.ID,
		job.Type,
		payload,
		job.Status,
		job.Retries,
		job.MaxRetries,
		job.CreatedAt,
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
			created_at
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
	)

	if err != nil {
		return Job{}, false
	}

	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &job.Payload); err != nil {
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
			created_at
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
		)

		if err != nil {
			continue
		}

		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &job.Payload); err != nil {
				continue
			}
		}

		jobs = append(jobs, job)
	}

	return jobs
}

func (s *Store) UpdateStatus(id string, status string) (Job, bool) {
	_, err := s.db.Exec(
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

	return s.Get(id)
}

func (s *Store) IncrementRetry(id string) (Job, bool) {
	_, err := s.db.Exec(
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

	return s.Get(id)
}
