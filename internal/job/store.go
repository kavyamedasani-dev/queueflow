package job

import "sync"

type Store struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewStore() *Store {
	return &Store{
		jobs: make(map[string]Job),
	}
}

func (s *Store) Save(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
}

func (s *Store) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	return job, exists
}

func (s *Store) List() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]Job, 0, len(s.jobs))

	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}

	return jobs
}

func (s *Store) UpdateStatus(id string, status string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[id]
	if !exists {
		return Job{}, false
	}

	job.Status = status
	s.jobs[id] = job

	return job, true
}

func (s *Store) IncrementRetry(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[id]
	if !exists {
		return Job{}, false
	}

	job.Retries++
	s.jobs[id] = job

	return job, true
}
