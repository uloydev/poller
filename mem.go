package poller

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrJobNotFound is returned by Store implementations when SetNext is called
// for a job that is not in the store.
var ErrJobNotFound = errors.New("poller: job not found")

// ErrQueueClosed is returned by Publish when the queue has been closed.
var ErrQueueClosed = errors.New("poller: queue closed")

// MemoryStore is a thread-safe, in-memory Store intended for tests and
// demos. Jobs are keyed by ID; Add inserts or replaces a job, SetNext
// advances an existing job's Next time, and ListDue returns the due jobs
// sorted by Next.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]Job)}
}

// Add inserts or replaces a job in the store.
func (s *MemoryStore) Add(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

// ListDue returns the jobs whose Next time is at or before now, ordered by
// Next. It returns ctx.Err() when ctx is canceled.
func (s *MemoryStore) ListDue(ctx context.Context, now time.Time) ([]Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	due := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if !job.Next.After(now) {
			due = append(due, job)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].Next.Before(due[j].Next)
	})
	return due, nil
}

// SetNext advances the Next time of the job with the same ID as job. It
// returns ErrJobNotFound when the job is not in the store, and ctx.Err()
// when ctx is canceled.
func (s *MemoryStore) SetNext(ctx context.Context, job Job, next time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.jobs[job.ID]
	if !ok {
		return ErrJobNotFound
	}
	current.Next = next
	s.jobs[job.ID] = current
	return nil
}

// MemoryQueue is a thread-safe, in-memory queue that both enqueues jobs as a
// Publisher and delivers them as a Consumer. Each job is delivered to exactly
// one consumer. Close stops delivery; jobs still queued at that point are
// dropped.
type MemoryQueue struct {
	ch     chan Job
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed bool
}

// NewMemoryQueue returns an empty MemoryQueue.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		ch:   make(chan Job),
		done: make(chan struct{}),
	}
}

// Publish enqueues a job, blocking until a consumer accepts it or ctx is
// canceled. It returns ErrQueueClosed when the queue has been closed.
func (q *MemoryQueue) Publish(ctx context.Context, job Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return ErrQueueClosed
	}
	select {
	case q.ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consume delivers queued jobs to handler until ctx is canceled or the queue
// is closed, then returns nil. It must not be called concurrently with
// another Consume on the same queue.
func (q *MemoryQueue) Consume(ctx context.Context, handler func(context.Context, Job)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-q.done:
			return nil
		case job := <-q.ch:
			handler(ctx, job)
		}
	}
}

// Close stops delivery of queued jobs and causes any running Consume loop to
// return. It is safe to call multiple times.
func (q *MemoryQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.once.Do(func() { close(q.done) })
}

// MemorySink is a thread-safe, in-memory Sink intended for tests and demos.
// Results are kept per entity in the order they were written.
type MemorySink struct {
	mu      sync.Mutex
	results map[string][]Result
}

// NewMemorySink returns an empty MemorySink.
func NewMemorySink() *MemorySink {
	return &MemorySink{results: make(map[string][]Result)}
}

// Write stores the result under entityID. It returns ctx.Err() when ctx is
// canceled.
func (s *MemorySink) Write(ctx context.Context, entityID string, result Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[entityID] = append(s.results[entityID], result)
	return nil
}

// Results returns a deep copy of the results stored for entityID, in write
// order, so callers may mutate the returned metrics without affecting the
// sink's state.
func (s *MemorySink) Results(entityID string) []Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := s.results[entityID]
	out := make([]Result, len(stored))
	for i, result := range stored {
		out[i] = Result{
			Metrics:  make([]Metric, len(result.Metrics)),
			PolledAt: result.PolledAt,
		}
		for j, metric := range result.Metrics {
			labels := make(map[string]string, len(metric.Labels))
			for k, v := range metric.Labels {
				labels[k] = v
			}
			out[i].Metrics[j] = Metric{
				Name:   metric.Name,
				Value:  metric.Value,
				Labels: labels,
			}
		}
	}
	return out
}

// Entities returns the entity IDs that have results, sorted.
func (s *MemorySink) Entities() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.results))
	for id := range s.results {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
