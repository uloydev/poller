package poller_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uloydev/poller"
)

type fakeStore struct {
	mu          sync.Mutex
	jobs        []poller.Job
	listErr     error
	setNext     map[string]time.Time
	setNextErrs map[string]error
}

func newFakeStore(jobs ...poller.Job) *fakeStore {
	return &fakeStore{jobs: jobs, setNext: make(map[string]time.Time)}
}

func (s *fakeStore) ListDue(_ context.Context, _ time.Time) ([]poller.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.jobs, nil
}

func (s *fakeStore) SetNext(_ context.Context, job poller.Job, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setNextErrs[job.ID]; err != nil {
		return err
	}
	s.setNext[job.ID] = next
	return nil
}

func (s *fakeStore) setNextTimes() map[string]time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]time.Time, len(s.setNext))
	for id, next := range s.setNext {
		out[id] = next
	}
	return out
}

type fakePublisher struct {
	mu        sync.Mutex
	published []poller.Job
	errs      map[string]error
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{errs: make(map[string]error)}
}

func (p *fakePublisher) Publish(_ context.Context, job poller.Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.errs[job.ID]; err != nil {
		return err
	}
	p.published = append(p.published, job)
	return nil
}

func (p *fakePublisher) publishedJobs() []poller.Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]poller.Job, len(p.published))
	copy(out, p.published)
	return out
}

type fakeLeader struct {
	leader bool
}

func (l fakeLeader) IsLeader() bool { return l.leader }

var errListDue = errors.New("store down")

func TestNewScheduler_RequiresStoreAndPublisher(t *testing.T) {
	tests := []struct {
		name      string
		store     poller.Store
		publisher poller.Publisher
	}{
		{name: "nil store", store: nil, publisher: newFakePublisher()},
		{name: "nil publisher", store: newFakeStore(), publisher: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := poller.NewScheduler(poller.SchedulerConfig{
				Store:     tt.store,
				Publisher: tt.publisher,
			})
			require.Error(t, err)
		})
	}
}

func TestScheduler_PublishesDueJobsAndAdvancesNext(t *testing.T) {
	now := time.Now()
	store := newFakeStore(poller.Job{
		ID: "dev-1", Next: now, Interval: 5 * time.Second, Jitter: 0,
	})
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(publisher.publishedJobs()) == 1
	}, 5*time.Second, time.Millisecond)

	published := publisher.publishedJobs()
	require.Len(t, published, 1)
	assert.Equal(t, "dev-1", published[0].ID)

	next, ok := store.setNextTimes()["dev-1"]
	require.True(t, ok, "SetNext must be called for the published job")
	assert.Equal(t, now.Add(5*time.Second), next)

	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)
}

func TestScheduler_AppliesJitterToNextPoll(t *testing.T) {
	now := time.Now()
	store := newFakeStore(poller.Job{
		ID: "dev-1", Next: now, Interval: 5 * time.Second, Jitter: time.Second,
	})
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(123 * time.Millisecond),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sched.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(store.setNextTimes()) == 1
	}, 5*time.Second, time.Millisecond)

	next := store.setNextTimes()["dev-1"]
	assert.Equal(t, now.Add(5*time.Second+123*time.Millisecond), next)
}

func TestScheduler_SkipsWhenNotLeader(t *testing.T) {
	store := newFakeStore(poller.Job{ID: "dev-1", Next: time.Now(), Interval: time.Second})
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Leader:     fakeLeader{leader: false},
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	assert.Empty(t, publisher.publishedJobs(), "follower must not publish")
	assert.Empty(t, store.setNextTimes(), "follower must not advance the schedule")
}

func TestScheduler_StopsOnContextCancel(t *testing.T) {
	store := newFakeStore()
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	cancel()

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestScheduler_ReturnsListDueError(t *testing.T) {
	store := newFakeStore(poller.Job{ID: "dev-1", Next: time.Now(), Interval: time.Second})
	store.listErr = errListDue
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, errListDue)
		require.ErrorContains(t, err, "list due jobs")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after store failure")
	}
}

func TestScheduler_ContinuesAfterPublishError(t *testing.T) {
	now := time.Now()
	store := newFakeStore(poller.Job{ID: "dev-1", Next: now, Interval: time.Second})
	publisher := newFakePublisher()
	publisher.errs["dev-1"] = errors.New("transport down")

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	assert.Empty(t, store.setNextTimes(), "failed publish must not advance the schedule")
}

func TestScheduler_ContinuesAfterSetNextError(t *testing.T) {
	now := time.Now()
	store := newFakeStore(poller.Job{ID: "dev-1", Next: now, Interval: time.Second})
	store.setNextErrs = map[string]error{"dev-1": errors.New("store busy")}
	publisher := newFakePublisher()

	sched, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  publisher,
		Tick:       time.Millisecond,
		JitterFunc: fixedJitter(0),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(publisher.publishedJobs()) >= 1
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	assert.GreaterOrEqual(t, len(publisher.publishedJobs()), 1,
		"job must be published even when SetNext fails")
}

func TestUniformJitter_StaysWithinBounds(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		jitter   time.Duration
	}{
		{name: "no jitter", interval: time.Second, jitter: 0},
		{name: "full jitter", interval: time.Second, jitter: time.Second},
		{name: "jitter capped at interval", interval: time.Second, jitter: 10 * time.Second},
		{name: "negative jitter", interval: time.Second, jitter: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 100 {
				offset := poller.UniformJitter(tt.interval, tt.jitter)
				assert.GreaterOrEqual(t, offset, time.Duration(0), "jitter must not be negative")
				assert.LessOrEqual(t, offset, tt.interval, "jitter must not exceed interval")
				if tt.jitter == 0 || tt.jitter < 0 {
					assert.Zero(t, offset)
				}
			}
		})
	}
}

func TestAlwaysLeader_IsLeader(t *testing.T) {
	assert.True(t, poller.AlwaysLeader{}.IsLeader())
}

func fixedJitter(offset time.Duration) poller.JitterFunc {
	return func(_, _ time.Duration) time.Duration { return offset }
}
