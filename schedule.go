package poller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

// JitterFunc computes the offset added to a job's Next time when the job is
// scheduled. interval is the job's poll interval and jitter is the job's
// configured Jitter; implementations must return an offset in [0, jitter].
type JitterFunc func(interval, jitter time.Duration) time.Duration

// UniformJitter returns a uniformly distributed offset in [0, jitter), capped
// at interval, so a job's next poll never strays more than one interval late.
// It returns 0 when jitter is not positive. It is safe for concurrent use.
func UniformJitter(interval, jitter time.Duration) time.Duration {
	if interval <= 0 || jitter <= 0 {
		return 0
	}
	if jitter > interval {
		jitter = interval
	}
	return time.Duration(rand.Int64N(int64(jitter))) //nolint:gosec // non-security randomness
}

// AlwaysLeader is a Leader that reports leadership unconditionally, for
// single-instance deployments.
type AlwaysLeader struct{}

// IsLeader always returns true.
func (AlwaysLeader) IsLeader() bool { return true }

// SchedulerConfig configures a Scheduler. Store and Publisher are required;
// every other field has a default (see NewScheduler).
type SchedulerConfig struct {
	// Store holds the job schedule. Required.
	Store Store
	// Publisher carries due jobs to workers. Required.
	Publisher Publisher
	// Leader gates publishing; only the leader publishes. Defaults to
	// AlwaysLeader.
	Leader Leader
	// Tick is the cadence of the scheduling loop. Defaults to 1s.
	Tick time.Duration
	// JitterFunc spreads each job's next poll time. Defaults to
	// UniformJitter.
	JitterFunc JitterFunc
	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewScheduler validates cfg and returns a Scheduler with defaults applied.
// Store and Publisher must be set; a nil Leader, zero Tick, nil JitterFunc,
// and nil Logger fall back to their defaults. It returns an error for any
// other invalid input.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	if cfg.Store == nil {
		return nil, errors.New("poller: scheduler requires a store")
	}
	if cfg.Publisher == nil {
		return nil, errors.New("poller: scheduler requires a publisher")
	}
	if cfg.Tick < 0 {
		return nil, errors.New("poller: scheduler tick must not be negative")
	}
	tick := cfg.Tick
	if tick == 0 {
		tick = time.Second
	}
	leader := cfg.Leader
	if leader == nil {
		leader = AlwaysLeader{}
	}
	jitter := cfg.JitterFunc
	if jitter == nil {
		jitter = UniformJitter
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		store:     cfg.Store,
		publisher: cfg.Publisher,
		leader:    leader,
		tick:      tick,
		jitter:    jitter,
		log:       logger,
	}, nil
}

// Scheduler publishes jobs that are due and advances their schedule. It runs
// a tick loop: on every tick it asks the Store for due jobs, then for each
// job publishes it and sets its next poll time.
//
// Delivery is at-least-once: a job is published before its next poll time is
// written back. If SetNext fails the job stays due and is published again on
// the following tick, which may produce a duplicate poll. Collectors and
// sinks must tolerate duplicates, for example by keying on the result's
// PolledAt timestamp.
type Scheduler struct {
	store     Store
	publisher Publisher
	leader    Leader
	tick      time.Duration
	jitter    JitterFunc
	log       *slog.Logger
}

// Run drives the tick loop until ctx is canceled or the Store fails. It
// returns ctx.Err() on cancellation and a wrapped error when ListDue fails.
// Failures to publish or to advance a single job are logged and skipped; the
// job stays due and is retried on a later tick.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if err := s.runTick(ctx, now); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) runTick(ctx context.Context, now time.Time) error {
	if !s.leader.IsLeader() {
		return nil
	}
	jobs, err := s.store.ListDue(ctx, now)
	if err != nil {
		return fmt.Errorf("list due jobs: %w", err)
	}
	for _, job := range jobs {
		s.schedule(ctx, job)
	}
	return nil
}

func (s *Scheduler) schedule(ctx context.Context, job Job) {
	next := job.Next.Add(job.Interval).Add(s.jitter(job.Interval, job.Jitter))
	if err := s.publisher.Publish(ctx, job); err != nil {
		s.log.Error("publish job", "job", job.ID, "error", err)
		return
	}
	if err := s.store.SetNext(ctx, job, next); err != nil {
		s.log.Error("set next poll time", "job", job.ID, "error", err)
	}
}
