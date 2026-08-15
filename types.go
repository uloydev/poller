package poller

import (
	"context"
	"time"
)

// Metric is a single named numeric sample with optional labels.
//
// Names and units follow the conventions in METRIC_CONVENTIONS.md: a
// vendor-prefixed snake_case name (`<vendor>_<name>`) with a unit suffix
// for durations, counters, rates, and ratios. Label values must not change
// between polls, so per-poll identifiers such as timestamps are forbidden.
type Metric struct {
	Name   string
	Value  float64
	Labels map[string]string
}

// Result is the outcome of one collection: the samples that were read and
// the moment they were read. PolledAt must be set by the Collector; consumers
// of the Sink use it as the metric timestamp.
type Result struct {
	Metrics  []Metric
	PolledAt time.Time
}

// Job is one unit of scheduled work. ID identifies the entity to collect,
// Next is the next time the job is due, Interval is how often the job runs,
// and Jitter is the maximum amount by which scheduling may spread the next
// poll time to avoid thundering herds. Jitter is bounded by Interval.
type Job struct {
	ID       string
	Next     time.Time
	Interval time.Duration
	Jitter   time.Duration
}

// Collector reads the current metrics of one entity. entityID is the
// identifier carried by the Job that triggered the collection. Collect must
// respect ctx: a canceled or expired context means the collection should
// stop, and the returned error should wrap ctx.Err().
type Collector interface {
	Collect(ctx context.Context, entityID string) (Result, error)
}

// Sink receives the result of a completed collection. Write must respect ctx
// and must not mutate result; implementations may treat it as the point where
// the metric leaves the framework.
type Sink interface {
	Write(ctx context.Context, entityID string, result Result) error
}

// Store holds the job schedule. ListDue returns the jobs whose Next time is
// at or before now, and SetNext advances a job's Next time after it has been
// scheduled. Implementations must return each due job exactly once per call
// and must respect ctx.
type Store interface {
	ListDue(ctx context.Context, now time.Time) ([]Job, error)
	SetNext(ctx context.Context, job Job, next time.Time) error
}

// Publisher hands a due job to whatever transport carries jobs to workers,
// typically a queue. Publish must respect ctx. A publisher that returns an
// error keeps the job due in the Store, so it is retried on the next tick.
type Publisher interface {
	Publish(ctx context.Context, job Job) error
}

// Leader reports whether this process owns the schedule. Only the leader
// publishes jobs; followers run their tick loop but publish nothing.
type Leader interface {
	IsLeader() bool
}

// Consumer delivers jobs to a handler until ctx is canceled or the consumer
// fails. Consume blocks for the lifetime of the consumption loop and returns
// the first error that ends it, or nil when it stops cleanly on ctx
// cancellation. Implementations must not call handler after Consume returns.
type Consumer interface {
	Consume(ctx context.Context, handler func(context.Context, Job)) error
}
