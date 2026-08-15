# poller

[![ci](https://github.com/uloydev/poller/actions/workflows/ci.yml/badge.svg)](https://github.com/uloydev/poller/actions/workflows/ci.yml)

A polling framework for Go: schedule work due at intervals, fan it out across
a bounded worker pool, collect values through a pluggable `Collector`, retry
with backoff, and shut down gracefully. Anything that implements the
`Collector` interface can be polled — a device, an HTTP endpoint, a cloud
API.

## Features

- **Due-based scheduling** — a `Scheduler` tick loop publishes jobs whose
  `Next` time has arrived and advances each job's schedule in a `Store`.
- **Leader election hook** — only the leader publishes, so any number of
  scheduler replicas can run against the same `Store`.
- **Jitter** — each job's next poll time is spread by a configurable
  `JitterFunc`, avoiding thundering herds after restarts.
- **Bounded concurrency** — a `Worker` runs at most `Concurrency`
  collections at once, with a per-collection timeout.
- **Retries with exponential backoff and jitter** — configurable retry count,
  base delay, and backoff function.
- **Graceful shutdown** — `Worker.Stop` stops consuming, drains in-flight and
  queued collections, and returns a wrapped `context.DeadlineExceeded` only
  if collections do not finish in time.
- **NATS transport** — the `natsq` subpackage provides a Publisher and a
  queue-group Consumer so workers share load across machines.
- **In-memory implementations** — a Store, queue, and Sink ship in the root
  package for tests and demos.

## Requirements

- Go 1.26 or newer.
- A NATS server only when you use `natsq`; the rest of the framework and all
  tests run without one.

## Install

```sh
go get github.com/uloydev/poller
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/uloydev/poller"
)

type thermostat struct{}

// Collect reads the current value of entityID. Names, units, and labels
// follow METRIC_CONVENTIONS.md.
func (thermostat) Collect(ctx context.Context, entityID string) (poller.Result, error) {
	if err := ctx.Err(); err != nil {
		return poller.Result{}, err
	}
	return poller.Result{
		PolledAt: time.Now(),
		Metrics: []poller.Metric{
			{Name: "thermostat_temperature", Value: 21.5},
		},
	}, nil
}

func main() {
	store := poller.NewMemoryStore()
	queue := poller.NewMemoryQueue()
	sink := poller.NewMemorySink()

	store.Add(poller.Job{
		ID:       "living-room",
		Next:     time.Now(),
		Interval: 5 * time.Second,
		Jitter:   time.Second, // spread next polls within [0, 1s)
	})

	scheduler, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  queue, // jobs flow into the queue
		JitterFunc: poller.UniformJitter,
	})
	if err != nil {
		panic(err)
	}

	worker, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:    queue, // jobs come out of the queue
		Collector:   thermostat{},
		Sink:        sink,
		Concurrency: 4,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = scheduler.Run(ctx) }()
	go func() { _ = worker.Run(ctx) }()

	time.Sleep(12 * time.Second)
	cancel() // scheduler stops; worker drains before returning

	for _, id := range sink.Entities() {
		results := sink.Results(id)
		fmt.Printf("%s: %d polls\n", id, len(results))
	}
}
```

## Architecture

```
                 +----------------+     Store (source of truth)
   tick loop     |                |     ListDue / SetNext
   ------------> |   Scheduler    |-------------------------+
                 |  (leader only) |                         |
                 +----------------+                         v
                       |  Publish(due job)            +------------+
                       v                              |   Store    |
                 +-------------+   queue-group /      +------------+
                 |   Queue     |   memory channel
                 +-------------+
                       |  Consume(job)
                       v
                 +-------------+   bounded pool,
                 |   Worker    |   per-job timeout,
                 | (n replicas)|   retry + backoff
                 +-------------+
                       |  Collect(ctx, entityID)
                       v
                 +-------------+
                 |  Collector  |   pluggable: device, API, ...
                 +-------------+
                       |  Write(entityID, Result)
                       v
                 +-------------+
                 |    Sink     |   storage, metrics backend, ...
                 +-------------+
```

The scheduler and the workers are independent halves connected only by the
queue. The scheduler decides *when*; the workers decide *how*. Either half can
be replicated: scheduler replicas race through the leader hook, worker
replicas share the queue.

## Scaling

- **More entities** — jobs are just rows in the `Store`; the scheduler
  publishes them all.
- **More load per entity** — raise `Concurrency`, add worker replicas on the
  same host sharing the queue.
- **More hosts** — run `natsq` with one queue group name across all workers;
  NATS distributes each subject's messages across the group. Add scheduler
  replicas with a real leader implementation so exactly one publishes.
- **Idempotent results** — scheduling is at-least-once: a job may be
  published twice if the store write back fails. Sinks should tolerate
  duplicates, e.g. by keying on `Result.PolledAt`.

## Graceful shutdown

1. Cancel the scheduler's context — it stops publishing, leaving queued jobs
   to the workers.
2. Call `worker.Stop(ctx)` on each worker — it stops consuming and then waits
   for in-flight and queued collections to finish, bounded by `ctx`.
3. `Stop` returns `nil` when the worker drained in time, or a wrapped
   `context.DeadlineExceeded` when a collection was still running — surface
   that as a warning; the next restart polls again.

In-flight work is always bounded: every collection and sink write runs under
`Timeout` (default 10s), so a hung collector cannot wedge the pool forever.

## Development

```sh
make ci   # gofumpt + goimports + golangci-lint + go vet + test -race + 80% coverage gate
```

Workflow rules: `AGENTS.md`. Go idioms: `DEVELOPMENT.md`. TDD cycle:
`TESTING.md`. Metric naming: `METRIC_CONVENTIONS.md`.

## License

MIT — see [LICENSE](LICENSE).
