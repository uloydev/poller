# poller

[![ci](https://github.com/uloydev/poller/actions/workflows/ci.yml/badge.svg)](https://github.com/uloydev/poller/actions/workflows/ci.yml)

Polling framework for Go: leader-elected scheduling, bounded worker pool, NATS queue-group sharding, jitter, retries with backoff. Anything implementing the `Collector` interface can be polled.

## Status

Early development. Roadmap:

- Scheduler: leader election via NATS key-value store, publishes poll jobs
- Worker pool: queue-group consumption, bounded concurrency, per-collection timeout
- `Collector` interface with a fake implementation for tests and demos
- Metric naming and units: `METRIC_CONVENTIONS.md`

## Requirements

- Go 1.26
- NATS server (for the full pipeline; unit tests run without it)

## Development

```sh
make ci    # fmt + lint + vet + test -race + coverage gate
```

Workflow rules: `AGENTS.md`. Go idioms: `DEVELOPMENT.md`. TDD cycle: `TESTING.md`.

## License

MIT
