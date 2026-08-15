# poller

Polling framework for Go: leader-elected scheduler, bounded worker pool, NATS queue-group sharding, jitter, retries. Anything implementing the `Collector` interface can be polled.

## Status

Pre-M1. Roadmap: scheduler (NATS KV election), worker pool, `Collector` interface, fake collector demo.

## Requirements

- Go 1.26

## Development

```sh
make ci    # fmt + lint + vet + test -race + coverage gate
```

Workflow rules: `AGENTS.md`. Go idioms: `DEVELOPMENT.md`. TDD cycle: `TESTING.md`.

## License

MIT
