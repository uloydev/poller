# TDD Workflow

Cycle: **red → green → refactor**. One test at a time, smallest increment that teaches something.

## Steps

1. **Red.** Write the failing test. Black-box (`package foo_test`) where practical. Run `go test ./... -run TestName`. Completion: fails for the intended reason — the assertion, not a typo or compile error.
2. **Green.** Minimal implementation. Completion: `go test ./...` passes and no existing test broke.
3. **Refactor.** Improve design inside the safety net. Completion: `make ci` green (fmt, lint, vet, race, coverage).

## Tooling

- stdlib `testing` + testify; hand-rolled mocks in test files, no mockery
- Integration tests (DB, NATS, containers): build tag `//go:build integration`, run `make test-integration`, kept out of the unit gate
- `go test -race ./...` always
- Coverage gate: 80% total, enforced in CI (`make ci` runs the gate locally too)

## Tests that count

- State machine transitions: table-driven, every edge row named
- Error paths: each wrapped error distinguishable by `errors.Is`
- Concurrency: race detector on, goroutines proven stopped by context cancel

## Discipline

- A test asserts one behavior
- Test names say the behavior: `TestEvaluate_FiresAfterDuration`
- No sleep-based waits in unit tests; use deterministic clocks or channels
