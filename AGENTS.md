# AGENTS.md

Agent instructions for this repository.

## Rules

1. **TDD red-green-refactor.** Write a failing test first. No production code without a test that goes red for the right reason, then minimal green, then refactor under green.
2. **`make ci` green before finishing any change.** fmt + lint + vet + test -race + coverage gate (80%). Zero tolerated issues.
3. **One repo per change.** Cross-repo work: point `go.mod` replace at the local path during development; release leaf repos first.
4. **Conventional commits.** `feat|fix|refactor|test|docs|ci|chore(scope): subject`.
5. **Never commit** secrets, config values, or webhook URLs.
6. **No mistakes, no hallucination.** Every claim verified against a primary source: code, tests, docs, or live tool output. Never guess API signatures, library versions, flags, or schema — read the source or run the tool first. Numbers, names, and file references exact, with `file:line` where useful. When a fact is unknown, say so and verify before acting; never invent output to look complete. If tool output contradicts an assumption, the tool wins.
7. **No AI slop.** Code reads like human-authored Go: precise naming, error messages that identify the cause, comments only where code cannot speak, no speculative abstraction, no dead branches, no boilerplate. Every line earns its place; rewrite anything that reads as generated.
8. **Production-ready.** Every change ships production quality: bounded goroutines, deadlines on I/O, retries with backoff and jitter, graceful shutdown (SIGTERM drains), structured logging with correlation IDs, config from env only, versioned migrations, `/health` + `/ready` + Prometheus metrics. Failure paths exercised by tests, not just happy paths.

## Idioms

- Errors wrap with `%w`; compare with `errors.Is`/`errors.As`.
- `context.Context` is the first parameter and is respected at every I/O boundary.
- Interfaces are small and defined at the consumer side; mocks live in test files.
- Exported symbols carry doc comments; tests are table-driven.
- No `init()`, no package globals, no panics in library code.

## Layout

```
cmd/<binary>/main.go   thin main: config, wiring, run
internal/              app-private packages
pkg/                   public API of the repo
```
