# Development Guide

## Environment

- Go 1.26 (pinned in `go.mod`)
- golangci-lint v2 (config `.golangci.yml`)
- Docker Compose dev stack (nms-app): PostgreSQL 16, ClickHouse, NATS
- Every repo has a `Makefile` with: `fmt`, `lint`, `vet`, `test`, `test-integration`, `ci`, `build`

## Repo layout

```
cmd/<binary>/main.go   thin main: config, wiring, run
internal/              app-private packages
pkg/                   public API of the repo
schema/                migrations (nms-app only)
```

No packages named `util`, `common`, or `misc`.

## Go idioms (strict)

1. **Errors.** Wrap with `%w` and context: `fmt.Errorf("read config: %w", err)`. Compare with `errors.Is`/`errors.As`. Error strings lowercase, no trailing punctuation.
2. **Context.** First parameter, deadline set on every remote call, checked at every loop iteration.
3. **Interfaces.** One to four methods, declared where consumed.
4. **Construction.** Constructors validate and return an error for invalid input; zero value stays usable where possible.
5. **No `init()`, no package globals, no panics** outside `main`. Libraries return errors.
6. **Tests.** Table-driven, stdlib `testing` + testify (`assert` soft, `require` for preconditions).
7. **Formatting.** gofumpt + goimports, enforced by lint.
8. **Docs.** Every exported symbol has a doc comment starting with the symbol name.
9. **Types.** Prefer generics or concrete types over `interface{}`; initialize maps/slices with `make` when size is known.

## Review checklist (pre-PR)

- [ ] `make ci` green
- [ ] Every export documented
- [ ] Each error wrapped exactly once per layer
- [ ] Goroutines bound by context cancellation
- [ ] Metrics and labels follow `docs/METRIC_CONVENTIONS.md`
