GO ?= go
COVERAGE_MIN ?= 80

.PHONY: fmt lint vet test test-integration build ci cover-gate

fmt:
	gofumpt -w .
	goimports -w .

lint:
	golangci-lint run

vet:
	$(GO) vet ./...

test:
	$(GO) test -race ./...

test-integration:
	$(GO) test -race -tags integration ./...

build:
	$(GO) build ./...

ci: fmt lint vet test cover-gate

cover-gate:
	@test_count=$$($(GO) list -f '{{len .TestGoFiles}}' ./... | awk '{s+=$$1} END{print s+0}'); \
	if [ "$$test_count" = "0" ]; then \
		echo "no tests yet — coverage gate skipped"; \
	else \
		$(GO) test -race -coverprofile=coverage.out ./...; \
		total=$$($(GO) tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | tr -d '%'); \
		echo "total coverage: $$total%"; \
		if [ "$$(echo "$$total >= $(COVERAGE_MIN)" | bc)" != "1" ]; then \
			echo "coverage below $(COVERAGE_MIN)%"; exit 1; \
		fi; \
	fi
