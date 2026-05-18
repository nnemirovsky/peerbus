.PHONY: build test lint fmt vet tidy clean

GO       ?= go
DISTDIR  ?= dist

build:
	$(GO) build -o $(DISTDIR)/peerbus-broker ./cmd/peerbus-broker
	$(GO) build -o $(DISTDIR)/peerbus-adapter ./cmd/peerbus-adapter

test:
	$(GO) test ./...

# lint uses golangci-lint when available, otherwise falls back to the
# always-available gofmt + go vet so it works on a clean checkout with no
# extra installs.
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint run"; \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found; falling back to gofmt + go vet"; \
		out=$$(gofmt -l .); \
		if [ -n "$$out" ]; then echo "gofmt needs to be run on:"; echo "$$out"; exit 1; fi; \
		$(GO) vet ./...; \
	fi

fmt:
	gofmt -w .

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(DISTDIR)
