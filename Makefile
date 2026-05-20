.PHONY: build run install test test-coverage lint fmt vet tidy \
        release release-snapshot deploy-validate clean help

GO       ?= go
BINDIR   ?= bin
DISTDIR  ?= dist
ARGS     ?=

# Build the single peerbus multi-command binary into $(BINDIR). The
# subcommand (serve / audit verify / adapter --adapter=<mode>) is selected
# at runtime.
build:
	$(GO) build -o $(BINDIR)/peerbus ./cmd/peerbus

# Pass flags/subcommands via ARGS, e.g. `make run ARGS='serve'` or
# `make run ARGS='adapter --adapter=generic'` or
# `make run ARGS='audit verify --db /tmp/peerbus.db'`.
run:
	$(GO) run ./cmd/peerbus $(ARGS)

# Install the single command to GOPATH/bin.
install:
	$(GO) install ./cmd/peerbus

test:
	$(GO) test ./... -race -count=1

test-coverage:
	$(GO) test ./... -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

# lint runs golangci-lint when available, otherwise falls back to the
# always-available gofmt + go vet so it works on a clean checkout with no
# extra installs.
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint run ./..."; \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found; falling back to gofmt + go vet"; \
		out=$$(gofmt -l .); \
		if [ -n "$$out" ]; then echo "gofmt needs to be run on:"; echo "$$out"; exit 1; fi; \
		$(GO) vet ./...; \
	fi

# fmt uses gofumpt (a strict gofmt superset). Install once with:
#   go install mvdan.cc/gofumpt@latest
fmt:
	gofumpt -w .

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

# Release (dry run): build all targets locally, no publish.
release-snapshot:
	goreleaser release --snapshot --clean

# Release: build + publish (CI runs this on a v* tag).
release:
	goreleaser release --clean

# deploy-validate statically validates the broker-only deployment manifest
# with `docker compose config -q`. This is intentionally NOT a Go test: the
# Go suite must not be coupled to deployment YAML. If docker is not installed
# the target prints a skip notice and SUCCEEDS, so a clean checkout without
# docker still passes the full local run; where docker is present the compose
# file must validate.
deploy-validate:
	@if command -v docker >/dev/null 2>&1; then \
		echo "docker compose -f deploy/compose.yml config -q"; \
		PEERBUS_TOKENS=validate PEERBUS_HMAC_SECRET=validate-only-not-a-real-secret \
			docker compose -f deploy/compose.yml config -q; \
	else \
		echo "docker not found; skipping compose validation (deploy-validate requires docker)"; \
	fi

clean:
	rm -rf $(BINDIR)/peerbus $(DISTDIR) coverage.out coverage.html

help:
	@echo "Build & Run"
	@echo "  make build                    Build the single peerbus binary into $(BINDIR)/"
	@echo "  make run ARGS='...'           Run peerbus (e.g. ARGS='serve' or ARGS='adapter --adapter=cc')"
	@echo "  make install                  Install peerbus to GOPATH/bin"
	@echo ""
	@echo "Test & Lint"
	@echo "  make test                     Run all tests (-race -count=1)"
	@echo "  make test-coverage            Generate coverage.out + coverage.html"
	@echo "  make lint                     Run golangci-lint (gofmt+vet fallback)"
	@echo "  make fmt                      Format with gofumpt"
	@echo "  make vet                      go vet ./..."
	@echo "  make tidy                     go mod tidy"
	@echo ""
	@echo "Release"
	@echo "  make release-snapshot         Dry-run release (no publish)"
	@echo "  make release                  Build and publish release"
	@echo ""
	@echo "Deploy"
	@echo "  make deploy-validate          Validate deploy/compose.yml"
	@echo ""
	@echo "  make clean                    Remove build/coverage artifacts"
