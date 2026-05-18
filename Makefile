.PHONY: build test lint fmt vet tidy clean deploy-validate

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
