# Contributing to peerbus

Thanks for contributing. peerbus is a small Go project — an agent-agnostic
durable message bus plus thin adapters. Read [`README.md`](README.md) for the
overview and [`CLAUDE.md`](CLAUDE.md) for the architecture map, the exact
build/run commands, and the load-bearing invariants. This file is the
process; it does not duplicate those.

## Prerequisites

- **Go 1.25+** (matches the `go` directive in `go.mod`; CI pins Go 1.25).
- **No cgo / no C toolchain** — `modernc.org/sqlite` is pure-Go. Do not add
  cgo dependencies.
- `golangci-lint` is optional; `make lint` falls back to `gofmt` + `go vet`.
- `docker` is optional; only needed for `make deploy-validate` to actually
  validate the compose manifest (it skips and succeeds without docker).

## Build and test

Before opening a PR, all of the following must be clean:

```sh
make test             # go test ./...
go test -race ./...    # CI runs this — must pass
gofmt -l .            # must print nothing
go vet ./...          # must be clean
make deploy-validate  # if you touched deploy/
```

`go build ./...` must succeed. CI runs gofmt check, `go vet`, build,
`go test ./...`, and `go test -race ./...`; a PR that fails any of these
will not merge.

## Commits

Scoped Conventional Commits with a **lowercase** description, matching the
existing history: `feat(scope): ...`, `fix: ...`, `refactor: ...`,
`docs: ...`. One logical change per commit.

## Pull request expectations

- **Every code change ships tests** — both the success path and the error
  paths. Tests are table-driven; every package has tests. See the testing
  conventions in [`CLAUDE.md`](CLAUDE.md) (in-process `httptest` broker +
  `coder/websocket` harness; restore the global mode registry with
  `unregisterMode` + `t.Cleanup` if your test registers a mode).
- **Do not break the load-bearing invariants** documented in
  [`CLAUDE.md`](CLAUDE.md) (HMAC canonical form, dedupe-by-id /
  ack-by-delivery-key, broker never mutating the signed envelope, single
  audit writer, exact-match version, no broadcast backfill, broker as a
  managed long-lived service). Changes that touch these need a clear
  rationale and full test coverage.
- **If you change the wire protocol, update
  [`docs/wire-protocol.md`](docs/wire-protocol.md)** in the same PR. That
  spec is meant to let anyone write a conforming adapter in any language from
  the doc alone — keep it authoritative.
- **Writing your own adapter** does not require changing this repo:
  implement the documented protocol against
  [`docs/wire-protocol.md`](docs/wire-protocol.md). Adapter integration
  guides live in [`docs/integrations/`](docs/integrations/).

## License and sign-off

peerbus is [MIT](LICENSE) licensed. By contributing you agree your
contributions are licensed under the MIT License and you certify the
[Developer Certificate of Origin](https://developercertificate.org/) —
sign off your commits with `git commit -s` (`Signed-off-by:` trailer).

## Where to look

- [`README.md`](README.md) — what peerbus is, honest taxonomy, quickstart.
- [`CLAUDE.md`](CLAUDE.md) — architecture map, commands, invariants, testing.
- [`docs/wire-protocol.md`](docs/wire-protocol.md) — the full wire spec.
- [`docs/integrations/`](docs/integrations/) — per-runtime adapter guides.
- [`docs/cc2cc-parity.md`](docs/cc2cc-parity.md) — the cc2cc parity matrix.
