# Multi-stage build for the peerbus binary.
#
# The image bakes in the full `peerbus` multi-command binary (v0.2.0+); the
# ENTRYPOINT is `peerbus` and CMD defaults to `["serve"]`, so by default the
# container is the long-lived broker service. CMD is overridable for ops
# tasks:
#   docker run --rm peerbus:latest --version
#   docker run --rm -v peerbus-data:/data peerbus:latest audit verify \
#                                                            --db /data/peerbus.db
#
# Adapter mode (`adapter --adapter=cc|generic`) is technically runnable too,
# but adapters are stdio MCP children of the agent runtime (Claude Code spawns
# the cc adapter per session; a drain-agent spawns the generic adapter). Running
# an adapter as a long-lived container service has no agent stdio to attach to
# and ends up either orphaned or replicated per session — the cc2cc failure
# mode the broker/adapter split designs out. Use the release binary, not the
# container, for adapters.
#
# Pure Go: the durable store uses modernc.org/sqlite (no cgo), so the build is
# CGO_ENABLED=0 and the final image is distroless/static (no libc, no shell).
# One static binary, nothing else.

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# CGO disabled: modernc.org/sqlite is pure Go. -s -w strips the binary.
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /out/peerbus ./cmd/peerbus

FROM gcr.io/distroless/static-debian12
LABEL org.opencontainers.image.source="https://github.com/nnemirovsky/peerbus"
LABEL org.opencontainers.image.description="peerbus — agent-agnostic durable message bus (broker by default; CMD overridable for audit/version)"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=build /out/peerbus /usr/local/bin/peerbus
# Durable SQLite store (queue + blake3 audit chain) lives on a mounted
# volume at /data. PEERBUS_DB should point here (see deploy/compose.yml).
VOLUME ["/data"]
# Broker config is read from PEERBUS_* env (see internal/broker/config.go):
#   PEERBUS_LISTEN       WS bind host:port (default 127.0.0.1:47821; set
#                        0.0.0.0:47821 in a container so the published port
#                        reaches it)
#   PEERBUS_TOKENS       comma-separated static bearer tokens (>=1 required)
#   PEERBUS_HMAC_SECRET  shared end-to-end HMAC-SHA256 secret (min length
#                        enforced; broker refuses to start otherwise)
#   PEERBUS_DB           durable SQLite path (point at /data)
# Default WS port. Keep in sync with PEERBUS_LISTEN.
EXPOSE 47821
# No HEALTHCHECK: the broker exposes no health endpoint/subcommand and the
# distroless image ships no shell or probe tooling. `restart: always` plus
# the broker's crash-on-misconfig (missing token / short HMAC secret exits
# non-zero) is the supervision contract. Add an external WS probe if your
# platform needs a liveness signal.
ENTRYPOINT ["/usr/local/bin/peerbus"]
CMD ["serve"]
