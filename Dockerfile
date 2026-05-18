# Multi-stage build for the peerbus broker.
#
# BROKER ONLY. peerbus's topology is one long-lived broker + many thin,
# ephemeral adapter processes. Adapters are spawned by each agent runtime
# (Claude Code spawns --adapter=cc per session; a drain-agent spawns
# --adapter=generic as its own stdio MCP child) and are NEVER containerized
# here. Containerizing an adapter — or worse, running the broker per session —
# is exactly the cc2cc orphaned-server failure mode the broker/adapter split
# designs out. This image is the managed, long-lived broker service only.
#
# Pure Go: the durable store uses modernc.org/sqlite (no cgo), so the build
# is CGO_ENABLED=0 and the final image is distroless/static (no libc, no
# shell). One static binary, nothing else.

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# CGO disabled: modernc.org/sqlite is pure Go. -s -w strips the binary.
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /out/peerbus-broker ./cmd/peerbus-broker

FROM gcr.io/distroless/static-debian12
LABEL org.opencontainers.image.source="https://github.com/nnemirovsky/peerbus"
LABEL org.opencontainers.image.description="peerbus broker — agent-agnostic durable message bus (broker only)"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=build /out/peerbus-broker /usr/local/bin/peerbus-broker
# Durable SQLite store (queue + blake3 audit chain) lives on a mounted
# volume at /data. PEERBUS_DB should point here (see deploy/compose.yml).
VOLUME ["/data"]
# Broker config is read from PEERBUS_* env (see internal/broker/config.go):
#   PEERBUS_LISTEN       WS bind host:port (default 127.0.0.1:8080; set
#                        0.0.0.0:8080 in a container so the published port
#                        reaches it)
#   PEERBUS_TOKENS       comma-separated static bearer tokens (>=1 required)
#   PEERBUS_HMAC_SECRET  shared end-to-end HMAC-SHA256 secret (min length
#                        enforced; broker refuses to start otherwise)
#   PEERBUS_DB           durable SQLite path (point at /data)
# Default WS port. Keep in sync with PEERBUS_LISTEN.
EXPOSE 8080
# No HEALTHCHECK: the broker exposes no health endpoint/subcommand and the
# distroless image ships no shell or probe tooling. `restart: always` plus
# the broker's crash-on-misconfig (missing token / short HMAC secret exits
# non-zero) is the supervision contract. Add an external WS probe if your
# platform needs a liveness signal.
ENTRYPOINT ["/usr/local/bin/peerbus-broker"]
CMD ["serve"]
