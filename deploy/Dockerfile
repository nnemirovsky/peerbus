# Multi-stage build for the peerbus broker (broker ONLY — adapters are
# spawned by each agent runtime, never containerized here; see compose.yml).
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# CGO disabled: the store uses modernc.org/sqlite (pure Go), no cgo needed.
RUN CGO_ENABLED=0 go build -o /out/peerbus-broker ./cmd/peerbus-broker

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/peerbus-broker /usr/local/bin/peerbus-broker
# Durable SQLite store lives on a mounted volume at /data (see compose.yml).
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/peerbus-broker"]
CMD ["serve"]
