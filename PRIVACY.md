# Privacy policy

peerbus runs on machines you control: one broker you operate and an adapter next to each agent. The project operates no server and peerbus collects no telemetry. Nothing is sent to the author or to any third party.

## What it reads

- **Broker:** its settings from `PEERBUS_LISTEN`, `PEERBUS_TOKENS`, `PEERBUS_HMAC_SECRET` and `PEERBUS_DB`, and the frames adapters send it.
- **Adapters:** their settings from `PEERBUS_URL`, `PEERBUS_NAME`, `PEERBUS_TOKEN` and `PEERBUS_HMAC_SECRET`, the arguments of the `bus.*` tools the agent calls, and the messages the broker delivers.

## What it writes

- **Broker:** one SQLite database at `PEERBUS_DB` (default `peerbus.db`, `/data/peerbus.db` in the compose setup). It holds the registered peer names with timestamps, every message envelope in full (sender, recipient, source, kind, body and signature) with its delivery and ack state, and an audit chain with one row per send, deliver and ack that records the message id, sender and recipient but not the body. Nothing is deleted automatically: messages and audit rows stay until you delete the database. The broker logs peer names and message ids to stderr, never bodies or secrets.
- **Adapters:** no files. The seen-id cache used to drop duplicates lives in memory. The Claude Code adapter logs connection events to stderr.

## Where it sends data

- **Adapters** send their bearer token, peer name and the messages the agent sends to the broker at `PEERBUS_URL` over WebSocket. Use a `wss://` URL to encrypt that traffic.
- **The broker** delivers each message to the peers it is addressed to, and a broadcast to every peer registered when it was sent.
- **Adapters** hand delivered messages to their agent. In Claude Code the message becomes part of the session, which Anthropic handles under its own terms, not this project's.

## Contact

Questions go to [GitHub issues](https://github.com/nnemirovsky/peerbus/issues).
