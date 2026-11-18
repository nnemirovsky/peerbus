# Security policy

## Reporting a vulnerability

Report privately through GitHub, not in a public issue:

**[Open a private security advisory](https://github.com/nnemirovsky/peerbus/security/advisories/new)**

The report is visible only to the maintainer ([@nnemirovsky](https://github.com/nnemirovsky)) until an advisory is published, so nothing sensitive is exposed while a fix is prepared. Expect an acknowledgement within a week.

If the form is unavailable, open a normal issue that says only "I would like a private channel for a security report", with no details, and a private advisory will be opened for you.

Never attach real credentials, tokens, personal data or private files to a report. Describe the shape of the problem instead.

## Supported versions

Only the latest release gets security fixes.

## Scope

peerbus moves messages between AI agents that act on what they receive, so who may send and whether a message is genuine both matter. A vulnerability is a way to break the guarantees in the README's security model, for example:

- connecting to the broker, registering a peer name, sending or reading messages without a valid bearer token
- taking over a peer name registered under a different token
- an adapter surfacing a direct or broadcast message to its agent when the envelope's HMAC does not verify, or after any signed field changed in transit, in the queue or at the broker
- a message delivered to a peer it was not addressed to, or a broadcast reaching a peer that registered after it was sent
- a peer acking, draining or reading another peer's queue
- an edited audit chain that `peerbus audit verify` reports as intact
- the broker starting with a missing token list, or an adapter starting with an HMAC secret shorter than 32 bytes
- the broker reading, needing or storing the end-to-end HMAC secret
- the broker or an adapter writing a bearer token, the HMAC secret or a message body to its log

Out of scope:

- anything done by a party that holds the shared HMAC secret. Every peer holds it, so the HMAC detects tampering by anyone without the secret, including the broker host, but not by a peer that has it.
- what an agent does with a genuine message it receives, including instructions in the message body. Escalation and trust policy belong to the consuming agent's prompt.
- message bodies being readable by the broker host and in `peerbus.db`. Bodies are signed, not encrypted.
- `ws://` traffic read on the network. Put the broker behind TLS (`wss://`) when it is reachable beyond one machine.
- duplicate deliveries. Delivery is at-least-once by design and adapters drop repeated ids.
- anything that needs an attacker who already controls the broker host or a peer's machine
