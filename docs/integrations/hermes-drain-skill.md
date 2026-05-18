# Integration pattern: the Hermes timed self-drain skill

This is a **documented pattern**, not shipped code. peerbus deliberately
ships the generic adapter and this written guidance — never Hermes prompts,
never a Hermes skill file, never any escalation logic. Everything below
describes what *the consuming agent's own prompt/skill* should do. If you are
looking for a peerbus snippet to copy into the bus, there isn't one, and
that is the design.

## Why the policy lives in Hermes, not in peerbus

peerbus is a role-neutral transport. The broker routes bytes; adapters sign,
verify, and hand messages to the host. Neither knows or cares what a message
*means* or whether a human should be told about it. That is intentional and
load-bearing:

- The bus carries a `source` tag (e.g. `source: peer-bus`) in every message
  envelope. That tag is the **only** hook peerbus provides. It is a fact
  ("this message arrived from the peer bus"), not a policy ("therefore wake
  the human").
- The decision of *whether to think, act silently, or escalate to the human*
  is a property of the consuming agent's judgement, and judgement belongs in
  that agent's prompt. Putting it in peerbus would (a) bake one agent's
  escalation taste into shared infrastructure, (b) make every other peer
  inherit Hermes's policy, and (c) recreate the role-coupling the
  broker/adapter split exists to avoid.

So: peerbus gives Hermes a clean inbound stream and a `source: peer-bus`
label. **Hermes's SOUL/skill decides what to do with it.** Keep it that way.

## The timed self-drain pattern

The generic adapter does not push. Inbound peer-bus messages wait in the
broker's durable queue until Hermes asks for them. Hermes therefore needs a
**self-drain skill**: a recurring, Hermes-owned behaviour that calls the
adapter's drain tool on a cadence Hermes controls, processes whatever comes
back, and goes quiet again.

Recommended shape of that skill (prose — adapt to Hermes's skill format):

1. **Trigger.** A timer or idle hook owned by Hermes — e.g. "every N minutes
   of wall clock" or "at the start of each turn if it has been more than N
   minutes since the last drain." The cadence is Hermes's call. peerbus
   imposes none. Pick something that balances latency against churn for the
   workload; a few minutes is usually fine for peer coordination.

2. **Drain.** Invoke the generic adapter's drain tool (the bus.drain MCP
   tool exposed by the `peerbus` MCP server — see
   [`generic-adapter.md`](./generic-adapter.md) for the wiring). It returns
   and acknowledges every message received since the last drain. Each item
   carries its sender and its `source` tag.

3. **Think first.** For each drained message tagged `source: peer-bus`,
   Hermes reasons about it *on its own* before doing anything human-facing.
   Most peer-bus traffic is coordination another agent expects Hermes to
   handle autonomously: status, hand-offs, data, "I finished X, your turn."
   Hermes should resolve those silently — act, reply on the bus, update its
   own state — and **not** surface them to the human.

4. **Escalate only on a real decision.** Hermes pings the human **only** when
   the drained message forces a genuine decision that is outside Hermes's
   authority or judgement to make alone: an ambiguous trade-off, a
   destructive or irreversible action, a policy/authority question, a
   conflict it cannot resolve, or an explicit request that the human be
   looped in. "A peer-bus message arrived" is *never* by itself a reason to
   escalate. The bar is "a real decision is needed," not "something
   happened."

5. **Go quiet.** After processing, Hermes returns to idle. No standing
   subscription, no busy-loop, no background chatter. The next drain is the
   next timer tick.

## The `source: peer-bus` escalation policy guidance

Write this policy explicitly into Hermes's SOUL/skill, in Hermes's own words.
The intent to encode:

> Messages tagged `source: peer-bus` are peer-to-peer agent coordination.
> Default to handling them yourself: think, act, reply on the bus, update
> state. Do **not** notify the human merely because a peer-bus message
> arrived. Escalate to the human **only** when the message compels a real
> decision you should not make unilaterally — irreversible/destructive
> actions, genuine ambiguity or conflicting goals, authority/policy
> questions, or an explicit ask to involve the human. When in doubt, prefer
> thinking and acting over interrupting; reserve human attention for
> decisions, not notifications.

Tuning notes for whoever owns the Hermes prompt:

- Make the "think first, escalate only on a real decision" rule **emphatic
  and unambiguous** in the prompt. This is the single most important line;
  a weak version of it turns the peer bus into a notification firehose and
  defeats the purpose of an autonomous peer.
- Keep the policy keyed off the `source` tag, not off the sender identity or
  message content heuristics — the tag is the stable contract peerbus
  guarantees.
- If different peer senders warrant different default handling, express that
  *in Hermes*, layered on top of the `source: peer-bus` base rule. peerbus
  will not grow per-sender policy hooks; that asymmetry is deliberate.
- Revisit the drain cadence and the escalation bar independently. They are
  orthogonal: cadence is a latency/cost knob; the escalation bar is a
  judgement policy. Neither lives in peerbus.

## What this pattern is NOT

- Not a peerbus feature. Nothing here is enforced or shipped by the bus.
- Not a Hermes prompt. This document does not contain Hermes's SOUL or skill
  text; it describes what that text should accomplish so the prompt owner can
  write it natively.
- Not push. The generic adapter never wakes Hermes. The timer is Hermes's.
  (Push-wake exists only for Claude Code via the `cc` adapter and is a
  separate path — see [`generic-adapter.md`](./generic-adapter.md).)
