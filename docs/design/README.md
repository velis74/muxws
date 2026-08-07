---
title: Implementation briefs - index
sidebar: false
search: false
---

# Implementation briefs

muxws is specified here, broken into milestones that can be implemented one at a time. Every file in
`briefs/` is a **work order**: one milestone, self-contained, with the rules it must satisfy
reproduced in full, the files to create, the tests to write, and a "done when" checklist.

[`CLAUDE.md`](./CLAUDE.md) in this directory carries the working rules for an implementing agent -
how to work a milestone, when to stop and ask, and the `GAPS.md` deliverable. This file is the map:
what each brief delivers and what order they go in.

## How to use these (read this before you open a brief)

1. **Read the brief, not the design doc.** A brief reproduces every normative rule it puts you under.
   If a brief tells you to read the design document to find out what to build, that is a bug in the
   brief - report it rather than going to fetch the rule yourself.
2. **The design document is for *why*, never for *what*.** When a rule looks arbitrary, wrong, or
   more expensive than it needs to be, the reasoning is in
   [`muxws-websocket-transport.md`](./muxws-websocket-transport.md). Almost every MUST in these
   briefs exists because a specific plausible implementation was tried in the design and produced a
   specific silent failure. The normative specification
   ([`muxws-spec.md`](./muxws-spec.md)) is the authority on *what*; the briefs quote it.
3. **Do not change a MUST without asking.** Not the wording, not the default, not the strength. If a
   rule cannot be implemented as written, stop and ask; do not weaken it to a SHOULD, do not widen a
   default, do not "simplify" an invariant. Several rules here look redundant and are not - the
   cross-cutting invariants (`WSM-INV-*`) each name the bug they prevent, and every one of those
   bugs passes a naive test suite.
4. **Rule ids are the contract.** `WSM-CDC-016` and the rest are stable. Cite them in
   commit messages and in test docstrings. A test named in a brief in `backtick` form, or marked
   **(spec)**, must exist under exactly that name - CI and later milestones look for it.
5. **Named tests are acceptance criteria, not suggestions.** Where a brief says "the test must fail
   by hang detection" or "run with real concurrency, not sequentially", that phrasing is the point of
   the test; a version that passes for the wrong reason is worse than no test.
6. **Each brief has a "Decisions this brief takes" or "Gaps" section.** Those are places the
   specification deliberately left open, or where the brief has chosen a shipped value the
   specification does not name. Implement the stated interim, mark it with whichever comment the
   brief asks for - `GAP G-n: interim, confirm with author` where a specification gap is still open,
   `NOTE: brief-level choice, not a spec rule` where the brief simply picked something - and invent
   nothing else.
7. **Do not start a milestone whose prerequisites are not green.** Each brief's §2 lists exactly what
   the previous ones left behind, and the "done when" of those milestones is the gate.
8. **Lint conventions are opposite between the two languages.** Python: ruff, line length 120,
   **double** quotes, isort with `lines-between-types =
   1`, `X | None` never `Optional[X]`, tests colocated as `<module>_test.py`. TypeScript: eslint +
   prettier via `eslint-config-velis`, `printWidth: 120`, **single** quotes, kebab-case file names,
   `for...in` forbidden, `import/order` with `newlines-between: 'always'`, tests colocated as
   `<module>.spec.ts`. There is no `tests/` directory in this repository. `S101` (bare `assert`)
   is allowed only in `*_test.py`; `B008` needs `# noqa` on FastAPI `Depends()` defaults; `S311`
   needs `# noqa` plus a comment wherever `random` is deliberate.

## muxws - PyPI `muxws` / npm `muxws`

A framing, multiplexing, stream-lifecycle, cancellation and connection-lifecycle protocol over one
WebSocket, in Python and TypeScript. It depends on nothing above it in the stack.

| # | Brief | Delivers |
|---|---|---|
| M0 | [`muxws-m0-scaffolding.md`](./briefs/muxws-m0-scaffolding.md) | The repository: two packages on one version stream, ruff/eslint/vitest/pytest configured, a VitePress docs workspace, an empty `conformance/` tree, and a test that enforces version parity. No protocol code. |
| M1 | [`muxws-m1-frames-and-codec.md`](./briefs/muxws-m1-frames-and-codec.md) | The frame model, the `Codec` port with `JsonCodec` and explicit registration, the full error hierarchy and `ResetCode`, and the fragmentation splitter/assembler as pure functions. `conformance/frames/` and `conformance/invalid/` written. No sockets. |
| M2 | [`muxws-m2-peer-core.md`](./briefs/muxws-m2-peer-core.md) | The Python `Peer` and `Stream` over an in-memory transport: parity id allocation, the five-state machine with every cell tested, `on_stream` dispatch, the memoized awaitable handle, `request`/`notify`, cancellation. |
| M3 | [`muxws-m3-transports.md`](./briefs/muxws-m3-transports.md) | Real WebSockets in both languages (Starlette, `websockets`, browser, Node `ws`), environment codec selection (`MUXWS_CODEC` / `VITE_MUXWS_CODEC`), the `muxws.v1.<codec>` subprotocol assertion, and the full TypeScript port of M2. |
| M4 | [`muxws-m4-connection-lifecycle.md`](./briefs/muxws-m4-connection-lifecycle.md) | `ping`/`pong` and `peer.ping()`, `goaway` with `last_stream` and the drain window, `peer.close()`, id exhaustion, and the per-peer `error_serializer`. **Much smaller than it was**: there is no `settings` frame, so no handshake phase, no ack ordering and no `protocol_version`; the concurrency limit moved to M5a. |
| M5a | [`muxws-m5a-fragmentation-and-writer.md`](./briefs/muxws-m5a-fragmentation-and-writer.md) | Fragmentation wired into the send path against the `MAX_FRAME_BYTES` constant, the one-unsent-fragment rule, the round-robin writer, **all three receive-side caps** (frame size, `max_payload_bytes` enforced as fragments accumulate, and the local concurrency limit answering with `REFUSED` - moved here from M4), `on_frame` and the `muxws.frames` logger. |
| M5b | [`muxws-m5b-reconnect-and-registry.md`](./briefs/muxws-m5b-reconnect-and-registry.md) | The dialer-only reconnect helper (jittered backoff, heartbeat, `hello` replay, established-only counter reset, and `connect()` raising when the *first* attempt fails), the socket-death fan-out that fails every stream shape with `ConnectionLost`, `peer.tags` and `PeerRegistry`. **This is the first production-usable release.** |
| M6 | [`muxws-m6-conformance.md`](./briefs/muxws-m6-conformance.md) | The full `conformance/sequences/` corpus replayed by both languages in both role assignments, the live cross-language CI matrix including a reconnect scenario, the msgpack codec with its own pair, `SPEC.md`, and the 1.0 wire freeze. |
| M7 | [`muxws-m7-documentation.md`](./briefs/muxws-m7-documentation.md) | The VitePress site: a guide, a complete two-language API reference, a runnable quick start, and tests that fail when a symbol is added without a page entry. |

M5 in the specification's §17 table is one milestone; it is split here into **M5a** and **M5b**
because the two halves share no code and have disjoint test suites. "muxws M5" elsewhere means both.
M0 and M7 are additions to the specification's §17 list, explained in their own briefs.

**M4 shrank and M5a grew, and nothing was renumbered.** The `settings` frame was removed from the
protocol entirely (WSM-CON-031): `MAX_FRAME_BYTES` is a protocol constant, `max_payload_bytes` and
the concurrency limit are each one peer's own defence, and no limit, version or capability is
exchanged anywhere. That deleted most of what M4 used to be, and moved the concurrency limit - now
answering with `REFUSED`, with no `STREAM_LIMIT` code and no `StreamLimit` exception - into M5a
beside the other receive-side caps. M4 is still its own milestone because `goaway` drain ordering
against in-flight streams needs one where it is the thing being tested.

## The dependency chain

```
M0 → M1 → M2 → M3 → M4 → M5a → M5b → M6 → M7
                           │
                           │  (muxws is production-usable here: Peer.notify,
                           │   PeerRegistry.peers_for, peer.tags, peer.close,
                           │   ConnectionLost)
                           ▼
                    first consumable release
```

Every milestone depends only on the ones before it. M5a comes before M5b because M5b's socket-death
fan-out discards the writer's queues.

## Where the rules come from

| Document | Role |
|---|---|
| [`muxws-spec.md`](./muxws-spec.md) | Normative. Every `WSM-*` rule id, with no rationale. |
| [`muxws-websocket-transport.md`](./muxws-websocket-transport.md) | The design brief: why each muxws rule exists, what was tried instead, and what failed. |

Every rule in the specification is claimed by at least one brief, and no brief requires a rule that
does not exist in it. If you find a counter-example, that is a defect in this directory,
not licence to improvise.
