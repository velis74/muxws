---
title: Implementation briefs - index
sidebar: false
search: false
---

# Implementation briefs

Two libraries are specified here, and each is broken into milestones that can be implemented one at
a time. Every file in this directory is a **work order**: one milestone, self-contained, with the
rules it must satisfy reproduced in full, the files to create, the tests to write, and a "done when"
checklist.

[`CLAUDE.md`](./CLAUDE.md) in this directory carries the working rules for an implementing agent -
how to work a milestone, when to stop and ask, and the `GAPS.md` deliverable. This file is the map:
what each brief delivers and what order they go in.

## How to use these (read this before you open a brief)

1. **Read the brief, not the design doc.** A brief reproduces every normative rule it puts you under.
   If a brief tells you to read the design document to find out what to build, that is a bug in the
   brief - report it rather than going to fetch the rule yourself.
2. **The design documents are for *why*, never for *what*.** When a rule looks arbitrary, wrong, or
   more expensive than it needs to be, the reasoning is in
   [`muxws-websocket-transport.md`](./muxws-websocket-transport.md) and
   [`backchannel-progress-and-dialogs.md`](../backchannel-progress-and-dialogs.md). Almost every
   MUST in these briefs exists because a specific plausible implementation was tried in the design
   and produced a specific silent failure. The normative specifications
   ([`muxws-spec.md`](./muxws-spec.md), [`backchannel-spec.md`](../backchannel-spec.md)) are the
   authority on *what*; the briefs quote them.
3. **Do not change a MUST without asking.** Not the wording, not the default, not the strength. If a
   rule cannot be implemented as written, stop and ask; do not weaken it to a SHOULD, do not widen a
   default, do not "simplify" an invariant. Several rules here look redundant and are not - the
   cross-cutting invariants (`WSM-INV-*`, `BC-INV-*`) each name the bug they prevent, and every one
   of those bugs passes a naive test suite.
4. **Rule ids are the contract.** `WSM-CDC-016`, `BC-DEL-010` and the rest are stable. Cite them in
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
   nothing else. The backchannel specification currently carries **no** open extraction gaps: its
   §9 records exactly one deliberately unsolved problem (a Celery worker blocked on an unanswered
   dialog), and that one is a hazard to document, not a decision to take.
7. **Do not start a milestone whose prerequisites are not green.** Each brief's §2 lists exactly what
   the previous ones left behind, and the "done when" of those milestones is the gate.
8. **Lint conventions are the same across both repositories and are opposite between the two
   languages.** Python: ruff, line length 120, **double** quotes, isort with `lines-between-types =
   1`, `X | None` never `Optional[X]`, tests colocated as `<module>_test.py`. TypeScript: eslint +
   prettier via `eslint-config-velis`, `printWidth: 120`, **single** quotes, kebab-case file names,
   `for...in` forbidden, `import/order` with `newlines-between: 'always'`, tests colocated as
   `<module>.spec.ts`. There is no `tests/` directory in either repository. `S101` (bare `assert`)
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

## backchannel - `dynamicforms-backchannel` / `@dynamicforms/backchannel`

Progress reporting and awaitable dialogs for long-running server-side operations. The store is the
truth; a push is only an accelerator.

| # | Brief | Delivers |
|---|---|---|
| M0 | [`backchannel-m0-scaffolding.md`](./backchannel-m0-scaffolding.md) | The repository: a Python package with **zero** required runtime dependencies, an npm package with four subpath exports, the toolchain, and the test that proves core imports none of redis/celery/fastapi/muxws/vue. |
| M1 | [`backchannel-m1-store-reporter-register-rest.md`](./backchannel-m1-store-reporter-register-rest.md) | *(server half)* Models, `settings`, the `BackchannelStore` port with `MemoryStore` and the exported conformance suite, the `Reporter` with inline-clock throttling and **no settable state**, `operation()` in both async and sync forms with its terminal-write-then-drop exit, the namespace-scoped register of **shared** operations with its percentage-free aggregate, private operations, `migrate_namespace`, the two polling REST endpoints, and the reader as the only caller of `transport.notify()`. |
| M1b | [`backchannel-m1b-ts-client-core.md`](./backchannel-m1b-ts-client-core.md) | *(client half, part 1)* The one snake↔camel wire converter, the polling ladder as a pure function, `Operation<T>` as a `PromiseLike`, `use_backchannel`, `run`, `track`, `subscribe`, `new_token`. Persists nothing; can never be talked into polling faster. |
| M1c | [`backchannel-m1c-ts-register-and-vue.md`](./backchannel-m1c-ts-register-and-vue.md) | *(client half, part 2)* `bc.register` (the shared register plus this tab's private operations merged locally), `bc.aggregate` with the domination rule and no percentage, `bc.selected` with its FIFO default, `bc.operations` as a deliberately smaller visible subset, and the `/vue` composables. |
| M2 | [`backchannel-m2-nesting-and-ambient.md`](./backchannel-m2-nesting-and-ambient.md) | `subtask()` / `split()` with per-child contribution accounting (not last-write-wins), and the `contextvars`-bound ambient `backchannel.progress` proxy with its `Token`-reset discipline. No wire change at all. |
| M3 | [`backchannel-m3-dialogs.md`](./backchannel-m3-dialogs.md) | *(server half)* `ask()`: write the `DialogRequest`, push `dialog.open`, and block until the store arbitrates a reply - **with no timeout and no default answer**. `resolve_dialog` / `withdraw_dialogs` / `await_dialog`, the reply endpoint's 204/404/409/410/422 matrix, and `waiting_input` derived as a disjunction. |
| M3b | [`backchannel-m3b-ts-dialogs.md`](./backchannel-m3b-ts-dialogs.md) | *(client half)* `Operation.dialog`, `answer()` resolving quietly on 409/404/410 and rejecting only on 422, a close rule written against dialog state rather than envelope arrival, and no countdown anywhere. |
| M4 | [`backchannel-m4-redis-and-celery.md`](./backchannel-m4-redis-and-celery.md) | Cross-process operation: `RedisStore` with five Lua scripts passing the conformance suite unchanged, `BLPOP`-based `await_dialog` with no overall deadline, the `bcx:{ns}` pub/sub backplane and its per-process reader lifecycle, `migrate`, and the Celery worker entry that owns the terminal write - including `dispatch_failed` and `dialog_timeout`. |
| M5 | [`backchannel-m5-muxws-transport.md`](./backchannel-m5-muxws-transport.md) | **← the cross-library dependency.** `MuxwsTransport`: one envelope becomes one `peer.notify()` per connected peer of the namespace, plus the `/muxws` client subpath. Swapping `NullTransport` for it changes latency and nothing else. **Requires `muxws` at muxws M5b or later** - see below. |
| M6 | [`backchannel-m6-cancellation.md`](./backchannel-m6-cancellation.md) | The sticky cancel flag, `OperationCancelled` raised by default from the next `set()`, `cancelled` as a terminal state distinct from `failed`, the store-side terminal-final guard, and the sentinel that wakes a dialog-blocked worker - which, with dialog timeouts gone, is the only thing that can. |
| M7 | [`backchannel-m7-results-and-multi-operation.md`](./backchannel-m7-results-and-multi-operation.md) | *(mechanism)* Collectable results: `Result` with the `kind` a frontend maps like a `dialog_id`, the three result decorators, `set_result()`, the parking in `waiting_input` **that holds no worker**, the three release paths arbitrated by `store.release_result`, the `result_released` application event, and `result_ttl` as the backstop. |
| M7b | [`backchannel-m7b-several-operations-and-vuetify.md`](./backchannel-m7b-several-operations-and-vuetify.md) | *(deliverables)* The two tab-hint rules (`progressUi.delay`, `dialogAdoptDelay`) and `failureLinger`, plus two shipped deliverables: the several-operations guide pages with a runnable example, and the `/vuetify` subpath with the dialog adapter and `BackchannelFooter` - which draws one selected operation's bar and never an aggregate one. |
| M8 | [`backchannel-m8-delivery-decision.md`](./backchannel-m8-delivery-decision.md) | `push_filter` at the last hop, the per-connection `watch` override map in `peer.tags` with its `{v, status, connection}` acknowledgement and its pruning on terminal, the server-issued connection id that also routes a private operation's progress, and `OperationSummary.progress_delivery` as the read-time combination of both halves - with dialogs, cancels and terminal states unsuppressible by either. |
| M9 | [`backchannel-m9-documentation.md`](./backchannel-m9-documentation.md) | The VitePress site for both languages, with tests asserting each sentence the specification requires the documentation to contain, that every public symbol is documented, and that no removed mechanism is described anywhere. |

M1's b/c halves and M3's b half are the design document's own M1 and M3 split by language; M7b is M7
split by subject, when the collectable-result mechanism outgrew the several-operations deliverables it
used to share a brief with. M0 and M9 are additions, explained in their own briefs. **No milestone was
renumbered by any of these splits.**

## The dependency chain

```
muxws:        M0 → M1 → M2 → M3 → M4 → M5a → M5b → M6 → M7
                                              │
                                              │  (muxws is production-usable here:
                                              │   Peer.notify, PeerRegistry.peers_for,
                                              │   peer.tags, peer.close, ConnectionLost)
                                              ▼
backchannel:  M0 → M1 ─┬─→ M2 ──→ M3 ─┬─→ M4 ─→ M5 ─→ M6 ─→ M7 ─→ M7b ─→ M8 ─→ M9
                       │              │         ▲
                       ├─→ M1b → M1c ─┴─→ M3b ──┘
```

- Within muxws every milestone depends only on the ones before it. M5a comes before M5b because
  M5b's socket-death fan-out discards the writer's queues.
- Within backchannel, M1b depends on M1 (for the wire shapes and the two endpoints) and M1c depends
  on M1b. M3b depends on M3 and on M1b/M1c. M5 onward depend on the whole of M1, M1b, M1c, M3 and
  M3b. M7b depends on M7: the footer renders result rows and the collect/dismiss affordances that
  M7 builds, and M8's prerequisites are both halves.
- **The one cross-library edge is backchannel M5 → muxws M5b.** backchannel M5 needs
  `Peer.notify()` (muxws M2), `peer.close()` (muxws M4), and `PeerRegistry.peers_for()` plus
  `peer.tags` (muxws M5b). muxws M5b is explicitly the release backchannel builds against; nothing
  earlier in backchannel touches muxws at all, and backchannel M0-M4 must keep passing with muxws
  not installed.
- backchannel M8 additionally leans on `peer.tags` (muxws M5b) for the per-connection override map.

## Where the rules come from

| Document | Role |
|---|---|
| [`muxws-spec.md`](./muxws-spec.md) | Normative. Every `WSM-*` rule id, with no rationale. |
| [`backchannel-spec.md`](../backchannel-spec.md) | Normative. Every `BC-*` rule id, with no rationale. |
| [`muxws-websocket-transport.md`](./muxws-websocket-transport.md) | The design brief: why each muxws rule exists, what was tried instead, and what failed. |
| [`backchannel-progress-and-dialogs.md`](../backchannel-progress-and-dialogs.md) | The same for backchannel. |

Every rule in both specifications is claimed by at least one brief, and no brief requires a rule that
does not exist in a specification. If you find a counter-example, that is a defect in this directory,
not licence to improvise.
