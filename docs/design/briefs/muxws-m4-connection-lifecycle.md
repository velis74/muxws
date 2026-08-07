---
title: muxws M4 - connection lifecycle
sidebar: false
search: false
outline: deep
---

# muxws M4 - connection lifecycle

> **This brief is much smaller than it was.** The `settings` frame it was mostly made of no longer
> exists (WSM-CON-031): there is no handshake phase, no announced limit, no `ack` ordering and no
> `protocol_version`. The stream quota went with it - the concurrency limit is now purely
> receiver-side, answers with `REFUSED`, and moved to **M5a** with the other receive-side caps
> (WSM-STM-036/037). M4 is kept as its own milestone anyway, because `goaway` drain ordering against
> in-flight streams needs a milestone where it is the thing being tested. Nothing was renumbered.

## 1. Goal

At the end of M4 a muxws connection has a defined end, not just a defined middle. `ping`/`pong`
frames round-trip without application involvement and `peer.ping()` returns a measured RTT in each
language's unit; `goaway` shuts a connection down in an orderly way with a drain window and a
`last_stream` cut-off, so streams the remote already began finish and streams it never processed are
refused locally rather than lost silently; id exhaustion takes the same path; `peer.close()` is
graceful. An application handler that raises produces a structured `reset(APPLICATION_ERROR)` through
an `error_serializer` supplied **per peer**. All of this exists in **both** languages and is exercised
over the in-memory transport plus at least one real transport.

There is no post-socket handshake: a connection is **established** the moment the socket is open with
the subprotocol accepted (WSM-CON-030, M3's work). A peer may open a stream on its first frame, and
this milestone adds nothing a peer must send or wait for before that.

## 2. Prerequisites

M1, M2 and M3 are done. They left behind:

- **M1** - `muxws/frames.py` / `ts/frames.ts`: the frame model with all six v1 types (`open`, `data`,
  `reset`, `ping`, `pong`, `goaway` - there is no `settings` frame), envelope validation,
  unknown-field tolerance. `muxws/errors.py` / `ts/errors.ts`: the full exception hierarchy including
  `ProtocolError`, `ConnectionClosed`, `ConnectionGoingAway`, `StreamClosed`, `StreamReset`,
  `StreamRefused`, plus `ResetCode` (nine members; `5` retired). `muxws/codecs/` / `ts/codec.ts`: the
  `Codec` port, `JsonCodec`, `register_codec` / `registerCodec`. The fragmentation splitter as a pure
  function (not yet wired to a socket), and `MAX_FRAME_BYTES`.
- **M2** - `muxws/peer.py`, `muxws/stream.py`: `Peer`, `Stream`, the five-state stream machine, parity
  id allocation, `on_stream`, `open`/`notify`/`request`/`send`/`end`/`reset`, the memoized awaitable
  handle, async iteration, cancellation. `muxws/transports/memory.py` - two peers wired to each other
  in memory with no socket.
- **M3** - `muxws/transports/starlette.py`, `muxws/transports/websockets_.py`, `ts/node.ts` and the
  browser dialer; the `SocketAdapter` port; `MUXWS_CODEC` / `VITE_MUXWS_CODEC` selection; the
  `muxws.v1.<codec>` subprotocol assertion; the full TypeScript port of M2.

## 3. Files to create or modify

| Path | Action |
|---|---|
| `muxws/lifecycle.py` | create - `GoawayState`, drain bookkeeping, the ping registry (nonce → pending future) |
| `muxws/lifecycle_test.py` | create |
| `muxws/peer.py` | modify - frame dispatch for `ping`/`pong`/`goaway`, `ping()`, `close()`, the post-`goaway` open refusal, id exhaustion |
| `muxws/peer_test.py` | modify |
| `muxws/errors.py` | modify - `default_error_serializer` |
| `muxws/__init__.py` | modify - `error_serializer=` on `connect()` / `accept()` / `serve()` |
| `ts/lifecycle.ts` | create |
| `ts/lifecycle.spec.ts` | create |
| `ts/peer.ts` | modify |
| `ts/peer.spec.ts` | modify |
| `ts/errors.ts` | modify - `defaultErrorSerializer` |
| `ts/index.ts` | modify - `errorSerializer` on `ConnectOptions` / `AcceptOptions` |
| `conformance/sequences/goaway-drains-then-closes.json` | create - the first fixture in this directory; M1 and M2 created it empty |

TypeScript file names are kebab-case (`lifecycle.ts`, never `lifeCycle.ts`). There is **no**
`connection_settings.py` / `connection-settings.ts` in this project: with no `settings` frame there is
no settings value object, no effective/announced/remote triple and no ack bookkeeping to hold.

## 4. Normative rules in force

Reproduced verbatim from the specification. These are the acceptance surface of this milestone.

### Establishment and versioning

- **WSM-CON-030** A connection is **established** when the socket is open and, for a codec-bearing
  subprotocol, that subprotocol was accepted (§2.3). There MUST NOT be a post-socket handshake phase,
  a capability exchange, or any frame either peer is required to send before any other. A peer MAY
  open a stream on its first frame.
- **WSM-CON-031** There MUST NOT be a `settings` frame. Every limit is either a protocol constant
  (`MAX_FRAME_BYTES`, WSM-FRG-004) or a local receiver-side defence (`max_payload_bytes`,
  WSM-FRG-035; the concurrency limit, WSM-STM-036). A limit MUST NOT appear on the wire in any form.
  There MUST NOT be an `encoding` setting either: the codec is fixed at the handshake
  (WSM-CDC-020..022) before any frame exists.
- **WSM-CON-009** The version component of the subprotocol name (`muxws.v1.`) is the **only** version
  on the wire and pins the breaking-change generation. Additive revisions - new frame types, new
  fields - MUST NOT be announced anywhere, because WSM-FRM-001/002 already make them safe to receive.
  A change that requires the remote to *act* on a new frame type rather than tolerate it MUST bump the
  generation, which a v1 acceptor rejects at the handshake (WSM-CDC-025).
- **WSM-CON-001, -002, -003, -004, -005, -006, -007, -008** *Retired.* They specified the `settings`
  frame: sending it first, acknowledging it, the defaults-until-ack window, the ack as the ordering
  point for a revised limit, and the `protocol_version` mismatch behaviour. All of them existed only
  to serve an exchange that no longer happens. None of these ids is reused.
- **WSM-FRM-003** *Retired.* It required a peer not to send an extension frame type unless the remote
  had advertised that extension in `settings.extensions`. There is no `settings` frame and no
  extension advertisement (WSM-CON-031); a v1 peer sends only the frame types in §3.2, and a frame
  type the remote must *act* on requires a new generation (WSM-CON-009). The id is not reused.
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`
  (or set it to `0`).

### Ping / pong

- **WSM-CON-010** `ping` MUST carry a `nonce`; the receiver MUST echo it verbatim in a `pong`,
  promptly, without application involvement.
- **WSM-CON-011** Native WebSocket ping/pong control frames MUST NOT be used for liveness (browsers
  do not expose them to JavaScript).
- **WSM-CON-012** The application MAY call `peer.ping()` to measure round-trip time; it returns
  seconds (Python) / milliseconds (TypeScript).

### Goaway and shutdown

- **WSM-CON-020** `goaway` carries `code`, `reason` and `last_stream` (the highest stream id from the
  *other* peer that this peer has processed and will still complete).
- **WSM-CON-021** After **sending** `goaway`, a peer MUST refuse new incoming `open`s with
  `reset(REFUSED)` and MUST open no new streams itself.
- **WSM-CON-022** After **receiving** `goaway`, `peer.open()` MUST raise `ConnectionGoingAway`
  synchronously at the call site.
- **WSM-CON-023** After receiving `goaway`, the receiver's own streams with an id greater than
  `last_stream` MUST be reset locally with `REFUSED` (they were never processed and are safe to retry
  on a new connection).
- **WSM-CON-024** Streams at or below `last_stream` MUST be allowed to finish until the drain timeout
  (default 10 s) elapses; then the socket MUST be closed.
- **WSM-CON-025** `peer.close()` MUST send `goaway(NO_ERROR)`, drain, then close.
- **WSM-SID-007** On exhaustion at 2^31-1, the exhausting peer MUST send `goaway` with `last_stream`
  set to the highest id it has processed, MUST stop opening new streams, MUST let in-flight streams
  drain, and MUST then close.

### Opens after `goaway`, and application errors

- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between
  sockets. It MUST NOT queue. There MUST NOT be a `StreamLimit` exception and `open()` MUST NOT fail
  for concurrency: the concurrency limit is the receiver's (WSM-STM-036) and surfaces asynchronously
  as `StreamRefused` on the pending await. *(This milestone adds the `ConnectionGoingAway` case and
  nothing else; there is no third case to add later.)*
- **WSM-STM-021** The code for a stream-level violation MUST be `PROTOCOL_ERROR`, except that size
  violations use `PAYLOAD_TOO_LARGE` and concurrency-limit rejection uses `REFUSED` (WSM-STM-036).
- **WSM-STM-022** *Retired.* It required concurrency-limit rejection to use `STREAM_LIMIT` rather than
  `REFUSED`, so that an opener would back off rather than retry at once. The distinction went with the
  announced quota: the limit is now the receiver's alone and is not advertised, so `REFUSED` - nothing
  ran, retry is the application's judgement - is the whole of what the opener can be told. The id is
  not reused, and reset code 5 is retired with it.
- **WSM-ERR-001** *Retired.* It required `StreamRefused` and `StreamLimit` to be sibling classes. With
  the announced quota gone there is no `StreamLimit` (WSM-STM-022, WSM-API-004) and `StreamRefused`
  covers every refusal. The id is not reused.
- **WSM-INV-007** *Retired* with WSM-STM-022. It required `STREAM_LIMIT` not to be reported as
  `REFUSED`. The id is not reused. What replaces it as the thing to get wrong: the concurrency limit
  MUST stay entirely receiver-side (WSM-STM-036) - a sender that "helpfully" tracks how many streams
  it has open and refuses locally has reinvented the announced quota, against a number it cannot know.
- **WSM-ERR-006** A handler that raises MUST produce `reset(APPLICATION_ERROR)` with a `reason` and
  an optional structured `payload`. The default serializer produces
  `{"type": "ValueError", "message": str(exc)}`. The peer MUST accept an `error_serializer` hook of
  signature `(exc) -> Any | None` - the value it returns becomes the reset's `payload`, and `None`
  means "send no payload". The documentation MUST state plainly that a public-facing deployment should
  redact it.
- **WSM-ERR-008** `error_serializer` MUST be supplied **per peer** - an argument to `connect()` and to
  the acceptor's peer construction (`accept()` / `serve()`) - and MUST NOT be a module-level default.
  One process may hold a browser-facing peer that redacts and an internal peer that does not, and a
  process-wide setting forces the wrong answer for one of them.
- **WSM-AUT-003** A connection whose credential expires mid-life SHOULD be closed with `goaway`; the
  dialer's reconnect helper re-authenticates by dialling again. *(The `goaway` half is this
  milestone's; the reconnect half is M5b's.)*

### Configuration defaults you must honour

| Setting | Python name | TypeScript name | Default |
|---|---|---|---|
| `peer.ping()` deadline | `timeout` | `timeoutMs` | 5.0 / 5 000 |
| goaway drain | `drain` | `drainMs` | 10.0 / 10 000 |

Durations are **seconds as floats in Python** and **milliseconds as integers in TypeScript**.

## 5. Decisions this milestone must lock down

Four of the five decisions this brief used to carry served the `settings` exchange - what to do with
a non-`settings` first frame, an unusable advertised `max_frame_bytes`, what "settings exchanged"
meant, and whether `notify()` counted against an announced quota. All four are gone with the frame;
do not reintroduce any of them, and do not invent a replacement for `peer._settings_exchanged`: the
established signal is now WSM-CON-030, which M3 already satisfies at socket-accept time. The fifth,
`error_serializer` as a `connect()` / `accept()` argument, is now normative as WSM-ERR-008 and is
reproduced above. One implementation detail remains open and is decided here:

1. **`default_error_serializer` lives in `muxws/errors.py` / `ts/errors.ts`** with the signature
   `Callable[[BaseException], Any | None]` / `(err: unknown) => unknown`, and is used when a peer is
   given no `error_serializer`. It is a plain function, not a mutable module attribute anyone can
   reassign - a process holding two connections must be able to redact on one and not the other
   (WSM-ERR-008), and a rebindable global is exactly the thing that makes that impossible.

## 6. Implementation notes

- **Pings are keyed by nonce, not by order.** Store `dict[str, Future]`; a `pong` with an unknown
  nonce is ignored, not an error (it may be a late echo of a ping whose deadline already expired).
  Generate the nonce with `secrets.token_hex(8)` or `uuid4().hex` - it is not the reconnect jitter,
  so `random` is not indicated here.
- **`last_stream` is about the *other* peer's ids.** When you send `goaway`, `last_stream` is the
  highest id **the remote opened** that you have dispatched. Getting the parity backwards here makes
  every drain silently reset everything; assert the parity in a test.
- **Drain is a deadline, not a poll loop.** `await asyncio.wait_for(all_draining_streams_closed,
  drain)` then close regardless. Streams still live at the deadline take the socket-death path from
  M2 (fail locally, do not send anything).
- **`open()` gains exactly one new synchronous raise here** (WSM-API-004): `ConnectionGoingAway`,
  once a `goaway` has been *received*. It does not gain a concurrency check in this milestone or any
  later one - counting your own live streams and refusing locally is the announced quota rebuilt by
  hand, against a number this peer cannot know (WSM-INV-007).
- **Lint that will bite here:**
  - `S311` - not applicable to this milestone. Use `secrets` for ping nonces; save the
    `random` + `# noqa: S311` pattern for M5b's reconnect jitter.
  - `ARG` - the `ping`/`pong`/`goaway` dispatch handlers will have unused parameters; prefix them
    `_frame`, `_peer`, matching the repo's `_request` / `_context` style.
  - `B008` - if any FastAPI-facing helper gains a `Depends()` default, add `# noqa: B008`.
  - `S101` - `assert` is only allowed in `*_test.py`; use explicit raises in `lifecycle.py`.
  - `UP` - write `int | None`, never `Optional[int]`.
  - isort with `lines-between-types = 1` - a blank line separates `import asyncio` from
    `from muxws.errors import ConnectionGoingAway`.
  - Prettier `singleQuote: true` on the TypeScript side (opposite of Python's `quote-style = "double"`).
  - `no-restricted-syntax` forbids `for...in` - walk the pending-ping map with `Object.entries(...)`.

## 7. Tests to write

Python in `muxws/*_test.py`, TypeScript in `ts/*.spec.ts`. Every test listed must exist in **both**
languages unless marked otherwise.

**Establishment**

1. `test_no_frame_is_required_before_any_other` - two memory peers connect and the dialer's **first**
   frame on the wire is its `open`; neither side sends anything before it and neither waits for
   anything. (WSM-CON-030)
2. `test_no_settings_frame_is_ever_emitted` - across the whole suite's captured traffic, no frame of
   type `settings` and no envelope key named `settings`, `ack`, `protocol_version`, `extensions`,
   `max_frame_bytes`, `max_concurrent_streams` or `max_payload_bytes` appears on the wire.
   (WSM-CON-031) A `grep` over the source for `"settings"` as a frame type is the cheap companion
   check in §8.
3. `test_connection_level_frames_omit_stream` - `ping`, `pong`, `goaway` have no `stream` field or
   `stream: 0`. (WSM-FRM-015)

**Ping / pong**

4. `test_ping_nonce_is_echoed_verbatim` - the `pong` carries the exact nonce string. (WSM-CON-010)
5. `test_pong_needs_no_application_involvement` - no `on_stream` handler registered; ping still
   round-trips. (WSM-CON-010)
6. `test_ping_returns_round_trip_time` - Python returns seconds as a float, TypeScript milliseconds;
   assert the unit by advancing an injected clock by a known amount. (WSM-CON-012)
7. `test_ping_timeout_raises_and_does_not_kill_connection` - a swallowed pong makes `peer.ping()`
   raise after `timeout` (default 5 s / 5 000 ms) while the connection stays open. (M5b turns this
   into liveness detection; here it is only the call.)
8. `test_unknown_pong_nonce_is_ignored` - no error, connection survives.
9. `ts/peer.spec.ts::native websocket ping frames are never used` - assert the socket adapter's
   `sendText`/`sendBytes` are the only send paths taken. (WSM-CON-011)

**Goaway and shutdown**

10. `test_goaway_carries_code_reason_and_last_stream` - and `last_stream` has the **remote's** parity.
    (WSM-CON-020)
11. `test_after_sending_goaway_incoming_opens_are_refused` - `reset(REFUSED)`, and local `open()`
    raises. (WSM-CON-021)
12. `test_open_after_receiving_goaway_raises_connection_going_away_synchronously` - the exception
    comes out of the call, not out of an await. (WSM-CON-022, WSM-API-004)
13. `test_streams_above_last_stream_are_reset_refused_locally` - and nothing is sent on the wire for
    them. (WSM-CON-023)
14. `test_streams_at_or_below_last_stream_finish_within_drain` - a stream that completes inside the
    drain window delivers its payload. (WSM-CON-024)
15. `test_drain_deadline_closes_socket_with_streams_still_live` - those streams fail locally.
    (WSM-CON-024)
16. `test_close_sends_goaway_no_error_then_drains_then_closes` - assert the frame order.
    (WSM-CON-025)
17. `test_id_exhaustion_sends_goaway_and_stops_opening` - drive the allocator to 2^31-1 by injecting
    the counter; assert `goaway`, no further `open`, drain, close. (WSM-SID-007)
18. Fixture `conformance/sequences/goaway-drains-then-closes.json`, replayed by both languages.

**Application errors**

19. `test_raising_handler_produces_application_error_with_default_payload` - payload equals
    `{"type": "ValueError", "message": "..."}`. (WSM-ERR-006)
20. `test_custom_error_serializer_replaces_the_payload` - supplied at `connect()`/`accept()`, and a
    serializer returning `None` produces a `reset` with `reason` but no `payload`. (WSM-ERR-006,
    WSM-ERR-008)
21. `test_error_serializer_is_per_peer` - two peers in one process with different serializers, one
    redacting and one not, each producing its own reset payload from the same exception; there is no
    module-level default either of them could have shared. (WSM-ERR-008)

## 8. Done when

```bash
ruff check . && ruff format --check .
pytest muxws -q --cov=muxws --cov-report=term-missing
npm run lint                 # eslint ts --fix && tsc --noEmit
npm test                     # vitest run --coverage
```

- [ ] All commands above pass; coverage of `lifecycle.py` and its TS mirror is ≥ 95 %, with
      `*_test.py` and demo files excluded per `[tool.coverage.run] omit`.
- [ ] Every test in §7 exists in both languages (except the one marked single-language) and passes.
- [ ] The fixture added to `conformance/sequences/` is read by both `pytest` and `vitest`.
- [ ] Observable: a Python acceptor and a TypeScript dialer exchange a first frame that is an
      ordinary `open`, a `peer.ping()` returns a plausible RTT in each language's unit, and
      `peer.close()` on either side produces `goaway(NO_ERROR)` → drain → socket close as seen by the
      other.
- [ ] `grep -rn "settings" muxws ts` finds no frame type, no envelope key and no value object named
      `settings` - only `muxws.conf.settings`, the codec-name singleton from M3 (WSM-CON-031).
- [ ] `grep -rn "StreamLimit\|STREAM_LIMIT" muxws ts` returns nothing, here and in every later
      milestone: the class and the reset code do not exist. (`max_concurrent_streams` appears in M5a,
      as a **local** `connect()` / `accept()` argument that never reaches the wire.)
- [ ] The version numbers in `pyproject.toml` and `package.json` are still identical (WSM-PKG-001).

## 9. Out of scope

- **The heartbeat timer** (`ping_interval` / `ping_timeout`, WSM-RCN-010/011). M4 builds the
  `ping`/`pong` frames and `peer.ping()`; M5b wires them to a liveness loop that declares the socket
  dead.
- **The reconnect helper**, `hello`, `on_close`, `on_reconnect`, `ConnectionLost` on socket death -
  all M5b (§7).
- **Every receive-side cap** - the frame-size check, `max_payload_bytes` enforced as fragments
  accumulate, and the local concurrency limit answering with `reset(REFUSED)` (WSM-STM-036/037) -
  **M5a** (§4.2-4.3). M4 enforces no size limit at all and imposes no ceiling on how many streams the
  remote may open; the splitter from M1 stays a pure function until M5a wires it in.
- **`on_frame` and the `muxws.frames` logger** - M5a (§12). **`PeerRegistry` and `tags`** - M5b (§9.5).
- **`window_update` / flow control** - reserved, never sent (WSM-BPR-001).
- **The msgpack codec and the cross-language CI matrix** - M6.
