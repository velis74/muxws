---
title: muxws M5b - reconnect, socket death and the peer registry
sidebar: false
search: false
outline: deep
---

# muxws M5b - reconnect, socket death and `PeerRegistry`

> M5 in the specification's §17 table is one milestone. It is split here into **M5a**
> (`muxws-m5a-fragmentation-and-writer.md`: the send/receive path) and **M5b** (this brief). "muxws
> M5" in any other document - including backchannel's M5 prerequisite - means both halves.
> **This is the last brief of the first production-usable release: at the end of M5b, backchannel can
> be built against muxws.**

## 1. Goal

At the end of M5b the dialer reconnects on its own: jittered exponential backoff, a heartbeat that
bounds detection of a dead socket, a `hello` payload replayed byte-identically on every connection,
an attempt counter that resets only on a truly *established* connection, and `on_close` /
`on_reconnect` callbacks with honest semantics. Every stream live when a socket dies fails with
`ConnectionLost` rather than hanging, and nothing attempted while the peer is between sockets is
buffered for the next one. `PeerRegistry` and `peer.tags` let a server find peers by
application-chosen keys, with tags that die with the socket.

## 2. Prerequisites

M1-M4 and **M5a** are done. They left behind:

- **M1** - the frame model, the exception hierarchy (`ConnectionClosed`, `ConnectionLost`,
  `StreamReset`), `ResetCode` including `CONNECTION_CLOSED` = 9.
- **M2** - `Peer`, `Stream`, the five-state machine, the memoized awaitable handle, async iteration,
  cancellation; `muxws/transports/memory.py`'s `memory_pair()` with `drop()` to simulate socket
  death.
- **M3** - real transports and the `SocketAdapter` port; `connect()` / `accept()` / `serve()`;
  `ConnectOptions` **already accepts and stores `hello`, `helloHeaders`, `reconnect`,
  `pingIntervalMs`, `pingTimeoutMs`, `helloTimeoutMs`, `onClose`, `onReconnect` without acting on
  them** - this milestone makes them act.
- **M4** - `ping`/`pong` frames with the nonce registry and `peer.ping()`; `goaway` + drain;
  `peer.close()`; the per-peer `error_serializer`. It left **no** handshake signal behind and there is
  nothing to wait for after the socket: a connection is established the moment the socket is open with
  the subprotocol accepted (WSM-CON-030). If you are looking for `peer._settings_exchanged`, it does
  not exist - the `settings` frame was deleted from the protocol (WSM-CON-031).
- **M5a** - the round-robin writer and its per-stream queues, plus `writer.discard_all()`;
  `muxws/observability.py` holding `CloseReason` and the `muxws.frames` logger; `peer.on_frame`.

## 3. Files to create or modify

| Path | Action |
|---|---|
| `muxws/reconnect.py` | create - `Reconnect` options, the backoff schedule, the reconnect loop, heartbeat, hello replay |
| `muxws/reconnect_test.py` | create |
| `muxws/registry.py` | create - `PeerRegistry`, `register`, `registered`, `peers_for` |
| `muxws/registry_test.py` | create |
| `muxws/observability.py` | modify - `CloseReason` gains `will_retry` |
| `muxws/peer.py` | modify - `tags`, `on_close`, `on_reconnect`, `is_open`, the socket-death fan-out |
| `muxws/peer_test.py` | modify |
| `ts/reconnect.ts`, `ts/reconnect.spec.ts` | create |
| `ts/registry.ts`, `ts/registry.spec.ts` | create |
| `ts/peer.ts`, `ts/peer.spec.ts` | modify |

TypeScript file names are kebab-case. `PeerRegistry` ships in both languages, but the **reconnect
helper exists on the dialer only** - an acceptor cannot dial and MUST NOT have one.

## 4. Normative rules in force

Reproduced verbatim from the specification.

### Backoff

```
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

| Option | Python | TypeScript | Default | Unit / range | Meaning |
|---|---|---|---|---|---|
| initial delay | `initial_delay` | `initialDelayMs` | 0.25 s / 250 | s / ms, > 0 | Delay before the first retry, before jitter. |
| growth factor | `factor` | `factor` | 2 | ≥ 1 | Multiplier per consecutive failed attempt. |
| cap | `max_delay` | `maxDelayMs` | 30 s / 30 000 | s / ms | Upper bound on the pre-jitter delay. |
| jitter | `jitter` | `jitter` | 0.3 | fraction 0..1 | Applied symmetrically: ±30 %. |
| attempt cap | `max_attempts` | `maxAttempts` | `None` / `Infinity` (unlimited) | count | After which the peer gives up and closes for good. |

- **WSM-RCN-001** The helper's entire persistent state MUST be an attempt counter.
- **WSM-RCN-002** Jitter MUST actually be applied to every computed delay.
  Test: `reconnect_test.py::test_jitter_disperses_n_simultaneous_reconnects` (N peers whose sockets
  die at the same simulated instant have distinct first-retry instants spread across the window).
- **WSM-RCN-003** The schedule MUST be a pure function of `(attempts, options, random draw)` and MUST
  be tested as one against an injected clock and an injected random source.
  Test: `reconnect_test.py::test_schedule_before_jitter_and_cap`.
- **WSM-RCN-004** The attempt counter MUST increment on every failed attempt and MUST reset **only
  when the connection is established**, where established means both of: the socket is open with the
  subprotocol accepted (WSM-CON-030), and the hello has been acknowledged. Resetting on socket-open
  MUST NOT be done. Test: `reconnect_test.py::test_counter_does_not_reset_when_hello_never_completes`
  (a server that accepts and then drops before hello must produce a growing delay sequence).
- **WSM-RCN-006** `connect()` MUST await the first attempt and MUST **raise** if it fails, with the
  underlying error, **regardless of the reconnect configuration**. Reconnection applies to connections
  that were established and then lost; it MUST NOT apply to establishing the first one. `connect()`
  MUST NOT return a peer that is retrying in the background.
  Test: `reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`.
- **WSM-INV-018** `connect()` MUST raise when the first attempt fails (WSM-RCN-006) - or a typo in the
  URL, an unreachable host or a codec mismatch never surfaces anywhere: the application holds a peer
  that looks alive and retries forever against something that will never answer.
- **WSM-RCN-005** Jitter uses an ordinary pseudo-random source. In Python this is `random` with
  `# noqa: S311` and a comment stating that reconnect jitter is not security-sensitive; `secrets`
  MUST NOT be used.
- **WSM-INV-012** The attempt counter MUST reset only on an *established* connection (WSM-RCN-004) -
  or a server that accepts sockets while its backend is down turns exponential backoff into a
  fixed-interval hammer at the initial delay.

### Heartbeat

| Option | Python | TypeScript | Default | Unit | Meaning |
|---|---|---|---|---|---|
| heartbeat interval | `ping_interval` | `pingIntervalMs` | 20 s / 20 000 | s / ms | How often a `ping` frame goes out on an idle socket. |
| heartbeat deadline | `ping_timeout` | `pingTimeoutMs` | 10 s / 10 000 | s / ms | How long a `pong` may take before the socket is declared dead. |

- **WSM-RCN-010** The peer MUST send a `ping` every `ping_interval` on an otherwise idle socket.
- **WSM-RCN-011** If no `pong` arrives within `ping_timeout`, the socket MUST be declared dead,
  closed locally, and the backoff path MUST run exactly as after a clean close. Detection MUST
  therefore be bounded by `ping_interval + ping_timeout`.
  Test: `reconnect_test.py::test_swallowed_pong_is_detected_within_interval_plus_timeout` (must not
  wait on anything resembling a TCP timeout).
- **WSM-CON-011** Native WebSocket ping/pong control frames MUST NOT be used for liveness (browsers
  do not expose them to JavaScript).

### Hello

| Option | Python | TypeScript | Default | Unit | Meaning |
|---|---|---|---|---|---|
| hello payload | `hello` | `hello` | `None` (no hello) | any codec value | Replayed verbatim on every connection. |
| hello headers | `hello_headers` | `helloHeaders` | `None` | object | Headers for the hello `open`, for symmetry with `open()`. |
| hello deadline | `hello_timeout` | `helloTimeoutMs` | 10 s / 10 000 | s / ms | How long the hello exchange may take before the attempt is failed. |

- **WSM-RCN-020** The hello MUST be captured once, at `connect()`, and replayed **verbatim** on every
  connection this peer ever makes - the first and every reconnect alike. It MUST NOT be re-read,
  recomputed, or supplied as a callback.
- **WSM-RCN-021** The hello MUST be sent as an ordinary
  `open(hello_payload, headers=hello_headers, end=True)` - delivered to the acceptor's own
  `on_stream` handler like any other stream. muxws MUST NOT flag it, interpret it, or mark it on the
  wire in any way.
- **WSM-RCN-022** The acknowledgement is the acceptor's handler returning (WSM-STM-035 - "a handler
  that returns without having ended its stream MUST end it implicitly", from M2 - ends the stream
  implicitly). No application code is required to send one.
- **WSM-RCN-023** The hello MUST go out before any application frame on that socket, and before
  `on_reconnect` fires.
- **WSM-RCN-024** `hello` MUST be optional. A peer given none sends none and is established as soon
  as WSM-CON-030 is satisfied.
- **WSM-RCN-025** A credential MUST NOT be carried in the hello (authentication is a handshake
  concern - see WSM-AUT-001, reproduced in M3).
- **WSM-RCN-026** A failed hello MUST be a failed connection attempt: if the hello stream is reset,
  or does not complete within `hello_timeout`, the peer MUST close the socket, MUST NOT fire
  `on_reconnect`, MUST increment the attempt counter and MUST back off.
  Test: `reconnect_test.py::test_reset_hello_and_timed_out_hello_both_back_off`.
- **WSM-RCN-027** Test: `reconnect_test.py::test_three_drops_replay_byte_identical_hellos` - three
  drops produce three byte-identical hellos, `on_reconnect` fires after each acknowledgement and never
  before, and an application registering no `on_reconnect` handler still ends up with a peer the
  server can find in its registry.
- **WSM-RCN-028** muxws MUST NOT mint or store a tab identity. The recommended (documented, not
  implemented) client lifetime is `sessionStorage`; `localStorage` is wrong (shared across tabs of the
  origin) and module scope is wrong (dies on reload).
- **WSM-INV-013** The hello MUST be replayed by the helper, not by the application (WSM-RCN-020) - or
  an application that forgets gets a socket the server cannot associate with anything: connected,
  healthy-looking, subscribed to nothing, reporting no error.

### What a reconnect restores, and socket death

- **WSM-RCN-030** `on_reconnect(attempt, peer)` MUST guarantee exactly two things and nothing more: a
  live socket, and an identity the acceptor has already accepted on it. It MUST fire once per
  re-established connection, after WSM-CON-030 **and** after the hello acknowledgement.
- **WSM-RCN-031** v1 MUST NOT resume streams across a reconnect. Every stream is gone, nothing is
  replayed, no in-flight frame is re-sent, and the new socket's id space starts empty.
- **WSM-RCN-032** `Peer` survives a reconnect; `Stream` objects do not. Any stream held across one is
  already closed.
- **WSM-RCN-033** On the acceptor side a reconnect produces a **new peer object with a fresh, empty
  `tags`**. An implementation MUST NOT carry tags forward.
  Test: `registry_test.py::test_reconnect_starts_with_empty_tags`.
- **WSM-RCN-040** `on_close(reason)` MUST fire on **every** socket loss, not only the final one, and
  exactly once per loss. `reason.will_retry` MUST be `False` only when `max_attempts` is exhausted or
  `close()` was called deliberately.
- **WSM-RCN-045** `CloseReason` MUST carry exactly four fields, the same four in both languages:
  `code` (the WebSocket close code), `reason` (its text), `was_clean` / `wasClean` (whether the close
  was orderly), and `will_retry` / `willRetry` (WSM-RCN-040). It MUST be one type per language, used
  for every socket loss.
- **WSM-API-009** `peer.id` MUST be a short random prefix minted **once per process** plus a monotonic
  **per-connection** counter, rendered `<prefix>-<counter>` (e.g. `a3f-17`). An id MUST NOT be reused
  within a process and MUST NOT be duplicated within a process; across processes only the prefix may
  coincide. Reuse is the failure being prevented - two connections under one name read as one
  connection in a log - so a scheme that recycles ids of closed connections MUST NOT be used.
  *(M2 mints it. It matters here because `Peer` survives a reconnect while its connection does not:
  each re-established connection takes the **next** counter value, so a log shows the reconnect as a
  new `conn=` rather than as one continuous connection, and no id from a dropped socket is reused.)*
- **WSM-RCN-041** At socket death, before `on_close` fires:
  - a pending `await stream` / `await stream.result()` MUST reject with `ConnectionLost`, resolving
    the memoized future once so a second await gets the same error rather than hanging;
  - an `async for` MUST raise `ConnectionLost` out of the loop at the next iteration, and MUST NOT
    terminate normally (a clean end would read as "the export finished");
  - an in-flight `request()` MUST raise `ConnectionLost`, never return a partial value and never hang;
  - `stream.send()`, `end()` and `reply()` MUST raise `ConnectionLost`;
  - `stream.cancel()` and `stream.reset()` MUST be no-ops;
  - `stream.closed` MUST be set.
  Test: `peer_test.py::test_socket_death_fails_every_shape` - N streams open in every shape at once,
  the test MUST fail by hang detection rather than by an error nobody raised.
- **WSM-RCN-042** While the peer is between sockets, `open()` MUST raise `ConnectionLost`
  synchronously and `notify()` / `request()` MUST reject with it. Nothing MUST be buffered for the
  next socket. Test: `peer_test.py::test_nothing_attempted_while_disconnected_appears_on_the_new_socket`.
- **WSM-RCN-043** `peer.is_open` MUST be `False` for the whole window between a socket loss and the
  next established connection.
- **WSM-RCN-044** `max_attempts` exhausted MUST fire `on_close` once with `will_retry` false and MUST
  never dial again.
- **WSM-STM-014** On socket death every live stream MUST transition to `closed` and be failed locally
  with a synthesised `reset(CONNECTION_CLOSED)` (reset code 9) **before** `on_close` fires.
- **WSM-ERR-002** `ConnectionLost` MUST be a `StreamReset` subclass; `ConnectionClosed` MUST NOT be.
  `ConnectionLost` is *a stream* failing because the connection did, and is what every stream-shaped
  call raises. `ConnectionClosed` is *the connection* ending, and is what `serve()` and peer-level
  calls raise.
- Reset code 9 `CONNECTION_CLOSED` is synthesised locally and **MUST NEVER appear on the wire**.
- **WSM-INV-010** Nothing MUST be queued while the peer is between sockets (WSM-RCN-042) - or a queue
  flushes into a server that has forgotten the sender, and a failure that would have reached a call
  site is turned into silent misdelivery.
- **WSM-INV-011** Every stream live at socket death MUST fail with `ConnectionLost`, never hang
  (WSM-RCN-041) - or a caller sees no error, no log and no timeout, just a spinner that never stops.

### Registry and tags

- **WSM-REG-001** `peer.tags` MUST be an ordinary dict with ordinary dict semantics: any key may be
  written and overwritten for as long as the socket lives, last write wins, no bookkeeping is paid for
  a rewrite. It MUST be created with the peer and MUST die with the socket.
- **WSM-REG-002** muxws MUST NOT read, interpret, persist, snapshot or restore `tags`.
- **WSM-REG-003** A direct read of `peer.tags` MUST see the newest value immediately, with no copy
  and no snapshot in that path.

| Member | Signature | Notes |
|---|---|---|
| `register` | `register(peer) -> None` | Indexes the peer under **every key its `tags` holds at that moment**. Idempotent and re-indexing. |
| `registered` | `registered(peer)` | Context manager: `register` plus explicit deregister. |
| `peers_for` | `peers_for(**tags) -> list[Peer]` | Every live peer whose `tags` match all given keys. |

- **WSM-REG-010** `register(peer)` MUST index every key present in `tags` at call time. It MUST NOT
  have any notion of which keys matter.
- **WSM-REG-011** A tag value that cannot serve as a lookup key (a dict, a list) MUST be passed over
  rather than raising. The peer is simply not findable by that key.
  Test: `registry_test.py::test_register_passes_over_unhashable_tag_value`.
- **WSM-REG-012** `register(peer)` called again after `tags` changed MUST replace that peer's index
  entries **wholesale**: found under the new values, no longer under the old ones.
  Test: `registry_test.py::test_reregister_replaces_entries_wholesale`.
- **WSM-REG-013** The registry MUST NOT watch the dict or re-index on assignment. A peer whose tags
  changed stays findable under the values of its last `register()`.
  Test: `registry_test.py::test_tag_written_after_register_is_not_found_until_reregister`.
- **WSM-REG-014** Test: `registry_test.py::test_overwriting_a_never_indexed_key_is_free` - register
  under `session`, overwrite a different, never-looked-up map-valued key a hundred times without
  re-registering; every read sees the newest value, `peers_for(session=...)` returns the peer
  unchanged throughout, and the registry's index does not grow by a single entry.
- **WSM-REG-015** `peers_for` MUST return a **list**, not a set, in a stable order. Callers MUST
  treat it as a snapshot: a peer in it may already be closing.
- **WSM-REG-016** Removal on close MUST be automatic, via the peer's own close hook. A consumer MUST
  NOT have to prune the index.
- **WSM-REG-017** The documented usage rule: **look up on keys you do not mutate, and mutate keys you
  do not look up**; a consumer needing both on one key MUST call `register(peer)` after each write.
- **WSM-REG-018** The registry is per-process. muxws MUST NOT ship a cross-process backplane.
- **WSM-INV-014** `tags` MUST NOT survive a reconnect (WSM-RCN-033) - or a tab that silenced
  something and then died stays silent for a successor that never asked to be.

## 5. Decisions this milestone must lock down

Both decisions this brief used to carry are now normative and reproduced in §4: `CloseReason`'s four
fields are WSM-RCN-045, and `connect()`'s behaviour on a failed first attempt is WSM-RCN-006. The
second changed shape as well as status - **there is no `retry_initial` option**. `connect()` raises
when the first attempt fails, with the underlying error, whatever `reconnect` was configured; a
caller who wants the first dial retried writes that loop itself, where it can decide what a permanent
failure looks like. Do not add the boolean back.

What remains is where the type lives:

1. **`CloseReason` is a frozen dataclass in `muxws/observability.py`** (M5a created the module), and
   the `CloseReason` interface already declared in `ts/index.ts`. It carries the same first three
   fields as `ConnectionClosed`, so a handler can log one and raise the other, plus `will_retry`.

## 6. Implementation notes

- **Heartbeat idle means idle.** Reset the interval timer whenever *any* frame is sent or received,
  not on a fixed schedule; otherwise a busy connection wastes a ping every 20 s. Detection bound is
  `ping_interval + ping_timeout`, which the named test asserts.
- **The established signal is the accepted subprotocol AND the hello ack**, in that order
  (WSM-CON-030 then WSM-RCN-004). With the `settings` exchange gone, the socket-open moment and the
  subprotocol moment coincide, so for a peer with no hello "established" really is socket-open - and
  for a peer *with* one, the hello ack is the whole of the difference. Reset the attempt counter at
  exactly that point and nowhere else. Resetting on socket-open when a hello is configured is the
  single most common way this gets written wrong, and it silently converts exponential backoff into a
  fixed-interval hammer.
- **Capture the hello at `connect()`**, deep-copy it if the language permits, and hold the encoded
  form so that "byte-identical" (WSM-RCN-027) is true by construction rather than by hoping the
  application did not mutate its dict.
- **Socket death is a fan-out, not a cascade.** Build one `fail_all(reason)` that walks
  `peer.streams`, synthesises `ConnectionLost` (code 9, never sent), resolves each memoized future
  *once*, pushes a sentinel into each iteration queue, sets `stream.closed`, calls M5a's
  `writer.discard_all()` - and only then fires `on_close`. WSM-RCN-041 lists six shapes; write the
  fan-out so that adding a seventh shape cannot be forgotten (one place that iterates the shapes).
- **Nothing is buffered between sockets** (WSM-RCN-042). When the socket dies the writer's queues are
  discarded, not held. `is_open` is `False` from the loss until the next *established* connection,
  not until the next socket-open.
- **The registry index is `dict[tuple[str, Hashable], set[Peer]]` plus a reverse
  `dict[Peer, set[key]]`** so `register` can remove the previous entries wholesale (WSM-REG-012) in
  one pass. Skip a tag value that raises on `hash()` (WSM-REG-011) - catch `TypeError`, do not
  pre-check types. In TypeScript, "hashable" means a `string | number | boolean | bigint | symbol`;
  objects and arrays are skipped.
- **`peer.tags` is a plain dict.** No proxy, no observer, no `__setitem__` override - WSM-REG-003 and
  WSM-REG-014 exist to make a plain dict provably the right choice.
- **Lint that will bite here:**
  - **`S311` fires on `random`** in `reconnect.py`. The correct resolution is `random` with
    `# noqa: S311` and a comment that reconnect jitter is not security-sensitive - **not `secrets`**
    (WSM-RCN-005 forbids `secrets` here). This is the opposite of M4, where the ping nonce uses
    `secrets` deliberately.
  - `ARG` - the injected clock and random source in tests, and unused hook parameters, are
    underscore-prefixed (`_clock`, `_frame`).
  - `S101` - `assert` only inside `*_test.py`.
  - `B` (bugbear) - `B006` will fire on a mutable default for `hello_headers`; use `None`.
  - `UP` - `float | None`, never `Optional[float]`.
  - isort with `lines-between-types = 1` - a blank line separates `import asyncio` from
    `from muxws.errors import ConnectionLost`.
  - `no-restricted-syntax` forbids `for...in` - walk `peer.tags` with `Object.entries(...)`.
  - `unicorn/filename-case: kebabCase` for every new TS file.
  - Prettier `printWidth: 120`, `singleQuote: true` on the TS side.

## 7. Tests to write

Both languages unless marked. Names marked **(spec)** are named in the specification and must appear
verbatim.

**Backoff and heartbeat**

1. `reconnect_test.py::test_schedule_before_jitter_and_cap` **(spec)** - pure function of
   `(attempts, options, draw)` against an injected clock and random source; asserts
   `0.25, 0.5, 1, 2, ... , 30, 30` for the defaults.
2. `reconnect_test.py::test_jitter_disperses_n_simultaneous_reconnects` **(spec)** - N peers dying
   at the same simulated instant have distinct first-retry instants spread across the ±30 % window.
3. `reconnect_test.py::test_counter_does_not_reset_when_hello_never_completes` **(spec)** - a server
   that accepts and then drops before hello produces a *growing* delay sequence (WSM-RCN-004).
4. `test_counter_resets_only_on_established` - socket-open alone does not reset it.
5. `reconnect_test.py::test_swallowed_pong_is_detected_within_interval_plus_timeout` **(spec)** -
   against an injected clock; the test must not wait on anything resembling a TCP timeout
   (WSM-RCN-011).
6. `test_heartbeat_timer_resets_on_any_traffic` - a busy socket emits no ping.
7. `test_dead_socket_takes_the_same_backoff_path_as_a_clean_close`.
8. `reconnect_test.py::test_max_attempts_exhausted_fires_on_close_once_and_never_dials_again`
   (WSM-RCN-044).
9. `test_acceptor_has_no_reconnect_helper` - constructing one on an acceptor raises.

**Hello**

10. `reconnect_test.py::test_three_drops_replay_byte_identical_hellos` **(spec)** - three drops
    produce three byte-identical hellos, `on_reconnect` fires after each acknowledgement and never
    before, and an application registering no `on_reconnect` handler still ends up with a peer the
    server can find in its registry (WSM-RCN-027).
11. `reconnect_test.py::test_reset_hello_and_timed_out_hello_both_back_off` **(spec)** - no
    `on_reconnect`, counter incremented, socket closed (WSM-RCN-026).
12. `test_hello_is_an_ordinary_open_reaching_on_stream` - the acceptor's handler sees it as a normal
    stream, and no wire field marks it (WSM-RCN-021).
13. `test_hello_precedes_every_application_frame_on_the_socket` (WSM-RCN-023).
14. `test_no_hello_means_established_at_subprotocol_accept` - a peer configured without a `hello`
    sends none, and is established (counter reset, `is_open` true, `on_reconnect` fired) as soon as
    the socket is open with the subprotocol accepted, with no frame exchanged first (WSM-RCN-024,
    WSM-CON-030).
15. `test_mutating_the_hello_object_after_connect_does_not_change_the_wire` (WSM-RCN-020).

**Socket death and the between-sockets window**

16. `peer_test.py::test_socket_death_fails_every_shape` **(spec)** - N streams open simultaneously in
    every shape (pending `await`, `await result()`, `async for`, in-flight `request()`, a sender
    mid-`send()`, a stream awaiting `cancel()`); each fails with `ConnectionLost`. **The test must
    fail by hang detection** (a global timeout that reports which shape hung) rather than by an error
    nobody raised (WSM-RCN-041).
17. `test_second_await_after_death_returns_the_same_error` - the memoized future resolved once
    (WSM-RCN-041, WSM-API-010).
18. `test_async_for_raises_rather_than_terminating_normally` (WSM-RCN-041).
19. `test_cancel_and_reset_are_noops_after_death` (WSM-RCN-041).
20. `peer_test.py::test_nothing_attempted_while_disconnected_appears_on_the_new_socket` **(spec)** -
    N `open`/`notify`/`request` calls during the gap all raise, and the new socket carries none of
    them (WSM-RCN-042, WSM-INV-010).
21. `test_is_open_false_for_the_whole_gap` (WSM-RCN-043).
22. `test_connection_closed_code_never_appears_on_the_wire` - grep the frame log for code 9.
23. `test_on_close_fires_once_per_loss_with_correct_will_retry` (WSM-RCN-040).
24. `test_streams_do_not_survive_reconnect` - a `Stream` held across one is closed (WSM-RCN-031/032).
25. `reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`
    **(spec)** - `connect()` to a URL nothing answers, with `reconnect=Reconnect()` and no attempt
    cap, raises the underlying error rather than returning a peer that retries in the background; no
    `retry_initial`-style option exists to change that (WSM-RCN-006, WSM-INV-018).
26. `test_close_reason_shape_is_identical_in_both_languages` - exactly the four fields of
    WSM-RCN-045, one type per language, used for every socket loss (decision 1).
26a. `test_peer_id_advances_on_every_reconnect` - the same `Peer` object reports a different `id`
    after each re-established connection, ids never repeat within the process, and no id from a
    dropped socket is handed out again (WSM-API-009).

**Registry and tags**

27. `registry_test.py::test_register_passes_over_unhashable_tag_value` **(spec)** (WSM-REG-011).
28. `registry_test.py::test_reregister_replaces_entries_wholesale` **(spec)** (WSM-REG-012).
29. `registry_test.py::test_tag_written_after_register_is_not_found_until_reregister` **(spec)**
    (WSM-REG-013).
30. `registry_test.py::test_overwriting_a_never_indexed_key_is_free` **(spec)** - register under
    `session`, overwrite a different, never-looked-up map-valued key a hundred times without
    re-registering; every read sees the newest value, `peers_for(session=...)` returns the peer
    unchanged throughout, and **the registry's index does not grow by a single entry**
    (WSM-REG-014).
31. `registry_test.py::test_reconnect_starts_with_empty_tags` **(spec)** - the acceptor-side peer
    object is new and its `tags` is empty (WSM-RCN-033, WSM-INV-014).
32. `test_peers_for_returns_a_list_in_stable_order` (WSM-REG-015).
33. `test_close_removes_the_peer_from_the_index_automatically` (WSM-REG-016).
34. `test_peers_for_matches_all_given_keys` - partial matches are excluded.

## 8. Done when

```bash
ruff check . && ruff format --check .
pytest muxws -q --cov=muxws --cov-report=term-missing
npm run lint
npm test
```

- [ ] All four commands pass. Coverage of `reconnect.py`, `registry.py` and their TS mirrors is ≥ 95 %.
- [ ] Every test in §7 exists and passes; every **(spec)**-marked name matches character for character.
- [ ] `grep -rn "secrets" muxws/reconnect.py` returns nothing, and `random` there carries
      `# noqa: S311` with the "not security-sensitive" comment.
- [ ] Observable: killing the acceptor process with three streams open makes every one of them raise
      `ConnectionLost`, then the dialer re-dials on a jittered delay, replays its hello, and fires
      `on_reconnect(attempt, peer)` exactly once - with `peer.tags` empty on the acceptor side.
- [ ] Observable: `peers_for(session="abc")` finds a peer registered under that tag and stops finding
      it the moment its socket closes.
- [ ] `pyproject.toml` and `package.json` versions still match (WSM-PKG-001), and the Python package
      still has zero required runtime dependencies (WSM-PKG-002).
- [ ] **The release gate:** a consumer can `pip install muxws` /
      `npm i muxws`, dial with a `hello`, register the acceptor-side peer under a tag,
      and find it again with `peers_for`. This is what backchannel M5 builds against.

## 9. Out of scope

- **Stream resumption across a reconnect.** Explicitly forbidden in v1 (WSM-RCN-031).
- **A cross-process registry backplane** (WSM-REG-018) - per-process only.
- **Minting or storing a tab identity** (WSM-RCN-028) - documented in M7, never implemented.
- **Fragmentation, the writer, receive-side caps and the frame logger** - M5a.
- **The msgpack codec, the full `conformance/sequences/` corpus, the live cross-language CI matrix,
  and the 1.0 wire freeze** - M6.
- **The VitePress documentation site** - M7. M5b writes docstrings, not pages.
