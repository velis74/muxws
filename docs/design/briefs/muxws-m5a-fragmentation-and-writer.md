---
title: muxws M5a - fragmentation, the writer and observability
sidebar: false
search: false
outline: deep
---

# muxws M5a - fragmentation on the wire, the round-robin writer, the receive-side caps, observability

> **This brief grew.** All three receive-side caps now live here, including the **local concurrency
> limit**, which M4 used to enforce against an announced quota. There is no announced quota and no
> `settings` frame any more (WSM-CON-031): a receiver decides on its own how many streams the remote
> may have open on it, refuses the excess with `reset(REFUSED)`, and tells the sender nothing in
> advance (WSM-STM-036/037). Nothing was renumbered; M4 kept `ping`/`pong`, `goaway` and `close()`.

> M5 in the specification's §17 table is one milestone. It is split here into **M5a** (this brief:
> the send/receive path) and **M5b** (`muxws-m5b-reconnect-and-registry.md`: the reconnect helper,
> socket death and `PeerRegistry`) because the two halves share no code, have disjoint test suites
> and are separately shippable. "muxws M5" in any other document means both halves. M5a comes first:
> M5b's `test_socket_death_fails_every_shape` needs the writer's queues to exist so it can assert
> they are discarded.

## 1. Goal

At the end of M5a a payload larger than `MAX_FRAME_BYTES` is fragmented automatically on the send
path, a stream holds at most **one** unsent fragment at a time, and a round-robin writer picks the
next frame across streams so a 200-byte progress update is never stuck behind a megabyte export. The
receive side measures whole encoded messages, rejects an over-cap message and an over-`max_payload_bytes`
payload with `reset(PAYLOAD_TOO_LARGE)` - the latter incrementally, as fragments accumulate, without
waiting for reassembly to finish - and answers an `open` beyond its own concurrency limit with
`reset(REFUSED)` without invoking the handler. All three caps are local: none is announced, and a
sender learns of one only from the reset it provokes. Every frame is loggable in one line under the
`muxws.frames` logger and reachable through `peer.on_frame`, and no payload content ever reaches a log
record. All of it in **both** languages.

## 2. Prerequisites

M1-M4 are done. They left behind:

- **M1** - `muxws/frames.py` / `ts/frames.ts` (all six v1 frame types, envelope validation);
  `muxws/errors.py` / `ts/errors.ts` (the exception hierarchy, `ResetCode`, `MAX_FRAME_BYTES`);
  `muxws/codecs/` / `ts/codec.ts` (the `Codec` port, `JsonCodec`); **the fragmentation splitter and
  assembler as pure functions** of `(payload, cap, codec)` - `encoded_length`, `split_frame`,
  `Assembler` - written and tested in M1, *not yet attached to a socket*.
- **M2** - `muxws/peer.py`, `muxws/stream.py`: `Peer`, `Stream`, the five-state machine, parity id
  allocation, the memoized awaitable handle, async iteration, cancellation;
  `muxws/transports/memory.py`'s `memory_pair()`.
- **M3** - real transports and the `SocketAdapter` port (`send_text`/`sendText`,
  `send_bytes`/`sendBytes`, `receive`, `close`); environment codec selection; the `muxws.v1.<codec>`
  subprotocol assertion; the full TypeScript port.
- **M4** - `muxws/lifecycle.py` / `ts/lifecycle.ts` with `ping`/`pong` and `peer.ping()`, `goaway`
  with `last_stream` and the drain window, `peer.close()`, and the per-peer `error_serializer`. M4
  left **no** limit bookkeeping behind: there is no settings value object, no announced/effective
  pair, and no concurrency ceiling anywhere yet. Every cap in this milestone is new here and local.

## 3. Files to create or modify

| Path | Action |
|---|---|
| `muxws/writer.py` | create - the round-robin frame writer and per-stream send queues |
| `muxws/writer_test.py` | create |
| `muxws/fragment.py` | modify - wire the M1 splitter into the send path; add the receive-side cap checks |
| `muxws/fragment_test.py` | modify |
| `muxws/observability.py` | create - `CloseReason`, the `muxws.frames` one-line logger, `on_frame` dispatch |
| `muxws/peer.py` | modify - send through the writer, the three receive-side caps, `on_frame` |
| `muxws/peer_test.py` | modify |
| `muxws/__init__.py` | modify - `max_payload_bytes=` and `max_concurrent_streams=` on `connect()` / `accept()` / `serve()` |
| `ts/writer.ts`, `ts/writer.spec.ts` | create |
| `ts/observability.ts` | create |
| `ts/fragment.ts`, `ts/fragment.spec.ts`, `ts/peer.ts`, `ts/peer.spec.ts` | modify |
| `ts/index.ts` | modify - `maxPayloadBytes` / `maxConcurrentStreams` on `AcceptOptions` |
| `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json` | create |
| `conformance/invalid/fragment-interrupted-by-non-fragment.json` | modify - now asserted against the live receive path |
| `conformance/invalid/open-beyond-receiver-concurrency-limit.json` | create - asserts `reset(code=4)` out and the connection surviving |

TypeScript file names are kebab-case.

## 4. Normative rules in force

Reproduced verbatim from the specification.

### Sizes

- **WSM-FRG-001** Every size limit in this specification MUST be measured as the byte length of the
  **fully encoded WebSocket message** - the complete codec output for the frame, envelope included,
  exactly as it goes on the wire. Not the pre-encoding payload, not the `fragment` field alone.
- **WSM-FRG-002** Under a text codec the TypeScript port MUST measure with
  `new TextEncoder().encode(text).length` (or an equivalent incremental byte count) and the Python
  port with `len(text.encode("utf-8"))`. A JavaScript string's `.length` MUST NOT be used (it counts
  UTF-16 code units and disagrees with Python on every non-BMP character).
- **WSM-FRG-003** Under a binary codec both ports MUST take the length of the produced buffer.
  Test: `fragment_test.py::test_slice_point_sweep_never_exceeds_cap` / `fragment.spec.ts`.
- **WSM-INV-003** Every size limit MUST be counted in bytes of the **codec's own output**
  (WSM-FRG-001) - or a sender budgeting against JSON text while msgpack bytes go on the wire produces
  over-cap frames on exactly the deployments that chose the compact codec.

### Sender

- **WSM-FRG-004** `MAX_FRAME_BYTES` is a **protocol constant of 65536** (64 KiB): the largest encoded
  message a sender may emit. It MUST NOT be negotiated, announced, or read from configuration. A
  receiver MUST accept any message up to the constant and MAY accept larger ones; a sender MUST always
  fragment at the constant regardless of what the remote appears willing to accept.
- **WSM-FRG-005** An implementation MAY expose the cap as a construction argument **for tests only**
  (the conformance runner uses it, WSM-TST-002). It MUST NOT be documented as deployment
  configuration and MUST NOT appear on the wire. A cap too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error at peer construction (WSM-FRG-034).
- **WSM-FRG-034** A test-override cap (WSM-FRG-005) too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error when the peer is constructed, not be discovered
  later as an infinite split loop.
- **WSM-FRG-010** A sender MUST fragment any logical payload whose encoded frame would exceed
  `MAX_FRAME_BYTES` (WSM-FRG-004). Fragmentation is mandatory, not an optimisation.
- **WSM-FRG-011** The sender MUST encode the logical payload with the connection's codec, slice that
  encoded form, and put each slice into a frame that the codec then encodes again.
- **WSM-FRG-012** Slices MUST be cut at a boundary the codec can represent: Unicode codepoint
  boundaries of the encoded text for a text codec, byte boundaries for a binary one. Under JSON in
  TypeScript this additionally means never splitting a surrogate pair. A splitter that would land
  mid-sequence MUST move the boundary backwards.
- **WSM-FRG-013** The sender MUST budget for the envelope and for re-encoding expansion: slice to
  `cap - reservation` bytes of encoded payload, where `reservation = min(512, cap // 2)`.
- **WSM-FRG-014** The sender MUST then **verify and re-split**: encode the frame, and if the encoded
  message still exceeds the cap, re-split that slice and try again. The reservation is a per-codec
  hint; the loop is the guarantee.
- **WSM-FRG-015** The splitter MUST be a pure function of `(payload, cap, codec)` and MUST be tested
  as one, independently of any socket.
- **WSM-FRG-016** Both ports MUST produce the **same fragment boundaries** for the same payload, cap
  and codec. Test: shared fixtures in `conformance/frames/`.
- **WSM-FRG-017** Fragments of one logical payload MUST be contiguous **on that stream**. Frames of
  other streams MAY and SHOULD interleave between them.
- **WSM-FRG-018** A stream MUST have at most **one unsent fragment queued** at any moment: fragment
  *n+1* is encoded only after fragment *n* has been handed to the socket.
- **WSM-FRG-019** The writer MUST select the next frame by **round-robin over the streams that have
  queued work**. A FIFO send queue MUST NOT be used (the ordering decision would be made at enqueue
  time, and interleaving becomes impossible).
  Test: `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json`.
- **WSM-FRG-020** `end: true` MUST appear only on the final fragment of a payload.
- **WSM-FRG-021** `headers` MUST NOT be fragmented. An `open` whose headers alone push the frame over
  the cap MUST be rejected by the receiver with `reset(PAYLOAD_TOO_LARGE)`.
- **WSM-INV-004** At most one unsent fragment per stream, and round-robin writer selection
  (WSM-FRG-018/019) - or a 1 MB payload adds a full second of latency to a 200-byte progress update on
  another stream.

### Receiver

- **WSM-FRG-030** The receiver MUST concatenate `fragment` values and hand the result to the codec
  for decoding when a fragment arrives without `more: true`.
- **WSM-FRG-031** A receiver that enforces a frame-size limit MUST measure the entire encoded message,
  never the `fragment` field on its own, and MUST reset that stream with `PAYLOAD_TOO_LARGE` when it
  is over. Enforcement above `MAX_FRAME_BYTES` is optional (WSM-FRG-004); a receiver MUST NOT reject a
  message at or below the constant.
- **WSM-FRG-032** A payload whose accumulated fragments exceed `max_payload_bytes` MUST be rejected
  with `reset(PAYLOAD_TOO_LARGE)` **as soon as the limit is crossed**, before further fragments are
  accepted and without waiting for reassembly to complete. Bounding memory is the limit's whole
  purpose, and a receiver that assembles the payload in order to measure it has already spent what the
  limit was protecting.
  Test: `fragment_test.py::test_oversize_payload_resets_on_the_crossing_fragment` (asserts the reset
  goes out before the final fragment arrives).
- **WSM-FRG-033** Receiving a non-fragment frame on a stream with a fragment assembly in progress is
  a **stream-level** protocol error. Test: `conformance/invalid/fragment-interrupted-by-non-fragment.json`.
- **WSM-FRG-035** `max_payload_bytes` - the largest reassembled payload this peer will accept - is a
  **local receiver setting**, default **67108864** (64 MiB). It MUST NOT be announced on the wire, and
  a sender MUST NOT be given any way to learn it other than the reset it produces.
- **WSM-STM-020** A frame illegal *for a stream* MUST reset **that stream** and leave the connection
  alone. Stream-level cases: `data` after receiving `end`; a non-fragment frame mid-reassembly
  (WSM-FRG-033); a frame the receiver declines as over-size (WSM-FRG-031); a payload over
  `max_payload_bytes` (WSM-FRG-032); an `open` beyond the receiver's own concurrency limit
  (WSM-STM-036).
- **WSM-STM-021** The code for a stream-level violation MUST be `PROTOCOL_ERROR`, except that size
  violations use `PAYLOAD_TOO_LARGE` and concurrency-limit rejection uses `REFUSED` (WSM-STM-036).

- **WSM-INV-017** `max_payload_bytes` MUST be enforced as fragments accumulate, not after reassembly
  (WSM-FRG-032) - or a receiver has already allocated everything the limit existed to bound by the
  time it decides to refuse it, and the limit protects nothing but a variable.

### The receiver's concurrency limit

- **WSM-STM-036** A peer MUST enforce a **local, receiver-side** limit on how many streams the remote
  may have open on it at once, default **100**, and MUST answer an `open` beyond it with
  `reset(REFUSED)` without invoking the handler. The limit MUST NOT be announced on the wire and MUST
  NOT be enforced by the sender: `open()` MUST NOT check it, MUST NOT raise for it, and MUST put the
  `open` frame on the wire like any other (WSM-API-004).
  Test: `peer_test.py::test_open_beyond_receiver_limit_is_refused_and_opener_raises_nothing_locally`.
- **WSM-STM-037** Streams this peer opened itself MUST NOT count against WSM-STM-036; the limit bounds
  work the *remote* can impose, and each peer counts only the other's opens.
- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between
  sockets. It MUST NOT queue. There MUST NOT be a `StreamLimit` exception and `open()` MUST NOT fail
  for concurrency: the concurrency limit is the receiver's (WSM-STM-036) and surfaces asynchronously
  as `StreamRefused` on the pending await.
- **WSM-INV-007** *Retired* with WSM-STM-022. It required `STREAM_LIMIT` not to be reported as
  `REFUSED`. The id is not reused. What replaces it as the thing to get wrong: the concurrency limit
  MUST stay entirely receiver-side (WSM-STM-036) - a sender that "helpfully" tracks how many streams
  it has open and refuses locally has reinvented the announced quota, against a number it cannot know.

### Backpressure

- **WSM-BPR-001** v1 MUST NOT implement per-stream flow control. `window_update` is reserved as a
  frame type and MUST NOT be sent.
- **WSM-BPR-002** The flow-control mechanisms in v1 are exactly the receiver's local concurrency limit
  (WSM-STM-036, how many producers can exist at once) and `MAX_FRAME_BYTES` (WSM-FRG-004, how long any
  one may hold the wire). Both are local defences, not agreements; there is no negotiated mechanism of
  any kind, and building one requires a new generation (WSM-CON-009).

### Observability

- **WSM-OBS-001** Every frame MUST be loggable in one line at `DEBUG` under the `muxws.frames`
  logger. Suggested shape:

```
muxws conn=a3f-17 dir=tx type=open   stream=7  end=0 bytes=214  headers=1
muxws conn=a3f-17 dir=rx type=data   stream=7  end=0 bytes=8192 frag=1/4
muxws conn=a3f-17 dir=tx type=reset  stream=9  code=1 reason="user navigated away"
muxws conn=a3f-17 dir=rx type=goaway last=7 code=0
```

`conn=` is `peer.id`: a per-process prefix plus a per-connection counter (WSM-API-009), so two lines
carrying the same `conn=` are always the same connection.

- **WSM-OBS-002** The peer MUST NOT log payload *contents* at any level (application data routinely
  contains secrets).
- **WSM-OBS-003** `peer.on_frame(handler)` MUST receive `(direction, frame, byte_length)` before
  encode and after decode.
- **WSM-API-016** In TypeScript the implementation MUST attach a default no-op rejection handler to
  the internal promise **at construction time** (not lazily on first `then`), and MUST surface the
  failure through the peer's frame/error hook instead.
  Test: `stream.spec.ts::reset stream nobody consumed reports no unhandled rejection and does reach the error hook`.
- **WSM-API-017** In Python, `peer.open(...)` on a line by itself MUST emit no `RuntimeWarning`.
  Test: `stream_test.py::test_unconsumed_open_emits_no_runtime_warning`.

## 5. Decisions this milestone must lock down

The one decision this brief used to carry - that `max_payload_bytes` is enforced incrementally - is
now normative as WSM-FRG-032 and is reproduced in §4. What is left is one implementation detail:

1. **Discard the partial buffer at the moment you reset.** WSM-FRG-032 says reset on the crossing
   fragment; releasing the bytes you already hold is the other half of the same intent, and a
   reassembly buffer left attached to a reset stream keeps exactly the memory the limit existed to
   bound. Drop the assembler with the stream, in the same step.

## 6. Implementation notes

- **The writer is the whole of WSM-FRG-017/018/019 and it is one object.** `Writer` holds
  `dict[int, StreamQueue]` plus a rotating cursor. `StreamQueue` holds *at most one encoded frame*
  and the remaining un-sliced tail of the current payload. The loop is: pick the next stream with
  work by round-robin, send its one queued frame, then (and only then) slice the next fragment from
  its tail. That ordering is what WSM-FRG-018 means; encoding ahead is the bug it forbids.
  A `list` used as a FIFO of frames anywhere in the send path is an automatic failure.
- **Do not measure the `fragment` field.** Both cap checks (send-side budget and receive-side
  enforcement) use the byte length of the *fully encoded message*. On send that means: slice to
  `cap - min(512, cap // 2)`, encode the frame, measure, and if still over, re-split - a loop, not a
  single calculation (WSM-FRG-014). Bound the loop at, say, 8 iterations and raise `InternalError`
  beyond it rather than spinning.
- **`end: true` rides only the last fragment** (WSM-FRG-020) - so the `end` flag must be attached
  when the tail empties, not when the payload is enqueued.
- **There is one cap and it is a constant.** You fragment against `MAX_FRAME_BYTES` and you accept
  anything up to it; there is no remote value to read, no local value to announce and no pair to keep
  in sync (WSM-FRG-004). The only way a smaller number ever reaches the peer is the test-only
  construction argument of WSM-FRG-005, which the conformance runner uses and which must be rejected
  at construction if it cannot hold an envelope plus one indivisible unit (WSM-FRG-034).
- **The two *local* caps are ordinary `connect()` / `accept()` arguments**: `max_payload_bytes`
  (default 67 108 864) and `max_concurrent_streams` (default 100), `maxPayloadBytes` /
  `maxConcurrentStreams` in TypeScript. Neither is ever encoded into a frame, and neither has a
  "remote" counterpart to consult - if you find yourself writing `remote.something`, the announced
  quota has grown back.
- **Count only the remote's opens** (WSM-STM-037). The counter is the number of *live* streams whose
  `open` this peer received, incremented at dispatch and decremented when the stream closes. Streams
  this peer opened itself are not in it, and `notify()` - which opens a real stream - is subject to
  the *remote's* limit like any other open, asynchronously, never to a local check.
- **`on_frame` fires before encode and after decode** (WSM-OBS-003), so the handler always sees the
  logical `Frame`, and `byte_length` is the encoded length - which on the tx side means you compute
  the encoding first and call the hook with the result, not the other way round.
- **The writer's queues are peer state, not stream state.** M5b's socket-death fan-out discards them;
  expose one `writer.discard_all()` so M5b has exactly one thing to call.
- **Lint that will bite here:**
  - `ARG` - unused hook parameters are underscore-prefixed (`_frame`, `_direction`).
  - `S101` - `assert` only inside `*_test.py`; raise explicitly in `writer.py` and `observability.py`.
  - `UP` - `float | None`, never `Optional[float]`.
  - `C4` - build the writer's per-stream map with a comprehension (`C408` is ignored, so `dict()`
    itself is fine).
  - `S311` does **not** apply here - there is no randomness in this milestone. Save the
    `random` + `# noqa: S311` pattern for M5b's reconnect jitter.
  - `no-restricted-syntax` forbids `for...in` - walk the writer's map with `Object.entries(...)`.
  - `unicorn/filename-case: kebabCase` for every new TS file.
  - Prettier `printWidth: 120`, `singleQuote: true` on the TS side; ruff `quote-style = "double"` on
    the Python side.

## 7. Tests to write

Both languages unless marked. Names marked **(spec)** are named in the specification and must appear
verbatim.

**Send path**

1. `fragment_test.py::test_slice_point_sweep_never_exceeds_cap` **(spec)** - sweep caps 64..8192 over
   a corpus of payloads; every produced encoded message is ≤ cap, measured in bytes of codec output.
2. `fragment_test.py::test_slice_point_inside_multibyte_codepoint_moves_back` **(spec)** - a payload
   of 4-byte codepoints; no fragment splits one. TS additionally: no surrogate pair split.
3. `fragment_test.py::test_control_character_payload_is_resplit_not_emitted_over_cap` **(spec)** - a
   payload of control characters whose JSON escaping expands 6×; the verify-and-re-split loop fires
   and nothing over-cap goes out.
4. `test_end_flag_only_on_final_fragment` (WSM-FRG-020).
5. `test_headers_are_never_fragmented` - and an `open` whose headers alone exceed the cap is rejected
   by the receiver with `PAYLOAD_TOO_LARGE` (WSM-FRG-021).
6. `test_fragments_of_one_payload_are_contiguous_on_that_stream` (WSM-FRG-017).
7. `writer_test.py::test_at_most_one_unsent_fragment_per_stream` - instrument the queue depth; it
   never exceeds 1, and fragment *n+1* is encoded only after *n* was handed to the adapter
   (WSM-FRG-018).
8. `writer_test.py::test_round_robin_selects_across_streams_not_fifo` - three streams with queued
   work produce a strictly rotating send order (WSM-FRG-019).
9. Fixture `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json` **(spec)** - a
   200-byte frame on stream B is delivered while a 1 MB payload on stream A is still fragmenting.
10. `test_both_ports_agree_on_fragment_boundaries` - replays `conformance/frames/` in both languages
    (WSM-FRG-016).
11. `test_fragmentation_uses_the_protocol_constant` - the split follows `MAX_FRAME_BYTES` and nothing
    else; there is no per-connection cap to read, and a peer told nothing by its remote still
    fragments at 65536 (WSM-FRG-004/010).
11a. `test_construction_rejects_a_cap_below_the_envelope_floor` - the test-only cap argument
    (WSM-FRG-005) set to 16 raises at peer construction, before any socket, rather than looping in
    the splitter (WSM-FRG-034).

**Receive path**

12. `test_fragments_are_reassembled_and_decoded_once` (WSM-FRG-030).
13. `test_over_cap_encoded_message_resets_stream_with_payload_too_large` - the whole message is
    measured, not the `fragment` field; a message *at* `MAX_FRAME_BYTES` is accepted; the connection
    survives (WSM-FRG-031, WSM-STM-020).
14. `fragment_test.py::test_oversize_payload_resets_on_the_crossing_fragment` **(spec)** - the reset
    goes out **before the final fragment arrives**, and the partial buffer is released at the same
    moment (WSM-FRG-032, WSM-INV-017, decision 1).
15. `test_max_payload_bytes_defaults_to_64_mib_and_is_never_announced` - the default is 67 108 864,
    it is settable per peer at `connect()` / `accept()`, and no frame anywhere carries it
    (WSM-FRG-035).
16. Fixture `conformance/invalid/fragment-interrupted-by-non-fragment.json` **(spec)** - a
    stream-level `reset(PROTOCOL_ERROR)`; the connection survives (WSM-FRG-033).

**The concurrency limit**

16a. `peer_test.py::test_open_beyond_receiver_limit_is_refused_and_opener_raises_nothing_locally`
    **(spec)** - the remote opens exactly `max_concurrent_streams` streams and then one more; the
    receiver answers `reset(REFUSED)` **without invoking the handler**, the opener's `open()` did not
    raise synchronously, and the refusal surfaces as `StreamRefused` on the pending await
    (WSM-STM-036, WSM-API-004).
16b. `test_own_opens_do_not_count_against_the_limit` - a peer with 100 streams it opened itself still
    accepts the remote's 100 (WSM-STM-037).
16c. `test_closing_a_stream_frees_a_slot` - saturate, end one, the next remote `open` is accepted.
16d. `test_the_limit_is_never_announced_and_never_checked_by_the_sender` - no frame carries it, and
    `open()` / `notify()` perform no local count at any saturation level (WSM-STM-036, WSM-INV-007).
16e. Fixture `conformance/invalid/open-beyond-receiver-concurrency-limit.json` - the outgoing frame is
    `reset(code=4)` and the connection survives.

**Backpressure**

17. `test_window_update_is_never_sent` (WSM-BPR-001) - and no flow-control mechanism other than the
    receiver's concurrency limit and `MAX_FRAME_BYTES` exists; nothing is negotiated (WSM-BPR-002).

**Observability**

18. `test_on_frame_fires_before_encode_and_after_decode_with_byte_length` (WSM-OBS-003).
19. `test_frame_log_line_shape_at_debug` - one line per frame under the `muxws.frames` logger
    (WSM-OBS-001).
20. `test_payload_contents_never_appear_in_any_log_record` - plant a sentinel string in a payload and
    assert it appears in no record at any level (WSM-OBS-002).
21. `stream.spec.ts::reset stream nobody consumed reports no unhandled rejection and does reach the error hook`
    **(spec)** - TypeScript only (WSM-API-016).
22. `stream_test.py::test_unconsumed_open_emits_no_runtime_warning` - Python only (WSM-API-017).

## 8. Done when

```bash
ruff check . && ruff format --check .
pytest muxws -q --cov=muxws --cov-report=term-missing
npm run lint
npm test
```

- [ ] All four commands pass. Coverage of `writer.py`, `fragment.py`, `observability.py` and their TS
      mirrors is ≥ 95 %.
- [ ] Every test in §7 exists and passes; every **(spec)**-marked name matches character for character.
- [ ] No FIFO frame queue exists in the send path - the writer selects by round-robin only.
- [ ] Observable: a 1 MB payload on one stream and a 200-byte payload on another, sent concurrently,
      deliver the small one long before the large one finishes, in both languages.
- [ ] Observable: a client that opens 101 streams against a default acceptor gets 100 handled and one
      `StreamRefused`, with nothing raised locally at the `open()` call site.
- [ ] `grep -rn "remote\." muxws/fragment.py muxws/peer.py ts/fragment.ts ts/peer.ts` finds no cap
      read from a remote-supplied value: every limit is the constant or a local argument.
- [ ] `grep -rn "\.length" ts/fragment.ts` finds no byte measurement taken from a JavaScript string's
      `.length` (WSM-FRG-002).
- [ ] `pyproject.toml` and `package.json` versions still match (WSM-PKG-001), and the Python package
      still has zero required runtime dependencies (WSM-PKG-002).

## 9. Out of scope

- **The reconnect helper, the heartbeat timer, `hello`, `on_close`, `on_reconnect`, socket death and
  `ConnectionLost` fan-out, `PeerRegistry` and `peer.tags`** - all **M5b**. M5a exposes
  `writer.discard_all()` for M5b to call and does nothing else about a dying socket.
- **Per-stream flow control.** `window_update` is reserved and MUST NOT be sent (WSM-BPR-001).
- **The msgpack codec, the full `conformance/sequences/` corpus, the live cross-language CI matrix,
  and the 1.0 wire freeze** - M6.
- **The VitePress documentation site** - M7. M5a writes docstrings, not pages.
