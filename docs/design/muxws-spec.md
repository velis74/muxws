---
title: muxws - normative specification
sidebar: false
search: false
outline: deep
---

# muxws normative specification (v1)

This is the extractable "what" of muxws: the rules an implementer must satisfy, with no rationale.
The reasoning behind every decision here lives in the design brief,
[muxws - multiplexed WebSocket transport](./muxws-websocket-transport.md), which is referenced once,
here, and never again.

Requirement levels are RFC 2119: **MUST**, **MUST NOT**, **SHOULD**, **MAY**. Every rule has a stable
id. Rules are cross-referenced by id only. Where a rule names a test, that test is the acceptance
criterion for it; test file names follow the repository convention (Python `<module>_test.py`
colocated with the source, TypeScript `<module>.spec.ts` colocated with the source, shared fixtures
under `conformance/`).

---

## 1. Scope and vocabulary

muxws is a framing, multiplexing, stream-lifecycle, cancellation and connection-lifecycle protocol
over one WebSocket, plus a reference implementation in Python and TypeScript. It is not a router, not
a serializer of domain objects, not an authentication mechanism, not a durable store, and not an RPC
framework.

| Term | Definition |
|---|---|
| **peer** | One end of one WebSocket connection. One symmetric type per language; there is no separate client type and server type. |
| **dialer** | The peer that called `connect(url)`. Allocates odd stream ids. |
| **acceptor** | The peer that was handed a socket by its framework and called `accept(socket)`. Allocates even stream ids. |
| **stream** | An independently addressed, independently cancellable, bidirectional sequence of frames sharing one id. |
| **frame** | One logical protocol unit. Exactly one frame per WebSocket message. |
| **codec** | The port that turns a logical frame into a WebSocket message and back. Fixed per process by deployment configuration. |
| **encoded message** | The complete codec output for one frame, envelope included, as it goes on the socket. |
| **payload** | An application value carried by `open`, `data` or `reset`. Opaque to muxws. |
| **fragment** | A slice of the codec-encoded form of one logical payload. |
| **local / remote** | `local` = the peer whose state is being described; `remote` = the other one. |
| **live stream** | A stream in state `open`, `half_closed_local` or `half_closed_remote` on this peer. |
| **high-water mark** | Per parity, the highest stream id that parity's owner has ever opened, as observed by this peer. |
| **established** | The socket is open and, for a codec-bearing subprotocol, that subprotocol was accepted (WSM-CON-030). The reconnect helper additionally requires the hello to have been acknowledged before it treats a connection as established (WSM-RCN-004). |
| **hello** | An application-supplied opening payload the reconnect helper replays on every connection. |

---

## 2. Codec seam

### 2.1 The port

- **WSM-CDC-001** A codec MUST expose `name: str` (the wire-visible name), `binary: bool`,
  `encode(frame) -> str | bytes`, and `decode(message) -> Frame`. Declarations in §9.4.
- **WSM-CDC-002** `binary` MUST be declared, not inferred from a value's type; the peer uses it to
  select the socket's text or binary send method and the expected inbound message type. A peer MUST
  NOT sniff incoming messages to decide which codec branch to take.
- **WSM-CDC-003** A peer MUST use exactly one codec for the life of its connection. There is no
  per-frame, per-stream or per-connection codec switching.
- **WSM-CDC-004** The library MUST ship and MUST itself register a `json` codec, and JSON MUST be the
  default. JSON is the interoperability baseline: both language ports MUST produce a JSON wire form
  that the other port decodes to an equal logical frame.
  Test: `conformance/frames/*.json` replayed by `frames_test.py` and `frames.spec.ts`.
- **WSM-CDC-005** Conformance for the JSON codec MUST be asserted as `decode(json_wire) == frame` and
  `decode(encode(frame)) == frame`, comparing parsed objects, never byte-identical output. Exactly one
  separate test MAY pin a canonical key order (`type`, then `stream`, then the remaining keys
  alphabetically) for the benefit of log diffing; no other test may depend on key order or whitespace.
- **WSM-CDC-006** Any codec other than `json` MUST be asserted by round-trip over the same logical
  frame corpus (`decode(encode(frame)) == frame`) and MUST NOT have wire bytes pinned in a fixture.
- **WSM-CDC-007** Every codec that ships MUST additionally have a live cross-language pair in CI (a
  Python peer and a TypeScript peer, both configured with that codec, running the sequence corpus). A
  codec without that pair MUST NOT ship.
- **WSM-CDC-008** Under a binary codec, raw bytes (`bytes` / `ArrayBuffer`) are a first-class payload
  type. Under JSON they are not, and muxws MUST NOT base64-encode bytes on the application's behalf.

### 2.2 Selection

- **WSM-CDC-010** The codec name MUST be read from deployment configuration, not from a call
  argument: Python `os.environ["MUXWS_CODEC"]` via a `muxws.conf.settings` singleton (default
  `"json"`), TypeScript `import.meta.env.VITE_MUXWS_CODEC` (default `"json"`).
- **WSM-CDC-011** `settings.codec` MUST be writable at runtime so an application may set it during
  bootstrap before connecting.
- **WSM-CDC-012** `connect()`, `accept()` and the peer constructor MUST accept a `codec=` /
  `codec:` override. It is documented as a test override and an escape hatch for a process holding two
  connections needing different codecs. No example outside the test suite may use it.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` /
  `registerCodec(name, codec)`. There MUST NOT be dynamic imports, lazy auto-registration,
  entry-point scanning, or any probing of whether a module happens to be installed.
- **WSM-CDC-014** A codec module MUST NOT register itself at import time (a side-effecting import can
  never be tree-shaken out).
- **WSM-CDC-015** The npm package MUST declare `"sideEffects": false` (at minimum for the codec
  subpaths).
- **WSM-CDC-016** A configured codec name that is not registered MUST raise `CodecNotRegistered` on
  the first connection attempt, **before any socket is opened**. The message MUST name the environment
  variable, the value found, and the registered set. The peer MUST NOT fall back to JSON, ever.
  Test: `codec_test.py::test_unregistered_name_raises_before_socket` (asserts no socket was opened).

### 2.3 Subprotocol assertion

- **WSM-CDC-020** The dialer MUST offer `muxws.v1.<codec>` (e.g. `muxws.v1.json`,
  `muxws.v1.msgpack`) as its **first** WebSocket subprotocol entry, where `<codec>` is its configured
  codec name.
- **WSM-CDC-021** The application MAY append further subprotocol entries (a bearer token is the
  common case). The acceptor MUST match only the entry carrying the `muxws.v1.` prefix and MUST
  ignore every other offered value entirely, leaving them for the application's authentication.
- **WSM-CDC-022** The acceptor MUST accept the connection only if the offered `muxws.v1.<codec>`
  name equals its own configured codec name, and MUST select exactly that value as the negotiated
  subprotocol. Otherwise it MUST refuse the WebSocket handshake: it MUST select **no** subprotocol
  and MUST answer the upgrade with HTTP **400**. It MUST NOT complete the handshake and close
  afterwards where the transport gives it the choice (WSM-CDC-028 is the exception, and only for
  transports that give it none).
- **WSM-CDC-023** This is an assertion, not a negotiation. There MUST NOT be a fallback encoding, a
  list of acceptable alternatives, per-connection multi-codec support, or any runtime codec branching
  in the peer.
- **WSM-CDC-024** A dialer whose handshake is refused MUST surface `CodecMismatch`, **composed by the
  dialer itself from the codec name it offered** - a browser cannot read the rejection body, so the
  diagnostic cannot come from the server. The message MUST name the offered codec and **both**
  environment variables, `VITE_MUXWS_CODEC` and `MUXWS_CODEC`, so the reader knows where to look on
  each side. It MUST NOT surface a bare connection failure.
  Test: `codec_test.py::test_mismatched_codecs_reject_handshake` (asserts no frame was exchanged).
- **WSM-CDC-025** A peer offering a different generation (`muxws.v2.<codec>`) MUST be rejected by a
  v1 acceptor at the handshake.
- **WSM-CDC-026** `accept()` MUST perform the WebSocket accept itself (it is the only party that
  knows which subprotocol to select). An application MUST NOT accept the socket before calling it.
- **WSM-CDC-027** For transports that complete the handshake before invoking the handler (the
  `websockets` library), the library MUST expose `select_subprotocol`, a plain callable installable in
  that transport's handshake hook, implementing WSM-CDC-021/022.
- **WSM-CDC-028** Where a transport offers neither hook, the peer MUST verify the negotiated
  subprotocol on the already-open socket and close it with the WebSocket policy-violation close code.
- **WSM-CDC-029** The acceptor MUST log the same failure at refusal time, naming the offered codec,
  its own configured codec and both environment variables. This is the half of the diagnostic that is
  readable where a response body is readable, and it MUST NOT be omitted on the grounds that
  WSM-CDC-024 already reports it - neither message is complete on its own.

---

## 3. Wire format

### 3.1 Envelope

One frame per WebSocket message. Field names are spelled out, never abbreviated, and stay snake_case
in both languages.

```json
{"type": "data", "stream": 7, "payload": {"rows": 128}, "end": false}
```

| Field | Type | Required | Default | Present on | Meaning |
|---|---|---|---|---|---|
| `type` | string | yes | - | every frame | Frame type (§3.3). |
| `stream` | int | on stream-level frames | - | `open`, `data`, `reset` | Stream id. Omitted (or `0`) on connection-level frames. |
| `payload` | any codec value | no | absent | `open`, `data`, `reset` | The application value. On `reset`, the optional structured error object. Absent means "no payload", distinct from `null` only if the application chooses to care. Mutually exclusive with `fragment`. |
| `fragment` | string (text codec) / bytes (binary codec) | no | absent | `open`, `data` | A slice of the codec-encoded logical payload. Mutually exclusive with `payload`. |
| `more` | bool | no | `false` | frames with `fragment` | `true` on every fragment but the last. |
| `headers` | object | no | absent | `open` | Application metadata; string keys, codec-encodable values. Never interpreted by muxws. |
| `end` | bool | no | `false` | `open`, `data` | Last frame this peer will send on this stream. |
| `trailers` | object | no | absent | frames with `end: true` | Post-body metadata. |
| `code` | int | yes | - | `reset`, `goaway` | Reset code (§8.1). |
| `reason` | string | no | absent | `reset`, `goaway` | Human-readable, for logs. MUST NOT be parsed. |
| `nonce` | string | yes | - | `ping`, `pong` | Opaque, echoed verbatim. Sender-chosen. |
| `last_stream` | int | yes | - | `goaway` | Highest id from the *other* peer this peer has processed and will still complete. |

- **WSM-FRM-001** A receiver MUST ignore unknown envelope fields.
- **WSM-FRM-002** A receiver MUST ignore unknown *frame types*, logging once, and MUST NOT treat them
  as any kind of error. Test: `peer_test.py::test_unknown_frame_type_is_ignored`.
- **WSM-FRM-003** *Retired.* It required a peer not to send an extension frame type unless the remote
  had advertised that extension in `settings.extensions`. There is no `settings` frame and no
  extension advertisement (WSM-CON-031); a v1 peer sends only the frame types in §3.2, and a frame
  type the remote must *act* on requires a new generation (WSM-CON-009). The id is not reused.
- **WSM-FRM-004** `payload` and `fragment` MUST NOT both appear on one frame.
- **WSM-FRM-005** A frame missing `type`, or a message the configured codec refuses to decode, is a
  **connection-level** protocol error (§5.4).
- **WSM-FRM-006** muxws MUST NOT define any message vocabulary inside `payload`: no `kind` field, no
  reserved key, no discriminator of any sort.

### 3.2 v1 frame set

`open`, `data`, `reset`, `ping`, `pong`, `goaway`. `window_update` is **reserved and
unimplemented in v1**. There is no `settings` frame (WSM-CON-031).

### 3.3 Per-type rules

- **WSM-FRM-010** `open` opens a stream. Sender: either peer, using its own parity. Fields: `stream`
  (required), `headers`, `payload`/`fragment`+`more`, `end`. `end: true` on `open` is the unary
  request shape.
- **WSM-FRM-011** `data` carries a payload chunk on an existing stream. Fields: `stream`,
  `payload`/`fragment`+`more`, `end`, `trailers`. A peer MUST NOT send `data` on a stream where its
  own side is already half-closed.
- **WSM-FRM-012** End of stream MUST be a flag, never its own frame type. A peer with nothing left to
  say sends `{"type": "data", "stream": N, "end": true}` with no payload. There MUST NOT be an `end`
  frame type.
- **WSM-FRM-013** Trailers MUST ride on the frame carrying `end: true`. There MUST NOT be a trailers
  frame type.
- **WSM-FRM-014** `reset` terminates a stream immediately in both directions. Sender: either peer, in
  any state except `closed`. Fields: `stream`, `code`, `reason`, optional `payload` for a structured
  error object.
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`
  (or set it to `0`).

---

## 4. Sizes and fragmentation

### 4.1 How sizes are counted

- **WSM-FRG-001** Every size limit in this specification MUST be measured as the byte length of the
  **fully encoded WebSocket message** - the complete codec output for the frame, envelope included,
  exactly as it goes on the wire. Not the pre-encoding payload, not the `fragment` field alone.
- **WSM-FRG-002** Under a text codec the TypeScript port MUST measure with
  `new TextEncoder().encode(text).length` (or an equivalent incremental byte count) and the Python
  port with `len(text.encode("utf-8"))`. A JavaScript string's `.length` MUST NOT be used (it counts
  UTF-16 code units and disagrees with Python on every non-BMP character).
- **WSM-FRG-003** Under a binary codec both ports MUST take the length of the produced buffer.
  Test: `fragment_test.py::test_slice_point_sweep_never_exceeds_cap` / `fragment.spec.ts`, asserted in
  bytes of that codec's output.
- **WSM-FRG-004** `MAX_FRAME_BYTES` is a **protocol constant of 65536** (64 KiB): the largest encoded
  message a sender may emit. It MUST NOT be negotiated, announced, or read from configuration. A
  receiver MUST accept any message up to the constant and MAY accept larger ones; a sender MUST always
  fragment at the constant regardless of what the remote appears willing to accept.
- **WSM-FRG-005** An implementation MAY expose the cap as a construction argument **for tests only**
  (the conformance runner uses it, WSM-TST-002). It MUST NOT be documented as deployment
  configuration and MUST NOT appear on the wire. A cap too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error at peer construction (WSM-FRG-034).

### 4.2 Sender rules

- **WSM-FRG-010** A sender MUST fragment any logical payload whose encoded frame would exceed
  `MAX_FRAME_BYTES` (WSM-FRG-004). Fragmentation is mandatory, not an optimisation.
- **WSM-FRG-011** The sender MUST encode the logical payload with the connection's codec, slice that
  encoded form, and put each slice into a frame that the codec then encodes again.
- **WSM-FRG-012** Slices MUST be cut at a boundary the codec can represent: Unicode codepoint
  boundaries of the encoded text for a text codec, byte boundaries for a binary one. Under JSON in
  TypeScript this additionally means never splitting a surrogate pair. A splitter that would land
  mid-sequence MUST move the boundary backwards.
  Test: `fragment_test.py::test_slice_point_inside_multibyte_codepoint_moves_back`.
- **WSM-FRG-013** The sender MUST budget for the envelope and for re-encoding expansion: slice to
  `cap - reservation` bytes of encoded payload, where `reservation = min(512, cap // 2)`.
- **WSM-FRG-014** The sender MUST then **verify and re-split**: encode the frame, and if the encoded
  message still exceeds the cap, re-split that slice and try again. The reservation is a per-codec
  hint; the loop is the guarantee.
  Test: `fragment_test.py::test_control_character_payload_is_resplit_not_emitted_over_cap`.
- **WSM-FRG-015** The splitter MUST be a pure function of `(payload, cap, codec)` and MUST be tested
  as one, independently of any socket.
- **WSM-FRG-016** Both ports MUST produce the **same fragment boundaries** for the same payload,
  cap and codec. Test: shared fixtures in `conformance/frames/`.
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

### 4.3 Receiver rules

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
  a **stream-level** protocol error (§5.4).
  Test: `conformance/invalid/fragment-interrupted-by-non-fragment.json`.
- **WSM-FRG-034** A test-override cap (WSM-FRG-005) too small to hold the envelope plus one
  indivisible unit MUST raise a configuration error when the peer is constructed, not be discovered
  later as an infinite split loop.
- **WSM-FRG-035** `max_payload_bytes` - the largest reassembled payload this peer will accept - is a
  **local receiver setting**, default **67108864** (64 MiB). It MUST NOT be announced on the wire, and
  a sender MUST NOT be given any way to learn it other than the reset it produces.

---

## 5. Streams

### 5.1 Id allocation and parity

- **WSM-SID-001** Stream ids MUST be allocated by the library. A caller MUST NOT be able to supply
  one, anywhere in the API.
- **WSM-SID-002** The dialer MUST allocate odd ids (1, 3, 5, ...); the acceptor MUST allocate even
  ids (2, 4, 6, ...).
- **WSM-SID-003** Id `0` is reserved for connection-level frames, which in practice omit the field.
- **WSM-SID-004** Opens MUST be monotonic per peer and ids MUST NOT be reused, even after a stream
  closes. `data` and `reset` MAY name any previously-opened id in any order.
- **WSM-SID-005** An `open` naming an id with the wrong parity, or an id not greater than the highest
  id that peer has previously opened, is a **connection-level** protocol error.
  Test: `conformance/invalid/open-wrong-parity.json`, `conformance/invalid/open-id-not-monotonic.json`.
- **WSM-SID-006** The id MUST be allocated and the `open` frame enqueued in one synchronous step,
  with no suspension point between them, so that wire order is allocation order by construction.
  Test: `peer_test.py::test_concurrent_opens_produce_increasing_ids_on_the_wire`.
- **WSM-SID-007** On exhaustion at 2^31-1, the exhausting peer MUST send `goaway` with `last_stream`
  set to the highest id it has processed, MUST stop opening new streams, MUST let in-flight streams
  drain, and MUST then close.
- **WSM-SID-008** `stream.id` MUST be readable on the line following `open()`, with nothing awaited
  in between. Test: `stream_test.py::test_id_readable_immediately_after_open`.

### 5.2 Retention and late frames

- **WSM-STM-001** A peer MUST retain exactly **`highest_open_seen` per parity plus the map of live
  streams**. It MUST NOT keep per-closed-stream bookkeeping (that is a per-connection memory leak).
- **WSM-STM-002** Any stream-level frame naming an id that is not currently live but does not exceed
  that peer's high-water mark MUST be **silently ignored** - no reset, no connection error, at most a
  counter. Test: `conformance/invalid/data-for-closed-id.json` (connection survives, nothing goes out).
- **WSM-STM-003** Any stream-level frame other than a valid `open` naming an id **above** that peer's
  high-water mark MUST be a **connection-level** protocol error.
  Test: `conformance/invalid/data-above-high-water-mark.json` (connection dies).

### 5.3 State machine

Five states, tracked per stream per peer.

| State | Entered by |
|---|---|
| `idle` | id allocated, nothing sent |
| `open` | `open` sent or received without `end` |
| `half_closed_local` | this peer sent `end: true` |
| `half_closed_remote` | this peer received `end: true` |
| `closed` | both ends sent `end`, or either sent `reset`, or the connection died |

Cell legend: `→ S` transition to state S; `ILL-S` stream-level protocol error (reset that stream,
connection survives); `ILL-C` connection-level protocol error (`goaway(PROTOCOL_ERROR)`, close
socket); `IGN` silently ignore; `RAISE` local API error at the call site, nothing sent - which error
is WSM-ERR-009 for `data`/`end` on a stream that is no longer open, and WSM-API-004 for `open`; `n/r`
not reachable.

| State \ Event | send `open` | recv `open` | send `data` | send `end` | recv `data` | recv `data(end)` | send `reset` | recv `reset` | socket death |
|---|---|---|---|---|---|---|---|---|---|
| `idle` | → `open` (→ `half_closed_local` if `end`) | → `open` (→ `half_closed_remote` if `end`) | n/r | n/r | ILL-C | ILL-C | n/r | ILL-C | → `closed` |
| `open` | RAISE | ILL-C | → `open` | → `half_closed_local` | → `open` | → `half_closed_remote` | → `closed` | → `closed` | → `closed` |
| `half_closed_local` | RAISE | ILL-C | RAISE | RAISE | → `half_closed_local` | → `closed` | → `closed` | → `closed` | → `closed` |
| `half_closed_remote` | RAISE | ILL-C | → `half_closed_remote` | → `closed` | ILL-S | ILL-S | → `closed` | → `closed` | → `closed` |
| `closed` | RAISE | ILL-C | RAISE | RAISE | IGN | IGN | NOOP | IGN | NOOP |

- **WSM-STM-010** Every row of this table, including every illegal cell, MUST have a test.
  Test: `stream_test.py` / `stream.spec.ts`, one case per cell.
- **WSM-STM-011** `idle` is not externally observable: it exists only inside the indivisible
  allocate-and-enqueue step of WSM-SID-006. Cells marked `n/r` MUST NOT be reachable through the
  public API.
- **WSM-STM-012** A stream that is `half_closed_local` on one peer MUST be `half_closed_remote` on
  the other. Both ends half-closed means `closed`.
- **WSM-STM-013** The unary shape is `open(end=true)` → `half_closed_local`, one inbound
  `data(end=true)` → `closed`.
- **WSM-STM-014** On socket death every live stream MUST transition to `closed` and be failed locally
  with a synthesised `reset(CONNECTION_CLOSED)` (§8.1 code 9) **before** `on_close` fires (§7.5).
- **WSM-STM-015** A `reset` for a stream that is already closed or was never opened MUST be ignored
  under WSM-STM-002.

### 5.4 Stream-level versus connection-level errors

- **WSM-STM-020** A frame illegal *for a stream* MUST reset **that stream** and leave the connection
  alone. Stream-level cases: `data` after receiving `end`; a non-fragment frame mid-reassembly
  (WSM-FRG-033); a frame the receiver declines as over-size (WSM-FRG-031); a payload over
  `max_payload_bytes` (WSM-FRG-032); an `open` beyond the receiver's own concurrency limit
  (WSM-STM-036).
- **WSM-STM-021** The code for a stream-level violation MUST be `PROTOCOL_ERROR`, except that size
  violations use `PAYLOAD_TOO_LARGE` and concurrency-limit rejection uses `REFUSED` (WSM-STM-036).
- **WSM-STM-022** *Retired.* It required concurrency-limit rejection to use `STREAM_LIMIT` rather than
  `REFUSED`, so that an opener would back off rather than retry at once. The distinction went with the
  announced quota: the limit is now the receiver's alone and is not advertised, so `REFUSED` - nothing
  ran, retry is the application's judgement - is the whole of what the opener can be told. The id is
  not reused, and reset code 5 is retired with it (§8.1).
- **WSM-STM-023** A frame illegal *for the connection* MUST end the connection with
  `goaway(PROTOCOL_ERROR)` followed by a socket close. Connection-level cases: a message the codec
  cannot decode; a missing `type`; a wrong-parity stream id; an `open` whose id is not greater than
  that peer's highest previous open; a stream-level frame naming an id above the high-water mark.
- **WSM-STM-024** The discriminator, when a new case arises: if the peers can still agree about the
  state of every *other* stream, the connection is kept.

### 5.5 Incoming stream dispatch

- **WSM-STM-030** A peer MUST have exactly one incoming-stream handler, registered with
  `on_stream(handler)`. Registering a second MUST replace the first and MUST log. There MUST NOT be a
  path table, method map, or per-action registration in muxws.
- **WSM-STM-031** The handler MUST be invoked as `(payload, stream)` and **only once the opening
  payload is fully reassembled**. A fragmented `open` MUST NOT reach the application in pieces.
- **WSM-STM-032** The reassembled opening payload MUST also be available as `stream.payload`.
- **WSM-STM-033** With no registered handler, an incoming `open` MUST be answered with
  `reset(REFUSED)` (nothing ran, so the opener may safely retry elsewhere).
- **WSM-STM-034** A registered handler that raises MUST produce `reset(APPLICATION_ERROR)`,
  **always**, regardless of whether it had already sent anything (`REFUSED` promises the operation
  definitively did not happen).
- **WSM-STM-035** A handler that returns without having ended its stream MUST end it implicitly.
  Test: `peer_test.py::test_handler_returning_ends_stream_implicitly`.
- **WSM-STM-036** A peer MUST enforce a **local, receiver-side** limit on how many streams the remote
  may have open on it at once, default **100**, and MUST answer an `open` beyond it with
  `reset(REFUSED)` without invoking the handler. The limit MUST NOT be announced on the wire and MUST
  NOT be enforced by the sender: `open()` MUST NOT check it, MUST NOT raise for it, and MUST put the
  `open` frame on the wire like any other (WSM-API-004).
  Test: `peer_test.py::test_open_beyond_receiver_limit_is_refused_and_opener_raises_nothing_locally`.
- **WSM-STM-037** Streams this peer opened itself MUST NOT count against WSM-STM-036; the limit bounds
  work the *remote* can impose, and each peer counts only the other's opens.

---

## 6. Connection lifecycle

### 6.1 Establishment and versioning

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

### 6.2 Ping / pong

- **WSM-CON-010** `ping` MUST carry a `nonce`; the receiver MUST echo it verbatim in a `pong`,
  promptly, without application involvement.
- **WSM-CON-011** Native WebSocket ping/pong control frames MUST NOT be used for liveness (browsers
  do not expose them to JavaScript).
- **WSM-CON-012** The application MAY call `peer.ping()` to measure round-trip time; it returns
  seconds (Python) / milliseconds (TypeScript).

### 6.3 Goaway and shutdown

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

---

## 7. Reconnect

The reconnect helper exists on the **dialer only**. An acceptor cannot dial and MUST NOT have one.

### 7.1 Backoff

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
- **WSM-RCN-005** Jitter uses an ordinary pseudo-random source. In Python this is `random` with
  `# noqa: S311` and a comment stating that reconnect jitter is not security-sensitive; `secrets`
  MUST NOT be used.
- **WSM-RCN-006** `connect()` MUST await the first attempt and MUST **raise** if it fails, with the
  underlying error, **regardless of the reconnect configuration**. Reconnection applies to connections
  that were established and then lost; it MUST NOT apply to establishing the first one. `connect()`
  MUST NOT return a peer that is retrying in the background.
  Test: `reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`.

### 7.2 Heartbeat

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

### 7.3 Hello and connection identity

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
- **WSM-RCN-022** The acknowledgement is the acceptor's handler returning (WSM-STM-035 ends the
  stream implicitly). No application code is required to send one.
- **WSM-RCN-023** The hello MUST go out before any application frame on that socket, and before
  `on_reconnect` fires.
- **WSM-RCN-024** `hello` MUST be optional. A peer given none sends none and is established as soon
  as WSM-CON-030 is satisfied.
- **WSM-RCN-025** A credential MUST NOT be carried in the hello (authentication is a handshake
  concern, §10).
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

### 7.4 What a reconnect restores

- **WSM-RCN-030** `on_reconnect(attempt, peer)` MUST guarantee exactly two things and nothing more: a
  live socket, and an identity the acceptor has already accepted on it. It MUST fire once per
  re-established connection, after WSM-CON-030 **and** after the hello acknowledgement.
- **WSM-RCN-031** v1 MUST NOT resume streams across a reconnect. Every stream is gone, nothing is
  replayed, no in-flight frame is re-sent, and the new socket's id space starts empty.
- **WSM-RCN-032** `Peer` survives a reconnect; `Stream` objects do not. Any stream held across one is
  already closed.
- **WSM-RCN-033** On the acceptor side a reconnect produces a **new peer object with a fresh, empty
  `tags`** (§9.5). An implementation MUST NOT carry tags forward.
  Test: `registry_test.py::test_reconnect_starts_with_empty_tags`.

### 7.5 Socket death

- **WSM-RCN-040** `on_close(reason)` MUST fire on **every** socket loss, not only the final one, and
  exactly once per loss. `reason.will_retry` MUST be `False` only when `max_attempts` is exhausted or
  `close()` was called deliberately.
- **WSM-RCN-045** `CloseReason` MUST carry exactly four fields, the same four in both languages:
  `code` (the WebSocket close code), `reason` (its text), `was_clean` / `wasClean` (whether the close
  was orderly), and `will_retry` / `willRetry` (WSM-RCN-040). It MUST be one type per language, used
  for every socket loss.
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

---

## 8. Error taxonomy

### 8.1 Reset codes

Numeric on the wire, named in both APIs. The same table is used by `reset` and `goaway`. The reaction
column is normative.

| Code | Name | Raised when | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, orderly shutdown; on `reset`, "done and no longer interested". | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` carries a structured error object. | Surface to the caller. Retry is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted and definitively not processed. Used for no registered handler, for post-`goaway` opens, and for an `open` beyond the receiver's own concurrency limit (WSM-STM-036). | Retry: elsewhere if another connection is available, otherwise after a delay - the receiver may be saturated. |
| 5 | - | **Retired.** Was `STREAM_LIMIT`, for rejection against an announced `max_concurrent_streams` (WSM-STM-022). The number MUST NOT be reused and MUST NOT appear on the wire. | - |
| 6 | `TIMEOUT` | A deadline expired locally; the reset informs the remote so it can stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts (WSM-FRG-031), or a payload exceeded the receiver's `max_payload_bytes` (WSM-FRG-032). | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream live at that instant. **MUST NEVER appear on the wire.** | Do not retry on this peer now; rebuild from `on_reconnect`. |

### 8.2 Exception hierarchy

```
MuxwsError
├── ProtocolError            # this peer or the remote violated the spec
├── ConnectionClosed         # socket died; carries .code, .reason, .was_clean
├── ConnectionGoingAway      # open() after goaway - raised synchronously out of open()
├── StreamAlreadyConsumed    # await and iterate, or two iterations, on one stream
├── StreamClosed             # send()/end()/reply() on a stream that closed normally
├── CodecError               # configuration; carries .configured and .available
│   ├── CodecNotRegistered   # configured name never registered - raised at startup
│   └── CodecMismatch        # acceptor's codec differs; the handshake was rejected
└── StreamReset              # carries .code (ResetCode), .reason, .stream_id
    ├── RemoteError          # code == APPLICATION_ERROR; carries .payload
    ├── StreamTimeout        # code == TIMEOUT
    ├── StreamRefused        # code == REFUSED; not processed - retry, elsewhere or later
    └── ConnectionLost       # code == CONNECTION_CLOSED; synthesised locally, never from the wire
```

- **WSM-ERR-001** *Retired.* It required `StreamRefused` and `StreamLimit` to be sibling classes. With
  the announced quota gone there is no `StreamLimit` (WSM-STM-022, WSM-API-004) and `StreamRefused`
  covers every refusal. The id is not reused.
- **WSM-ERR-002** `ConnectionLost` MUST be a `StreamReset` subclass; `ConnectionClosed` MUST NOT be.
  `ConnectionLost` is *a stream* failing because the connection did, and is what every stream-shaped
  call raises. `ConnectionClosed` is *the connection* ending, and is what `serve()` and peer-level
  calls raise.
- **WSM-ERR-003** All muxws errors MUST be raised from the awaiting call site and MUST NOT be
  swallowed into a callback.
- **WSM-ERR-004** TypeScript MUST mirror this hierarchy with classes of the same names, delivered as
  promise rejections and as `throw` inside `for await`. Both languages MUST set a `name` /
  `__class__` discriminator so cross-language tests can assert on error identity.
- **WSM-ERR-005** `CodecNotRegistered` and `CodecMismatch` MUST sit outside `StreamReset`: neither is
  a stream failure and neither is retryable.
- **WSM-ERR-006** A handler that raises MUST produce `reset(APPLICATION_ERROR)` with a `reason` and
  an optional structured `payload`. The default serializer produces
  `{"type": "ValueError", "message": str(exc)}`. The peer MUST accept an `error_serializer` hook of
  signature `(exc) -> Any | None` - the value it returns becomes the reset's `payload`, and `None`
  means "send no payload". The documentation MUST state plainly that a public-facing deployment should
  redact it.
- **WSM-ERR-007** muxws MUST NOT map exceptions to status codes of any kind.
- **WSM-ERR-008** `error_serializer` MUST be supplied **per peer** - an argument to `connect()` and to
  the acceptor's peer construction (`accept()` / `serve()`) - and MUST NOT be a module-level default.
  One process may hold a browser-facing peer that redacts and an internal peer that does not, and a
  process-wide setting forces the wrong answer for one of them.
- **WSM-ERR-009** `send()`, `end()` and `reply()` on a stream that is no longer open MUST raise, and
  the type MUST distinguish the three cases: `StreamClosed` when the stream closed **normally** (both
  ends ended), that stream's own `StreamReset` subclass when it was reset, and `ConnectionLost` when
  the socket died (WSM-RCN-041). `StreamClosed` MUST NOT be a `StreamReset` subclass and MUST NOT be
  a `ProtocolError`: a normal close racing a last `send()` is an expected outcome, not a failure and
  not a caller bug. Test: `stream_test.py::test_send_after_normal_close_raises_stream_closed`.

### 8.3 Timeouts and cancellation

- **WSM-ERR-010** `request()` MUST have **no default timeout**. A stream lives until it ends, is
  reset, or the connection dies.
- **WSM-ERR-011** When a timeout is given and expires, the peer MUST send `reset(TIMEOUT)` (so the
  remote stops working) and MUST raise `StreamTimeout` locally.
- **WSM-ERR-012** `stream.cancel()` MUST send `reset(CANCELLED)` and close the stream locally
  **immediately**, without waiting for acknowledgement. Payloads still in flight from the remote MUST
  be discarded.
- **WSM-ERR-013** On the remote side of a `reset(CANCELLED)`, the handler task MUST be cancelled: in
  Python by `task.cancel()` (the handler observes `asyncio.CancelledError` at its next `await`); in
  TypeScript by aborting `stream.signal`, with any subsequent `await stream.send(...)` rejecting with
  `StreamReset`.
- **WSM-ERR-014** Local `asyncio.CancelledError` propagating out of `await stream.result()` or an
  `async for` MUST cause the peer to send `reset(CANCELLED)` and re-raise `CancelledError`. It MUST
  NOT be swallowed. Test: `stream_test.py::test_local_cancellation_sends_reset_and_reraises`.
- **WSM-ERR-015** An incoming `reset(APPLICATION_ERROR)` on a stream this peer is consuming MUST
  raise `RemoteError` out of the pending `await` or the `async for`.

---

## 9. Public API

Naming rule across the two ports: module-level factory functions keep the Python name verbatim
(`connect`, `accept`, `serve`, `register_codec` → `registerCodec` is the one exception, being a
method-shaped verb); classes are PascalCase in both (`Peer`, `Stream`, `PeerRegistry`); methods and
options-object keys are camelCase in TypeScript. Wire field names stay snake_case in both.
Durations are **seconds as floats in Python** and **milliseconds as integers in TypeScript**.

### 9.1 Call shapes

| Shape | Wire | Python | TypeScript |
|---|---|---|---|
| Unary | `open(end)` → `data(end)` | `await peer.request(p)` or `await peer.open(p, end=True)` | `await peer.request(p)` |
| Streaming response | `open(end)` → `data`… `data(end)` | `async for chunk in peer.open(p)` | `for await (const c of peer.open(p))` |
| Bidirectional | `open` → interleaved `data` both ways → `data(end)` both ways | `s = peer.open(p)`, then `s.send()` plus iteration | same |
| One-shot push | `open(end)`, nothing awaited | `await peer.notify(p)` | `await peer.notify(p)` |

- **WSM-API-001** `peer.open()` MUST be **synchronous** and MUST return a `Stream`.
- **WSM-API-002** `Stream` MUST be simultaneously awaitable (resolving with the remote's **first**
  payload) and async-iterable (yielding every reassembled payload until the remote ends).
- **WSM-API-003** `open()` MUST take zero mandatory arguments; `payload` defaults to `null`/`None`.
- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between
  sockets. It MUST NOT queue. There MUST NOT be a `StreamLimit` exception and `open()` MUST NOT fail
  for concurrency: the concurrency limit is the receiver's (WSM-STM-036) and surfaces asynchronously
  as `StreamRefused` on the pending await.
- **WSM-API-005** `notify()` MUST be async and MUST return nothing (`None` / `void`). It MUST NOT
  return a `Stream` or any awaitable handle.
- **WSM-API-006** `request()` MUST be `open(payload, end=True)` awaited **to the stream's end**, plus
  a check that raises if the remote sent more than one payload.
- **WSM-API-007** `await stream` MUST resolve on the **first** payload and MUST NOT police a second
  one - that check belongs to `request()` alone.
  Test: `stream_test.py::test_open_resolves_first_payload_while_request_raises` - against a remote
  sending two payloads then ending, `await peer.open(p)` resolves with the first and does not raise,
  while `await peer.request(p)` raises.
- **WSM-API-008** `connect()`, `accept()`, `serve()`, `notify()`, `request()`, `ping()`, `close()`
  and every `Stream` send method MUST be async. Everything else MUST NOT be.
- **WSM-API-009** `peer.id` MUST be a short random prefix minted **once per process** plus a monotonic
  **per-connection** counter, rendered `<prefix>-<counter>` (e.g. `a3f-17`). An id MUST NOT be reused
  within a process and MUST NOT be duplicated within a process; across processes only the prefix may
  coincide. Reuse is the failure being prevented - two connections under one name read as one
  connection in a log - so a scheme that recycles ids of closed connections MUST NOT be used.
- **WSM-API-018** `open()` MUST NOT take a `timeout` argument in either language. `open()` returns
  immediately, so there is nothing for a deadline on it to bound; deadlines live on the awaits,
  `stream.result(timeout=)` and `peer.request(timeout=)`.

### 9.2 Awaitable-handle semantics

- **WSM-API-010** `Stream.__await__` MUST delegate to a **memoized** future - one per stream, created
  lazily on the first await, resolved with the stream's first payload or rejected with the stream's
  `StreamReset`.
- **WSM-API-011** Awaiting an already-awaited stream MUST NOT be an error and MUST return the same
  value again. Test: `stream_test.py::test_await_twice_returns_same_value`.
- **WSM-API-012** `result(timeout=...)` MUST be the same future with a deadline wrapped around the
  wait, never a second source of the value.
- **WSM-API-013** Iteration MUST read payloads off the stream's own queue and MUST NOT touch that
  future. The consumption claim MUST be recorded on the stream, not on the future.
- **WSM-API-014** The first of the two shapes to be used claims the stream; the other MUST raise
  `StreamAlreadyConsumed`, with an error naming both uses. Two `async for` loops over one stream is
  the same error. Test: `stream_test.py::test_await_then_iterate_raises_and_first_consumer_got_everything`.
- **WSM-API-015** In TypeScript `Stream<T>` MUST implement `PromiseLike<T>` (`then`/`catch`/`finally`
  delegating to an internal promise) and MUST NOT subclass `Promise` (species semantics would make
  every derived call construct a bogus `Stream`).
- **WSM-API-016** In TypeScript the implementation MUST attach a default no-op rejection handler to
  the internal promise **at construction time** (not lazily on first `then`), and MUST surface the
  failure through the peer's frame/error hook instead.
  Test: `stream.spec.ts::reset stream nobody consumed reports no unhandled rejection and does reach the error hook`.
- **WSM-API-017** In Python, `peer.open(...)` on a line by itself MUST emit no `RuntimeWarning`.
  Test: `stream_test.py::test_unconsumed_open_emits_no_runtime_warning`.

### 9.3 Python declarations

```python
async def connect(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    hello: Any = None,
    hello_headers: dict[str, Any] | None = None,
    reconnect: Reconnect | None = None,
    ping_interval: float = 20.0,
    ping_timeout: float = 10.0,
    hello_timeout: float = 10.0,
    max_payload_bytes: int = 67_108_864,          # local receive limit, WSM-FRG-035
    max_concurrent_streams: int = 100,            # local receive limit, WSM-STM-036
    error_serializer: ErrorSerializer | None = None,   # per peer, WSM-ERR-008
    codec: Codec | None = None,          # test override only, WSM-CDC-012
) -> Peer: ...                           # raises if the FIRST attempt fails, WSM-RCN-006

async def accept(
    socket: SocketAdapter,
    *,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
) -> Peer: ...

async def serve(socket: SocketAdapter, *, handler: StreamHandler, **peer_options: Any) -> None: ...
def register_codec(name: str, codec: Codec) -> None: ...
def select_subprotocol(connection: Any, subprotocols: list[str]) -> str: ...  # raises to refuse

MAX_FRAME_BYTES: Final[int] = 65_536      # protocol constant, WSM-FRG-004

ErrorSerializer = Callable[[BaseException], Any | None]

@dataclass(frozen=True)
class CloseReason:                        # WSM-RCN-045
    code: int
    reason: str
    was_clean: bool
    will_retry: bool

class Reconnect:
    initial_delay: float = 0.25
    factor: float = 2
    max_delay: float = 30.0
    jitter: float = 0.3
    max_attempts: int | None = None      # None = unlimited
```

**Peer**

| Member | Signature | Notes |
|---|---|---|
| `open` | `open(payload=None, *, headers=None, end=False) -> Stream` | **Not a coroutine.** No `timeout` (WSM-API-018). WSM-API-001, WSM-API-004. |
| `notify` | `async notify(payload=None, *, headers=None) -> None` | `open(payload, end=True)` returning nothing. |
| `request` | `async request(payload=None, *, headers=None, timeout=None) -> Any` | WSM-API-006. |
| `on_stream` | `on_stream(handler: StreamHandler)` | Decorator or plain call. One per peer; a second replaces it and logs. |
| `on_close` | `on_close(handler: Callable[[CloseReason], None])` | Fires on every socket loss. |
| `on_reconnect` | `on_reconnect(handler: Callable[[int, Peer], None])` | Fires once per re-established connection, after WSM-CON-030 **and** hello ack. |
| `on_frame` | `on_frame(handler)` | `(direction, frame, byte_length)`, before encode / after decode. |
| `ping` | `async ping(timeout: float = 5.0) -> float` | Round-trip seconds. |
| `close` | `async close(code=ResetCode.NO_ERROR, reason=None, drain=10.0) -> None` | `goaway`, drain, close. |
| `serve` | `async serve() -> None` | Runs the read loop until the socket closes. Needed on the acceptor side; `connect()` starts it. |
| `id` | `str` | Connection id for logs: per-process prefix + per-connection counter, e.g. `a3f-17`. WSM-API-009. |
| `tags` | `dict[str, Any]` | §9.5. |
| `streams` | `Mapping[int, Stream]` | Live streams, read-only. |
| `is_open` | `bool` | WSM-RCN-043. |

**Stream**

| Member | Signature | Notes |
|---|---|---|
| `id` | `int` | Read-only. Never an argument anywhere. |
| `headers` | `dict` | The `open` frame's headers; empty for locally opened streams. |
| `payload` | `Any` | The opening payload, already reassembled. |
| `send` | `async send(payload, *, end=False) -> None` | Fragments automatically above the cap. Raises per WSM-ERR-009 on a stream that is no longer open. |
| `end` | `async end(payload=None, *, trailers=None) -> None` | Raises per WSM-ERR-009. |
| `reply` | `async reply(payload, *, trailers=None) -> None` | `send` + `end`. Raises per WSM-ERR-009. |
| `__await__` | `await stream -> Any` | First payload from the memoized future. Claims the stream against iteration. |
| `result` | `async result(timeout=None) -> Any` | Same future plus a deadline. Raises `StreamReset` if reset first; raises if the stream ends without producing a payload. |
| `cancel` | `async cancel(reason=None) -> None` | `reset(CANCELLED)`. |
| `reset` | `async reset(code, reason=None) -> None` | The general form. |
| `__aiter__` | `async for payload in stream` | Reassembled payloads until end; raises `StreamReset` if reset. Claims the stream against `await`. |
| `trailers` | `dict \| None` | Populated when the stream ends. |
| `closed` | `asyncio.Event` | Set on close, including on socket death. WSM-API-023. |

Handler type: `StreamHandler = Callable[[Any, Stream], Awaitable[None]]`.

### 9.4 TypeScript declarations

```ts
export const MAX_FRAME_BYTES = 65_536;   // protocol constant, WSM-FRG-004

export function connect(url: string, options?: ConnectOptions): Promise<Peer>;  // rejects if the
                                         // FIRST attempt fails, WSM-RCN-006
export function accept(socket: SocketAdapter, options?: AcceptOptions): Promise<Peer>;
export function serve(socket: SocketAdapter,
                      options: AcceptOptions & { handler: StreamHandler }): Promise<void>;
export function registerCodec(name: string, codec: Codec): void;
export function selectSubprotocol(offered: string[]): string | null;

export type ErrorSerializer = (err: unknown) => unknown;

export interface AcceptOptions {
  maxPayloadBytes?: number;              // default 67_108_864, WSM-FRG-035
  maxConcurrentStreams?: number;         // default 100, WSM-STM-036
  errorSerializer?: ErrorSerializer;     // per peer, WSM-ERR-008
  codec?: Codec;                         // test override only
}

export interface ConnectOptions extends AcceptOptions {
  headers?: Record<string, string>;      // node only; browsers cannot set them
  hello?: unknown;
  helloHeaders?: Record<string, unknown>;
  reconnect?: ReconnectOptions;
  pingIntervalMs?: number;               // default 20_000
  pingTimeoutMs?: number;                // default 10_000
  helloTimeoutMs?: number;               // default 10_000
  onStream?: StreamHandler;              // sugar for pre-connection registration
  onClose?: (reason: CloseReason) => void;
  onReconnect?: (attempt: number, peer: Peer) => void;
}

export interface ReconnectOptions {
  initialDelayMs?: number;               // default 250
  factor?: number;                       // default 2
  maxDelayMs?: number;                   // default 30_000
  jitter?: number;                       // default 0.3
  maxAttempts?: number;                  // default Infinity
}

export interface OpenOptions {
  payload?: unknown;
  headers?: Record<string, unknown>;
  end?: boolean;
}

export interface RequestOptions extends OpenOptions {
  timeoutMs?: number;                    // only on calls that wait, WSM-API-018
}

export declare class Peer {
  readonly id: string;
  readonly tags: Record<string, unknown>;
  readonly streams: ReadonlyMap<number, Stream>;
  readonly isOpen: boolean;
  open<T = unknown>(payload?: unknown, options?: OpenOptions): Stream<T>;   // synchronous
  open<T = unknown>(options: OpenOptions): Stream<T>;
  notify(payload?: unknown, options?: { headers?: Record<string, unknown> }): Promise<void>;
  request<T = unknown>(payload?: unknown, options?: RequestOptions): Promise<T>;
  onStream(handler: StreamHandler): void;
  onClose(handler: (reason: CloseReason) => void): void;
  onReconnect(handler: (attempt: number, peer: Peer) => void): void;
  onFrame(handler: (direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void): void;
  ping(timeoutMs?: number): Promise<number>;                                // default 5_000
  close(options?: { code?: ResetCode; reason?: string; drainMs?: number }): Promise<void>;
  serve(): Promise<void>;
}

export declare class Stream<T = unknown> implements PromiseLike<T>, AsyncIterable<T> {
  readonly id: number;
  readonly headers: Record<string, unknown>;
  readonly payload: unknown;
  readonly trailers: Record<string, unknown> | null;
  readonly closed: Promise<void>;                                           // WSM-API-023
  readonly signal: AbortSignal;                                             // TS only
  send(payload: unknown, options?: { end?: boolean }): Promise<void>;
  end(options?: { payload?: unknown; trailers?: Record<string, unknown> }): Promise<void>;
  reply(payload: unknown, options?: { trailers?: Record<string, unknown> }): Promise<void>;
  result(options?: { timeoutMs?: number }): Promise<T>;
  cancel(reason?: string): Promise<void>;
  reset(code: ResetCode, reason?: string): Promise<void>;
  then<R1 = T, R2 = never>(onOk?: ((v: T) => R1 | PromiseLike<R1>) | null,
                           onErr?: ((e: unknown) => R2 | PromiseLike<R2>) | null): PromiseLike<R1 | R2>;
  [Symbol.asyncIterator](): AsyncIterator<T>;
}

export interface Codec {
  readonly name: string;
  readonly binary: boolean;
  encode(frame: Frame): string | ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
}

export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}

export interface CloseReason { code: number; reason: string; wasClean: boolean; willRetry: boolean; }

export type StreamHandler = (payload: any, stream: Stream) => void | Promise<void>;
```

- **WSM-API-020** `open` and `request` MUST accept `(payload?)`, `(payload?, options?)` **or**
  `(options)`, following the repository's existing positional-or-options-object overload convention.
  `open`'s options object MUST NOT carry `timeoutMs`; `request`'s MUST (WSM-API-018).
- **WSM-API-023** `stream.closed` MUST be `asyncio.Event` in Python and **`Promise<void>` in
  TypeScript**, resolving when the stream closes, including on socket death. TypeScript has no Event
  primitive, and a promise is what composes with `await` and `Promise.race`. The promise MUST resolve
  and MUST NOT reject - a stream that closed by being reset still closed, and the reset reaches the
  awaits and the iterator instead - so WSM-API-016's unhandled-rejection precaution does not apply to
  it.
- **WSM-API-021** The socket adapter protocol (`send_text`/`sendText`, `send_bytes`/`sendBytes`,
  `receive`, `close`, plus the handshake hook) MUST be the **only** place transport-specific code
  lives. Text and binary sends MUST be separate methods, never one polymorphic `send`.
- **WSM-API-022** The Node acceptor MUST live behind the `muxws/node` subpath export so
  the browser entry point never pulls in `ws`. The peer implementation MUST be shared; only the socket
  adapter differs.

### 9.5 `tags` and `PeerRegistry`

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

---

## 10. Auth, headers and routing

- **WSM-AUT-001** Authentication MUST happen at the WebSocket upgrade, before `accept()` is called.
  muxws MUST NOT interpret credentials anywhere.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application, and
  MUST NOT be used for re-authentication by the library.
- **WSM-AUT-003** A connection whose credential expires mid-life SHOULD be closed with `goaway`; the
  dialer's reconnect helper re-authenticates by dialling again.
- **WSM-AUT-004** muxws MUST NOT look at the opening payload for routing purposes: no path matching,
  no method dispatch, no handler table (WSM-STM-030).

---

## 11. Backpressure

- **WSM-BPR-001** v1 MUST NOT implement per-stream flow control. `window_update` is reserved as a
  frame type and MUST NOT be sent.
- **WSM-BPR-002** The flow-control mechanisms in v1 are exactly the receiver's local concurrency limit
  (WSM-STM-036, how many producers can exist at once) and `MAX_FRAME_BYTES` (WSM-FRG-004, how long any
  one may hold the wire). Both are local defences, not agreements; there is no negotiated mechanism of
  any kind, and building one requires a new generation (WSM-CON-009).

---

## 12. Observability

- **WSM-OBS-001** Every frame MUST be loggable in one line at `DEBUG` under the `muxws.frames`
  logger. Suggested shape:

```
muxws conn=a3f-17 dir=tx type=open   stream=7  end=0 bytes=214  headers=1
muxws conn=a3f-17 dir=rx type=data   stream=7  end=0 bytes=8192 frag=1/4
muxws conn=a3f-17 dir=tx type=reset  stream=9  code=1 reason="user navigated away"
muxws conn=a3f-17 dir=rx type=goaway last=7 code=0
```

- **WSM-OBS-002** The peer MUST NOT log payload *contents* at any level (application data routinely
  contains secrets).
- **WSM-OBS-003** `peer.on_frame(handler)` MUST receive `(direction, frame, byte_length)` before
  encode and after decode.

---

## 13. Cross-cutting invariants

Each of these is violated most often by accident; the clause after the dash is the failure it
prevents.

- **WSM-INV-001** muxws MUST NOT depend on any package above it in the stack - not backchannel, not
  fastapi-viewsets, not the frontend kit; not as an import, an optional extra, or a `TYPE_CHECKING`
  annotation - or the claim that anyone wanting multiplexed streams can install it stops being true.
- **WSM-INV-002** There MUST be one symmetric `Peer` type per language - or server push needs a
  second, parallel mechanism with its own correlation and cancellation story.
- **WSM-INV-003** Every size limit MUST be counted in bytes of the **codec's own output**
  (WSM-FRG-001) - or a sender budgeting against JSON text while msgpack bytes go on the wire produces
  over-cap frames on exactly the deployments that chose the compact codec.
- **WSM-INV-004** At most one unsent fragment per stream, and round-robin writer selection
  (WSM-FRG-018/019) - or a 1 MB payload adds a full second of latency to a 200-byte progress update on
  another stream.
- **WSM-INV-005** Id allocation and `open` enqueue MUST be one indivisible synchronous step
  (WSM-SID-006) - or two concurrent `open()` calls put a non-monotonic id sequence on the wire, which
  is a protocol error the peer commits against itself.
- **WSM-INV-006** Late frames below the high-water mark MUST be ignored and frames above it MUST kill
  the connection (WSM-STM-002/003) - or "ignore late frames" degenerates into "ignore everything" and
  a genuine id-space disagreement goes undetected.
- **WSM-INV-007** *Retired* with WSM-STM-022. It required `STREAM_LIMIT` not to be reported as
  `REFUSED`. The id is not reused. What replaces it as the thing to get wrong: the concurrency limit
  MUST stay entirely receiver-side (WSM-STM-036) - a sender that "helpfully" tracks how many streams
  it has open and refuses locally has reinvented the announced quota, against a number it cannot know.
- **WSM-INV-008** A handler that raises MUST always produce `APPLICATION_ERROR`, never `REFUSED`
  (WSM-STM-034) - or a handler that debits an account and then raises invites the client to retry the
  debit.
- **WSM-INV-009** The awaited future MUST be memoized and the consumption claim MUST live on the
  stream (WSM-API-010/013) - or a stream's payloads get split silently between an `await` and an
  `async for`, and neither consumer looks wrong locally.
- **WSM-INV-010** Nothing MUST be queued while the peer is between sockets (WSM-RCN-042) - or a queue
  flushes into a server that has forgotten the sender, and a failure that would have reached a call
  site is turned into silent misdelivery.
- **WSM-INV-011** Every stream live at socket death MUST fail with `ConnectionLost`, never hang
  (WSM-RCN-041) - or a caller sees no error, no log and no timeout, just a spinner that never stops.
- **WSM-INV-012** The attempt counter MUST reset only on an *established* connection (WSM-RCN-004) -
  or a server that accepts sockets while its backend is down turns exponential backoff into a
  fixed-interval hammer at the initial delay.
- **WSM-INV-013** The hello MUST be replayed by the helper, not by the application (WSM-RCN-020) - or
  an application that forgets gets a socket the server cannot associate with anything: connected,
  healthy-looking, subscribed to nothing, reporting no error.
- **WSM-INV-014** `tags` MUST NOT survive a reconnect (WSM-RCN-033) - or a tab that silenced
  something and then died stays silent for a successor that never asked to be.
- **WSM-INV-015** An unregistered codec name MUST be a loud startup failure, never a silent JSON
  fallback (WSM-CDC-016) - or a deployment believes it is running msgpack, is not, and may never find
  out because both ends fell back.
- **WSM-INV-016** muxws MUST NOT define a message vocabulary (WSM-FRM-006) - or two independent
  consumers sharing one socket must both nest their own vocabulary inside an imposed one, and every
  change to either needs a muxws release.
- **WSM-INV-017** `max_payload_bytes` MUST be enforced as fragments accumulate, not after reassembly
  (WSM-FRG-032) - or a receiver has already allocated everything the limit existed to bound by the
  time it decides to refuse it, and the limit protects nothing but a variable.
- **WSM-INV-018** `connect()` MUST raise when the first attempt fails (WSM-RCN-006) - or a typo in the
  URL, an unreachable host or a codec mismatch never surfaces anywhere: the application holds a peer
  that looks alive and retries forever against something that will never answer.

---

## 14. Configuration

Every setting, its default, unit and range. **Nothing in this table is exchanged on the wire.** The
first row is a protocol constant, the next two are local receive-side limits, and the rest are local
to one peer.

| Setting | Where | Python name | TypeScript name | Default | Unit / range |
|---|---|---|---|---|---|
| max frame bytes | protocol constant | `MAX_FRAME_BYTES` | `MAX_FRAME_BYTES` | 65536 | bytes; not configurable (test override only, WSM-FRG-005) |
| max payload bytes | `connect()` / `accept()` | `max_payload_bytes` | `maxPayloadBytes` | 67108864 | bytes, local receive limit |
| max concurrent streams | `connect()` / `accept()` | `max_concurrent_streams` | `maxConcurrentStreams` | 100 | int ≥ 1, local receive limit |
| error serializer | `connect()` / `accept()` | `error_serializer` | `errorSerializer` | default serializer | per peer, WSM-ERR-008 |
| codec name | environment | `MUXWS_CODEC` → `muxws.conf.settings.codec` | `VITE_MUXWS_CODEC` | `"json"` | a registered codec name |
| codec override | call argument | `codec=` | `codec` | `None` | tests / bridges only |
| reconnect initial delay | `connect()` | `initial_delay` | `initialDelayMs` | 0.25 / 250 | s / ms, > 0 |
| reconnect factor | `connect()` | `factor` | `factor` | 2 | ≥ 1 |
| reconnect cap | `connect()` | `max_delay` | `maxDelayMs` | 30.0 / 30 000 | s / ms |
| reconnect jitter | `connect()` | `jitter` | `jitter` | 0.3 | fraction, 0..1 |
| reconnect attempt cap | `connect()` | `max_attempts` | `maxAttempts` | `None` / `Infinity` | count |
| heartbeat interval | `connect()` | `ping_interval` | `pingIntervalMs` | 20.0 / 20 000 | s / ms |
| heartbeat deadline | `connect()` | `ping_timeout` | `pingTimeoutMs` | 10.0 / 10 000 | s / ms |
| hello deadline | `connect()` | `hello_timeout` | `helloTimeoutMs` | 10.0 / 10 000 | s / ms |
| `peer.ping()` deadline | call argument | `timeout` | `timeoutMs` | 5.0 / 5 000 | s / ms |
| goaway drain | `peer.close()` | `drain` | `drainMs` | 10.0 / 10 000 | s / ms |
| request timeout | call argument | `timeout` | `timeoutMs` | `None` (no default) | s / ms |
| fragmentation reservation | internal | - | - | `min(512, cap // 2)` | bytes |

---

## 15. Repository layout and packaging

```
muxws/
├── muxws/                     # PyPI: muxws
│   ├── __init__.py            # connect / accept / serve / register_codec / registry re-exports
│   ├── conf.py                # settings singleton; reads MUXWS_CODEC
│   ├── frames.py              # frame dataclasses, validation
│   ├── codecs/{__init__.py, json_.py, msgpack_.py}
│   ├── peer.py stream.py errors.py registry.py reconnect.py
│   ├── transports/            # starlette.py, websockets_.py, memory.py
│   └── *_test.py              # colocated pytest
├── ts/                        # npm: muxws
│   ├── index.ts codec.ts msgpack.ts node.ts
│   ├── frames.ts stream.ts peer.ts errors.ts reconnect.ts registry.ts
│   └── *.spec.ts              # colocated vitest
├── conformance/{frames,sequences,invalid}/*.json
├── SPEC.md pyproject.toml package.json
```

- **WSM-PKG-001** Both packages MUST ship from one repository on one version stream, with identical
  version numbers in `pyproject.toml` and `package.json`.
- **WSM-PKG-002** The Python package MUST have **zero required runtime dependencies**. `starlette`
  and `websockets` are optional extras selected by which transport is imported; `msgpack` is an
  optional extra selected by which codec is registered.
- **WSM-PKG-003** The TypeScript browser entry point MUST have zero runtime dependencies. `ws` is an
  optional peer dependency for `node.ts`; `@msgpack/msgpack` an optional peer dependency reachable
  only through the `/msgpack` subpath.
- **WSM-PKG-004** File names in the TypeScript package MUST be kebab-case; TypeScript strings use
  single quotes, Python strings double quotes; Python line length 120.
- **WSM-PKG-005** The wire format is versioned by the generation integer in the subprotocol name
  (`muxws.v1.<codec>`) - a single monotonically increasing integer, bumped only for a breaking wire
  change - independently of the packages' semver. There is no second, finer version anywhere
  (WSM-CON-009).

---

## 16. Conformance corpus

- **WSM-TST-001** `conformance/frames/*.json` MUST be a list of `{"name", "frame", "json_wire"}`
  triples - a logical frame plus the JSON rendering pinned alongside it - read verbatim by both
  `pytest` and `vitest`.
- **WSM-TST-002** `conformance/sequences/*.json` MUST be scripted exchanges replayed by both
  implementations against the in-memory transport. Fixtures MUST refer to streams by `stream_ref`
  (an ordinal the runner resolves), never by a raw id. A fixture's optional top-level
  `max_frame_bytes` is an instruction to the **runner** to construct both peers with a lowered cap
  (WSM-FRG-005); it is not a wire value and MUST NOT be encoded into any frame.

```json
{
  "name": "unary-request-with-server-push-interleaved",
  "max_frame_bytes": 64,
  "steps": [
    {"peer": "dialer",   "call": "request", "payload": {"action": "list"}, "as": "r1"},
    {"expect_frame": {"type": "open", "stream": 1, "end": true}},
    {"peer": "acceptor", "call": "open",    "payload": {"event": "tick"}},
    {"expect_frame": {"type": "open", "stream": 2}},
    {"peer": "acceptor", "call": "reply",   "stream_ref": 1, "payload": {"items": []}},
    {"expect_frame": {"type": "data", "stream": 1, "end": true}},
    {"expect_result": {"ref": "r1", "value": {"items": []}}}
  ]
}
```

- **WSM-TST-003** `conformance/invalid/*.json` MUST cover, each asserting which frame goes out and
  whether the connection survives: wrong parity; an `open` id not greater than that peer's highest
  previous open; `data` after `end`; an over-cap encoded message; a message the configured codec
  refuses to decode; a fragment sequence interrupted by a non-fragment frame; a stream-level frame
  above the high-water mark (connection dies); a `data` frame for an already-closed id (connection
  survives, nothing goes out).
- **WSM-TST-004** CI MUST run the live cross-language matrix in **both role assignments** (Python
  acceptor with TypeScript dialer, and TypeScript acceptor with Python dialer) over the same scenario
  script: concurrent unary requests interleaved with a streaming export and a server push, one
  cancelled mid-flight, and a `goaway` shutdown.
- **WSM-TST-005** The same matrix MUST include one reconnect scenario: kill the acceptor process with
  streams open, restart it, and assert the dialer re-dials on a jittered delay, replays a
  byte-identical hello the other language's acceptor accepts, fires `on_reconnect` exactly once after
  it, and that every open stream raised `ConnectionLost` in the meantime.

---

## 17. Implementation order

Each milestone is independently shippable and independently testable. Do not start the next until the
previous one's tests pass in both languages where both are in scope.

| M | Contents | Rules covered |
|---|---|---|
| M1 | Frames and the codec seam, both languages, no sockets: frame model and validation, `ResetCode`, the error hierarchy, the `Codec` port with `JsonCodec`, `register_codec`/`registerCodec`, the fragmentation splitter/assembler as a pure function. `conformance/frames/` and `conformance/invalid/` written here. | §3, §4, §8.1-8.2, WSM-CDC-001..006, WSM-TST-001/003 |
| M2 | Python peer core over an in-memory transport: `Peer`, `Stream`, state machine, id allocation, `on_stream`, `open`/`notify`/`send`/`end`/`reset`, awaitable handle, `request`, async iteration, cancellation mapping both ways. Every state-table cell tested. | §5, §9.1-9.3, §8.3 |
| M3 | Real transports: Starlette/FastAPI acceptor, `websockets` dialer, browser `WebSocket` dialer, `ws` acceptor, plus the TypeScript port of M2. Environment codec selection, the startup failure, the subprotocol assertion. | WSM-CDC-010..028, §9.4, WSM-API-015..017 |
| M4 | Connection liveness and shutdown: `ping`/`pong` liveness timers, `goaway` with drain semantics, graceful `close()`. Smaller than it was - the `settings` exchange it was mostly made of no longer exists, and the concurrency limit moved to M5 with the other receive-side caps - but kept separate because `goaway` drain ordering against in-flight streams needs a milestone where it is the thing being tested. | §6 |
| M5 | Robustness: fragmentation wired into the send path with the one-unsent-fragment rule and round-robin writer, all three receive-side caps (frame size, `max_payload_bytes` enforced as fragments accumulate, the local concurrency limit answering with `REFUSED`), `PeerRegistry`, `on_frame`, and the reconnect helper on both clients. First production-usable release. | §4.2-4.3, §7, §9.5, §12, WSM-STM-036/037 |
| M6 | Conformance, documentation, wire freeze: full `conformance/sequences/`, the live Python↔TS matrix in both role assignments, `SPEC.md` normative under the v1 generation, the forward-compatibility test (an unknown frame type is ignored and the connection survives), msgpack shipped in both languages with its own cross-language pair. Tag 1.0; the JSON wire form is frozen. | §16, WSM-CDC-007, WSM-FRM-002 |


---

There are no open extraction gaps. Every question this document was extracted with has been answered
by the decisions now stated as rules; the design brief's *Open questions* are recommendations with
triggers, not holes in this specification.
