# muxws — normative specification for the `muxws.v1.` generation

This document specifies **muxws**: framing, multiplexing, stream lifecycle, cancellation and
connection lifecycle over one WebSocket. This document and the `conformance/` corpus are sufficient
to write an implementation that interoperates with the Python and TypeScript implementations in this
repository.

The stream model is that of **HTTP/2 and HTTP/3**: many independent streams over one connection,
either peer able to open one, headers then body then optional trailers, per-stream cancellation, and
`goaway` for graceful shutdown. muxws differs from those protocols in two respects. A WebSocket runs
over one stream-oriented connection — a TCP socket, or a Unix domain socket where both ends are on
the same machine — so there is **one global message order** and no per-stream loss recovery; the
frame cap is therefore a protocol constant (WSM-FRG-004), not a negotiated limit. And there is **no
`SETTINGS` exchange** (WSM-CON-031): every limit is either a protocol constant or a local defence of
one peer.

Requirement levels are RFC 2119: **MUST**, **MUST NOT**, **SHOULD**, **MAY**. Every rule carries a
stable id and is cross-referenced by id only. Rules appear in **id order** (§5), after the wire form
they refer to (§2–§4).

**The only version in this system is on the wire: the generation integer in the subprotocol name
`muxws.v1.<codec>`** (WSM-CON-009, WSM-PKG-005). This document has no version of its own. There is no
spec revision number, no `protocol_version` field and no settings frame announcing capabilities.
The packages' semver versions the *packages*, never the wire.

A disagreement between this document and a brief under `docs/design/briefs/` is a defect and is
recorded in `GAPS.md`. `docs/design/muxws-websocket-transport.md` contains the design rationale for
these rules; it is not normative.

---

## 1. Scope and vocabulary

Out of scope for muxws: routing, serialization of domain objects, authentication, durable storage
and RPC.

| Term | Definition |
|---|---|
| **peer** | One end of one WebSocket connection. One symmetric type per language; there is no separate client type and server type. |
| **dialer** | The peer that called `connect(url)`. Allocates odd stream ids. |
| **acceptor** | The peer that was handed a socket by its framework and called `accept(socket)`. Allocates even stream ids. |
| **stream** | An independently addressed, independently cancellable, bidirectional sequence of frames sharing one id. |
| **frame** | One logical protocol unit. Exactly one frame per WebSocket message. |
| **codec** | The component that converts a logical frame into a WebSocket message and back. Fixed per process by deployment configuration. |
| **encoded message** | The complete codec output for one frame, envelope included, as it goes on the socket. |
| **payload** | An application value carried by `open`, `data` or `reset`. Opaque to muxws. |
| **fragment** | A slice of the codec-encoded form of one logical payload. |
| **local / remote** | `local` = the peer whose state is being described; `remote` = the other one. |
| **live stream** | A stream in state `open`, `half_closed_local` or `half_closed_remote` on this peer. |
| **high-water mark** | Per parity, the highest stream id that parity's owner has ever opened, as observed by this peer. |
| **established** | The socket is open and, for a codec-bearing subprotocol, that subprotocol was accepted (WSM-CON-030). The reconnect helper additionally requires the hello to have been acknowledged (WSM-RCN-004). |
| **hello** | An application-supplied opening payload the reconnect helper replays on every connection. |

---

## 2. The wire

### 2.1 The subprotocol name

A dialer offers `muxws.v1.<codec>` as its **first** subprotocol entry; an acceptor selects exactly
that value or refuses the upgrade (WSM-CDC-020..022). The `v1` is the generation, and it is the only
version anywhere on the wire.

```text
Sec-WebSocket-Protocol: muxws.v1.json, bearer.eyJhbGciOi...   # dialer offers; entry 1 is muxws's
Sec-WebSocket-Protocol: muxws.v1.json                         # acceptor selects; or HTTP 400
```

### 2.2 The envelope

One frame per WebSocket message. Field names are spelled out, never abbreviated, and stay snake_case
in every language.

```json
{"type": "data", "stream": 7, "payload": {"rows": 128}}
```

| Field | Type | Required | Default | Present on | Meaning |
|---|---|---|---|---|---|
| `type` | string | yes | — | every frame | Frame type (§2.3). |
| `stream` | int | on stream-level frames | — | `open`, `data`, `reset` | Stream id. Omitted (or `0`) on connection-level frames. |
| `payload` | any codec value | no | absent | `open`, `data`, `reset` | The application value. On `reset`, the optional structured error object. Absent means "no payload", which is a different frame from `"payload": null`. Mutually exclusive with `fragment`. |
| `fragment` | string (text codec) / bytes (binary codec) | no | absent | `open`, `data`, `reset` | A slice of the codec-encoded logical payload. Mutually exclusive with `payload`. |
| `more` | bool | no | `false` | frames with `fragment` | `true` on every fragment but the last. |
| `headers` | object | no | absent | `open`, and the first `data` a peer sends on a stream | Application metadata; string keys, codec-encodable values. Never interpreted by muxws. Each peer may send headers once per stream (WSM-FRM-016). |
| `end` | bool | no | `false` | `open`, `data` | Last frame this peer will send on this stream. |
| `trailers` | object | no | absent | frames with `end: true` | Post-body metadata. |
| `code` | int | yes | — | `reset`, `goaway` | Reset code (§2.4). |
| `reason` | string | no | absent | `reset`, `goaway` | Human-readable, for logs. MUST NOT be parsed. |
| `nonce` | string | yes | — | `ping`, `pong` | Opaque, echoed verbatim. Sender-chosen. |
| `last_stream` | int | yes | — | `goaway` | Highest id from the *other* peer this peer has processed and will still complete. |

`fragment` is listed on `reset` as well as on `open` and `data` because a `reset` carrying a large
structured error object goes through the same splitter as any other stream-level frame, and both
implementations fragment it (`conformance/frames/v1-frames.json` contains one).

The envelope consists of exactly these twelve fields. Both reference implementations omit a field
whose value equals its default, and the wire forms in the corpus are written that way. A receiver
applies the defaults whether a field is omitted or present.

An absent `payload` key and an explicit `"payload": null` are **different frames**, and the corpus
contains both. WSM-CDC-005's `decode(encode(frame)) == frame` therefore requires a decoder to keep
them distinct; the reference implementations use an `ABSENT` sentinel. Whether an application treats
the two differently is up to the application.

A receiver ignores envelope fields it does not know (WSM-FRM-001) and frame types it does not know
(WSM-FRM-002). A decoder MUST drop unknown fields rather than preserve them.

### 2.3 The v1 frame set

`open`, `data`, `reset`, `ping`, `pong`, `goaway`. These six are the complete v1 frame set.

- `window_update` is **reserved and unimplemented**: the name is reserved, and a v1 peer MUST NOT
  send it (WSM-BPR-001).
- There is **no `settings` frame** (WSM-CON-031), and no `ack`, `protocol_version` or `extensions`
  field anywhere.
- There is **no `end` frame type** (WSM-FRM-012) and **no trailers frame type** (WSM-FRM-013): both
  are flags on `data`.

```json
{"type":"open","stream":1,"end":true,"payload":{"action":"list"}}
{"type":"open","stream":3,"headers":{"trace":"abc123","attempt":2},"payload":{"action":"export"}}
{"type":"data","stream":3,"headers":{"content-type":"text/csv","rows-estimated":40000}}
{"type":"data","stream":7,"payload":{"rows":128}}
{"type":"data","stream":7,"end":true,"payload":{"rows":4},"trailers":{"checksum":"deadbeef"}}
{"type":"data","stream":7,"end":true}
{"type":"reset","stream":13,"code":2,"payload":{"type":"ValueError","message":"no such report"},"reason":"handler raised"}
{"type":"ping","nonce":"8f14e45fceea167a"}
{"type":"pong","nonce":"8f14e45fceea167a"}
{"type":"goaway","code":0,"last_stream":7,"reason":"shutting down"}
```

The second `data` line carries the answering side's leading headers: the peer that did not open the
stream sends metadata about its response on a frame with no payload. That frame is the peer's
**first** on the stream, the only frame on which it may send headers (WSM-FRM-016). The opener sends
its headers on `open`.

A fragmented payload has `headers` on the first fragment only, `end` and `trailers` on the last only,
`code` and `reason` on every fragment of a `reset`, and `more: true` on all but the last:

```json
{"type":"open","stream":11,"fragment":"{\"body\":\"aaaa","more":true}
{"type":"open","stream":11,"end":true,"fragment":"aaaa\"}"}
{"type":"reset","stream":53,"code":2,"fragment":"{\"type\":\"ValueError\",","more":true,"reason":"handler raised"}
```

### 2.4 Reset codes

Numeric on the wire, named in every API. The same table serves `reset` and `goaway`. The reaction
column is normative.

| Code | Name | Raised when | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, orderly shutdown; on `reset`, "done and no longer interested". | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` carries a structured error object. | Surface to the caller. Retry is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted and definitively not processed: no registered handler, a post-`goaway` open, or an open beyond the receiver's own concurrency limit (WSM-STM-036). | Retry — elsewhere if another connection is available, otherwise after a delay. |
| 6 | `TIMEOUT` | A deadline expired locally; the reset tells the remote to stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts (WSM-FRG-031), or a payload exceeded the receiver's `max_payload_bytes` (WSM-FRG-032). | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream live at that instant. **MUST NEVER appear on the wire.** | Do not retry on this peer now; rebuild from `on_reconnect`. |

An unrecognised code MUST reset the named stream and
leave the connection alive. An implementation that converts the wire value directly into a closed
enumeration raises inside the read loop; the peer then reports itself open while every pending await
hangs.

### 2.5 What is *not* on the wire

The wire carries none of the following:

- A limit of any kind. `MAX_FRAME_BYTES` is a protocol constant; `max_payload_bytes` and the
  concurrency limit are local defences of the receiver (WSM-CON-031).
- A capability list.
- A version other than the subprotocol generation.
- A credential (§5.2).
- muxws-defined vocabulary inside `payload`: no `kind`, reserved key or discriminator (WSM-FRM-006).
- A status. The answering side's `headers` (WSM-FRM-016) belong to the application; muxws defines no
  success key and reads none (WSM-AUT-002). A failed exchange is a typed `reset` (§2.4).

---

## 3. Sizes, fragmentation and the codec's output

Every size in this specification is **bytes of the fully encoded WebSocket message**, envelope
included, exactly as it goes on the socket (WSM-FRG-001). The size of the payload before encoding and
the size of the `fragment` field alone are not sizes in this sense.

- `MAX_FRAME_BYTES` = **65536**. A protocol constant (WSM-FRG-004).
- `max_payload_bytes` — the largest reassembled payload this peer accepts — defaults to **67108864**
  and is local to the receiver (WSM-FRG-035).

For the JSON codec, the two reference implementations produce the same encoded form for every value
the shared corpus may contain, which WSM-FRG-016 depends on: compact separators (`,` and `:`, no
spaces), no `\u` escaping of non-ASCII, and `NaN`/`Infinity` refused rather than emitted. They
**differ** on floats and on integers outside ±(2^53−1) — `1.0` against `1`, `1e-07` against `1e-7`
— following each language's standard JSON serializer. This does not affect interoperability:
fragment boundaries are chosen by the sender and are not observable to the receiver. A shared
fixture must not contain a float, because the two implementations would cut it at different
boundaries. A third port MUST match the compact, un-escaped form.

Fragmentation is defined over the codec's encoding of the **payload alone** (WSM-FRG-011), so a port
needs a payload-level entry point to its codec as well as the frame-level pair. Both reference
implementations name it `encode_payload` / `decode_payload` (`encodePayload` / `decodePayload`);
WSM-CDC-001 fixes the four members a codec MUST expose and does not forbid more.

---

## 4. Streams

### 4.1 Ids

The dialer allocates odd ids, the acceptor even (WSM-SID-002); `0` is reserved for connection-level
frames, which in practice omit the field (WSM-SID-003). Opens are monotonic per peer and ids are
never reused (WSM-SID-004). The library allocates them; no API anywhere takes one (WSM-SID-001).

### 4.2 State machine

Five states, tracked per stream per peer, against nine events: 45 cells, each covered by a test
(WSM-STM-010).

| State | Entered by |
|---|---|
| `idle` | id allocated, nothing sent |
| `open` | `open` sent or received without `end` |
| `half_closed_local` | this peer sent `end: true` |
| `half_closed_remote` | this peer received `end: true` |
| `closed` | both ends sent `end`, or either sent `reset`, or the connection died |

Legend: `→ S` transition to S; `ILL-S` stream-level protocol error (reset that stream, connection
survives); `ILL-C` connection-level protocol error (`goaway(PROTOCOL_ERROR)`, close socket); `IGN`
silently ignore; `RAISE` local API error at the call site with nothing sent (WSM-ERR-009 for
`data`/`end`, WSM-API-004 for `open`); `n/r` not reachable through the public API.

| State \ Event | send `open` | recv `open` | send `data` | send `end` | recv `data` | recv `data(end)` | send `reset` | recv `reset` | socket death |
|---|---|---|---|---|---|---|---|---|---|
| `idle` | → `open` (→ `half_closed_local` if `end`) | → `open` (→ `half_closed_remote` if `end`) | n/r | n/r | ILL-C | ILL-C | n/r | ILL-C | → `closed` |
| `open` | RAISE | ILL-C | → `open` | → `half_closed_local` | → `open` | → `half_closed_remote` | → `closed` | → `closed` | → `closed` |
| `half_closed_local` | RAISE | ILL-C | RAISE | RAISE | → `half_closed_local` | → `closed` | → `closed` | → `closed` | → `closed` |
| `half_closed_remote` | RAISE | ILL-C | → `half_closed_remote` | → `closed` | ILL-S | ILL-S | → `closed` | → `closed` | → `closed` |
| `closed` | RAISE | ILL-C | RAISE | RAISE | IGN | IGN | NOOP | IGN | NOOP |

### 4.3 Stream-level versus connection-level

A case not listed below is classified by one test: **if the peers can still agree about the state of
every *other* stream, the connection is kept** (WSM-STM-024).

| Level | Cases | Answer |
|---|---|---|
| stream | `data` after receiving `end`; a non-fragment frame mid-reassembly (WSM-FRG-033); an over-size frame the receiver declines (WSM-FRG-031); a payload over `max_payload_bytes` (WSM-FRG-032); an `open` beyond the receiver's concurrency limit (WSM-STM-036) | `reset` that stream: `PROTOCOL_ERROR`, except `PAYLOAD_TOO_LARGE` for the size cases and `REFUSED` for the concurrency case |
| connection | a message the codec cannot decode; a missing `type`; a wrong-parity stream id; an `open` whose id is not greater than that peer's highest previous open; any stream-level frame naming an id **above** the high-water mark | `goaway(PROTOCOL_ERROR)`, then close the socket |
| neither | a stream-level frame for an id that is not live but is **at or below** the high-water mark | ignore silently — no reset, no connection error, at most a counter |

---

## 5. The rules, in id order

### 5.1 `WSM-API-` — public API shape

- **WSM-API-001** `peer.open()` MUST be **synchronous** and MUST return a `Stream`.
- **WSM-API-002** `Stream` MUST be simultaneously awaitable (resolving with the remote's **first**
  payload) and async-iterable (yielding every reassembled payload until the remote ends).
- **WSM-API-003** `open()` MUST take zero mandatory arguments; `payload` defaults to `null` / `None`.
- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two connection-state
  cases: `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is
  between sockets. The only other synchronous failures are those of the peer's open interceptors
  (WSM-API-027). It MUST NOT queue, and `open()` MUST NOT fail for concurrency: that limit is the
  receiver's (WSM-STM-036) and surfaces asynchronously as `StreamRefused` on the pending await.
- **WSM-API-005** `notify()` MUST be async and MUST return nothing. It MUST NOT return a `Stream` or
  any awaitable handle.
- **WSM-API-006** `request()` MUST be `open(payload, end=true)` awaited **to the stream's end**, plus
  a check that raises if the remote sent more than one payload.
- **WSM-API-007** `await stream` MUST resolve on the **first** payload and MUST NOT check for a
  second one; only `request()` performs that check.
  Test: `stream_test.py::test_open_resolves_first_payload_while_request_raises`.
- **WSM-API-008** The async surface is closed, and it is exactly this: `connect()`, `accept()`,
  `serve()`; `peer.notify()`, `peer.request()`, `peer.ping()`, `peer.close()`; every `Stream` method
  that sends or waits — `send()`, `send_headers()`/`sendHeaders()` (WSM-API-024), `end()`,
  `reply()`, `reset()`, `cancel()` (WSM-ERR-012) and
  `result()` (WSM-API-012); and the four members of the socket adapter protocol (WSM-API-021), which
  form the transport interface and are not called by applications. **Everything else MUST NOT be
  async.** This includes `peer.open()` (WSM-API-001), whose synchronous form makes allocation and
  enqueue one indivisible step (WSM-INV-005/WSM-SID-006), and the four hook registrations
  `on_stream`/`on_frame`/`on_close`/`on_reconnect`: a registration that suspends lets a stream the
  acceptor pushes arrive before the handler is registered (WSM-STM-033). The three interceptor
  registrations of WSM-API-026 are synchronous as well.
  Test: `peer_test.py::test_the_public_api_is_async_exactly_where_the_rule_says`, which compares the
  partition by set equality in **both** directions, so the "everything else" clause is asserted as
  well as the named members.
- **WSM-API-009** `peer.id` MUST be a short random prefix minted **once per process** plus a
  monotonic **per-connection** counter, rendered `<prefix>-<counter>` (e.g. `a3f-17`). An id MUST NOT
  be reused or duplicated within a process; across processes only the prefix may coincide. Two
  connections with the same id appear as one connection in a log, so a scheme that recycles the ids of
  closed connections MUST NOT be used.
- **WSM-API-010** `Stream`'s await MUST delegate to a **memoized** future — one per stream, created
  lazily on the first await, resolved with the stream's first payload or rejected with the stream's
  `StreamReset`.
- **WSM-API-011** Awaiting an already-awaited stream MUST NOT be an error and MUST return the same
  value again. Test: `stream_test.py::test_await_twice_returns_same_value`.
- **WSM-API-012** `result(timeout=...)` MUST be that same future with a deadline wrapped around the
  wait, never a second source of the value.
- **WSM-API-013** Iteration MUST read payloads off the stream's own queue and MUST NOT touch that
  future. The consumption claim MUST be recorded on the stream, not on the future.
- **WSM-API-014** The first of the two shapes to be used claims the stream; the other MUST raise
  `StreamAlreadyConsumed`, with an error naming both uses. Two iterations over one stream is the same
  error. Test: `stream_test.py::test_await_then_iterate_raises_and_first_consumer_got_everything`.
- **WSM-API-015** In TypeScript `Stream<T>` MUST implement `PromiseLike<T>` and MUST NOT subclass
  `Promise` (species semantics would make every derived promise a `Stream`).
  Consequence: `await` unwraps thenables recursively, so a `Stream` cannot be the resolution value of
  a promise — awaiting a `Promise<Stream>` yields the stream's first payload instead. Wrap it in an
  object (`Promise<{ stream: Stream }>`).
- **WSM-API-016** In TypeScript the implementation MUST attach a default no-op rejection handler to
  the internal promise **at construction time**, not lazily on first `then`, and MUST surface the
  failure through the peer's frame/error hook instead.
- **WSM-API-017** In Python, `peer.open(...)` on a line by itself MUST emit no `RuntimeWarning`.
  Test: `stream_test.py::test_unconsumed_open_emits_no_runtime_warning`.
- **WSM-API-018** `open()` MUST NOT take a `timeout` argument in any language. `open()` returns
  immediately, so there is nothing for a deadline on it to bound; deadlines live on the awaits,
  `stream.result(timeout=)` and `peer.request(timeout=)`.
- **WSM-API-020** `open` and `request` MUST accept `(payload?)`, `(payload?, options?)` **or**
  `(options)`. `open`'s options object MUST NOT carry a timeout; `request`'s MUST (WSM-API-018).
- **WSM-API-021** The socket adapter protocol (`send_text`/`sendText`, `send_bytes`/`sendBytes`,
  `receive`, `close`, plus the handshake hook) MUST be the **only** place transport-specific code
  lives. Text and binary sends MUST be separate methods, never one polymorphic `send`.
- **WSM-API-022** The Node acceptor MUST live behind the `muxws/node` subpath export so the browser
  entry point never pulls in `ws`. The peer implementation MUST be shared; only the socket adapter
  differs.
- **WSM-API-023** `stream.closed` MUST be an `asyncio.Event` in Python and a **`Promise<void>`** in
  TypeScript, settling when the stream closes, including on socket death. That promise MUST resolve
  and MUST NOT reject — a stream that closed by being reset still closed, and the reset reaches the
  awaits and the iterator instead — so WSM-API-016's precaution does not apply to it.
- **WSM-API-024** Sending the leading headers of WSM-FRM-016 MUST be possible **without a payload**:
  `stream.send_headers()` / `stream.sendHeaders()`, which puts a payload-less `data` frame on the
  wire. Sending them on the same frame as the first payload MUST also be possible, as a `headers` argument to
  `send()`, `end()` and `reply()`. Both MUST raise `ProtocolError` once this side has sent any frame
  on the stream: the wire allows headers once per peer per stream, and silently dropping a second set
  would leave the sender unaware that it was not sent. On a stream this peer opened, the `open` **is**
  that first frame, so `send_headers()` there MUST raise, with a message that names `open(headers=)`
  as the place for them.
  Test: `stream_test.py::test_leading_headers_are_sendable_once_and_only_first`;
  `ts/stream.spec.ts` *"sends leading headers once, and refuses a second set"*.
- **WSM-API-025** The two sets of leading headers MUST be two attributes, and each MUST read the
  same from either end of the stream. `stream.headers` holds the `open`'s headers, on both peers;
  `stream.reply_headers` / `stream.replyHeaders` holds the answering side's — what the opener
  received, and what the answering peer itself sent. Both MUST be an empty mapping when there are
  none, never `None`/`undefined`.
  `stream.reply_headers_arrived` / `replyHeadersArrived` MUST be an `asyncio.Event` in Python and a
  `Promise<void>` in TypeScript, settling at the instant `reply_headers` can no longer change: the
  answering side's first frame on the stream, or the close of a stream that was never answered. It
  MUST settle on **every** path, including a stream reset before any answer and one whose socket
  dies; otherwise an await on headers that never arrive hangs indefinitely (WSM-INV-011), and the
  remote can cause that by not answering.
  Test: `stream_test.py::test_reply_headers_arrived_settles_on_every_path`;
  `ts/stream.spec.ts` *"settles replyHeadersArrived on every path"*.
- **WSM-API-026** A peer MUST offer three header interceptor registrations, all synchronous:
  `peer.intercept_open(interceptor)` / `peer.interceptOpen(interceptor)` for the headers of every
  stream this peer opens (WSM-API-027), `peer.intercept_reply_headers(interceptor)` /
  `peer.interceptReplyHeaders(interceptor)` for the leading headers this peer answers with
  (WSM-API-028), and `peer.intercept_incoming_headers(interceptor)` /
  `peer.interceptIncomingHeaders(interceptor)` for the leading headers the remote sends (WSM-API-029).
  Each call MUST return an unregister function taking no arguments, which removes that one
  registration; calling it again MUST do nothing. Registering one function twice is two
  registrations. Interceptors MUST run in registration order, and a registration or unregistration
  made while interceptors are running takes effect from the next call. Interceptors belong to the
  `Peer` object, not to its connection: a dialer that re-establishes its connection keeps every
  registration (WSM-RCN-032 discards streams, not registrations). `connect()`, `accept()` and
  `serve()` MUST take the three as sequences — `open_interceptors`, `reply_headers_interceptors`,
  `incoming_headers_interceptors` / `openInterceptors`, `replyHeadersInterceptors`,
  `incomingHeadersInterceptors` — and register them in order before the peer sends or reads its first
  frame: `serve()` returns no peer to register on, and a `connect()` caller registering on the
  returned peer is too late for a stream the acceptor pushes in answer to the hello (WSM-STM-033). An
  interceptor is application code; muxws itself still reads no header (WSM-AUT-002).
  Test: `reconnect_test.py::test_header_interceptors_persist_across_a_reconnect_and_do_not_run_on_the_hello`;
  `reconnect_test.py::test_connect_and_accept_register_their_interceptors_before_any_frame`;
  `peer_test.py::test_serve_registers_the_interceptors_it_is_given`;
  `ts/reconnect.spec.ts` *"keeps both registrations on the Peer and leaves the hello alone"* and
  *"registers connect()'s interceptor arrays before the first hello goes out"*;
  `ts/node.spec.ts` *"runs them on the peer each builds, in order, before its first frame"*.
- **WSM-API-027** `open()`, and through it `notify()` and `request()`, MUST call every open
  interceptor synchronously, after the checks of WSM-API-004 and before a stream id is allocated, as
  `(headers, payload)`. `headers` is a fresh mutable mapping holding a shallow copy of the caller's
  `headers`, empty when the caller passed none; `payload` is the opening payload. An interceptor adds,
  replaces or deletes keys by mutating that mapping, and the mapping as the last interceptor left it
  is what the `open` frame carries and what `stream.headers` holds. The caller's own mapping MUST NOT
  be modified, and a change made to the interceptors' mapping after `open()` returned MUST NOT reach
  the frame. When the caller passed no headers and the interceptors left the mapping empty, the
  `open` frame MUST carry no `headers` field, so interceptors that add nothing leave the wire
  byte-identical. The reconnect helper's hello is not opened through `open()` and MUST NOT pass
  through open interceptors, since it is replayed byte-identical (WSM-RCN-027).
  An open interceptor that raises MUST fail the `open()` call synchronously with that exception,
  unchanged: no later interceptor runs, no stream id is allocated, nothing is enqueued, and the peer
  stays usable. `notify()` and `request()` raise it from their await. An interceptor that returns an
  awaitable (a coroutine or other awaitable in Python, a thenable in TypeScript) MUST fail the call
  the same way with a `TypeError`: interceptors are synchronous and nothing awaits them. Python closes
  the coroutine first, so no `RuntimeWarning` follows. Any other return value is ignored. After the
  last interceptor every key of the mapping MUST be a string — the envelope's `headers` has string
  keys (§2.2) — and a key that is not (a non-`str` key in Python, a symbol key in TypeScript) MUST
  fail the call the same way with a `TypeError`, rather than reach a codec that would drop or coerce
  it.
  Test: `peer_test.py::test_open_interceptors_shape_the_headers_of_every_open`;
  `peer_test.py::test_an_open_interceptor_that_raises_fails_the_open_allocates_no_id_and_sends_nothing`;
  `ts/peer.spec.ts` *"shapes the headers of every open in registration order, on a copy"* and
  *"fails the open with the interceptor error and spends nothing"*.
- **WSM-API-028** Reply headers interceptors MUST be called once per stream the remote opened,
  synchronously, by the call that sends this peer's first frame on that stream (WSM-FRM-016):
  `send_headers()` / `sendHeaders()`, `send()`, `end()`, `reply()`, or the implicit end of
  WSM-STM-035. The call is `(headers, stream)`, with `headers` a fresh mutable mapping holding a
  shallow copy of the headers that call was given, empty when it was given none. The mapping as the
  last interceptor left it is what the frame carries and what `stream.reply_headers` /
  `stream.replyHeaders` holds, under the copy, empty-mapping and failure rules of WSM-API-027, with
  one addition: a failed call leaves the first frame unspent, so a later call on the stream may still
  carry headers. A failure during the implicit end of WSM-STM-035 is the handler's, and resets the
  stream as WSM-STM-034 requires. A frame on a stream this peer opened calls no reply headers
  interceptor, and neither does a `reset`, which carries no headers.
  Test: `peer_test.py::test_reply_headers_interceptors_shape_the_first_frame_of_every_answer`;
  `peer_test.py::test_a_reply_headers_interceptor_that_raises_fails_the_call_and_sends_nothing`;
  `ts/peer.spec.ts` *"shapes the first frame of every answer, on a copy, whichever call sends it"* and
  *"fails the call that sends the answer and leaves its first frame unspent"*.
- **WSM-API-029** Incoming interceptors MUST be called exactly once per stream, synchronously inside
  the read loop, when the remote's first stream-level frame on that stream is accepted. For a stream
  the remote opened that frame is the `open` (the first fragment of a fragmented one), and the call
  comes after the checks of WSM-SID-005 and WSM-STM-036 and before the handler is invoked. For a
  stream this peer opened it is the remote's first `data`, and the call comes after the check of
  WSM-FRM-016 and before that frame's payload is delivered. The call is `(headers, stream)`:
  `headers` is a read-only view of the headers that frame carried (`stream.headers` for the first
  case, `stream.reply_headers` / `stream.replyHeaders` for the second, empty when the frame carried
  none) — a `MappingProxyType` in Python, a frozen copy in TypeScript — and `stream.local` tells the
  two cases apart. A `reset` is not an answer and calls no interceptor (WSM-FRM-016); neither does a
  stream that closes unanswered, nor an `open` refused under WSM-STM-036, which creates no stream.
  Return values are ignored. An incoming interceptor that raises MUST be logged and MUST change
  nothing else: the remaining interceptors still run, the frame is processed as if no interceptor
  were registered, the stream is not reset, and the connection survives. One that returns an
  awaitable MUST be logged the same way, and the awaitable is not awaited: Python closes a returned
  coroutine, TypeScript attaches a rejection handler to a returned thenable.
  Test: `peer_test.py::test_incoming_interceptors_see_each_streams_leading_headers_once`;
  `peer_test.py::test_an_incoming_interceptor_that_raises_is_logged_and_changes_nothing`;
  `ts/peer.spec.ts` *"hands each stream's leading headers to incoming interceptors once, frozen"* and
  *"logs an incoming interceptor that throws and changes nothing else"*.

**Call shapes.** Unary is `open(end)` → `data(end)`; a streaming response is `open(end)` → `data`…
`data(end)`, optionally led by a payload-less `data(headers)` announcing what is coming
(WSM-FRM-016); bidirectional is `open` → interleaved `data` both ways → `data(end)` both ways; a
one-shot push is `open(end)` with nothing awaited. Durations are **seconds as floats in Python** and
**milliseconds in TypeScript**; wire field names stay snake_case in every language.

### 5.2 `WSM-AUT-` — authentication, headers, routing

- **WSM-AUT-001** Two obligations with different subjects; only the second binds the library.
  **On the deploying application:** authentication MUST happen at the WebSocket upgrade, before
  `accept()` is called. muxws cannot enforce this, and no test of muxws can observe it. muxws
  provides no other place for authentication, and that is tested: `accept()` takes no credential
  argument, `connect()` passes `headers=` to the upgrade and to nowhere else, and `Peer` has no
  post-upgrade authentication hook.
  **On the library:** muxws MUST NOT interpret credentials anywhere. Tested by
  `peer.spec.ts` *"never interprets per-stream headers"* and
  `peer_test.py::test_per_stream_headers_arrive_unchanged_and_change_nothing` (WSM-AUT-002), and by
  the upgrade tests in `transports/websockets_test.py` and `ts/node.spec.ts`.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application and
  MUST NOT be used for re-authentication by the library. This binds both directions: the answering
  side's leading headers (WSM-FRM-016) are delivered as sent and change no outcome either. A library
  that read the answer's metadata would in effect define a status code, which WSM-FRM-006 forbids
  inside `payload` and this rule forbids in `headers`.
  Header interceptors (WSM-API-026) are application code running inside the peer; they do not make
  muxws a reader of headers.
- **WSM-AUT-003** **On the deploying application** (a SHOULD): a connection whose credential expires
  mid-life SHOULD be closed with `goaway`. muxws does not know what a credential is or when one
  expires (WSM-AUT-001), so it cannot do this and cannot be tested for it.
  **On the library** (a testable statement about the reconnect helper): re-authentication happens by
  re-dialling. The dial callable that `connect()` passes to the reconnect loop closes over the
  `headers` it was given, so every attempt presents the same credential at a fresh upgrade, and there
  is no in-band re-authentication path.
  Test: `reconnect_test.py::test_every_reconnect_presents_the_same_credential_at_a_fresh_upgrade`,
  which reads the `Authorization` header from the HTTP **upgrade** request on each of three dials;
  upgrade headers are not visible in frames.
- **WSM-AUT-004** muxws MUST NOT look at the opening payload for routing: no path matching, no method
  dispatch, no handler table (WSM-STM-030).

### 5.3 `WSM-BPR-` — backpressure

- **WSM-BPR-001** v1 MUST NOT implement per-stream flow control. `window_update` is reserved as a
  frame type and MUST NOT be sent.
- **WSM-BPR-002** The flow-control mechanisms in v1 are exactly the receiver's local concurrency
  limit (WSM-STM-036 — how many producers can exist at once) and `MAX_FRAME_BYTES` (WSM-FRG-004 — how
  long any one may hold the wire). Both are local defences, not agreements; there is no negotiated
  mechanism of any kind, and building one requires a new generation (WSM-CON-009).

### 5.4 `WSM-CDC-` — codecs

- **WSM-CDC-001** A codec MUST expose `name: str` (the wire-visible name), `binary: bool`,
  `encode(frame) -> str | bytes` and `decode(message) -> Frame`.
- **WSM-CDC-002** `binary` MUST be declared, not inferred from a value's type; the peer uses it to
  select the socket's text or binary send method and the expected inbound message type. A peer MUST
  NOT sniff incoming messages to decide which codec branch to take.
- **WSM-CDC-003** A peer MUST use exactly one codec for the life of its connection. There is no
  per-frame, per-stream or per-connection codec switching.
- **WSM-CDC-004** The library MUST ship and MUST itself register a `json` codec, and JSON MUST be the
  default. JSON is the interoperability baseline: every port MUST produce a JSON wire form that the
  others decode to an equal logical frame. Test: `conformance/frames/*.json`.
- **WSM-CDC-005** Conformance for the JSON codec MUST be asserted as `decode(json_wire) == frame` and
  `decode(encode(frame)) == frame`, comparing **parsed objects**, never byte-identical output.
  Exactly one separate test MAY pin a canonical key order (`type`, then `stream`, then the remaining
  keys alphabetically) for the benefit of log diffing; no other test may depend on key order or
  whitespace.
- **WSM-CDC-006** Any codec other than `json` MUST be asserted by round-trip over the same logical
  frame corpus (`decode(encode(frame)) == frame`) and MUST NOT have wire bytes pinned in a fixture —
  two msgpack libraries make different but equally valid choices about int width and map format, and
  pinned bytes would make a legal encoder fail.
- **WSM-CDC-007** Every codec that ships MUST additionally have a live cross-language pair in CI (one
  peer per language, both configured with that codec, running the sequence corpus). A codec without
  that pair MUST NOT ship.
- **WSM-CDC-008** Under a binary codec, raw bytes are a first-class payload type. Under JSON they are
  not, and muxws MUST NOT base64-encode bytes on the application's behalf.
- **WSM-CDC-010** The codec name MUST be read from deployment configuration, not from a call
  argument: Python `os.environ["MUXWS_CODEC"]` via a `muxws.conf.settings` singleton (default
  `"json"`), TypeScript `import.meta.env.VITE_MUXWS_CODEC` (default `"json"`).
- **WSM-CDC-011** `settings.codec` MUST be writable at runtime so an application may set it during
  bootstrap before connecting.
- **WSM-CDC-012** `connect()`, `accept()` and the peer constructor MUST accept a `codec=` override,
  documented as a test override and as the means for a process holding two connections that need
  different codecs to configure each. No example outside the test suite may use it.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` / `registerCodec(name,
  codec)`. There MUST NOT be dynamic imports, lazy auto-registration, entry-point scanning, or any
  probing of whether a module happens to be installed.
- **WSM-CDC-014** A codec module MUST NOT register itself at import time — a bundler cannot remove an
  import that has side effects.
- **WSM-CDC-015** The npm package MUST declare `"sideEffects": false`, at minimum for the codec
  subpaths.
- **WSM-CDC-016** A configured codec name that is not registered MUST raise `CodecNotRegistered` on
  the first connection attempt, **before any socket is opened**. The message MUST name the
  environment variable, the value found, and the registered set. The peer MUST NOT fall back to JSON,
  ever. Test: `codec_test.py::test_unregistered_name_raises_before_socket` (asserts no socket was
  opened).
- **WSM-CDC-020** The dialer MUST offer `muxws.v1.<codec>` as its **first** WebSocket subprotocol
  entry, where `<codec>` is its configured codec name.
- **WSM-CDC-021** The application MAY append further subprotocol entries (a bearer token is the
  common case). The acceptor MUST match only the entry carrying the `muxws.v1.` prefix and MUST
  ignore every other offered value entirely, leaving them for the application's authentication.
- **WSM-CDC-022** The acceptor MUST accept the connection only if the offered `muxws.v1.<codec>` name
  equals its own configured codec name, and MUST select exactly that value as the negotiated
  subprotocol. Otherwise it MUST refuse the upgrade: select **no** subprotocol and answer HTTP
  **400**. It MUST NOT complete the handshake and close afterwards where the transport gives it the
  choice (WSM-CDC-028 is the exception, and only for transports that give it none).
- **WSM-CDC-023** This is an assertion, not a negotiation. There MUST NOT be a fallback encoding, a
  list of acceptable alternatives, per-connection multi-codec support, or any runtime codec branching
  in the peer.
- **WSM-CDC-024** A dialer whose handshake is refused MUST surface `CodecMismatch`, **composed by the
  dialer itself from the codec name it offered** — a browser cannot read the rejection body, so the
  diagnostic cannot come from the server. The message MUST name the offered codec and **both**
  environment variables, so the reader knows where to look on each side. It MUST NOT surface a bare
  connection failure. Test: `codec_test.py::test_the_dialer_composes_its_own_mismatch_error`, and
  `transports/websockets_test.py::test_mismatched_codecs_reject_handshake` /
  `ts/node.spec.ts` over a real socket — which assert the class, the message, and that the
  `muxws.frames` logger recorded nothing in either direction.
- **WSM-CDC-025** A peer offering a different generation (`muxws.v2.<codec>`) MUST be rejected by a v1
  acceptor at the handshake.
- **WSM-CDC-026** `accept()` MUST perform the WebSocket accept itself — it is the only party that
  knows which subprotocol to select. An application MUST NOT accept the socket before calling it.
- **WSM-CDC-027** For transports that complete the handshake before invoking the handler, the library
  MUST expose a plain callable installable in that transport's handshake hook, implementing
  WSM-CDC-021/022. **Where one hook cannot both select and refuse, the library MUST expose one for
  each**, and both MUST be named in the transport's documentation as required rather than optional.
  Python's `websockets` needs a single `select_subprotocol`, which refuses by raising. Node's `ws`
  needs two: `handleProtocols` selects and cannot refuse — whatever it returns, `ws` answers 101 — so
  `refuseMismatchedUpgrade` wraps `shouldHandle`, which is the hook that can abort an upgrade with a
  status.
- **WSM-CDC-028** Where a transport offers neither hook, the peer MUST verify the negotiated
  subprotocol on the already-open socket and close it with the WebSocket policy-violation close code.
- **WSM-CDC-029** The acceptor MUST log the same failure at refusal time, naming the offered codec,
  its own configured codec and both environment variables: the dialer's message does not contain the
  acceptor's configured codec, and the dialer cannot read the acceptor's log.

### 5.5 `WSM-CON-` — connection lifecycle

- **WSM-CON-009** The version component of the subprotocol name (`muxws.v1.`) is the **only** version
  on the wire and pins the breaking-change generation. Additive revisions — new frame types, new
  fields — MUST NOT be announced anywhere, because WSM-FRM-001/002 already make them safe to receive.
  A change that requires the remote to *act* on a new frame type rather than tolerate it MUST bump the
  generation, which a v1 acceptor rejects at the handshake (WSM-CDC-025).
- **WSM-CON-010** `ping` MUST carry a `nonce`; the receiver MUST echo it verbatim in a `pong`,
  promptly, without application involvement.
- **WSM-CON-011** Native WebSocket ping/pong control frames MUST NOT be used for liveness — browsers
  do not expose them to JavaScript.
- **WSM-CON-012** The application MAY call `peer.ping()` to measure round-trip time; it returns
  seconds (Python) / milliseconds (TypeScript).
- **WSM-CON-020** `goaway` carries `code`, `reason` and `last_stream` — the highest stream id from
  the *other* peer that this peer has processed and will still complete.
- **WSM-CON-021** After **sending** `goaway`, a peer MUST refuse new incoming opens with
  `reset(REFUSED)` and MUST open no new streams itself.
- **WSM-CON-022** After **receiving** `goaway`, `peer.open()` MUST raise `ConnectionGoingAway`
  synchronously at the call site.
- **WSM-CON-023** After receiving `goaway`, the receiver's own streams with an id greater than
  `last_stream` MUST be reset locally with `REFUSED` — they were never processed and are safe to
  retry on a new connection.
- **WSM-CON-024** Streams at or below `last_stream` MUST be allowed to finish until the drain timeout
  (default 10 s) elapses; then the socket MUST be closed. Drain is a deadline, not a poll loop.
- **WSM-CON-025** `peer.close()` MUST send `goaway(NO_ERROR)`, drain, then close.
- **WSM-CON-030** A connection is **established** when the socket is open and, for a codec-bearing
  subprotocol, that subprotocol was accepted. There MUST NOT be a post-socket handshake phase, a
  capability exchange, or any frame either peer is required to send before any other. A peer MAY open
  a stream on its first frame.
- **WSM-CON-031** There MUST NOT be a `settings` frame. Every limit is either a protocol constant
  (`MAX_FRAME_BYTES`, WSM-FRG-004) or a local receiver-side defence (`max_payload_bytes`,
  WSM-FRG-035; the concurrency limit, WSM-STM-036). A limit MUST NOT appear on the wire in any form.
  There MUST NOT be an `encoding` setting either: the codec is fixed at the handshake before any
  frame exists.

### 5.6 `WSM-ERR-` — errors, timeouts, cancellation

The hierarchy, mirrored class-for-class in every port, with a `name` / `__class__` discriminator so a
cross-language test can assert on error identity:

```text
MuxwsError
├── ProtocolError              # this peer or the remote violated the spec
├── ConnectionClosed           # socket died; carries .code, .reason, .was_clean
├── ConnectionGoingAway        # open() after goaway — raised synchronously out of open()
├── StreamAlreadyConsumed      # await and iterate, or two iterations, on one stream
├── StreamClosed               # send()/end()/reply() on a stream that closed normally
├── CodecError                 # configuration; carries .configured and .available
│   ├── CodecNotRegistered     # configured name never registered — raised at startup
│   └── CodecMismatch          # acceptor's codec differs; the handshake was rejected
├── OriginRefused              # accept() refused the handshake's Origin; carries .origin
├── TransportUrlError          # base: this transport cannot open that address (also a ValueError)
├── TransportUnsupportedError  # base: this runtime has no such transport (also a RuntimeError)
└── StreamReset                # carries .code (ResetCode), .reason, .stream_id
    ├── RemoteError            # code == APPLICATION_ERROR; carries .payload
    ├── StreamTimeout          # code == TIMEOUT
    ├── StreamRefused          # code == REFUSED; not processed — retry, elsewhere or later
    └── ConnectionLost         # code == CONNECTION_CLOSED; synthesised locally, never from the wire
```

The tree contains the two transport bases but not their subclasses. It lists the **shared** classes,
which every port defines under the same names. A concrete transport error is named after a
transport, or after a third-party package that only one port uses (`WebsocketsNotInstalledError` in
Python, `WsNotInstalledError` in TypeScript, for the same failure), and a port defines such a class
only where it ships that transport. WSM-ERR-016 specifies where those classes live and why they are
not part of this tree.

- **WSM-ERR-002** `ConnectionLost` MUST be a `StreamReset` subclass; `ConnectionClosed` MUST NOT be.
  `ConnectionLost` is *a stream* failing because the connection did, and is what every stream-shaped
  call raises. `ConnectionClosed` is *the connection* ending, and is what `serve()` and peer-level
  calls raise.
- **WSM-ERR-003** All muxws errors MUST be raised from the awaiting call site and MUST NOT be
  swallowed into a callback.
- **WSM-ERR-004** Every port MUST mirror this hierarchy with classes of the same names, delivered as
  rejections and as throws inside an async iteration, and MUST set a discriminator so cross-language
  tests can assert on error identity. *The hierarchy this rule quantifies over is the shared one in
  the block above, the two transport bases included.* A transport-specific subclass of either base is
  governed by WSM-ERR-016 instead, and MUST be mirrored only by a port that actually ships that
  transport; a TypeScript port does not define a class named after a Python distribution.
- **WSM-ERR-005** `CodecNotRegistered` and `CodecMismatch` MUST sit outside `StreamReset`: neither is
  a stream failure and neither is retryable.
- **WSM-ERR-006** A handler that raises MUST produce `reset(APPLICATION_ERROR)` with a `reason` and
  an optional structured `payload`. The default serializer produces
  `{"type": "<exception class>", "message": "<str(exc)>"}`. The peer MUST accept an
  `error_serializer` hook of signature `(exc) -> Any | None` — the value it returns becomes the
  reset's `payload`, and `None` means "send no payload". The documentation MUST state that a
  public-facing deployment should redact it. `type` is *not* portable: it names the remote's own
  exception class, so an interop assertion may compare `message` but not `type`.
- **WSM-ERR-007** muxws MUST NOT map exceptions to status codes of any kind.
- **WSM-ERR-008** `error_serializer` MUST be supplied **per peer** — an argument to `connect()` and
  to the acceptor's peer construction — and MUST NOT be a module-level default. One process may hold
  a browser-facing peer that redacts and an internal peer that does not, which a process-wide setting
  cannot express.
- **WSM-ERR-009** `send()`, `end()` and `reply()` on a stream that is no longer open MUST raise, and
  the type MUST distinguish the three cases: `StreamClosed` when the stream closed **normally**, that
  stream's own `StreamReset` subclass when it was reset, and `ConnectionLost` when the socket died.
  `StreamClosed` MUST NOT be a `StreamReset` subclass and MUST NOT be a `ProtocolError`: a normal
  close racing a last `send()` is an expected outcome, not a failure and not a caller bug.
  Test: `stream_test.py::test_send_after_normal_close_raises_stream_closed`.
- **WSM-ERR-010** `request()` MUST have **no default timeout**. A stream lives until it ends, is
  reset, or the connection dies.
- **WSM-ERR-011** When a timeout is given and expires, the peer MUST send `reset(TIMEOUT)` — so the
  remote stops working — and MUST raise `StreamTimeout` locally.
- **WSM-ERR-012** `stream.cancel()` MUST send `reset(CANCELLED)` and close the stream locally
  **immediately**, without waiting for acknowledgement. Payloads still in flight from the remote MUST
  be discarded.
- **WSM-ERR-013** On the remote side of a `reset(CANCELLED)` the handler task MUST be cancelled: in
  Python by `task.cancel()`; in TypeScript by aborting `stream.signal`, with any subsequent
  `await stream.send(...)` rejecting with `StreamReset`.
- **WSM-ERR-014** Local cancellation propagating out of an await on the result or out of an async
  iteration MUST cause the peer to send `reset(CANCELLED)` and re-raise. It MUST NOT be swallowed.
  Test: `stream_test.py::test_local_cancellation_sends_reset_and_reraises`. A port whose language
  cannot observe an abandoned await (JavaScript) implements this where cancellation is observable — a
  `for await` that breaks, returns or throws, and an abort signal passed to `result()` — and MUST NOT
  claim to implement it for a dropped promise.
- **WSM-ERR-015** An incoming `reset(APPLICATION_ERROR)` on a stream this peer is consuming MUST
  raise `RemoteError` out of the pending await or the async iteration.
- **WSM-ERR-016** Every exception a transport raises out of `connect()` MUST be a `MuxwsError`, and
  the two shared bases carry the distinction the caller acts on. A transport that cannot open the
  address it was given MUST raise a subclass of `TransportUrlError`; a transport this runtime cannot
  provide **at all** — no `AF_UNIX`, an optional dependency that is not installed, an entry point a
  bundling rule (WSM-API-022) keeps the dependency out of — MUST raise a subclass of
  `TransportUnsupportedError`. `TransportUrlError` means *retype the address*;
  `TransportUnsupportedError` means *the address is fine, change where or how you are running*, and a
  port MUST NOT collapse the two. Both bases MUST live in the shared error module (`muxws/errors.py`,
  `ts/errors.ts`), MUST be exported from the package root, and MUST NOT acquire a dependency of their
  own — an application must be able to write `except TransportUrlError` without importing, and
  possibly without being *able* to import, the transport that raised.
  In Python `TransportUrlError` MUST also be a `ValueError` and `TransportUnsupportedError` a
  `RuntimeError`, so that code catching the builtin around a URL or a platform check catches them,
  and so does a single `except MuxwsError` handler around every muxws call. A port with a single
  inheritance chain MUST choose `MuxwsError` and carry the identity in its discriminator
  (WSM-ERR-004): `instanceof MuxwsError` is the one handler every muxws error must reach. Emulating
  multiple inheritance with `Symbol.hasInstance` MUST NOT be used: it makes `instanceof` return true
  for a prototype chain that does not contain the class, which neither debuggers nor the type system
  reflect.
  Concrete, transport-specific errors MUST subclass one of the two bases, MUST be defined in their
  own transport's module, and MUST NOT be exported from the package root; they are reached as
  `from muxws.transports.<transport> import <Error>` and `import { <Error> } from 'muxws/node'`. The
  adapter interface is public (WSM-API-021) and third parties write adapters; a third party cannot
  add a class to `muxws/errors.py`, so a third-party transport could not meet a root-export
  requirement. Where a port's packaging gives a transport no module path of its own, its concrete
  errors MUST be exported from the entry point that ships that transport and MUST NOT be reachable
  from any other. A transport module MUST stay importable when the dependency it adapts is absent;
  otherwise `except <Transport>NotInstalledError` itself raises the `ImportError` the class replaces.
  **An error lives in the module whose contract requires it, not in the module that raises it.** The
  `SocketAdapter` protocol requires every adapter to raise `ConnectionClosed` (WSM-API-021), so
  `ConnectionClosed` is in the shared module. An error the adapter contract does not mention — a URL
  grammar, a missing package, a missing kernel feature — belongs to the transport that raises it, and
  MUST NOT be moved to the shared module because a second transport needs a similar error.
  A missing optional dependency MUST NOT reach the caller as a bare `ImportError` or
  `ERR_MODULE_NOT_FOUND`, and the message MUST name the install that fixes it —
  `pip install muxws[websockets]`, `npm install ws`. The failure is permanent and local and has one
  remedy, which the message states. A translated failure MUST chain the original — `raise ... from
  exc` in Python, `{ cause }` in TypeScript — and MUST include the underlying library's wording rather
  than replace it. An import failure that is *not* the dependency being absent — a broken install, a
  syntax error inside it — MUST be re-raised untouched, so a corrupt package is never reported as an
  uninstalled one.
  An address the transport cannot parse MUST be detected **before the dial is attempted** and MUST
  NOT be translated inside the failed-dial handler: that handler classifies a refused handshake
  (WSM-CDC-024), and a refused handshake and an unparseable address are different failures. A *dial* failure is not an
  address failure: an unreachable host, a refused connection or a TLS error MUST keep propagating as
  whatever the platform raised.
  A transport MUST NOT define a class for a failure it cannot produce. Each port MUST mirror the two
  bases (WSM-ERR-004) and MUST define a concrete subclass only for a transport it ships and a failure
  that transport can reach; an entry point that refuses a scheme it cannot carry MAY raise a base
  directly rather than define a class for a transport it does not have.
  Test: `errors_test.py::test_every_muxws_error_defined_outside_errors_py_derives_from_a_transport_base`,
  `::test_the_transport_bases_are_root_exported_and_their_subclasses_are_not`,
  `::test_the_two_transport_bases_carry_their_builtin_as_well_as_muxwserror`;
  `transports/websockets_test.py::test_a_url_websockets_cannot_parse_arrives_as_a_transport_url_error`,
  `::test_the_librarys_own_diagnostic_survives_the_translation`,
  `::test_a_url_containing_the_refusal_status_is_not_a_codec_mismatch`,
  `::test_a_missing_websockets_package_names_the_extra_that_installs_it`,
  `::test_a_websockets_that_is_installed_but_broken_keeps_its_own_import_error`,
  `::test_this_transport_module_still_imports_with_its_dependency_blocked`,
  `::test_an_unreachable_acceptor_is_not_reported_as_a_codec_mismatch`;
  `transports/unix_test.py::test_both_refusals_are_catchable_as_muxws_errors_and_as_their_builtin_bases`,
  `::test_a_malformed_unix_url_is_a_transport_url_error_and_a_missing_af_unix_is_not`;
  `ts/errors.spec.ts` *"places the two transport bases under MuxwsError and outside StreamReset"*,
  *"defines no concrete transport error, because each one belongs to its transport"*,
  *"chains the original on a transport base, and invents no chain when there was none"*;
  `ts/node.spec.ts` *"arrives as a WsUrlError wrapping the message of `ws`, not as ws's own SyntaxError"*;
  `ts/unix.spec.ts` *"refuses a request target that is not an absolute path, by name"*,
  *"names muxws/node as the entry point that can dial a socket file"*;
  `ts/transport-errors.spec.ts` *"names the install rather than the module the resolver could not
  find"*, *"rethrows a broken install untouched, so it is never reported as an absent one"*;
  `ts/packaging.spec.ts` *"lets each entry point export only the concrete classes its own transports
  own"*, *"leaves node:net a real import, so the unix dial has a socket to open"*.
  **The platform-`WebSocket` entry point does not satisfy the first sentence; `GAPS.md` lists the
  deviation.**

### 5.7 `WSM-FRG-` — sizes and fragmentation

- **WSM-FRG-001** Every size limit MUST be measured as the byte length of the **fully encoded
  WebSocket message** — the complete codec output for the frame, envelope included — and not as the
  size of the pre-encoding payload or of the `fragment` field alone.
- **WSM-FRG-002** Under a text codec, TypeScript MUST measure with
  `new TextEncoder().encode(text).length` (or an equivalent incremental byte count) and Python with
  `len(text.encode("utf-8"))`. A JavaScript string's `.length` MUST NOT be used: it counts UTF-16
  code units and disagrees with Python on every non-BMP character.
- **WSM-FRG-003** Under a binary codec both ports MUST take the length of the produced buffer.
  Test: `fragment_test.py::test_slice_point_sweep_never_exceeds_cap`, asserted in bytes of that
  codec's output.
- **WSM-FRG-004** `MAX_FRAME_BYTES` is a **protocol constant of 65536**: the largest encoded message
  a sender may emit. It MUST NOT be negotiated, announced, or read from configuration. A receiver
  MUST accept any message up to the constant and MAY accept larger ones; a sender MUST always
  fragment at the constant regardless of what the remote appears willing to accept.
- **WSM-FRG-005** An implementation MAY expose the cap as a construction argument **for tests only**
  (the conformance runner uses it, WSM-TST-002). It MUST NOT be documented as deployment
  configuration and MUST NOT appear on the wire.
- **WSM-FRG-010** A sender MUST fragment any logical payload whose encoded frame would exceed
  `MAX_FRAME_BYTES`.
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
  message still exceeds the cap, re-split that slice and try again. The reservation is an estimate;
  the verify-and-re-split loop is what guarantees the cap.
  Test: `fragment_test.py::test_control_character_payload_is_resplit_not_emitted_over_cap`.
- **WSM-FRG-015** The splitter MUST be a pure function — same arguments, same result, no argument
  mutated — and MUST be tested as one, independently of any socket.
- **WSM-FRG-016** Both ports MUST produce the **same fragment boundaries** for the same payload, cap
  and codec. Test: `conformance/frames/v1-fragment-boundaries.json`. See §3 for the one place this
  cannot hold in general (floats and integers beyond ±(2^53−1)), which is why no shared fixture
  carries one.
- **WSM-FRG-017** Fragments of one logical payload MUST be contiguous **on that stream**. Frames of
  other streams MAY and SHOULD interleave between them.
- **WSM-FRG-018** A stream MUST have at most **one unsent fragment queued** at any moment: fragment
  *n+1* is encoded only after fragment *n* has been handed to the socket.
- **WSM-FRG-019** The writer MUST select the next frame by **round-robin over the streams that have
  queued work**. A FIFO send queue MUST NOT be used: it fixes the order at enqueue time and cannot
  interleave streams.
  Test: `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json`, and
  `writer_test.py::test_a_small_frame_overtakes_a_fragmented_payload` end to end through a real peer
  pair, which also verifies that the peer's send path goes through the writer.
- **WSM-FRG-020** `end: true` MUST appear only on the final fragment of a payload. A fragment
  sequence MUST end with a fragment carrying `more: false`, even when that fragment carries no bytes.
- **WSM-FRG-021** `headers` MUST NOT be fragmented. A frame whose headers alone push it over the cap
  MUST be rejected by the receiver with `reset(PAYLOAD_TOO_LARGE)`. This applies to every frame that
  may carry headers (WSM-FRM-016), not to the `open` alone: an answering peer's leading `data`
  carries the same field and is subject to the same rule.
- **WSM-FRG-030** The receiver MUST concatenate `fragment` values and hand the result to the codec
  for decoding when a fragment arrives without `more: true`.
- **WSM-FRG-031** A receiver that enforces a frame-size limit MUST measure the entire encoded
  message, never the `fragment` field on its own, and MUST reset that stream with
  `PAYLOAD_TOO_LARGE` when it is over. Enforcement above `MAX_FRAME_BYTES` is optional; a receiver
  MUST NOT reject a message at or below the constant.
- **WSM-FRG-032** A payload whose accumulated fragments exceed `max_payload_bytes` MUST be rejected
  with `reset(PAYLOAD_TOO_LARGE)` **as soon as the limit is crossed**, before further fragments are
  accepted and without waiting for reassembly to complete. The limit bounds receiver memory; a
  receiver that measures after reassembly has already allocated the memory the limit bounds.
  Test: `caps_test.py::test_oversize_payload_resets_on_the_crossing_fragment` (the reset goes out
  before the final fragment arrives).
- **WSM-FRG-033** Receiving a non-fragment frame on a stream with a fragment assembly in progress is
  a **stream-level** protocol error.
  Test: `conformance/invalid/fragment-interrupted-by-non-fragment.json`.
- **WSM-FRG-034** A test-override cap too small to hold the envelope plus one indivisible unit MUST
  raise a configuration error when the peer is constructed, not be discovered later as an infinite
  split loop.
- **WSM-FRG-035** `max_payload_bytes` — the largest reassembled payload this peer will accept — is a
  **local receiver setting**, default **67108864**. It MUST NOT be announced on the wire, and a
  sender MUST NOT be given any way to learn it other than the reset it produces.

### 5.8 `WSM-FRM-` — the envelope and the frame types

- **WSM-FRM-001** A receiver MUST ignore unknown envelope fields.
- **WSM-FRM-002** A receiver MUST ignore unknown *frame types*, logging once, and MUST NOT treat them
  as any kind of error. Tests: `peer_test.py::test_unknown_frame_type_is_ignored`,
  `::test_each_unknown_frame_type_is_logged_once`.
- **WSM-FRM-004** `payload` and `fragment` MUST NOT both appear on one frame.
- **WSM-FRM-005** A frame missing `type`, or a message the configured codec refuses to decode, is a
  **connection-level** protocol error (§4.3).
- **WSM-FRM-006** muxws MUST NOT define any message vocabulary inside `payload`: no `kind` field, no
  reserved key, no discriminator of any sort.
- **WSM-FRM-010** `open` opens a stream. Sender: either peer, using its own parity. Fields: `stream`
  (required), `headers`, `payload`/`fragment`+`more`, `end`. `end: true` on `open` is the unary
  request shape.
- **WSM-FRM-011** `data` carries a payload chunk on an existing stream. Fields: `stream`,
  `headers` (first one only, WSM-FRM-016), `payload`/`fragment`+`more`, `end`, `trailers`. A peer
  MUST NOT send `data` on a stream where its own side is already half-closed.
- **WSM-FRM-012** End of stream MUST be a flag, never its own frame type. A peer with nothing left to
  say sends `{"type": "data", "stream": N, "end": true}` with no payload. There MUST NOT be an `end`
  frame type.
- **WSM-FRM-013** Trailers MUST be carried on the frame carrying `end: true`. There MUST NOT be a trailers
  frame type.
- **WSM-FRM-014** `reset` terminates a stream immediately in both directions. Sender: either peer, in
  any state except `closed`. Fields: `stream`, `code`, `reason`, optional `payload` for a structured
  error object.
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`, or set it
  to `0`.
- **WSM-FRM-016** `headers` MAY appear on the **first stream-level frame a peer sends on a stream** and
  MUST NOT appear on any later one. For the peer that opened the stream that frame is the `open`;
  for the peer answering it, its first `data`. The two directions are independent: each peer may
  send headers once, and one peer's headers do not affect whether the other may send them. A
  receiver that sees `headers` on a later frame MUST reset that stream with `PROTOCOL_ERROR`; that is
  a stream-level error and MUST NOT take the connection down (§4.3).
  Two consequences. `reset` never carries headers, however early it arrives: a reset is not an
  answer, and the field table lists `headers` on `open` and `data` only. And "first frame" means the
  first frame *sent*, not the first *carrying a payload*: a peer with headers to send and no payload
  yet sends `{"type":"data","stream":N,"headers":{...}}`, a legal frame with no payload, and may send
  no further headers on that stream.
  Tests: `conformance/frames/v1-frames.json` (`data-with-headers`),
  `conformance/sequences/answering-side-announces-headers.json`,
  `conformance/invalid/headers-on-a-later-frame.json`.

### 5.9 `WSM-INV-` — cross-cutting invariants

Each invariant is followed, after the dash, by the failure it prevents.

- **WSM-INV-001** muxws MUST NOT depend on any package above it in the stack — not as an import, an
  optional extra, or a type-checking-only annotation — otherwise muxws cannot be installed without
  that package.
- **WSM-INV-002** There MUST be one symmetric `Peer` type per language — otherwise server push needs
  a second, parallel mechanism with its own correlation and cancellation.
- **WSM-INV-003** Every size limit MUST be counted in bytes of the **codec's own output**
  (WSM-FRG-001) — otherwise a sender that budgets against JSON text while msgpack bytes go on the
  wire produces over-cap frames under the msgpack codec.
- **WSM-INV-004** At most one unsent fragment per stream, and round-robin writer selection
  (WSM-FRG-018/019) — otherwise a 1 MB payload on one stream delays a 200-byte frame on another
  stream until the whole payload has been sent.
- **WSM-INV-005** Id allocation and `open` enqueue MUST be one indivisible synchronous step
  (WSM-SID-006) — otherwise two concurrent `open()` calls can put a non-monotonic id sequence on the
  wire, which the remote treats as a connection-level protocol error (WSM-SID-005).
- **WSM-INV-006** Late frames below the high-water mark MUST be ignored and frames above it MUST kill
  the connection (WSM-STM-002/003) — otherwise frames for ids that were never opened are ignored as
  well, and an id-space disagreement between the peers goes undetected.
- **WSM-INV-007** The concurrency limit MUST stay entirely receiver-side (WSM-STM-036) — otherwise a
  sender that counts its open streams and refuses locally enforces a limit whose value it cannot know.
- **WSM-INV-008** A handler that raises MUST always produce `APPLICATION_ERROR`, never `REFUSED`
  (WSM-STM-034) — otherwise a handler that debits an account and then raises tells the client that
  retrying the debit is safe.
- **WSM-INV-009** The awaited future MUST be memoized and the consumption claim MUST live on the
  stream (WSM-API-010/013) — otherwise a stream's payloads are split between an await and an
  iteration without an error to either consumer.
- **WSM-INV-010** Nothing MUST be queued while the peer is between sockets (WSM-RCN-042) — otherwise
  queued frames are delivered to a server that holds no state for the sender, and an error that would
  have reached the call site becomes a silent misdelivery.
- **WSM-INV-011** Every stream live at socket death MUST fail with `ConnectionLost`, never hang
  (WSM-RCN-041) — otherwise a caller gets no error, no log and no timeout, and its await never
  completes.
- **WSM-INV-012** The attempt counter MUST reset only on an *established* connection (WSM-RCN-004) —
  otherwise a server that accepts sockets while its backend is down turns exponential backoff into
  retries at a fixed interval equal to the initial delay.
- **WSM-INV-013** The hello MUST be replayed by the helper, not by the application (WSM-RCN-020) —
  otherwise an application that does not replay it holds a connected socket that the server cannot
  associate with any client, with no subscriptions and no error reported.
- **WSM-INV-014** `tags` MUST NOT survive a reconnect (WSM-RCN-033) — otherwise a tag such as a mute
  flag, set for one connection, applies to the next connection, which never set it.
- **WSM-INV-015** An unregistered codec name MUST be a loud startup failure, never a silent JSON
  fallback (WSM-CDC-016) — otherwise a deployment configured for msgpack runs JSON with no error,
  because both ends fall back.
- **WSM-INV-016** muxws MUST NOT define a message vocabulary (WSM-FRM-006) — otherwise two
  independent consumers sharing one socket must each nest their own vocabulary inside the muxws one,
  and every change to either requires a muxws release.
- **WSM-INV-017** `max_payload_bytes` MUST be enforced as fragments accumulate, not after reassembly
  (WSM-FRG-032) — otherwise the receiver has already allocated the memory the limit bounds by the
  time it refuses the payload.
- **WSM-INV-018** `connect()` MUST raise when the first attempt fails (WSM-RCN-006) — otherwise a typo
  in the URL, an unreachable host or a codec mismatch is never reported: the application holds a peer
  that reports itself alive and retries indefinitely against an endpoint that cannot answer.

### 5.10 `WSM-OBS-` — observability

- **WSM-OBS-001** Every frame MUST be loggable in one line at `DEBUG` under the `muxws.frames`
  logger:

```text
muxws conn=a3f-17 dir=tx type=open   stream=7 end=0 bytes=214 headers=1
muxws conn=a3f-17 dir=rx type=data   stream=7 end=0 bytes=8192 frag=more
muxws conn=a3f-17 dir=tx type=reset  stream=9 bytes=96 code=1 reason='user navigated away'
muxws conn=a3f-17 dir=rx type=goaway bytes=62 code=0 last=7 reason='shutting down'
```

These lines are the exact format both reference implementations emit. `frag=` is `more` or `last`,
not an index and a total: a receiver does not know how many fragments a payload has, and the format
is the same in both directions. `bytes=` is on every line, `type=` is padded to six characters, and
each optional key appears only when the field is set. The two implementations differ in the quoting
of `reason`: Python uses `repr()`, TypeScript `JSON.stringify` (`GAPS.md`).

- **WSM-OBS-002** The peer MUST NOT log payload *contents* at any level — application data routinely
  contains secrets.
- **WSM-OBS-003** `peer.on_frame(handler)` MUST receive `(direction, frame, byte_length)` before
  encode and after decode.

### 5.11 `WSM-PKG-` — packaging

- **WSM-PKG-001** Both packages MUST ship from one repository on one version stream, with identical
  version numbers in `pyproject.toml` and `package.json`. That number versions the *packages*; it is
  not a wire version (WSM-PKG-005).
- **WSM-PKG-002** The Python package MUST have **zero required runtime dependencies**. `starlette`
  and `websockets` are optional extras selected by which transport is imported; `msgpack` is an
  optional extra selected by which codec is registered.
- **WSM-PKG-003** The TypeScript browser entry point MUST have zero runtime dependencies. `ws` is an
  optional peer dependency for the Node subpath; `@msgpack/msgpack` an optional peer dependency
  reachable only through the `/msgpack` subpath.
- **WSM-PKG-004** File names in the TypeScript package MUST be kebab-case; TypeScript strings use
  single quotes, Python strings double quotes; Python line length 120.
- **WSM-PKG-005** The wire format is versioned by the generation integer in the subprotocol name
  (`muxws.v1.<codec>`) — a single monotonically increasing integer, bumped only for a breaking wire
  change — independently of the packages' semver. **There is no second, finer version anywhere**
  (WSM-CON-009).

### 5.12 `WSM-RCN-` — reconnect

The reconnect helper exists on the **dialer only**. An acceptor cannot dial and MUST NOT have one.

```text
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

- **WSM-RCN-001** The helper's entire persistent state MUST be an attempt counter.
- **WSM-RCN-002** Jitter MUST be applied to every computed delay, the capped ones included.
  Test: `reconnect_test.py::test_jitter_disperses_n_simultaneous_reconnects`.
- **WSM-RCN-003** The schedule MUST be a pure function of `(attempts, options, random draw)` and MUST
  be tested as one against an injected clock and an injected random source.
  Test: `reconnect_test.py::test_schedule_before_jitter_and_cap`.
- **WSM-RCN-004** The attempt counter MUST increment on every failed attempt and MUST reset **only
  when the connection is established**, where established means both of: the socket is open with the
  subprotocol accepted (WSM-CON-030), and the hello has been acknowledged. Resetting on socket-open
  MUST NOT be done.
  Test: `reconnect_test.py::test_counter_does_not_reset_when_hello_never_completes`.
- **WSM-RCN-005** Jitter MUST use an ordinary, non-cryptographic pseudo-random source.
- **WSM-RCN-006** `connect()` MUST await the first attempt and MUST **raise** if it fails, with the
  underlying error, **regardless of the reconnect configuration**. Reconnection applies to
  connections that were established and then lost; it MUST NOT apply to establishing the first one.
  `connect()` MUST NOT return a peer that is retrying in the background.
  Test: `reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`.
- **WSM-RCN-010** The peer MUST send a `ping` every `ping_interval` on an otherwise idle socket.
- **WSM-RCN-011** If no `pong` arrives within `ping_timeout`, the socket MUST be declared dead,
  closed locally, and the backoff path MUST run exactly as after a clean close. Detection MUST
  therefore be bounded by `ping_interval + ping_timeout`. A locally-declared death closes with
  WebSocket code **1000**: 1006 means "closed abnormally" and a peer may never put it on the wire, so
  a transport that validates it rejects the close and leaves the socket open — after which nothing
  re-dials.
  Test: `reconnect_test.py::test_an_unanswered_ping_is_detected_within_interval_plus_timeout`.
- **WSM-RCN-020** The hello MUST be captured once, at `connect()`, and replayed **verbatim** on every
  connection this peer ever makes. It MUST NOT be re-read, recomputed, or supplied as a callback.
- **WSM-RCN-021** The hello MUST be sent as an ordinary
  `open(hello_payload, headers=hello_headers, end=true)` — delivered to the acceptor's own handler
  like any other stream. muxws MUST NOT flag it, interpret it, or mark it on the wire in any way.
- **WSM-RCN-022** The acknowledgement is the acceptor's handler returning (WSM-STM-035 ends the
  stream implicitly). No application code is required to send one.
- **WSM-RCN-023** The hello MUST go out before any application frame on that socket, and before
  `on_reconnect` fires.
- **WSM-RCN-024** `hello` MUST be optional. A peer given none sends none and is established as soon
  as WSM-CON-030 is satisfied.
- **WSM-RCN-025** **On the deploying application:** a credential MUST NOT be carried in the hello —
  authentication is a handshake concern (§5.2), and the hello is application payload that is
  *replayed verbatim on every reconnect*, so a credential put there is a credential that outlives its
  own expiry. muxws cannot enforce this: it never creates hello content and cannot tell a credential
  from any other value in an opaque payload. The documentation states it: `docs/guide/reconnect.md`
  has a `danger` admonition.
  **On the library:** muxws MUST add nothing of its own to the hello and MUST NOT rewrite what it was
  given. Tested by `reconnect_test.py::test_three_drops_replay_byte_identical_hellos`: byte identity
  across three drops shows that muxws added and changed no field.
- **WSM-RCN-026** A failed hello MUST be a failed connection attempt: if the hello stream is reset,
  or does not complete within `hello_timeout`, the peer MUST close the socket, MUST NOT fire
  `on_reconnect`, MUST increment the attempt counter and MUST back off. The error `connect()` reports
  for a first-attempt hello failure is the underlying one, unwrapped: `StreamTimeout` for the
  deadline, the `StreamReset` itself for a reset.
  Test: `reconnect_test.py::test_reset_hello_and_timed_out_hello_both_back_off`.
- **WSM-RCN-027** Test: `reconnect_test.py::test_three_drops_replay_byte_identical_hellos` — three
  drops produce three byte-identical hellos, `on_reconnect` fires after each acknowledgement and
  never before, and an application registering no `on_reconnect` handler still ends up with a peer
  the server can find in its registry.
- **WSM-RCN-028** Two clauses with different subjects; only the first binds the library.
  **On the library (a MUST, testable):** muxws MUST NOT mint or store a tab identity. It has
  no notion of a tab id, a session id or a client id; it touches no browser storage API; and the
  `hello` that would carry one is opaque payload it never inspects and replays byte-for-byte
  (WSM-RCN-025, WSM-RCN-027). The nonce of a `ping` is per-connection and the `peer.id` of
  WSM-API-009 is per-process; neither is an identity that survives a reconnect.
  **On the deploying application (a recommendation, stated in the documentation):** an application
  that wants a stable identity across reconnects should mint one itself and put it in the hello. In a
  browser the recommended storage is `sessionStorage`: one value per tab, kept across a reload of
  that tab and discarded when the tab closes. `localStorage` is unsuitable because it is shared by
  every tab of the origin, so several tabs would present one identity; module scope is unsuitable
  because it is lost on reload. This guidance is in `docs/guide/reconnect.md`. muxws MUST NOT
  implement it (the library clause above).
- **WSM-RCN-030** `on_reconnect(attempt, peer)` MUST guarantee exactly two things: a live socket,
  and an identity the acceptor has already accepted on it. It MUST fire once per re-established
  connection, after WSM-CON-030 **and** after the hello acknowledgement.
- **WSM-RCN-031** v1 MUST NOT resume streams across a reconnect. Every stream is gone, nothing is
  replayed, no in-flight frame is re-sent, and the new socket's id space starts empty.
- **WSM-RCN-032** `Peer` survives a reconnect; `Stream` objects do not. Any stream held across one is
  already closed.
- **WSM-RCN-033** On the acceptor side a reconnect produces a **new peer object with a fresh, empty
  `tags`**. An implementation MUST NOT carry tags forward.
  Test: `registry_test.py::test_reconnect_starts_with_empty_tags`.
- **WSM-RCN-040** `on_close(reason)` MUST fire on **every** socket loss, not only the final one, and
  exactly once per loss. `will_retry` MUST be `false` only when `max_attempts` is exhausted or
  `close()` was called deliberately. At most one `will_retry: false` close may ever fire for one
  peer; whichever path reaches it first wins and the other is suppressed.
- **WSM-RCN-041** At socket death, before `on_close` fires:
  - a pending await on the stream's result MUST reject with `ConnectionLost`, resolving the memoized
    future once so a second await gets the same error rather than hanging;
  - an async iteration MUST raise `ConnectionLost` out of the loop at the next iteration, and MUST
    NOT terminate normally — a normal end is indistinguishable from a completed stream;
  - an in-flight `request()` MUST raise `ConnectionLost`, never return a partial value and never
    hang;
  - `stream.send()`, `end()` and `reply()` MUST raise `ConnectionLost`;
  - `stream.cancel()` and `stream.reset()` MUST be no-ops;
  - `stream.closed` MUST be set.
  Test: `peer_test.py::test_socket_death_fails_every_shape` — the test MUST detect a hang with a
  bounded wait.
- **WSM-RCN-042** While the peer is between sockets, `open()` MUST raise `ConnectionLost`
  synchronously and `notify()` / `request()` MUST reject with it. Nothing MUST be buffered for the
  next socket.
  Test: `peer_test.py::test_nothing_attempted_while_disconnected_appears_on_the_new_socket`.
- **WSM-RCN-043** `peer.is_open` MUST be `false` for the whole window between a socket loss and the
  next established connection — which, for a peer with a hello, includes the hello window. A peer
  needs a *separate* internal predicate for "is there a wire I can put this frame on": a stream reset
  during the hello window still has to reach the remote, and reading `is_open` for that question
  leaves the remote holding a stream this side has closed.
- **WSM-RCN-044** `max_attempts` exhausted MUST fire `on_close` once with `will_retry` false and MUST
  never dial again. `max_attempts = 0` means the first loss reports `will_retry: false` and no dial
  follows.
- **WSM-RCN-045** `CloseReason` MUST carry exactly four fields, the same four in every language:
  `code` (the WebSocket close code), `reason` (its text), `was_clean` (whether the close was orderly)
  and `will_retry` (WSM-RCN-040). It MUST be one type per language, used for every socket loss.

### 5.13 `WSM-REG-` — `tags` and `PeerRegistry`

- **WSM-REG-001** `peer.tags` MUST be an ordinary dict with ordinary dict semantics: any key may be
  written and overwritten for as long as the socket lives, last write wins, and a rewrite incurs no
  registry work. It MUST be created with the peer and MUST die with the socket.
- **WSM-REG-002** muxws MUST NOT read, interpret, persist, snapshot or restore `tags`.
- **WSM-REG-003** A direct read of `peer.tags` MUST see the newest value immediately, with no copy
  and no snapshot in that path.
- **WSM-REG-010** `register(peer)` MUST index every key present in `tags` at call time. It MUST NOT
  have any notion of which keys matter.
- **WSM-REG-011** A tag value that cannot serve as a lookup key (a dict, a list) MUST be skipped
  without raising. The peer is then not found by that key.
  Test: `registry_test.py::test_register_skips_an_unhashable_tag_value`.
- **WSM-REG-012** `register(peer)` called again after `tags` changed MUST replace that peer's index
  entries **wholesale**: found under the new values, no longer under the old ones.
  Test: `registry_test.py::test_reregister_replaces_entries_wholesale`.
- **WSM-REG-013** The registry MUST NOT watch the dict or re-index on assignment. A peer whose tags
  changed stays findable under the values of its last `register()`.
  Test: `registry_test.py::test_tag_written_after_register_is_not_found_until_reregister`.
- **WSM-REG-014** Test: `registry_test.py::test_overwriting_an_unindexed_key_leaves_the_index_unchanged` — register
  under one key, overwrite a different, never-looked-up map-valued key a hundred times without
  re-registering; every read sees the newest value, the lookup returns the peer unchanged
  throughout, and the registry's index does not grow by a single entry.
- **WSM-REG-015** `peers_for` MUST return a **list**, not a set, in a stable order. Callers MUST
  treat it as a snapshot: a peer in it may already be closing.
- **WSM-REG-016** Removal on close MUST be automatic, via the peer's own close hook. A consumer MUST
  NOT have to prune the index.
- **WSM-REG-017** The documented usage rule: a key used for lookup MUST NOT change after
  `register(peer)`, and a consumer that changes such a key MUST call `register(peer)` after each write.
- **WSM-REG-018** The registry is per-process. muxws MUST NOT ship a cross-process backplane.

### 5.14 `WSM-SID-` — stream ids

- **WSM-SID-001** Stream ids MUST be allocated by the library. A caller MUST NOT be able to supply
  one, anywhere in the API.
- **WSM-SID-002** The dialer MUST allocate odd ids (1, 3, 5, …); the acceptor MUST allocate even ids
  (2, 4, 6, …).
- **WSM-SID-003** Id `0` is reserved for connection-level frames, which in practice omit the field.
- **WSM-SID-004** Opens MUST be monotonic per peer and ids MUST NOT be reused, even after a stream
  closes. `data` and `reset` MAY name any previously-opened id in any order.
- **WSM-SID-005** An `open` naming an id with the wrong parity, or an id not greater than the highest
  id that peer has previously opened, is a **connection-level** protocol error.
  Test: `conformance/invalid/open-wrong-parity.json`, `conformance/invalid/open-id-not-monotonic.json`.
- **WSM-SID-006** The id MUST be allocated and the `open` frame enqueued in one synchronous step,
  with no suspension point between them, so that wire order is allocation order by construction.
  Test: `peer_test.py::test_concurrent_opens_produce_increasing_ids_on_the_wire`.
- **WSM-SID-007** On exhaustion at 2^31−1, the exhausting peer MUST send `goaway` with `last_stream`
  set to the highest id it has processed, MUST stop opening new streams, MUST let in-flight streams
  drain, and MUST then close. All four clauses apply. The trigger is the allocation of the last id,
  not a later call failing; this keeps `open()` synchronous (WSM-API-001) and lets the stream that
  took the last id drain.
- **WSM-SID-008** `stream.id` MUST be readable on the line following `open()`, with nothing awaited
  in between. Test: `stream_test.py::test_id_readable_immediately_after_open`.

### 5.15 `WSM-STM-` — streams: retention, states, dispatch

- **WSM-STM-001** A peer MUST retain exactly **`highest_open_seen` per parity plus the map of live
  streams**. It MUST NOT keep per-closed-stream bookkeeping, which grows without bound over the life
  of a connection.
- **WSM-STM-002** Any stream-level frame naming an id that is not currently live but does not exceed
  that peer's high-water mark MUST be **silently ignored** — no reset, no connection error, at most a
  counter. Test: `conformance/invalid/data-for-closed-id.json` (connection survives, nothing goes
  out).
- **WSM-STM-003** Any stream-level frame other than a valid `open` naming an id **above** that peer's
  high-water mark MUST be a **connection-level** protocol error.
  Test: `conformance/invalid/data-above-high-water-mark.json` (connection dies).
- **WSM-STM-010** Every row of the §4.2 table, including every illegal cell, MUST have a test — one
  case per cell.
- **WSM-STM-011** `idle` is not externally observable: it exists only inside the indivisible
  allocate-and-enqueue step of WSM-SID-006. Cells marked `n/r` MUST NOT be reachable through the
  public API.
- **WSM-STM-012** A stream that is `half_closed_local` on one peer MUST be `half_closed_remote` on
  the other. Both ends half-closed means `closed`.
- **WSM-STM-013** The unary shape is `open(end=true)` → `half_closed_local`, one inbound
  `data(end=true)` → `closed`.
- **WSM-STM-014** On socket death every live stream MUST transition to `closed` and be failed locally
  with a synthesised `reset(CONNECTION_CLOSED)` **before** `on_close` fires (WSM-RCN-041). That
  synthesised reset MUST NOT go on the wire (§2.4, code 9).
- **WSM-STM-015** A `reset` for a stream that is already closed or was never opened MUST be ignored
  under WSM-STM-002.
- **WSM-STM-020** A frame illegal *for a stream* MUST reset **that stream** and leave the connection
  alone. The cases are listed in §4.3.
- **WSM-STM-021** The code for a stream-level violation MUST be `PROTOCOL_ERROR`, except that size
  violations use `PAYLOAD_TOO_LARGE` and concurrency-limit rejection uses `REFUSED`.
- **WSM-STM-023** A frame illegal *for the connection* MUST end the connection with
  `goaway(PROTOCOL_ERROR)` followed by a socket close. The cases are listed in §4.3.
- **WSM-STM-024** The discriminator, when a new case arises: if the peers can still agree about the
  state of every *other* stream, the connection is kept.
- **WSM-STM-030** A peer MUST have exactly one incoming-stream handler, registered with
  `on_stream(handler)`. Registering a second MUST replace the first and MUST log. There MUST NOT be a
  path table, method map, or per-action registration in muxws.
- **WSM-STM-031** The handler MUST be invoked as `(payload, stream)` and **only once the opening
  payload is fully reassembled**. A fragmented `open` MUST NOT reach the application in pieces.
- **WSM-STM-032** The reassembled opening payload MUST also be available as `stream.payload`.
- **WSM-STM-033** With no registered handler, an incoming `open` MUST be answered with
  `reset(REFUSED)` — nothing ran, so the opener may safely retry elsewhere.
- **WSM-STM-034** A registered handler that raises MUST produce `reset(APPLICATION_ERROR)`,
  **always**, regardless of whether it had already sent anything: `REFUSED` promises the operation
  definitively did not happen.
- **WSM-STM-035** A handler that returns without having ended its stream MUST end it implicitly.
  Test: `peer_test.py::test_handler_returning_ends_stream_implicitly`.
- **WSM-STM-036** A peer MUST enforce a **local, receiver-side** limit on how many streams the remote
  may have open on it at once, default **100**, and MUST answer an `open` beyond it with
  `reset(REFUSED)` without invoking the handler. The limit MUST NOT be announced on the wire and MUST
  NOT be enforced by the sender: `open()` MUST NOT check it, MUST NOT raise for it, and MUST put the
  `open` frame on the wire like any other (WSM-API-004).
  Test: `caps_test.py::test_open_beyond_receiver_limit_is_refused_and_opener_raises_nothing_locally`.
- **WSM-STM-037** Streams this peer opened itself MUST NOT count against WSM-STM-036; the limit
  bounds work the *remote* can impose, and each peer counts only the other's opens.

### 5.16 `WSM-TST-` — the conformance corpus

- **WSM-TST-001** `conformance/frames/*.json` MUST be a list of `{"name", "frame", "json_wire"}`
  triples — a logical frame plus the JSON rendering pinned alongside it — read verbatim by every
  language's suite.
- **WSM-TST-002** `conformance/sequences/*.json` MUST be scripted exchanges replayed by every
  implementation against the in-memory transport. Fixtures MUST refer to streams by `stream_ref` (an
  ordinal the runner resolves), never by a raw id — a raw id fixes one side's parity, and the fixture
  then fails when the roles are swapped. A fixture's optional top-level
  `max_frame_bytes` is an instruction to the **runner** to construct both peers with a lowered cap
  (WSM-FRG-005); it is not a wire value and MUST NOT be encoded into any frame.
- **WSM-TST-003** `conformance/invalid/*.json` MUST cover, each asserting which frame goes out and
  whether the connection survives: wrong parity; an `open` id not greater than that peer's highest
  previous open; `data` after `end`; an over-cap encoded message; a message the configured codec
  refuses to decode; a fragment sequence interrupted by a non-fragment frame; a stream-level frame
  above the high-water mark (connection dies); a `data` frame for an already-closed id (connection
  survives, nothing goes out); `headers` on a frame that is not the sender's first on the stream
  (WSM-FRM-016, connection survives).
- **WSM-TST-004** CI MUST run the live cross-language matrix in **both role assignments** over the
  same scenario script: concurrent unary requests interleaved with a streaming export and a server
  push, one cancelled mid-flight, and a `goaway` shutdown.
- **WSM-TST-005** The same matrix MUST include one reconnect scenario: kill the acceptor process with
  streams open, restart it, and assert that the dialer re-dials on a jittered delay, replays a
  byte-identical hello the other language's acceptor accepts, fires `on_reconnect` exactly once after
  it, and that every open stream raised `ConnectionLost` in the meantime.

The corpus schema — the three fixture kinds, the step vocabulary, what `expect_frame` matches and
what it does not, and the runner instructions `max_frame_bytes` and `max_concurrent_streams` — is
specified in `conformance/README.md`, which is normative for runner implementations.

---

## 6. Configuration

Every setting, its default and its unit. **Nothing in this table is exchanged on the wire.** The
first row is a protocol constant, the next two are local receive-side limits, and the rest are local
to one peer.

| Setting | Where | Python name | TypeScript name | Default | Unit / range |
|---|---|---|---|---|---|
| max frame bytes | protocol constant | `MAX_FRAME_BYTES` | `MAX_FRAME_BYTES` | 65536 | bytes; not configurable (test override only, WSM-FRG-005) |
| max payload bytes | `connect()` / `accept()` | `max_payload_bytes` | `maxPayloadBytes` | 67108864 | bytes, local receive limit |
| max concurrent streams | `connect()` / `accept()` | `max_concurrent_streams` | `maxConcurrentStreams` | 100 | int ≥ 1, local receive limit |
| error serializer | `connect()` / `accept()` | `error_serializer` | `errorSerializer` | default serializer | per peer, WSM-ERR-008 |
| codec name | environment | `MUXWS_CODEC` → `muxws.conf.settings.codec` | `VITE_MUXWS_CODEC` | `"json"` | a registered codec name |
| codec override | call argument | `codec=` | `codec` | none | tests / bridges only |
| reconnect initial delay | `connect()` | `initial_delay` | `initialDelayMs` | 0.25 / 250 | s / ms, > 0 |
| reconnect factor | `connect()` | `factor` | `factor` | 2 | ≥ 1 |
| reconnect cap | `connect()` | `max_delay` | `maxDelayMs` | 30.0 / 30 000 | s / ms |
| reconnect jitter | `connect()` | `jitter` | `jitter` | 0.3 | fraction, 0..1 |
| reconnect attempt cap | `connect()` | `max_attempts` | `maxAttempts` | unlimited | count |
| heartbeat interval | `connect()` | `ping_interval` | `pingIntervalMs` | 20.0 / 20 000 | s / ms |
| heartbeat deadline | `connect()` | `ping_timeout` | `pingTimeoutMs` | 10.0 / 10 000 | s / ms |
| hello deadline | `connect()` | `hello_timeout` | `helloTimeoutMs` | 10.0 / 10 000 | s / ms |
| `peer.ping()` deadline | call argument | `timeout` | `timeoutMs` | 5.0 / 5 000 | s / ms |
| goaway drain | `peer.close()` | `drain` | `drainMs` | 10.0 / 10 000 | s / ms |
| request timeout | call argument | `timeout` | `timeoutMs` | none | s / ms |
| fragmentation reservation | internal | — | — | `min(512, cap // 2)` | bytes |

---

## 7. Proving a third port

The steps, in order; each step assumes the previous one passes:

1. **The frame corpus.** `conformance/frames/v1-frames.json`: for every triple,
   `decode(json_wire) == frame` and `decode(encode(frame)) == frame`, comparing parsed objects
   (WSM-CDC-004/005). Then `v1-fragment-boundaries.json`: the exact cut points, element for element
   (WSM-FRG-016).
2. **The invalid corpus.** All nine cases of WSM-TST-003, each asserting the outgoing frame *and*
   whether the connection lived. The empty `expect_out` of `data-for-closed-id` asserts that no frame
   is sent.
3. **The sequence corpus**, replayed in **both role assignments** (WSM-TST-002). It tests behaviour
   over exchanges of frames, beyond the encoding of single frames.
4. **A live cross-language pair in CI**, in both role assignments, for **every** codec the port ships
   (WSM-CDC-007), plus the reconnect scenario (WSM-TST-005).

The corpus does not cover two things, which a port tests on a real transport: whether each component
is used by the peer (for example, whether the send path goes through the writer), and anything the
in-memory transport discards, including the WebSocket close code (WSM-RCN-011).

**The JSON wire form is frozen.** `test_json_wire_is_frozen` hashes the sorted
`conformance/frames/*.json` corpus against a committed digest. Any change to the corpus, including an
added triple, requires updating that digest.

---

## Appendix A — checking this document against the suites

To find the tests that cite a rule:

```bash
grep -rl "WSM-STM-036" --include='*_test.py' --include='*.spec.ts' \
  muxws ts demo docs interop
```

The grep has three limitations.

1. Tests cite combined ids (`WSM-API-006/007`, `WSM-RCN-041/WSM-STM-014`), so `WSM-API-007` must also
   be searched for as a bare number after a family prefix.
2. Some rules are witnessed by a fixture rather than by a citation; for those, search `conformance/`
   for the fixture file name.
3. Test files are under `muxws/`, `ts/`, `demo/backend_python/`, `demo/backend_node/` and
   `docs/examples/`. Search the whole repository.

Rules without a citing test, and rules whose witness is incomplete, are listed in `GAPS.md`.
