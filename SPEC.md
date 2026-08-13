# muxws — normative specification for the `muxws.v1.` generation

This document specifies **muxws**: framing, multiplexing, stream lifecycle, cancellation and
connection lifecycle over one WebSocket. It is written to be implemented from — a third port should
need nothing else to interoperate with the Python and TypeScript ports in this repository.

The stream model is deliberately that of **HTTP/2 and HTTP/3**: many independent streams over one
connection, either peer able to open one, headers then body then optional trailers, per-stream
cancellation, and `goaway` for graceful shutdown. An implementer who knows those protocols will find
this one familiar, and that is the intent. Two differences are load-bearing and are *not* accidents
of an unfinished design. A WebSocket is a single TCP connection, so there is **one global message
order** and no per-stream loss recovery — which is why the frame cap is a protocol constant
(WSM-FRG-004) rather than a negotiated limit. And there is **no `SETTINGS` exchange at all**
(WSM-CON-031): every limit here is either a constant or one peer's private defence.

Requirement levels are RFC 2119: **MUST**, **MUST NOT**, **SHOULD**, **MAY**. Every rule carries a
stable id and is cross-referenced by id only. Rules appear in **id order** (§5), after the wire form
they talk about (§2–§4).

**There is exactly one version in this system, and it is on the wire: the generation integer in the
subprotocol name `muxws.v1.<codec>`** (WSM-CON-009, WSM-PKG-005). This document therefore carries no
version of its own, and a reader who goes looking for a finer one — a spec revision, a
`protocol_version` field, a settings frame announcing capabilities — is looking for something that
was deliberately deleted (§6). The packages' semver versions the *packages*, never the wire.

Where this document and a milestone brief under `docs/design/briefs/` disagree, one of them is a
defect: record it in `GAPS.md` rather than quietly picking. `docs/design/muxws-websocket-transport.md`
holds the reasoning behind these decisions and is not implemented from.

---

## 1. Scope and vocabulary

muxws is not a router, not a serializer of domain objects, not an authentication mechanism, not a
durable store and not an RPC framework.

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
| **established** | The socket is open and, for a codec-bearing subprotocol, that subprotocol was accepted (WSM-CON-030). The reconnect helper additionally requires the hello to have been acknowledged (WSM-RCN-004). |
| **hello** | An application-supplied opening payload the reconnect helper replays on every connection. |

---

## 2. The wire

### 2.1 The subprotocol name

A dialer offers `muxws.v1.<codec>` as its **first** subprotocol entry; an acceptor selects exactly
that value or refuses the upgrade (WSM-CDC-020..022). The `v1` is the generation, and it is the only
version anywhere on the wire.

```
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
| `headers` | object | no | absent | `open`, and the first `data` a peer sends on a stream | Application metadata; string keys, codec-encodable values. Never interpreted by muxws. Each peer gets one chance per stream (WSM-FRM-016). |
| `end` | bool | no | `false` | `open`, `data` | Last frame this peer will send on this stream. |
| `trailers` | object | no | absent | frames with `end: true` | Post-body metadata. |
| `code` | int | yes | — | `reset`, `goaway` | Reset code (§2.4). |
| `reason` | string | no | absent | `reset`, `goaway` | Human-readable, for logs. MUST NOT be parsed. |
| `nonce` | string | yes | — | `ping`, `pong` | Opaque, echoed verbatim. Sender-chosen. |
| `last_stream` | int | yes | — | `goaway` | Highest id from the *other* peer this peer has processed and will still complete. |

`fragment` is listed on `reset` as well as on `open` and `data` because a `reset` carrying a large
structured error object goes through the same splitter as any other stream-level frame, and both
ports fragment it (`conformance/frames/v1-frames.json` pins one).

Those twelve are the whole envelope. Both reference ports omit a field sitting at its default rather
than spelling it out, which is what makes the pinned wire in the corpus short; a receiver applies the
defaults either way.

An absent `payload` key and an explicit `"payload": null` are **different frames**, and the corpus
carries both. WSM-CDC-005's `decode(encode(frame)) == frame` therefore obliges a decoder to keep them
apart — the reference ports do it with an `ABSENT` sentinel — even though whether an application
cares about the difference is its own business.

A receiver ignores envelope fields it does not know (WSM-FRM-001) and frame types it does not know
(WSM-FRM-002). A decoder MUST drop unknown fields rather than preserve them, or
`decode(encode(frame)) == frame` starts passing on garbage.

### 2.3 The v1 frame set

`open`, `data`, `reset`, `ping`, `pong`, `goaway`. Six, and no others.

- `window_update` is **reserved and unimplemented**: the name is spoken for, and a v1 peer MUST NOT
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

The second `data` line is the answering side's leading headers: a peer that did not open the stream
says what is coming before it starts sending it, on a frame that carries no payload at all. That
frame is its **first** on the stream, which is the only place headers may ride (WSM-FRM-016) — the
opener's equivalent is the `headers` on `open`.

A fragmented payload looks like this — `headers` on the first fragment only, `end` and `trailers` on
the last only, `code` and `reason` repeated on every fragment of a `reset`, `more: true` on all but
the last:

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
| 5 | *(retired)* | **Retired.** Was `STREAM_LIMIT`, for rejection against an announced `max_concurrent_streams` (WSM-STM-022). | The number MUST NOT be reused and MUST NOT be sent. A peer *receiving* it treats it as any unknown code: reset that stream, keep the connection. |
| 6 | `TIMEOUT` | A deadline expired locally; the reset tells the remote to stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts (WSM-FRG-031), or a payload exceeded the receiver's `max_payload_bytes` (WSM-FRG-032). | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream live at that instant. **MUST NEVER appear on the wire.** | Do not retry on this peer now; rebuild from `on_reconnect`. |

An unrecognised code — a future generation's, or the retired 5 — MUST reset the named stream and
leave the connection alive. Converting the wire value straight into a closed enumeration raises out
of the read loop, and the peer then looks open with every await hanging.

### 2.5 What is *not* on the wire

No limit, in any form: `MAX_FRAME_BYTES` is a protocol constant, `max_payload_bytes` and the
concurrency limit are each one receiver's own defence (WSM-CON-031). No capability list. No version
beyond the subprotocol generation. No credential (§5.2). No muxws-defined vocabulary inside `payload`
— no `kind`, no reserved key, no discriminator of any sort (WSM-FRM-006). No status: the answering
side's `headers` (WSM-FRM-016) are the application's, key for key, and muxws neither defines a
success key nor reads one (WSM-AUT-002). A failed exchange is a `reset`, which is typed (§2.4).

---

## 3. Sizes, fragmentation and the codec's output

Every size in this specification is **bytes of the fully encoded WebSocket message**, envelope
included, exactly as it goes on the socket (WSM-FRG-001). Not the payload before encoding, not the
`fragment` field alone.

- `MAX_FRAME_BYTES` = **65536**. A protocol constant (WSM-FRG-004).
- `max_payload_bytes` — the largest reassembled payload this peer accepts — defaults to **67108864**
  and is local to the receiver (WSM-FRG-035).

For the JSON codec, the two reference ports agree on the encoded form for every value the shared
corpus may contain, which is what makes WSM-FRG-016 achievable: compact separators (`,` and `:`, no
spaces), no `\u` escaping of non-ASCII, and `NaN`/`Infinity` refused rather than emitted. They
**disagree** on floats and on integers outside ±(2^53−1) — `1.0` against `1`, `1e-07` against `1e-7`
— which is inherited from the languages, not chosen here. It costs nothing on the wire, because
boundaries are the sender's business and a receiver never learns where the cuts were; it does mean a
shared fixture must not carry a float. A third port MUST match the compact, un-escaped form.

Fragmentation is defined over the codec's encoding of the **payload alone** (WSM-FRG-011), so a port
needs a payload-level entry point to its codec as well as the frame-level pair. Both reference ports
spell it `encode_payload` / `decode_payload` (`encodePayload` / `decodePayload`); WSM-CDC-001 fixes
the four members a codec MUST expose and does not forbid more.

---

## 4. Streams

### 4.1 Ids

The dialer allocates odd ids, the acceptor even (WSM-SID-002); `0` is reserved for connection-level
frames, which in practice omit the field (WSM-SID-003). Opens are monotonic per peer and ids are
never reused (WSM-SID-004). The library allocates them; no API anywhere takes one (WSM-SID-001).

### 4.2 State machine

Five states, tracked per stream per peer. Nine events. Forty-five cells, every one of which has a
test (WSM-STM-010).

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

The discriminator, when a new case arises: **if the peers can still agree about the state of every
*other* stream, the connection is kept** (WSM-STM-024).

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
- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between
  sockets. It MUST NOT queue. There MUST NOT be a `StreamLimit` exception and `open()` MUST NOT fail
  for concurrency: that limit is the receiver's (WSM-STM-036) and surfaces asynchronously as
  `StreamRefused` on the pending await.
- **WSM-API-005** `notify()` MUST be async and MUST return nothing. It MUST NOT return a `Stream` or
  any awaitable handle.
- **WSM-API-006** `request()` MUST be `open(payload, end=true)` awaited **to the stream's end**, plus
  a check that raises if the remote sent more than one payload.
- **WSM-API-007** `await stream` MUST resolve on the **first** payload and MUST NOT police a second
  one — that check belongs to `request()` alone.
  Test: `stream_test.py::test_open_resolves_first_payload_while_request_raises`.
- **WSM-API-008** The async surface is closed, and it is exactly this: `connect()`, `accept()`,
  `serve()`; `peer.notify()`, `peer.request()`, `peer.ping()`, `peer.close()`; every `Stream` method
  that sends or waits — `send()`, `send_headers()`/`sendHeaders()` (WSM-API-024), `end()`,
  `reply()`, `reset()`, `cancel()` (WSM-ERR-012) and
  `result()` (WSM-API-012); and the four members of the socket adapter protocol (WSM-API-021), which
  are the transport seam rather than a call an application makes. **Everything else MUST NOT be
  async**, and the two that matter are `peer.open()` (WSM-API-001 — a synchronous open is what makes
  allocation and enqueue one indivisible step, WSM-INV-005/WSM-SID-006) and the four hook
  registrations `on_stream`/`on_frame`/`on_close`/`on_reconnect`, since a registration that suspends
  is one an acceptor can race a pushed stream against (WSM-STM-033).
  Test: `peer_test.py::test_the_public_api_is_async_exactly_where_the_rule_says`, which compares the
  partition by equality in **both** directions — the enumeration above is the whole rule, so a list
  that only checked the named members would leave the "everything else" half unasserted. The
  enumeration was completed in the M8 audit: it previously named seven calls and "every `Stream` send
  method", which read literally made the shipped `result()`, `cancel()` and `reset()` violations of a
  rule they were required by. No behaviour changed; the rule now says what 1.0 does.
- **WSM-API-009** `peer.id` MUST be a short random prefix minted **once per process** plus a
  monotonic **per-connection** counter, rendered `<prefix>-<counter>` (e.g. `a3f-17`). An id MUST NOT
  be reused or duplicated within a process; across processes only the prefix may coincide. Reuse is
  the failure being prevented — two connections under one name read as one connection in a log — so a
  scheme that recycles the ids of closed connections MUST NOT be used.
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
  `Promise` (species semantics would make every derived call construct a bogus `Stream`).
  Consequence a port must know: `await` unwraps thenables recursively, so a `Stream` can never be the
  resolution value of a promise — `Promise<Stream>` silently yields the stream's first payload
  instead. Box it (`Promise<{ stream: Stream }>`).
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
  wire. Riding them along with the first payload MUST also be possible, as a `headers` argument to
  `send()`, `end()` and `reply()`. Both MUST raise `ProtocolError` once this side has sent any frame
  on the stream — the wire rule is one chance per peer per stream, and a call that silently dropped
  the second set would leave the sender believing it had announced something it had not. On a stream
  this peer opened, the `open` **is** that first frame, so `send_headers()` there MUST raise and say
  that `open(headers=)` is the place.
  Test: `stream_test.py::test_leading_headers_are_sendable_once_and_only_first`;
  `ts/stream.spec.ts` *"sends leading headers once, and refuses a second set"*.
- **WSM-API-025** The two sets of leading headers MUST be two attributes, and each MUST read the
  same from either end of the stream. `stream.headers` is the `open`'s, which both peers already
  see today; `stream.reply_headers` / `stream.replyHeaders` is the answering side's — what the
  opener received, and what the answering peer itself announced. Both MUST be an empty mapping when
  there are none, never `None`/`undefined`. Overloading `stream.headers` to mean "whatever the
  *other* peer sent" MUST NOT be done: it reads as one attribute and is two, and it would take the
  opener's own headers away from the opener, which is where they are today.
  `stream.reply_headers_arrived` / `replyHeadersArrived` MUST be an `asyncio.Event` in Python and a
  `Promise<void>` in TypeScript, settling at the instant `reply_headers` can no longer change: the
  answering side's first frame on the stream, or the close of a stream that was never answered. It
  MUST settle on **every** path, including a stream reset before any answer and one whose socket
  dies — an await on metadata that will never come is the spinner that never stops (WSM-INV-011),
  and here it is one the remote can cause.
  Test: `stream_test.py::test_reply_headers_arrived_settles_on_every_path`;
  `ts/stream.spec.ts` *"settles replyHeadersArrived on every path"*.

**Call shapes.** Unary is `open(end)` → `data(end)`; a streaming response is `open(end)` → `data`…
`data(end)`, optionally led by a payload-less `data(headers)` announcing what is coming
(WSM-FRM-016); bidirectional is `open` → interleaved `data` both ways → `data(end)` both ways; a
one-shot push is `open(end)` with nothing awaited. Durations are **seconds as floats in Python** and
**milliseconds in TypeScript**; wire field names stay snake_case in every language.

### 5.2 `WSM-AUT-` — authentication, headers, routing

- **WSM-AUT-001** Two obligations with two different subjects, and they are separated here because
  only one of them is the library's.
  **On the deploying application:** authentication MUST happen at the WebSocket upgrade, before
  `accept()` is called. muxws cannot enforce this and no test of muxws can witness it; what muxws
  does is leave no other place to put it, and that part is witnessed — `accept()` takes no credential
  argument, `connect()` passes `headers=` to the upgrade and to nowhere else, and there is no
  post-upgrade authentication hook on `Peer` at all.
  **On the library:** muxws MUST NOT interpret credentials anywhere. Witnessed by
  `peer.spec.ts` *"never interprets per-stream headers"* and
  `peer_test.py::test_per_stream_headers_arrive_unchanged_and_change_nothing` (WSM-AUT-002), and by
  the upgrade tests in `transports/websockets_test.py` and `ts/node.spec.ts`.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application and
  MUST NOT be used for re-authentication by the library. This binds both directions: the answering
  side's leading headers (WSM-FRM-016) are delivered as sent and change no outcome either, and a
  library that started reading the answer's metadata would be inventing a status code — which is
  what WSM-FRM-006 forbids inside `payload` and there is no reason to permit beside it.
- **WSM-AUT-003** **On the deploying application** (a SHOULD, and its call): a connection whose
  credential expires mid-life SHOULD be closed with `goaway`. muxws neither knows what a credential
  is nor when one expires (WSM-AUT-001), so it cannot do this and cannot be tested for it.
  **On the library** (a statement of fact about the reconnect helper, and testable): re-authentication
  is re-dialling. The dial callable `connect()` hands to the reconnect loop closes over the `headers`
  it was given, so every attempt presents the same credential at a fresh upgrade and there is no
  second, in-band path.
  Test: `reconnect_test.py::test_every_reconnect_presents_the_same_credential_at_a_fresh_upgrade`,
  which reads the `Authorization` header at the **upgrade** across three dials — a test that watched
  frames could not tell a header that was sent from one that was dropped.
- **WSM-AUT-004** muxws MUST NOT look at the opening payload for routing: no path matching, no method
  dispatch, no handler table (WSM-STM-030).

### 5.3 `WSM-BPR-` — backpressure

- **WSM-BPR-001** v1 MUST NOT implement per-stream flow control. `window_update` is reserved as a
  frame type and MUST NOT be sent.
- **WSM-BPR-002** The flow-control mechanisms in v1 are exactly the receiver's local concurrency
  limit (WSM-STM-036 — how many producers can exist at once) and `MAX_FRAME_BYTES` (WSM-FRG-004 — how
  long any one may hold the wire). Both are local defences, not agreements; there is no negotiated
  mechanism of any kind, and building one requires a new generation (WSM-CON-009).

### 5.4 `WSM-CDC-` — the codec seam

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
  documented as a test override and an escape hatch for a process holding two connections that need
  different codecs. No example outside the test suite may use it.
- **WSM-CDC-013** Registration MUST be explicit: `register_codec(name, codec)` / `registerCodec(name,
  codec)`. There MUST NOT be dynamic imports, lazy auto-registration, entry-point scanning, or any
  probing of whether a module happens to be installed.
- **WSM-CDC-014** A codec module MUST NOT register itself at import time — a side-effecting import
  can never be tree-shaken out.
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

  This clause is here because its absence caused the failure. The rule named only Python's hook, so
  both ports shipped an acceptor that answered HTTP 101 where WSM-CDC-022 requires 400, and did so for
  three milestones. Nothing caught it: every test dialled with the same language's dialer, which
  recovers through WSM-CDC-028 and raises `CodecMismatch` anyway, so the *outcome* was right in the
  only configuration ever exercised. A cross-language dial is where it showed.
- **WSM-CDC-028** Where a transport offers neither hook, the peer MUST verify the negotiated
  subprotocol on the already-open socket and close it with the WebSocket policy-violation close code.
- **WSM-CDC-029** The acceptor MUST log the same failure at refusal time, naming the offered codec,
  its own configured codec and both environment variables. It MUST NOT be omitted on the grounds that
  WSM-CDC-024 already reports it — neither message is complete on its own.

### 5.5 `WSM-CON-` — connection lifecycle

- **WSM-CON-001, -002, -003, -004, -005, -006, -007, -008** *Retired.* They specified the `settings`
  frame: sending it first, acknowledging it, the defaults-until-ack window, the ack as the ordering
  point for a revised limit, and `protocol_version` mismatch behaviour. All of them existed only to
  serve an exchange that no longer happens. **None of these ids is reused.**
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

```
MuxwsError
├── ProtocolError            # this peer or the remote violated the spec
├── ConnectionClosed         # socket died; carries .code, .reason, .was_clean
├── ConnectionGoingAway      # open() after goaway — raised synchronously out of open()
├── StreamAlreadyConsumed    # await and iterate, or two iterations, on one stream
├── StreamClosed             # send()/end()/reply() on a stream that closed normally
├── CodecError               # configuration; carries .configured and .available
│   ├── CodecNotRegistered   # configured name never registered — raised at startup
│   └── CodecMismatch        # acceptor's codec differs; the handshake was rejected
└── StreamReset              # carries .code (ResetCode), .reason, .stream_id
    ├── RemoteError          # code == APPLICATION_ERROR; carries .payload
    ├── StreamTimeout        # code == TIMEOUT
    ├── StreamRefused        # code == REFUSED; not processed — retry, elsewhere or later
    └── ConnectionLost       # code == CONNECTION_CLOSED; synthesised locally, never from the wire
```

- **WSM-ERR-001** *Retired.* It required `StreamRefused` and `StreamLimit` to be sibling classes.
  With the announced quota gone there is no `StreamLimit` (WSM-STM-022, WSM-API-004) and
  `StreamRefused` covers every refusal. **The id is not reused.**
- **WSM-ERR-002** `ConnectionLost` MUST be a `StreamReset` subclass; `ConnectionClosed` MUST NOT be.
  `ConnectionLost` is *a stream* failing because the connection did, and is what every stream-shaped
  call raises. `ConnectionClosed` is *the connection* ending, and is what `serve()` and peer-level
  calls raise.
- **WSM-ERR-003** All muxws errors MUST be raised from the awaiting call site and MUST NOT be
  swallowed into a callback.
- **WSM-ERR-004** Every port MUST mirror this hierarchy with classes of the same names, delivered as
  rejections and as throws inside an async iteration, and MUST set a discriminator so cross-language
  tests can assert on error identity.
- **WSM-ERR-005** `CodecNotRegistered` and `CodecMismatch` MUST sit outside `StreamReset`: neither is
  a stream failure and neither is retryable.
- **WSM-ERR-006** A handler that raises MUST produce `reset(APPLICATION_ERROR)` with a `reason` and
  an optional structured `payload`. The default serializer produces
  `{"type": "<exception class>", "message": "<str(exc)>"}`. The peer MUST accept an
  `error_serializer` hook of signature `(exc) -> Any | None` — the value it returns becomes the
  reset's `payload`, and `None` means "send no payload". The documentation MUST state plainly that a
  public-facing deployment should redact it. Note for a third port: `type` is *not* portable — it
  names the remote's own exception class, so an interop assertion may compare `message` and not
  `type`.
- **WSM-ERR-007** muxws MUST NOT map exceptions to status codes of any kind.
- **WSM-ERR-008** `error_serializer` MUST be supplied **per peer** — an argument to `connect()` and
  to the acceptor's peer construction — and MUST NOT be a module-level default. One process may hold
  a browser-facing peer that redacts and an internal peer that does not, and a process-wide setting
  forces the wrong answer for one of them.
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
  cannot observe an abandoned await (JavaScript) carries this where it *can* be observed — a
  `for await` that breaks, returns or throws, and an abort signal passed to `result()` — and MUST NOT
  pretend to carry it for a dropped promise.
- **WSM-ERR-015** An incoming `reset(APPLICATION_ERROR)` on a stream this peer is consuming MUST
  raise `RemoteError` out of the pending await or the async iteration.

### 5.7 `WSM-FRG-` — sizes and fragmentation

- **WSM-FRG-001** Every size limit MUST be measured as the byte length of the **fully encoded
  WebSocket message** — the complete codec output for the frame, envelope included. Not the
  pre-encoding payload, not the `fragment` field alone.
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
  `MAX_FRAME_BYTES`. Fragmentation is mandatory, not an optimisation.
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
  queued work**. A FIFO send queue MUST NOT be used — the ordering decision would be made at enqueue
  time, and interleaving becomes impossible.
  Test: `conformance/sequences/small-frame-overtakes-a-fragmented-payload.json`, and
  `writer_test.py::test_a_small_frame_overtakes_a_fragmented_payload` end to end through a real peer
  pair — a writer with its own green tests can still be bypassed by the send path.
- **WSM-FRG-020** `end: true` MUST appear only on the final fragment of a payload. A fragment
  sequence MUST end with a fragment carrying `more: false`, even when that fragment carries no bytes.
- **WSM-FRG-021** `headers` MUST NOT be fragmented. A frame whose headers alone push it over the cap
  MUST be rejected by the receiver with `reset(PAYLOAD_TOO_LARGE)`. This binds every frame headers
  may ride (WSM-FRM-016), not the `open` alone: an answering peer's leading `data` is the same frame
  with the same field on it, and a splitter that special-cased `open` would fragment the one place
  the rule was never checked.
- **WSM-FRG-030** The receiver MUST concatenate `fragment` values and hand the result to the codec
  for decoding when a fragment arrives without `more: true`.
- **WSM-FRG-031** A receiver that enforces a frame-size limit MUST measure the entire encoded
  message, never the `fragment` field on its own, and MUST reset that stream with
  `PAYLOAD_TOO_LARGE` when it is over. Enforcement above `MAX_FRAME_BYTES` is optional; a receiver
  MUST NOT reject a message at or below the constant.
- **WSM-FRG-032** A payload whose accumulated fragments exceed `max_payload_bytes` MUST be rejected
  with `reset(PAYLOAD_TOO_LARGE)` **as soon as the limit is crossed**, before further fragments are
  accepted and without waiting for reassembly to complete. Bounding memory is the limit's whole
  purpose, and a receiver that assembles the payload in order to measure it has already spent what
  the limit was protecting.
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
  as any kind of error. Test: `peer_test.py::test_unknown_frame_type_is_ignored`.
- **WSM-FRM-003** *Retired.* It required a peer not to send an extension frame type unless the remote
  had advertised that extension in `settings.extensions`. There is no `settings` frame and no
  extension advertisement (WSM-CON-031); a v1 peer sends only the six frame types of §2.3, and a frame
  type the remote must *act* on requires a new generation (WSM-CON-009). **The id is not reused**, and
  `test_unadvertised_extension_is_never_sent` is not written. A reader arriving here looking for the
  forward-compatibility story wants WSM-FRM-001 and WSM-FRM-002: tolerate, never advertise.
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
- **WSM-FRM-013** Trailers MUST ride on the frame carrying `end: true`. There MUST NOT be a trailers
  frame type.
- **WSM-FRM-014** `reset` terminates a stream immediately in both directions. Sender: either peer, in
  any state except `closed`. Fields: `stream`, `code`, `reason`, optional `payload` for a structured
  error object.
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`, or set it
  to `0`.
- **WSM-FRM-016** `headers` MAY ride the **first stream-level frame a peer sends on a stream** and
  MUST NOT appear on any later one. For the peer that opened the stream that frame is the `open`;
  for the peer answering it, its first `data`. The two directions are independent — each peer has
  its own one chance, and neither can spend the other's. A receiver that sees `headers` on a later
  frame MUST reset that stream with `PROTOCOL_ERROR`; that is a stream-level error and MUST NOT take
  the connection down (§4.3).
  Two consequences a port must get right. `reset` never carries headers, however early it arrives: a
  reset is not an answer, and the field table lists it on `open` and `data` only. And "first frame"
  means *sent*, not *carrying a payload* — a peer with metadata to announce and nothing yet to say
  sends `{"type":"data","stream":N,"headers":{...}}`, which is a legal frame with no payload, and has
  then spent its chance.
  Tests: `conformance/frames/v1-frames.json` (`data-with-headers`),
  `conformance/sequences/answering-side-announces-headers.json`,
  `conformance/invalid/headers-on-a-later-frame.json`.

### 5.9 `WSM-INV-` — cross-cutting invariants

Each of these is violated most often by accident; the clause after the dash is the failure it
prevents.

- **WSM-INV-001** muxws MUST NOT depend on any package above it in the stack — not as an import, an
  optional extra, or a type-checking-only annotation — or the claim that anyone wanting multiplexed
  streams can install it stops being true.
- **WSM-INV-002** There MUST be one symmetric `Peer` type per language — or server push needs a
  second, parallel mechanism with its own correlation and cancellation story.
- **WSM-INV-003** Every size limit MUST be counted in bytes of the **codec's own output**
  (WSM-FRG-001) — or a sender budgeting against JSON text while msgpack bytes go on the wire produces
  over-cap frames on exactly the deployments that chose the compact codec.
- **WSM-INV-004** At most one unsent fragment per stream, and round-robin writer selection
  (WSM-FRG-018/019) — or a 1 MB payload adds a full second of latency to a 200-byte progress update
  on another stream.
- **WSM-INV-005** Id allocation and `open` enqueue MUST be one indivisible synchronous step
  (WSM-SID-006) — or two concurrent `open()` calls put a non-monotonic id sequence on the wire, which
  is a protocol error the peer commits against itself.
- **WSM-INV-006** Late frames below the high-water mark MUST be ignored and frames above it MUST kill
  the connection (WSM-STM-002/003) — or "ignore late frames" degenerates into "ignore everything" and
  a genuine id-space disagreement goes undetected.
- **WSM-INV-007** *Retired* with WSM-STM-022. It required `STREAM_LIMIT` not to be reported as
  `REFUSED`. **The id is not reused.** What replaces it as the thing to get wrong: the concurrency
  limit MUST stay entirely receiver-side (WSM-STM-036) — a sender that "helpfully" tracks how many
  streams it has open and refuses locally has reinvented the announced quota, against a number it
  cannot know.
- **WSM-INV-008** A handler that raises MUST always produce `APPLICATION_ERROR`, never `REFUSED`
  (WSM-STM-034) — or a handler that debits an account and then raises invites the client to retry the
  debit.
- **WSM-INV-009** The awaited future MUST be memoized and the consumption claim MUST live on the
  stream (WSM-API-010/013) — or a stream's payloads get split silently between an await and an
  iteration, and neither consumer looks wrong locally.
- **WSM-INV-010** Nothing MUST be queued while the peer is between sockets (WSM-RCN-042) — or a queue
  flushes into a server that has forgotten the sender, and a failure that would have reached a call
  site is turned into silent misdelivery.
- **WSM-INV-011** Every stream live at socket death MUST fail with `ConnectionLost`, never hang
  (WSM-RCN-041) — or a caller sees no error, no log and no timeout, just a spinner that never stops.
- **WSM-INV-012** The attempt counter MUST reset only on an *established* connection (WSM-RCN-004) —
  or a server that accepts sockets while its backend is down turns exponential backoff into a
  fixed-interval hammer at the initial delay.
- **WSM-INV-013** The hello MUST be replayed by the helper, not by the application (WSM-RCN-020) — or
  an application that forgets gets a socket the server cannot associate with anything: connected,
  healthy-looking, subscribed to nothing, reporting no error.
- **WSM-INV-014** `tags` MUST NOT survive a reconnect (WSM-RCN-033) — or a tab that silenced
  something and then died stays silent for a successor that never asked to be.
- **WSM-INV-015** An unregistered codec name MUST be a loud startup failure, never a silent JSON
  fallback (WSM-CDC-016) — or a deployment believes it is running msgpack, is not, and may never find
  out because both ends fell back.
- **WSM-INV-016** muxws MUST NOT define a message vocabulary (WSM-FRM-006) — or two independent
  consumers sharing one socket must both nest their own vocabulary inside an imposed one, and every
  change to either needs a muxws release.
- **WSM-INV-017** `max_payload_bytes` MUST be enforced as fragments accumulate, not after reassembly
  (WSM-FRG-032) — or a receiver has already allocated everything the limit existed to bound by the
  time it decides to refuse it, and the limit protects nothing but a variable.
- **WSM-INV-018** `connect()` MUST raise when the first attempt fails (WSM-RCN-006) — or a typo in
  the URL, an unreachable host or a codec mismatch never surfaces anywhere: the application holds a
  peer that looks alive and retries forever against something that will never answer.

### 5.10 `WSM-OBS-` — observability

- **WSM-OBS-001** Every frame MUST be loggable in one line at `DEBUG` under the `muxws.frames`
  logger:

```
muxws conn=a3f-17 dir=tx type=open   stream=7 end=0 bytes=214 headers=1
muxws conn=a3f-17 dir=rx type=data   stream=7 end=0 bytes=8192 frag=more
muxws conn=a3f-17 dir=tx type=reset  stream=9 bytes=96 code=1 reason='user navigated away'
muxws conn=a3f-17 dir=rx type=goaway bytes=62 code=0 last=7 reason='shutting down'
```

That is the rendering both reference ports emit, not an illustration: `frag=` says `more` or `last`
and **not** an index out of a total, because a receiver never learns how many fragments a payload was
cut into and a line whose shape depended on the direction would be two formats. `bytes=` is on every
line, `type=` is padded to six, and each optional key appears only when the field is set. The one
place the two ports disagree today is the quoting of `reason` — Python renders it with `repr()` and
TypeScript with `JSON.stringify` — which is a defect in one of them rather than a licence for a third
port to choose; see Appendix B.

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

```
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

- **WSM-RCN-001** The helper's entire persistent state MUST be an attempt counter.
- **WSM-RCN-002** Jitter MUST actually be applied to every computed delay, the capped ones included.
  Test: `reconnect_test.py::test_jitter_disperses_n_simultaneous_reconnects`.
- **WSM-RCN-003** The schedule MUST be a pure function of `(attempts, options, random draw)` and MUST
  be tested as one against an injected clock and an injected random source.
  Test: `reconnect_test.py::test_schedule_before_jitter_and_cap`.
- **WSM-RCN-004** The attempt counter MUST increment on every failed attempt and MUST reset **only
  when the connection is established**, where established means both of: the socket is open with the
  subprotocol accepted (WSM-CON-030), and the hello has been acknowledged. Resetting on socket-open
  MUST NOT be done.
  Test: `reconnect_test.py::test_counter_does_not_reset_when_hello_never_completes`.
- **WSM-RCN-005** Jitter uses an ordinary pseudo-random source; a cryptographic one MUST NOT be used.
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
  Test: `reconnect_test.py::test_swallowed_pong_is_detected_within_interval_plus_timeout`.
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
  own expiry. muxws cannot enforce this: it never mints hello content and cannot tell a credential
  from any other value in an opaque payload. Discharged by documentation, which must state it
  plainly — `docs/guide/reconnect.md` does, as a `danger` admonition.
  **On the library:** muxws MUST add nothing of its own to the hello and MUST NOT rewrite what it was
  handed. Witnessed by `reconnect_test.py::test_three_drops_replay_byte_identical_hellos` — byte
  identity across three drops means no field muxws contributed and no field it touched.
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
- **WSM-RCN-028** This id carried one MUST and one recommendation in a single breath, and the
  recommendation was being read as an unmet obligation on the library. They are separated here. The
  id is not renumbered and neither clause is weakened; only the subject of each is now stated.
  **On the library (a MUST, and witnessable):** muxws MUST NOT mint or store a tab identity. It has
  no notion of a tab id, a session id or a client id; it touches no browser storage API; and the
  `hello` that would carry one is opaque payload it never inspects and replays byte-for-byte
  (WSM-RCN-025, WSM-RCN-027). The nonce of a `ping` and the `peer.id` of WSM-API-009 are
  per-connection and per-process respectively, and neither survives a reconnect — that is the
  distinction being protected.
  **On the deploying application (a RECOMMENDATION, discharged by documentation):** an application
  that wants a stable identity across reconnects should mint one itself and put it in the hello. In a
  browser the recommended lifetime is `sessionStorage` — one value per tab, surviving a reload of
  that tab and nothing more, which is exactly the lifetime of the thing being identified.
  `localStorage` is wrong, because it is shared across every tab of the origin and three tabs then
  claim to be one client; module scope is wrong, because it dies on reload. This is guidance, not an
  obligation muxws can fail to meet, and it lives in `docs/guide/reconnect.md`. muxws MUST NOT
  implement it, which is the library-side MUST above.
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
  exactly once per loss. `will_retry` MUST be `false` only when `max_attempts` is exhausted or
  `close()` was called deliberately. At most one `will_retry: false` close may ever fire for one
  peer; whichever path reaches it first wins and the other is suppressed.
- **WSM-RCN-041** At socket death, before `on_close` fires:
  - a pending await on the stream's result MUST reject with `ConnectionLost`, resolving the memoized
    future once so a second await gets the same error rather than hanging;
  - an async iteration MUST raise `ConnectionLost` out of the loop at the next iteration, and MUST
    NOT terminate normally — a clean end would read as "the export finished";
  - an in-flight `request()` MUST raise `ConnectionLost`, never return a partial value and never
    hang;
  - `stream.send()`, `end()` and `reply()` MUST raise `ConnectionLost`;
  - `stream.cancel()` and `stream.reset()` MUST be no-ops;
  - `stream.closed` MUST be set.
  Test: `peer_test.py::test_socket_death_fails_every_shape` — the test MUST fail by hang detection
  rather than by an error nobody raised.
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
  written and overwritten for as long as the socket lives, last write wins, no bookkeeping is paid
  for a rewrite. It MUST be created with the peer and MUST die with the socket.
- **WSM-REG-002** muxws MUST NOT read, interpret, persist, snapshot or restore `tags`.
- **WSM-REG-003** A direct read of `peer.tags` MUST see the newest value immediately, with no copy
  and no snapshot in that path.
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
- **WSM-REG-014** Test: `registry_test.py::test_overwriting_a_never_indexed_key_is_free` — register
  under one key, overwrite a different, never-looked-up map-valued key a hundred times without
  re-registering; every read sees the newest value, the lookup returns the peer unchanged
  throughout, and the registry's index does not grow by a single entry.
- **WSM-REG-015** `peers_for` MUST return a **list**, not a set, in a stable order. Callers MUST
  treat it as a snapshot: a peer in it may already be closing.
- **WSM-REG-016** Removal on close MUST be automatic, via the peer's own close hook. A consumer MUST
  NOT have to prune the index.
- **WSM-REG-017** The documented usage rule: **look up on keys you do not mutate, and mutate keys you
  do not look up**; a consumer needing both on one key MUST call `register(peer)` after each write.
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
  drain, and MUST then close. All four clauses, and the trigger is *taking* the last id rather than
  the next call reporting it — that keeps `open()` synchronous (WSM-API-001) and still gives the
  stream just allocated its drain window.
- **WSM-SID-008** `stream.id` MUST be readable on the line following `open()`, with nothing awaited
  in between. Test: `stream_test.py::test_id_readable_immediately_after_open`.

### 5.15 `WSM-STM-` — streams: retention, states, dispatch

- **WSM-STM-001** A peer MUST retain exactly **`highest_open_seen` per parity plus the map of live
  streams**. It MUST NOT keep per-closed-stream bookkeeping — that is a per-connection memory leak.
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
- **WSM-STM-022** *Retired.* It required concurrency-limit rejection to use `STREAM_LIMIT` rather than
  `REFUSED`, so that an opener would back off rather than retry at once. The distinction went with the
  announced quota: the limit is now the receiver's alone and is not advertised, so `REFUSED` —
  nothing ran, retry is the application's judgement — is the whole of what the opener can be told.
  **The id is not reused, and reset code 5 is retired with it** (§2.4). A reader arriving here
  looking for a stream-limit code or a `StreamLimit` exception will not find one anywhere: see
  WSM-STM-036, WSM-API-004 and WSM-ERR-001.
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
  ordinal the runner resolves), never by a raw id — a raw id bakes in one side's parity and the
  fixture then fails the moment the roles are swapped. A fixture's optional top-level
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

The corpus's own schema — the three fixture kinds, the step vocabulary, what `expect_frame` matches
and what it deliberately does not — is `conformance/README.md`, and it is normative for anyone
writing a runner.

---

## 6. Retired ids and numbers

Nothing in this list is ever reused. A retired id keeps its number so that a reader who meets it in an
old commit, an old log or an old branch finds the reason it went away rather than a different rule
wearing its name.

| Retired | Was | Why it went | Where to look instead |
|---|---|---|---|
| `WSM-CON-001`…`-008` | the `settings` frame: send-first, ack, defaults-until-ack, ack-as-ordering-point, `protocol_version` mismatch | there is no `settings` frame | WSM-CON-031, WSM-CON-009 |
| `WSM-FRM-003` | a peer MUST NOT send an extension frame type unless the remote advertised it in `settings.extensions` | there is no advertisement, because there is no `settings` frame; a frame type the remote must *act* on is a new generation | WSM-FRM-001, WSM-FRM-002, WSM-CON-009 |
| `WSM-STM-022` | concurrency rejection uses `STREAM_LIMIT`, not `REFUSED` | the quota is no longer announced, so `REFUSED` is the whole of what the opener can be told | WSM-STM-036, WSM-API-004 |
| **reset code 5** | `STREAM_LIMIT` | retired with WSM-STM-022 | never sent; if received, treated as any unknown code (§2.4) |
| `WSM-ERR-001` | `StreamRefused` and `StreamLimit` as sibling classes | there is no `StreamLimit` | WSM-ERR-002, WSM-API-004 |
| `WSM-INV-007` | `STREAM_LIMIT` must not be reported as `REFUSED` | retired with WSM-STM-022 | WSM-INV-007's replacement text, WSM-STM-036 |

Two tests named in earlier drafts are deliberately **not written**:
`test_unadvertised_extension_is_never_sent` (WSM-FRM-003) and anything asserting a `StreamLimit`
class (WSM-ERR-001). The suites instead assert the absence:
`errors_test.py::test_no_stream_limit_class_exists`,
`frames_test.py::test_no_settings_frame_exists`,
`lifecycle_test.py::test_no_settings_frame_is_ever_emitted`.

---

## 7. Configuration

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

## 8. Proving a third port

In order, because each step is cheap only once the previous one holds:

1. **The frame corpus.** `conformance/frames/v1-frames.json`: for every triple,
   `decode(json_wire) == frame` and `decode(encode(frame)) == frame`, comparing parsed objects
   (WSM-CDC-004/005). Then `v1-fragment-boundaries.json`: the exact cut points, element for element
   (WSM-FRG-016).
2. **The invalid corpus.** All nine cases of WSM-TST-003, each asserting the outgoing frame *and*
   whether the connection lived. The empty `expect_out` of `data-for-closed-id` is an assertion, not
   an absence of one.
3. **The sequence corpus**, replayed in **both role assignments** (WSM-TST-002). This is what proves
   agreement about *sequences* rather than about how one frame is spelled.
4. **A live cross-language pair in CI**, in both role assignments, for **every** codec the port ships
   (WSM-CDC-007), plus the reconnect scenario (WSM-TST-005).

Two things the corpus cannot see, and which a port must therefore test on a real transport: whether
a component is actually wired into the peer at all — a writer with its own green tests can still be
bypassed by the send path — and anything the in-memory transport discards, which includes the
WebSocket close code (WSM-RCN-011).

**The JSON wire form is frozen.** `test_json_wire_is_frozen` hashes the sorted
`conformance/frames/*.json` corpus against a committed digest, so changing the wire requires
deliberately editing that digest. Adding a triple changes it; that is the point.

---

## Appendix A — checking this document against the suites

Every rule id here is meant to resolve to at least one test. The check is a grep, not a promise:

```bash
grep -rl "WSM-STM-036" --include='*_test.py' --include='*.spec.ts' \
  muxws ts demo docs interop
```

Three things a naive grep gets wrong, and the third of them made the previous edition of Appendix B
wrong in two places.

1. Tests cite combined ids (`WSM-API-006/007`, `WSM-RCN-041/WSM-STM-014`), so `WSM-API-007` must be
   searched for as a bare number after a family prefix too.
2. Some rules are witnessed by a fixture rather than by a citation, in which case the fixture file
   name is the thing to search for under `conformance/`.
3. **Test files are not only under `muxws/` and `ts/`.** They are also under `demo/backend_python/` and
   `docs/examples/`. The grep in the previous edition named `muxws ts` and nothing else, and so
   reported `WSM-FRG-010` as uncited when `demo/backend_python/handlers_test.py` names it twice. Search the
   repository, not two directories of it.

## Appendix B — the honesty list

**What this list is.** Every rule in §5 is meant to be held by something that can fail. This appendix
records, for each rule that is *not* held by an ordinary citing test, what does hold it and how
strongly — because "no test" and "unenforced" are different claims, and the previous edition
conflated them. It is a snapshot; refresh it by running Appendix A's grep over every id.

**Why it is worth maintaining.** Three times in this project a rule was written, cited, believed and
silently false, and each was found by an audit or by a consumer rather than by the suite:
`WSM-RCN-011` (a close code no in-memory transport can carry), `WSM-CDC-022` (a status code invisible
to every peer-level test), `WSM-INV-004` (a writer that was correct and was never asked). The shape
is always the same — **the rig could not see the thing the rule is about** — so an entry here that
says "witnessed" is worth only as much as the mutation that was shown to kill the witness. Where a
witness was added in the M8 audit, the mutation that proves it can fail is given with it.

### The count

Of the **218** individually numbered rules in §5:

| | rules with no citing test |
|---|---|
| before the M8 audit | **36** |
| after it | **20** |
| after `WSM-AUT-003` was witnessed | **19** |

Of those 19: **fourteen** are exercised by a test that simply does not name them; **one**
(`WSM-PKG-004`) is enforced by the linters; **two** (`WSM-TST-004`, `WSM-TST-005`) by a CI job; and
**two** (`WSM-RCN-025`, `WSM-RCN-028`) are rules whose library half is held by a differently-named
test and whose other half binds the deploying application rather than muxws.

**Every rule in this document now has a witness of some kind**, and the kind is named for each. That
is a weaker claim than "everything is tested" and it is the one worth making: fourteen of the
nineteen would survive a rename of the test that holds them without anyone noticing, and the two CI
jobs are falsifiable only for the codecs their matrix enumerates.

Narrowed further: the previous edition's *"no witness at all"* table held **19** ids and now holds
**none**. Fifteen gained a citing test in the M8 audit (`WSM-CDC-007` among them, which had one all
along); `WSM-PKG-004` is held by the linters; `WSM-RCN-025` and `WSM-RCN-028` are split rules whose
library half an existing test holds; and `WSM-AUT-003` was the last, closed by reading the
`Authorization` header at the upgrade across three dials rather than by watching frames.

Two corrections to the previous snapshot, both of which made this document overstate its own
ignorance: `WSM-CDC-007` was cited all along in `muxws/conformance_test.py` and
`ts/conformance.spec.ts`, and `WSM-FRG-010` in `demo/backend_python/handlers_test.py`. The second was missed
because of Appendix A's under-scoped grep; the first was simply an error.

### Retired — no test is owed

§6: `WSM-CON-001` (standing for `-001`…`-008`) and `WSM-STM-022`. Their consequences are asserted by
the absence tests listed in §6.

### Witnessed by a test written for this rule (added in the M8 audit)

Each row names the rule, the test, and **the mutation that was applied to the frozen library to prove
the test can fail.** Every mutation below was run; every one produced a failure; the library was
restored after each. A row without a demonstrated mutation does not belong in this table.

| Rule | Test | Mutation that makes it fail |
|---|---|---|
| `WSM-API-003` | `peer_test.py::test_open_takes_zero_mandatory_arguments_and_defaults_payload_to_null`; `ts/peer.spec.ts` *"takes zero mandatory arguments and defaults payload to null"* | (a) `open(payload: Any, …)` — payload made mandatory; (b) the `open` frame built with `payload if payload is not None else ABSENT`, so the wire carries no `payload` key. Each fails a different assertion, so both halves are separately live. In TypeScript (b) is `?? ABSENT` in `resolveCall`; the "zero mandatory" half is caught by `tsc` rather than by vitest — see *non-test witnesses*. |
| `WSM-API-008` | `peer_test.py::test_the_public_api_is_async_exactly_where_the_rule_says` | `async def open`. Set equality in both directions over 167 public callables, so the rule's second sentence is asserted rather than assumed. |
| `WSM-AUT-002` | `peer_test.py::test_per_stream_headers_arrive_unchanged_and_change_nothing`; `ts/peer.spec.ts` *"never interprets per-stream headers"* | Four, all caught in Python: the receiver folds header keys to lower case; the sender strips `authorization`; the acceptor refuses a stream whose `expires_at` has passed; **and the acceptor branches on `authorization` to emit one extra frame that changes no outcome** — the last is caught only by the frame-for-frame comparison of the with-headers and without-headers exchanges, which is the assertion that makes this a witness for "interprets" rather than for "delivers". |
| `WSM-AUT-004` | `peer_test.py::test_the_opening_payload_is_never_looked_at_for_routing` | (a) `on_stream(handler, path=None)`; (b) `_start_handler` refuses any open whose payload names no `path`. Four payloads that ask to be routed — two path/method pairs, a bare string, a `null` — all reach one handler in order. |
| `WSM-ERR-003` | `peer_test.py::test_no_hook_can_intercept_an_error_on_its_way_to_the_call_site` | a `Peer.on_error` hook added — fails on the exhaustive hook-name set. The rule's force is that there is nowhere to divert an error *to*; see *bounded witnesses* for what this test does not prove. |
| `WSM-ERR-007` | `errors_test.py::test_nothing_in_the_error_hierarchy_carries_a_status_code` | (a) `RemoteError.status_code = 500`; (b) `default_error_serializer` grows a `"status": 500` key. Both caught. |
| `WSM-ERR-010` | `peer_test.py::test_request_arms_no_deadline_unless_it_is_given_one`; `ts/peer.spec.ts` *"has no default request timeout"* | (a) `timeout: float \| None = 30.0` on `Peer.request` — caught by the signature half; (b) `_collect_unary` substituting `30.0` for `None` with the signature left alone — caught by the `asyncio.wait_for` spy, with `timeout=0.02` as the control that proves the spy fires at all. The two halves are independently live. TypeScript counts `setTimeout` calls and is killed by a default in either `collectUnary` or `resolveCall`. |
| `WSM-FRM-006`, `WSM-INV-016` | `peer_test.py::test_a_payload_spelled_like_an_envelope_is_carried_and_never_read`, `::test_an_envelope_lookalike_payload_survives_being_fragmented`; `ts/peer.spec.ts` *"defines no vocabulary inside payload"* | (a) the sender lifts `end` out of the payload — caught twice over, by the subscript recorder and by the envelope assertions; (b) a *discarded* read `_ = payload["kind"]` — caught by the recorder alone; (c) on the fragmented path, the receiver peeks inside a fragment for `"type":"reset"` and resets; (d) the reassembler merges a nested `payload` key into the envelope. In TypeScript, `toMapping` deleting `payload.kind` in place, and the receiver acting on `payload.type`. |
| `WSM-FRM-012`, `-013` | `frames_test.py::test_v1_frame_types_is_exactly_the_six_of_the_specification`, `::test_end_and_trailers_are_flags_and_not_frame_types`; `ts/frames.spec.ts` `describe('the v1 frame set')`; `ts/peer.spec.ts` *"puts only the six v1 frame types on the wire"* | adding `end` and `trailers` to `V1_FRAME_TYPES`; removing `goaway` from it; and `toMapping` spelling a trailer-bearing frame as `{"type":"trailers"}`. All three caught. Asserted by **set equality**, never by membership — every prior reference to this constant asked whether a type it already had was in it, which a seventh member satisfies just as well. |
| `WSM-INV-001` | `packaging_test.py::test_no_library_module_imports_anything_above_it_in_the_stack`, `::test_every_third_party_import_is_an_extra_and_lives_only_in_its_own_module`, `::test_the_import_walker_sees_what_it_is_trusted_to_see` | (a) a `TYPE_CHECKING`-only `from fastapi import WebSocket` in `stream.py`; (b) a lazy `import httpx` inside a `Peer` method; (c) `import msgpack` in `writer.py`. All three caught. An AST walk, not a subprocess import-blocker, because the rule names the type-checking-only annotation explicitly and a runtime blocker structurally cannot see one. The third test is the control: a walker that found no modules would make the other two vacuously green. |
| `WSM-SID-001` | `peer_test.py::test_no_public_entry_point_lets_a_caller_supply_a_stream_id`; `ts/peer.spec.ts` *"accepts no stream id at any public entry point"*, *"… on request, notify or the stream sends"* | `stream_id: int \| None = None` added to `Peer.open`. In TypeScript, `'stream'` added to `OPEN_OPTION_KEYS`/`REQUEST_OPTION_KEYS` and read in `allocateAndEnqueue`. **See the known deviation below: this rule is not fully satisfied by 1.0.** |
| `WSM-AUT-001` (library clause) | `ts/peer.spec.ts` *"never interprets per-stream headers"* plus the signatures named in the rule | the mutations listed for `WSM-AUT-002`. The rule's other clause has a different subject; see *genuinely unwitnessable*. |

### Exercised by a differently-named test

These have no citation, but a test does hold them. Left uncited deliberately in some cases and by
oversight in others; either way the witness exists and is named here.

| Rule | The test that actually holds it |
|---|---|
| `WSM-API-013` | `stream_test.py::test_await_then_iterate_raises_and_first_consumer_got_everything`, `test_result_timeout_uses_the_same_future` |
| `WSM-BPR-002` | `caps_test.py::test_window_update_is_never_sent` with `test_the_limit_is_never_announced_and_never_checked_by_the_sender` |
| `WSM-CDC-003` | `codec_test.py::test_no_codec_branching_exists_in_the_peer` |
| `WSM-FRG-013` | `codecs/json_test.py::test_payload_round_trip_is_independent_of_the_envelope`, `writer_test.py::test_the_writer_cuts_where_the_splitter_cuts` |
| `WSM-FRG-033` | `conformance/invalid/fragment-interrupted-by-non-fragment.json` via `conformance_test.py::test_invalid_corpus_produces_the_declared_frame_and_survival` |
| `WSM-FRM-010`, `-011`, `-014` | the frame corpus (`frames_test.py::test_conformance_wire_decodes_to_frame`) plus `stream_test.py::test_every_state_table_cell` |
| `WSM-INV-003` | `fragment_test.py::test_binary_codec_slices_at_byte_boundaries` |
| `WSM-INV-009` | `stream_test.py::test_await_twice_returns_same_value`, `test_await_then_iterate_raises_and_first_consumer_got_everything` |
| `WSM-SID-003` | `lifecycle_test.py::test_connection_level_frames_omit_stream` |
| `WSM-STM-012`, `-013` | `stream_test.py::test_every_state_table_cell`, `test_send_with_end_half_closes_in_one_call` |
| `WSM-STM-015` | `peer_test.py::test_frame_for_closed_id_is_ignored` |

### Non-test witnesses

Real enforcement, by something other than a test. Each was run against a deliberate violation and
each rejected it; none of them is a promise.

| Rule | What enforces it | Verified by |
|---|---|---|
| `WSM-PKG-004` | `eslint` and `ruff`, via `npm run lint:ci` and `ruff check .` | In a scratch tree using this repository's own `eslint.config.js` and `pyproject.toml`: `ts/streamState.ts` → `Filename is not in kebab case. Rename it to 'stream-state.ts'` (`unicorn/filename-case`); a double-quoted TypeScript string → `prettier/prettier`; a 153-column line → `vue/max-len`. On the Python side a single-quoted string → `Q000` and a 136-column line → `E501`. Renaming the file to `stream-state.ts` passes. **All three clauses of the rule are enforced** — it is a witness, but a lint job rather than a test, and a lint job does not run in `pytest`. |
| `WSM-API-003` (the "zero mandatory arguments" half) | `tsc --noEmit`, via `npm run lint:ci` | `?` is erased at runtime and `Function.length` is 2 either way, so vitest cannot see this half in TypeScript. Changing the overload to `open<T>(payload: unknown, …)` produces `ts/peer.spec.ts(1668,32): error TS2554: Expected 1-2 arguments, but got 0` — the type-checker fails at the new test's own bare `pair.dialer.open()`. The witness is the call site; the checker is the thing that reads it. |
| `WSM-TST-004` | the `cross-language` workflow's `scenario` jobs (`interop/drive.sh <acceptor> <dialer>`, both role assignments × both codecs) | a job, not a test |
| `WSM-TST-005` | the `cross-language` workflow's `reconnect` job (`interop/drive.sh <acceptor> <dialer> reconnect`, both role assignments) | a job, not a test |
| `WSM-CDC-007` (for the codecs it names) | the same workflow, plus `conformance_test.py::test_sequence_corpus_replays_under_msgpack` and `::test_the_binary_codec_fixture_is_replayed_rather_than_skipped_everywhere` | `interop/drive.sh python ts corpus json 12` passes locally and fails on each of three deliberate breaks: `PREFIX = "muxws.v9."` in `subprotocol.py` (`Unexpected server response: 400` — the pair genuinely cannot form and the job says so), a wrong pinned fixture count, and a codec pinned to `msgpack` for a JSON-configured run. **But see the gap below: this witnesses two codecs, not the rule's quantifier.** |

### Genuinely unwitnessable, and what would change that

A negative rule is not automatically untestable — this repository already witnesses absences by
reading its own source (`test_no_codec_branching_exists_in_the_peer`,
`test_window_update_is_never_sent`, and now the AST import walk of `WSM-INV-001`). The entries below
survive that objection: each names a subject the test rig genuinely cannot reach, and says what would
have to exist for it to.

| Rule / clause | Why no test can hold it | What would change that |
|---|---|---|
| `WSM-AUT-001`, application clause — *authentication happens at the upgrade, before `accept()`* | The subject is the deploying application's server, which muxws does not contain. A test of muxws can only show that muxws offers nowhere else to put authentication, which it does. | Nothing, in this repository. It is an obligation on a deployment, and the rule now says so in its own text. |
| `WSM-AUT-003`, application clause — *a connection whose credential expires SHOULD be closed with `goaway`* | Same subject, and a SHOULD. muxws does not know what a credential is or when one expires; that is `WSM-AUT-001`. | Nothing. |
| `WSM-RCN-025`, application clause — *no credential in the hello* | muxws never mints hello content and cannot distinguish a credential from any other value in an opaque payload. | Nothing behavioural. A prose assertion in `docs_test.py` — which already parses the documentation — would be a real, falsifiable witness that the `danger` admonition at `docs/guide/reconnect.md` still exists. That is a witness for the *documentation*, not for the rule, and it is the honest most that is available. |
| `WSM-RCN-028`, application clause — *`sessionStorage` is the recommended client lifetime* | It is a recommendation to an application about code muxws does not ship. It was never an unmet obligation; the rule text now says which half is which. | Nothing, and nothing should. |

**No rule is left without a witness of some kind.** `WSM-AUT-003`'s *library* clause — the reconnect
helper re-authenticates by dialling again — was the last, and it is now
`reconnect_test.py::test_every_reconnect_presents_the_same_credential_at_a_fresh_upgrade`. It asserts
at the **upgrade** and not on the wire, deliberately: `api._websocket_dialer` closes over `headers`
once and `ConnectionLoop` re-invokes that same closure, so what has to be observed is the HTTP
request, three times over. A test that watched frames could not tell a header that was sent from one
that was dropped, which is why every existing reconnect test — all of which drive
`DialableServer.dial` rather than that closure — left the clause uncovered. Proven by mutation:
dropping `additional_headers` from the dial fails it.

**`WSM-RCN-028`'s library clause is witnessable in TypeScript and is not yet witnessed.**
`grep -rn "localStorage\|sessionStorage" ts/` returns nothing, and the recommended pattern appears
only in `docs/guide/reconnect.md` as a fragment the application writes. An absence test over the
shipped bundle — it references neither storage API — is falsifiable and belongs in `ts/*.spec.ts`. In
Python the clause is held indirectly by
`reconnect_test.py::test_three_drops_replay_byte_identical_hellos`: an identity minted by muxws would
have to appear in the hello, and byte identity across three drops says none did.

**`WSM-CDC-007`'s quantifier is unwitnessed, and the missing witness is one assertion.** The rule is
"***Every*** codec that ships MUST have a live cross-language pair… A codec without that pair MUST NOT
ship." The workflow's matrix is a literal `include:` list of four legs naming `json` and `msgpack`,
and `conformance_test.py` pins the configured set as the literal `{JsonCodec.name,
MsgpackCodec.name}`. Register a third codec in both ports tomorrow and everything stays green: no
matrix leg exists for it, and nothing in either suite asserts the *set* of shipped codecs —
`registered_codecs()` is only ever asked `in` / `not in` (`codecs/registry_test.py:34`,
`codecs/msgpack__test.py:240,250`, `ts/codec.spec.ts`). The missing witness is a test asserting
`registered_codecs()` equals the set the CI matrix covers. It is writable in either language and it
must agree with a YAML file under `.github/`, which is why it is recorded here rather than guessed
at.

### Bounded witnesses — what the new tests still cannot see

A witness that is believed to prove more than it does is the failure mode this appendix exists for,
so the limits found while verifying the M8 additions are recorded rather than left to be
rediscovered.

- **`peer_test.py::test_an_envelope_lookalike_payload_survives_being_fragmented` compares against the
  object it handed to the library.** A library that consumed a reserved key by deleting it from the
  caller's own dict mutilates both sides of the comparison equally and the test passes — verified:
  popping `"kind"` from the caller's payload in `_allocate_and_enqueue` fails the sibling test and
  leaves this one green. The fix is the one its TypeScript counterpart already uses, snapshotting the
  payload before the send. The test remains a real witness for the merge and peek mutations listed
  above; it is not a witness for in-place consumption.
- **`ts/peer.spec.ts` *"never interprets per-stream headers"* compares stream-level frames only.** Its
  trace filters on `frame.stream === id`, so an acceptor that branches on `authorization` to emit a
  connection-level `ping` passes — verified. The Python twin compares the whole outbound frame list
  and catches it. The TypeScript claim is "no stream-level behaviour changed", not "nothing changed".
- **`WSM-ERR-010`'s Python witness sees deadlines armed through `asyncio.wait_for`.** That is what
  `_collect_unary` uses, and a default introduced there or in the signature is caught. A default built
  from `asyncio.timeout()` or `loop.call_later` would evade the spy. The TypeScript counterpart has
  the mirror-image limit: it counts `setTimeout`, so a deadline expressed some other way is invisible
  to it, and "still pending after the settle window" only rules out defaults shorter than that window.
  An airtight version needs a controllable clock in both ports.
- **`WSM-ERR-003`'s witness is the exhaustive hook-name set, not the delivery path.** That the
  exception reaches the `await` is asserted, and that only four hooks exist is asserted; that an
  observer *could not* consume a frame if one tried is not, because this implementation gives an
  observer no way to. The rule is held by there being nowhere to divert an error to, which is exactly
  what the hook-name set pins.

### One-port citations

A grep that finds a citation says nothing about *which* port it was found in, and for a handful of
rules that turns out to matter. **Twenty-five rules are now cited in one language only** — eighteen in
Python, seven in TypeScript. That is up from seventeen before the M8 audit, and the increase is the
audit's own doing: five of the rules it newly witnessed were witnessed in Python alone. They are
listed below rather than left for the next grep to rediscover.

*One-port by nature — no gap.* `WSM-API-017`, `WSM-PKG-002` are Python-language rules;
`WSM-API-015`, `WSM-API-020`, `WSM-API-022`, `WSM-CDC-015` are TypeScript-shaped. `WSM-STM-022` is
retired and owes nothing.

*Citation gap only — an equivalent test exists in the other port and does not name the rule.*
`WSM-API-012`, `WSM-FRG-010`, `WSM-FRG-011`, `WSM-INV-002`, `WSM-SID-008`, `WSM-STM-010`,
`WSM-STM-023`, `WSM-TST-001` (Python-cited; twins in `ts/peer.spec.ts`, `ts/stream.spec.ts`,
`ts/frames.spec.ts`); `WSM-API-002`, `WSM-API-023`, `WSM-AUT-001` (TypeScript-cited; twins in
`stream_test.py` and, for `WSM-AUT-001`, in
`peer_test.py::test_per_stream_headers_arrive_unchanged_and_change_nothing`).

*Real one-port gaps — no equivalent exists in the other port.*

- **`WSM-API-016` is a TypeScript-only rule whose only citation in the repository is in Python.** The
  sharpest one in the list. `stream_test.py` names it, in a docstring that opens *"The Python side of
  WSM-API-016"*. The rule is about attaching a no-op rejection handler at construction, which only
  TypeScript can get wrong. The TypeScript twin exists — `ts/stream.spec.ts` *"reports no unhandled
  rejection and does reach the error hook"* — and does not name it. A grep says witnessed; the
  language that can violate the rule never mentions it. The citation belongs in `ts/stream.spec.ts`.
- **`WSM-CDC-026` has no TypeScript witness at all.** `transports/starlette_test.py` holds it in
  Python, including the negative case where the application accepted first. Nothing in TypeScript
  asserts that `accept()` performs the upgrade or that the application must not accept first. It is
  partly structural — `ws` completes the handshake before the handler runs, which is why
  `WSM-CDC-027` exists and is tested — but `ts/node.ts` still exports `accept(socket)` and has no
  equivalent assertion.
- **`WSM-API-008`, `WSM-AUT-004`, `WSM-ERR-003`, `WSM-ERR-007`, `WSM-INV-001` were witnessed in Python
  in the M8 audit and nowhere else.** Four of them are writable in TypeScript essentially as written.
  `WSM-INV-001` is the one worth a note: `ts/packaging.spec.ts` already asserts that the entry point
  imports nothing optional, with controls, but it does so by *running* the import — and an
  `import type` is erased before it runs, exactly as a Python `if TYPE_CHECKING:` block is invisible
  to a subprocess blocker. The rule names the type-checking-only annotation explicitly, so the
  TypeScript witness has to read the source, as the Python one now does.

### A known deviation: `WSM-SID-001` is not fully satisfied by 1.0

`Stream` is a value export from `ts/index.ts` and a member of `muxws.__all__`, and its constructor
takes a stream id as its second positional argument in both ports. This puts a forged id on the wire,
with no cast in TypeScript and no private access in Python:

```python
forged = Stream(peer, 999, local=True)
await forged.send({"forged": True})
# tx {"type":"data","stream":999} -> rx goaway: data on stream 999, above the high-water mark 0
```

Verified in Python against a live pair: the frame goes out, and the remote correctly kills the
connection over an id its counterpart never allocated. The TypeScript constructor has the same shape
and the same export.

`WSM-SID-001` says a caller MUST NOT be able to supply an id **anywhere in the API**, and on a strict
reading this is a violation. The rule is not weakened here and the id is not retired: the wording is
right and the surface is wrong. What 1.0 actually guarantees is that no *stream-originating call* —
`open`, `request`, `notify`, `send`, `end` — takes one, and that is what the tests assert. The
constructor is pinned by set equality in
`peer_test.py::_ID_ARGUMENTS_THAT_ARE_NOT_AN_ALLOCATION`, with the gap named in the comment, so a
sibling cannot appear silently. Closing it properly means marking `Stream`'s constructor internal and
allocating through a factory the peer owns, which is a public API change and therefore work for the
next generation (WSM-PKG-005), not a patch to a frozen 1.0.

### Three holes no grep for an id would have found

**The splitter may drop a fragmented `reset`'s `reason` and nothing fails.** Making `reason` ride only
the first fragment leaves the closing fragment — the one the receiver acts on — with no reason at all,
and the whole Python suite still passes. The corpus triple named
`fragmented-reset-repeats-code-and-reason-on-every-fragment` pins how such a frame *decodes*; no
fixture asserts that the splitter *produces* it, because the boundary corpus splits a `data` frame
only. A reset's reason is what reaches a human in a log, so the gap is worth closing.

**How an acceptor refuses is now asserted, and the shape of the hole is worth keeping.** Until M6 both
reference acceptors answered **HTTP 101 with no `Sec-WebSocket-Protocol` header** and closed
afterwards, which WSM-CDC-022 forbids: `websockets`' `select_subprotocol` hook and `ws`'s
`handleProtocols` both treat "no selection" as "no subprotocol", not as "refuse the upgrade". Every
test dialled with the *same language's* dialer, which recovers via WSM-CDC-028's post-handshake check
and raises `CodecMismatch`, so both suites stayed green through three milestones; a cross-language
dial does not, and surfaced `ws`'s bare `Error: Server sent no subprotocol` instead — precisely what
WSM-CDC-024 forbids. The witness had to be an **HTTP request**, because a status code is invisible to
every peer-level test and to the whole conformance corpus. It is now one:
`transports/websockets_test.py` and `ts/node.spec.ts` each speak the upgrade by hand and assert the
status, that no subprotocol came back, and that the connection handler was never reached. Python
raises `NegotiationError` out of the hook (an `InvalidHandshake`, which `websockets` answers 400);
`ws` needs a second hook, `refuseMismatchedUpgrade`, because `handleProtocols` cannot refuse — a
`ws` acceptor that installs only the first one still violates WSM-CDC-022, and WSM-CDC-027 names no
hook for it.

**Nothing asserts that the two ports render the same log line.** §5.10's `reason=` differs between
them — `repr()` against `JSON.stringify` — and each port's own test asserts its own spelling, so the
divergence is invisible to both. A line format is a text interface an operator greps; one shape or
the other is right, and no test is in a position to say which.

**And one divergence in a published constant.** `V1_FRAME_TYPES` is a genuine `frozenset` in Python
and a mutable `Set` cast to `ReadonlySet` in TypeScript, so `(V1_FRAME_TYPES as Set<string>).add(…)`
succeeds at runtime in one port and not the other. No rule requires immutability and the constant is
read by neither implementation — it is published for consumers — so no test asserts either shape. If
§2.3's "six, and no others" is meant to be enforceable by a consumer rather than only by this suite,
the TypeScript constant should be frozen to match; that is a rule this document does not currently
have.
