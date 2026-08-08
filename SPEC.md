# muxws — normative specification for the `muxws.v1.` generation

This document specifies **muxws**: framing, multiplexing, stream lifecycle, cancellation and
connection lifecycle over one WebSocket. It is written to be implemented from — a third port should
need nothing else to interoperate with the Python and TypeScript ports in this repository.

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
| `headers` | object | no | absent | `open` | Application metadata; string keys, codec-encodable values. Never interpreted by muxws. |
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
{"type":"data","stream":7,"payload":{"rows":128}}
{"type":"data","stream":7,"end":true,"payload":{"rows":4},"trailers":{"checksum":"deadbeef"}}
{"type":"data","stream":7,"end":true}
{"type":"reset","stream":13,"code":2,"payload":{"type":"ValueError","message":"no such report"},"reason":"handler raised"}
{"type":"ping","nonce":"8f14e45fceea167a"}
{"type":"pong","nonce":"8f14e45fceea167a"}
{"type":"goaway","code":0,"last_stream":7,"reason":"shutting down"}
```

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
— no `kind`, no reserved key, no discriminator of any sort (WSM-FRM-006).

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
- **WSM-API-008** `connect()`, `accept()`, `serve()`, `notify()`, `request()`, `ping()`, `close()`
  and every `Stream` send method MUST be async. Everything else MUST NOT be.
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

**Call shapes.** Unary is `open(end)` → `data(end)`; a streaming response is `open(end)` → `data`…
`data(end)`; bidirectional is `open` → interleaved `data` both ways → `data(end)` both ways; a
one-shot push is `open(end)` with nothing awaited. Durations are **seconds as floats in Python** and
**milliseconds in TypeScript**; wire field names stay snake_case in every language.

### 5.2 `WSM-AUT-` — authentication, headers, routing

- **WSM-AUT-001** Authentication MUST happen at the WebSocket upgrade, before `accept()` is called.
  muxws MUST NOT interpret credentials anywhere.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application and
  MUST NOT be used for re-authentication by the library.
- **WSM-AUT-003** A connection whose credential expires mid-life SHOULD be closed with `goaway`; the
  dialer's reconnect helper re-authenticates by dialling again.
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
  MUST expose `select_subprotocol`, a plain callable installable in that transport's handshake hook,
  implementing WSM-CDC-021/022.
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
- **WSM-FRG-021** `headers` MUST NOT be fragmented. An `open` whose headers alone push the frame over
  the cap MUST be rejected by the receiver with `reset(PAYLOAD_TOO_LARGE)`.
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
- **WSM-FRM-015** `ping`, `pong` and `goaway` are connection-level and MUST omit `stream`, or set it
  to `0`.

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
- **WSM-RCN-025** A credential MUST NOT be carried in the hello — authentication is a handshake
  concern (§5.2).
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
- **WSM-RCN-028** muxws MUST NOT mint or store a tab identity. The recommended (documented, not
  implemented) client lifetime is `sessionStorage`; `localStorage` is wrong — shared across tabs of
  the origin — and module scope is wrong, because it dies on reload.
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
  survives, nothing goes out).
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
2. **The invalid corpus.** All eight cases of WSM-TST-003, each asserting the outgoing frame *and*
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
grep -rl "WSM-STM-036" --include='*_test.py' --include='*.spec.ts' muxws ts
```

Two things a naive grep gets wrong. Tests cite combined ids (`WSM-API-006/007`,
`WSM-RCN-041/WSM-STM-014`), so `WSM-API-007` must be searched for as a bare number after a family
prefix too; and some rules are witnessed by a fixture rather than by a citation, in which case the
fixture file name is the thing to search for under `conformance/`.

## Appendix B — rules with no citation in any test

These ids appear in no `*_test.py` or `*.spec.ts` file, even after resolving the combined-id
shorthand. It is a **snapshot**, and the way to refresh it is to run the grep of Appendix A over
every id in this document. The list is part of the specification's honesty, not a licence to ignore
the rules; several of them *are* exercised by a test that simply does not name them, and that test is
given where it exists.

**Retired — no test is owed** (§6): `WSM-CON-001` (standing for `-001`…`-008`) and `WSM-STM-022`.
Their consequences are asserted by the absence tests listed in §6.

**Exercised by a differently-named test:**

| Rule | The test that actually holds it |
|---|---|
| `WSM-API-013` | `stream_test.py::test_await_then_iterate_raises_and_first_consumer_got_everything`, `test_result_timeout_uses_the_same_future` |
| `WSM-BPR-002` | `caps_test.py::test_window_update_is_never_sent` with `test_the_limit_is_never_announced_and_never_checked_by_the_sender` |
| `WSM-CDC-003` | `codec_test.py::test_no_codec_branching_exists_in_the_peer` |
| `WSM-FRG-010`, `-011`, `-013` | `fragment_test.py::test_slice_point_sweep_never_exceeds_cap`, `codecs/json_test.py::test_payload_round_trip_is_independent_of_the_envelope`, `writer_test.py::test_the_writer_cuts_where_the_splitter_cuts` |
| `WSM-FRG-033` | `conformance/invalid/fragment-interrupted-by-non-fragment.json` via `conformance_test.py::test_invalid_corpus_produces_the_declared_frame_and_survival` |
| `WSM-FRM-010`, `-011`, `-014` | the frame corpus (`frames_test.py::test_conformance_wire_decodes_to_frame`) plus `stream_test.py::test_every_state_table_cell` |
| `WSM-INV-003` | `fragment_test.py::test_binary_codec_slices_at_byte_boundaries` |
| `WSM-INV-009` | `stream_test.py::test_await_twice_returns_same_value`, `test_await_then_iterate_raises_and_first_consumer_got_everything` |
| `WSM-SID-003` | `lifecycle_test.py::test_connection_level_frames_omit_stream` |
| `WSM-STM-012`, `-013` | `stream_test.py::test_every_state_table_cell`, `test_send_with_end_half_closes_in_one_call` |
| `WSM-STM-015` | `peer_test.py::test_frame_for_closed_id_is_ignored` |
| `WSM-TST-004` | the `cross-language` workflow's `scenario` jobs (`interop/drive.sh <acceptor> <dialer>`, both role assignments × both codecs) — a job, not a test |
| `WSM-TST-005` | the `cross-language` workflow's `reconnect` job (`interop/drive.sh <acceptor> <dialer> reconnect`, both role assignments) — a job, not a test |

**No witness at all.** These are the ones to write next, or to accept knowingly:

| Rule | Why it is unwitnessed |
|---|---|
| `WSM-API-003` | nothing asserts that `open()` is callable with no arguments |
| `WSM-API-008` | which calls are async is asserted nowhere; it is visible only in the signatures |
| `WSM-AUT-001`…`-004` | negative rules about what muxws must not interpret; `peer_test.py::test_second_on_stream_replaces_and_logs` is the closest thing, for `-004` |
| `WSM-CDC-007` | the live cross-language pair is a CI job, and a job is not a test; nothing in either suite can fail when it is missing |
| `WSM-ERR-003`, `-007`, `-010` | negative and structural: nothing swallowed into a callback, no status-code mapping, no default request timeout |
| `WSM-FRM-006`, `WSM-INV-016` | "muxws defines no vocabulary inside `payload`" has no positive assertion |
| `WSM-FRM-012`, `-013` | nothing asserts that `V1_FRAME_TYPES` is exactly the six of §2.3, so the absence of an `end` or trailers frame type is unpoliced |
| `WSM-INV-001` | no test proves muxws imports nothing above it in the stack |
| `WSM-PKG-004` | enforced by `ruff` and `eslint` (`unicorn/filename-case`), not by a test |
| `WSM-RCN-025`, `-028` | documentation rules: no credential in the hello, no tab identity minted |
| `WSM-SID-001` | the parity test covers allocation, nothing covers "a caller can never supply an id" |

Two further holes, none of which a grep for an id would have found.

**The splitter may drop a fragmented `reset`'s `reason` and nothing fails.** Making `reason` ride only the
first fragment leaves the closing fragment — the one the receiver acts on — with no reason at all,
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
