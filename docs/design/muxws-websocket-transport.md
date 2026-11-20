---
title: muxws - multiplexed WebSocket transport
sidebar: false
search: false
outline: deep
---

# muxws

> Design rationale for the rules in `SPEC.md`. Not normative: where the two differ, `SPEC.md` applies.

muxws is a WebSocket protocol and reference implementation that mimics the *semantics* of HTTP/3:
many independent, concurrently interleaved streams over one socket, either peer able to open a
stream, per-stream cancellation, request/response and streaming-response shapes - plus unsolicited
pushes from the acceptor, which follow from the symmetry without a separate mechanism.

muxws knows nothing about viewsets, progress reporting, dialogs, tasks or Celery. The dependency
direction is strict and never inverted:

```
fastapi-viewsets  --optional-->  backchannel  --optional-->  muxws
        \                                                      ^
         \--------------------- optional ---------------------/
```

muxws depends on none of the packages above it and must be usable standalone by anyone who wants
multiplexed streams over a WebSocket. fastapi-viewsets' WebSocket transport adapter depends on muxws
directly, not through backchannel, because the two packages use different parts of muxws - see
"Where muxws sits in the stack" for the whole graph and "Two vocabularies over one socket" for what
happens when both of them are on one connection at once.

---

## Purpose and scope

A browser and a server that talk over one WebSocket usually implement the same three things
themselves: a correlation id so replies can be matched to requests, a convention for "this reply has
more parts coming", and a way to cancel one operation. muxws is those three things, specified once,
implemented once per language, with a state machine behind them so the edge cases (cancel racing
with completion, socket death mid-stream, a peer that stops reading) have defined answers instead of
ad-hoc ones.

The scope is exactly: **framing, multiplexing, stream lifecycle, cancellation, connection lifecycle.**

### Non-goals

- **Routing.** muxws does not look at the opening payload. No path matching, no method dispatch, no
  handler table. There is one registration point for incoming streams; the application decides what
  the opening payload means. (See "One registration point, not a router".)
- **Serialization of domain objects.** Payloads are whatever the connection's codec can encode - JSON
  values under the default codec. Turning a `datetime` or an ORM instance into one is the
  application's job, exactly as it is for an HTTP body.
- **Authentication and authorization.** Auth happens at the WebSocket handshake (see "Auth is a
  handshake concern"). muxws never interprets credentials.
- **Durable state, delivery guarantees, resumption across reconnects.** A dropped socket resets every
  stream on it and takes the peer's `tags` with it. Anything that must survive a reconnect needs
  persistence, which is an application concern - and `backchannel` is the worked example of doing that
  correctly one layer up.
- **RPC ergonomics** - no schema, no codegen, no typed method registry. That belongs to whoever
  builds on top (fastapi-viewsets' future WS adapter is one such consumer).
- **Broadcast / pub-sub / rooms.** muxws gives you a peer and a way to find peers by an
  application-supplied key. Fan-out is a loop over peers, in application code.
- **Replacing HTTP.** File uploads, caching, CDNs, and anything that benefits from being a plain
  URL should stay plain HTTP.

---

## Where muxws sits in the stack

muxws is the bottom of a stack that will eventually be composed whole in a real application:
`muxws`, `dynamicforms-backchannel` and `dynamicforms-fastapi-viewsets` on the Python
side, their npm counterparts on the frontend, plus `@dynamicforms/vuetify-modal-form-kit` - which
itself sits on `@dynamicforms/vue-forms` and `@dynamicforms/vuetify-inputs` - rendering the dialogs
backchannel asks for.

```
   @dynamicforms/vuetify-modal-form-kit
   (on @dynamicforms/vue-forms + @dynamicforms/vuetify-inputs)
                   |
                   |  renders what backchannel asks
                   v
   fastapi-viewsets  ------ optional ------>  backchannel
          |                                        |
          |  optional                              |  optional
          |  (its own WS transport adapter)        |  (MuxwsTransport)
          v                                        v
          +----------------> muxws <---------------+
                    depends on none of the above
```

The graph encodes one rule: **muxws has no dependency on any package above it**: no import, no
optional extra and no `TYPE_CHECKING` import. A muxws that referenced viewsets or progress tokens
would not be usable on its own and would fail the test in "How fastapi-viewsets will consume this".

The two packages that depend on muxws use different parts of it: backchannel uses exactly three
things (`notify`, `peers_for`, and symmetric `open` as the property those stand on), while the
fastapi-viewsets WS adapter uses `request`, `stream.send` and `stream.cancel` and never touches the
registry. The backchannel document states which package owns which decision when all of them are
deployed together, and how a dialog reaches a Vuetify modal from a Celery worker; this section
states only the dependency direction.

---

## One implementation, not two

The central architectural claim, and the first question the design has to answer, in the user's own
words:

> "ne vem, če je sploh treba ločiti med 'serversko' in 'front-end' implementacijo. Po mojem je samo
> implementacija protokola ter API, ki se kliče, da se nek ukaz pošlje na drugo stran"

There is **one symmetric `Peer` type per
language**, not a server implementation and a client implementation. A peer owns a socket, encodes
and decodes frames, tracks stream state, dispatches incoming streams to one handler, and exposes
`open` / `notify` / `request` / `send` / `cancel`. Everything in that list is direction-agnostic.

"Client" and "server" differ in exactly two things, and both are constructor-time facts:

1. **Who dials.** One side calls `connect(url)`; the other side is handed a socket by its web
   framework, after that framework's own authentication has passed, and calls `accept(socket)` to
   complete the WebSocket handshake (which includes asserting the codec subprotocol - see "A mismatch
   guard, not a negotiation"). After that moment the two objects are the same type with the same
   methods.
2. **Stream-id parity.** The dialer allocates odd ids, the acceptor even ones. This is the only
   asymmetry that persists for the life of the connection, and its entire purpose is to make id
   allocation collision-free without negotiation.

The symmetry has two consequences:

**Server push needs no additional protocol mechanism.** There is no push frame, no subscription
mechanism, no "server-initiated message" special case. The acceptor calls `peer.open(payload)` (or
`peer.notify(payload)`, which is the same thing ended immediately) with an even id, and the dialer's
`on_stream` handler fires. The dialer can answer on that stream, stream back, or cancel it - all the
same code paths as any other stream, in both directions. A protocol in which the server only replies
needs a separate push mechanism with its own correlation and cancellation; muxws has none.

**A Python peer may be the dialer and a TypeScript peer may be the acceptor.** Service-to-service
Python, a sidecar process that dials into a hub, a Node process accepting connections - all of these
are supported by construction, not by writing new code. The two language ports are ports of the
*same* peer, which is also what makes the cross-language conformance suite (see Testing) meaningful:
either port can be the dialer or the acceptor.

---

## Problem: one socket, many concurrent operations

With one JSON message in and one out, one at a time, two concurrent operations cannot be told apart:
when two replies are in flight, nothing on the wire identifies which reply belongs to which request.
The universal fix is a correlation id in every message.

A correlation id needs three additions: an end marker for a reply in several parts (a *stream*),
*cancellation* of one operation without closing the socket, and *local* failure of every in-flight
operation with a distinguishable error when the socket closes.

A correlation id with a lifecycle, an end marker, a cancel signal and a defined failure on socket
loss is a stream, the model of HTTP/2 and HTTP/3. muxws takes that model - stream ids, frames,
per-stream state, connection-level control frames - and puts it on a WebSocket.

**One WebSocket per operation.** Each one costs a full HTTP
upgrade (TLS session reuse helps, the round trip does not disappear), browsers cap concurrent
connections per origin, every socket needs its own auth and its own reconnect logic, and the server
pays a connection's worth of memory per in-flight operation. HTTP/2 multiplexes over one connection
for the same reasons.

**Socket.IO and generic pub-sub libraries.** They provide named events and rooms, a broadcast model,
without request/response correlation, a typed end, per-operation cancellation or a terminating
streaming reply; muxws provides these four.

---

## Problem: who invents the stream id

The user's first guess at the mandatory API surface:

> "Pazi, da bo API čist ter maksimalno preprost za uporabo, z minimalnim številom obveznih parametrov
> (na hitro se mi zdi stream id ter payload edino, kar bi bilo res obvezno?)"

**The stream id is never a caller argument.** It is allocated
by the library, appears on the wire, and is exposed read-only as `stream.id` for logging and
correlation - but no caller ever invents one.

If callers picked ids, two things break. First, both peers can open streams, so two independently
chosen ids can be equal; the collision raises no error and merges two operations into one stream.
Second, an id is only meaningful within one connection: after a reconnect every id is stale, and any
caller that cached one is now addressing a stream that does not exist. Making the library the sole
allocator makes both failure modes unrepresentable.

Parity settles the collision problem without a handshake, exactly as HTTP/2 does it:

- **The dialer allocates odd ids:** 1, 3, 5, ...
- **The acceptor allocates even ids:** 2, 4, 6, ...
- **Id 0 is reserved** for connection-level frames (`ping`, `pong`, `goaway`), which in
  practice omit the field entirely.
- **Opens are monotonic per peer**, and ids are never reused, even after a stream closes. An `open`
  frame naming an id not greater than the highest id that peer has previously *opened* is a
  connection-level protocol error: it means the peers disagree about the id space, and there is no
  safe recovery. `data` and `reset` may name any previously-opened id in any order - streams
  interleave, so once stream 9 is open, every frame on the still-live stream 7 names a lower id.
- **Exhaustion** at 2^31-1: the exhausting peer sends `goaway` with `last_stream` set to the highest
  id it has processed, stops opening new streams, lets in-flight ones drain, and closes. A client
  peer with the reconnect helper enabled then dials a fresh socket with a fresh id space. At one
  stream per user action the limit is not reached in practice; the rule defines the behaviour when
  it is.

So the required surface for starting a stream is `open(payload)` - and `payload` itself has a
default of `null`, because a stream whose meaning is entirely in its headers is legitimate. `open()`
has no mandatory arguments. Continuing a stream is `stream.send(payload)`. Everything
else - headers, the end flag - is a keyword argument with a default.

Because the caller does not choose the id, `open()` returns an object that carries it and serves
both as a handle (`stream.id`, `stream.send`, `stream.cancel`) and as an awaitable; see "`open()` is
synchronous, and the handle it returns is itself awaitable".

---

## What is and is not mimicked

muxws mimics only the *semantic* model of HTTP/3.

**What is not mimicked.** A WebSocket runs over one TCP connection (or one HTTP/2
stream, if the WebSocket runs over h2, which adds blocking by the other streams of that HTTP/2
connection). Therefore:

- **No independent per-stream loss recovery.** In QUIC, a lost packet stalls
  only the stream it belonged to. Under TCP, one lost segment stalls *every* muxws stream until it is
  retransmitted. Head-of-line blocking at the transport layer is unavoidable and no framing choice
  above it can remove it.
- **No 0-RTT, no connection migration, no separate congestion control per stream.** Those live in
  QUIC, below where muxws sits.
- **Global message ordering.** WebSocket delivers messages in the order sent, across all streams.
  There is exactly one send queue.

**What is mimicked:** multiplexed independent streams over one connection; headers plus body plus
(optional) trailers; unary request/response and streaming-response shapes on the same primitive;
per-stream cancellation that does not disturb other streams; connection-level graceful shutdown via
`goaway`. Not the capability negotiation: HTTP/2 and HTTP/3 open with a `SETTINGS` exchange and muxws
has none at all, for reasons "Limits are local, and the frame cap is a constant" gives in full.

**The resulting engineering rule.** Because ordering is global and there is one send queue, *a
single large frame blocks every other stream for as long as it takes to transmit*. A 1 MB DATA frame
on a stream doing a bulk export, on a link with 1 MB/s of usable uplink, adds a full second of
latency to a 200-byte progress update on another stream. A sender that does not fragment produces
this delay.

Therefore: **the implementation MUST fragment any logical payload whose encoded frame would exceed
the protocol constant `MAX_FRAME_BYTES`, 65536 bytes (64 KiB)** - the exact slicing rule is in
"Fragmentation" below - **and MUST interleave frames from other streams between the fragments.** Two
rules implement the interleaving:

- **A stream may have at most one unsent fragment queued at any moment.** The producing task encodes
  fragment *n+1* only after fragment *n* has been handed to the socket. A sender that enqueues all
  sixteen fragments of a 1 MB payload at once fixes their order before the writer selects them.
- **The writer selects the next frame by round-robin over the streams that have queued work.** A
  FIFO queue cannot interleave fragments already in it, because their order is fixed at enqueue
  time.

At 1 MB/s those two rules together bound the blocking window at roughly 64 ms - one fragment - for a
progress frame on another stream; at LAN speeds it is proportionally shorter. **The cap is a
constant, not a negotiated value**: it bounds how long one stream may occupy a socket that has a
single global message order, and a larger frame lengthens that time. A negotiated cap would be
raised by links with spare bandwidth, which lengthens the time other streams wait. Fragmentation is
therefore not optional and not tunable, and a receiver that declines an over-constant frame resets
that stream with `PAYLOAD_TOO_LARGE` rather than accepting it. Fragmentation in the sender
compensates for the single message order of a WebSocket.

---

## Wire format

**One frame per WebSocket message.** How a logical frame becomes that message is the job of the
connection's **codec**, which is a deployment decision rather than a wire constant - the port, how it
is chosen, and how the two peers are prevented from disagreeing about it are all in "The codec is a
deployment decision" below.

**JSON text is the default codec.** It is readable in the browser devtools Network tab without
tooling. For the traffic muxws targets (control messages, progress updates, moderate result sets),
its size overhead is small. A deployment that needs a more compact encoding sets another codec in
its `.env`; the rest of this section applies unchanged.

JSON is also the **interoperability baseline**: it is the one encoding every muxws peer in either
language must implement, and must produce identically, so the conformance corpus in "Testing
strategy" asserts on exact wire bytes. Everything below - the envelope fields, the size accounting,
the fragmentation rule, the frame types, the state machine - is specified over the *logical* frame
and holds under any codec. The JSON rendering is shown throughout because it is readable; the rules
apply to every codec.

Field names are spelled out rather than abbreviated, for the same debuggability reason.

### Envelope

```json
{"type": "data", "stream": 7, "payload": {"rows": 128}, "end": false}
```

| Field | Type | Present on | Meaning |
|---|---|---|---|
| `type` | string | every frame | Frame type, see below. |
| `stream` | int | stream-level frames | Stream id. Omitted (or `0`) on connection-level frames. |
| `payload` | any codec value | `open`, `data`, `reset` | The application value - any JSON value under the default codec, plus raw bytes under a binary one. On `reset` it is the optional structured error object (see error codes). Omitted means "no payload", which is distinct from `null` only if the application chooses to care. |
| `fragment` | string or bytes | `open`, `data` | A slice of the codec-encoded form of the logical payload, cut at a boundary the codec can represent: a Unicode codepoint for a text codec, a byte for a binary one. Mutually exclusive with `payload`. |
| `more` | bool | frames with `fragment` | `true` on every fragment but the last. |
| `headers` | object | `open` | Application metadata, string keys, codec-encodable values. Never interpreted by muxws. |
| `end` | bool | `open`, `data` | This is the last frame this peer will send on this stream. Default `false`. |
| `trailers` | object | frames with `end: true` | Post-body metadata (row counts, checksums, a partial-failure note). |
| `code` | int | `reset`, `goaway` | See error codes. |
| `reason` | string | `reset`, `goaway` | Human-readable, for logs. Never parsed. |
| `nonce` | string | `ping`, `pong` | Opaque, echoed verbatim in the `pong`. Sender-chosen; a counter or a short random string. |
| `last_stream` | int | `goaway` | The highest id from the *other* peer that this peer has processed and will still complete. |

Unknown fields are ignored, and unknown *frame types* are ignored too (logged once). This is a
forward-compatibility rule: a v1 peer never sends a frame type this document does not define, so
nothing on a v1 connection can rely on the other side doing something with one. A frame type that
must be *acted on* rather than tolerated needs both peers to be known to understand it, and the only
place this design has to say that is the subprotocol generation - see "Versioning lives in the
subprotocol".

### How sizes are counted

**Every size limit in this specification is the byte length of the fully encoded WebSocket message** -
the complete codec output for the frame, envelope included, exactly as it goes on the wire. Not the
length of the payload before encoding, not the length of a `fragment` field, and - for a text codec in
particular - not a JavaScript string's `.length`, which counts UTF-16 code units and therefore
disagrees with Python's byte count on every non-BMP character. Under the JSON codec the TypeScript
port measures with `TextEncoder().encode(text).length` (or the equivalent incremental count) and the
Python port measures `len(text.encode("utf-8"))`; under a binary codec both take the length of
the produced buffer. In every case the number compared against `MAX_FRAME_BYTES` is the length of what
the socket is about to send.

Measuring the codec's output, not the JSON text, makes both ports cut a payload at the same
boundaries under every codec, which the conformance corpus asserts. A sender that measured JSON text
while sending msgpack bytes would produce frames over the cap.

### Fragmentation

Any frame that may carry `payload` may instead carry `fragment` + `more`. The receiver concatenates
the `fragment` values and hands the result back to the codec for decoding when a fragment arrives
without `more: true`. The sender's rule is specified precisely, because the rule ("slice the payload
every `MAX_FRAME_BYTES` bytes") produces frames that no conforming receiver will accept and slices
that cannot be encoded at all.

The shape of the rule is codec-independent: **the sender encodes the logical payload with the
connection's codec, slices that encoded form, and puts each slice into a frame that the codec then
encodes again.** Two facts make slicing every `MAX_FRAME_BYTES` bytes wrong under every codec; the
JSON codec illustrates both.

First, a slice has to land on a boundary the codec can represent. Under JSON, `fragment` is a
*string*: a cut at an arbitrary byte offset can land in the middle of a multi-byte UTF-8 sequence,
and a half sequence is not representable as a JSON string value - the frame cannot be encoded. Under
a binary codec every byte offset is representable; the rule is the same, and the splitter is written
once against it.

Second, the slice is *re-encoded and enveloped* before it goes on the wire, and that step expands it.
JSON string escaping expands by up to six times per character (one 0x1f byte becomes the six
characters `\u001f`), so a slice of exactly `MAX_FRAME_BYTES` always
yields a message larger than `MAX_FRAME_BYTES`. A binary codec expands by a
small header constant rather than by a factor - but never by nothing, so a sender that slices to
exactly the cap is over it under every codec.

So, normatively:

- **The cap bounds the whole encoded message**, as the codec produced it. A receiver measures the
  entire frame against `MAX_FRAME_BYTES` and may reset the stream with `PAYLOAD_TOO_LARGE` if it is
  over. It never measures the `fragment` field on its own. A receiver is permitted to be more
  tolerant than that and accept a larger frame - the constant binds the *sender* absolutely and the
  receiver only as a floor of what it must accept - because accepting a larger frame breaks nothing,
  and rejecting one is a local defence, not a protocol requirement.
- **Slices are cut at a boundary the codec can encode** - Unicode codepoint boundaries of the encoded
  text for a text codec, byte boundaries for a binary one. Under JSON in TypeScript that additionally
  means never splitting a surrogate pair, since a lone surrogate is not a codepoint. A splitter that
  would land mid-sequence moves the boundary backwards.
- **The sender budgets for the envelope and for expansion, then verifies.** The rule: slice to
  `MAX_FRAME_BYTES - 512` bytes of encoded payload, encode the frame, and if the encoded message is
  still over the cap (under JSON, a payload of nothing but control characters can triple in size),
  re-split that slice and try again. The 512-byte reservation covers the envelope keys, the stream
  id, and the ordinary expansion of well-behaved content; the verify-and-re-split loop covers
  everything else. On a small cap (the splitter takes the cap as an argument, and the conformance
  fixtures use 64 and 1024 bytes, the only values other than the constant), the reservation is
  `min(512, cap // 2)`, and the loop, not the reservation, is what guarantees the result fits. A cap
  too small to hold the envelope plus one indivisible unit is a configuration error raised when the
  peer is constructed, rather than discovered later as an infinite split loop. The splitter is a
  pure function of (payload, cap, codec) and is tested as one - see M1.

The reservation is a per-codec hint; the verify-and-re-split loop is the guarantee. A new codec
therefore needs no change to this section: a different expansion changes the number of iterations,
and an inaccurate reservation costs extra encodes, never an over-cap frame.

The remaining rules are about ordering. Fragments of one logical payload must be contiguous *on that
stream* - frames of *other* streams may and should interleave between them, and at most one unsent
fragment may be queued at a time (see "The resulting engineering rule"). Receiving a non-fragment
frame on a stream with a fragment assembly in progress is a stream-level protocol error. `end: true`
may appear only on the final fragment. `headers` are never fragmented: an `open` whose headers alone
push the frame over the cap is a `PAYLOAD_TOO_LARGE` reset - headers are metadata, and data of that
size belongs in the payload.

### Frame types

**`open`** - opens a stream. Sender: either peer, using its own parity. Fields: `stream` (required),
`headers`, `payload`/`fragment`, `end`. `end: true` on the `open` is the unary request shape: "here
is the whole request, I will send nothing more". Receiving an `open` for an id with the wrong parity,
or for an id not greater than the highest id that peer has previously *opened*, is a connection-level
protocol error.

**`data`** - a payload chunk on an existing stream. Sender: either peer, on a stream where its own
side is not yet half-closed. Fields: `stream`, `payload`/`fragment`/`more`, `end`, `trailers`.

**End of stream is a flag, not a frame.** There is no separate `end` frame type: a peer that has no
more payloads sends `{"type": "data", "stream": 7, "end": true}` with no payload. This saves a frame
type and a state transition, and leaves no question whether an end frame's payload is data. The API
exposes this as `stream.end()`.

**Trailers are a field, not a frame.** They are carried on the frame that carries `end: true`.
HTTP/2 needs a separate frame because its body is an opaque octet stream that cannot carry
structured metadata; ours is a structured value the codec can encode alongside the body, whatever
that codec is. This keeps "the stream ended" a single event carrying everything known at the end,
rather than two events that a receiver has to correlate.

**`reset`** - terminates a stream immediately in both directions. Sender: either peer, in any state
except `closed`. Fields: `stream`, `code`, `reason`, and optionally `payload` for a structured error
object. A `reset` for a stream that is already closed or was never opened is ignored, under the
general rule for late frames below.

**`ping` / `pong`** - connection-level liveness. `ping` carries a `nonce`; the receiver echoes it in a
`pong` verbatim, promptly, without application involvement. The peer's own liveness timer uses these;
the application may also call `peer.ping()` to measure round-trip time. Native WebSocket ping/pong
control frames are not usable here because browsers do not expose them to JavaScript at all.

**`goaway`** - connection-level graceful shutdown. Fields: `code`, `reason`, `last_stream` (the
highest stream id from the *other* peer that this peer has processed and will still complete). After
sending `goaway` a peer refuses new incoming `open`s with `reset(REFUSED)` and opens no new streams
itself. After receiving one, `peer.open()` raises `ConnectionGoingAway` synchronously at the call
site - it is not an async call, so the failure arrives as a raised exception rather than as a
rejected future that the caller might not await - and any of the receiver's own streams with an id
greater than `last_stream` are reset locally with `REFUSED` - they were never processed, so they are
safe to retry on a new connection. Streams at or below `last_stream` are allowed to finish until a
drain timeout (default 10 s) elapses, then the socket closes.

**`window_update`** - **reserved, not implemented in v1.** See "Backpressure".

### Limits are local, and the frame cap is a constant

**There is no `settings` frame and no negotiation of any kind.** Each limit such an exchange would
announce is handled as follows.

`max_frame_bytes` is now the protocol constant `MAX_FRAME_BYTES` = 65536. **A larger frame lengthens
the wait of other streams.** The cap does not protect a receiver's memory (`max_payload_bytes`
does); it bounds how long one stream may occupy a socket that has one global message order. Raising
it lengthens the time a progress update waits behind an export; lowering it below 64 KiB adds
nothing to the interleaving rules. As a constant, the two peers cannot disagree about the value in
force.

`max_payload_bytes` - the largest reassembled payload a receiver will accept - is a **local receiver
setting, default 67108864 (64 MiB)**, and it never appears on the wire. It is a memory bound, and a
memory bound concerns only the receiver, and announcing it would only let a sender predict the
reset. Because bounding memory is the whole purpose, the reset goes out **as soon as the accumulated
fragments exceed the limit**, not after reassembly completes: a receiver that checked after
reassembly would already hold the whole payload in memory.

`max_concurrent_streams` is likewise **local and receiver-side**, default 100, and a receiver resets
opens beyond it with `REFUSED`. An announced stream quota is part of flow control, which this design
defers (`window_update` stays reserved). The local limit bounds how much work one remote peer can
make this one hold at once, as `max_payload_bytes` bounds memory. `open()` therefore does not raise
for the limit, and there is no `StreamLimit` exception: a sender no longer knows a limit it could
fail against, and a stream refused for saturation is a `REFUSED` reset like any other refusal.

`protocol_version` and `extensions` are the subject of the next subsection.

**muxws is a connection manager, not only a codec:** a codec is a pure function without connection
state, and muxws holds connection state. Multiplexing requires tracking which streams are live and
in which state, reassembling fragments that arrive interleaved with other streams' frames, resolving
the awaitable handle that `open()` handed back when the matching payload arrives, and keeping a
liveness timer so a half-open socket is discovered rather than waited on. This state cannot move to
the application, because **the application never sees a stream open**: it sees a handler invoked
with a reassembled payload, or an awaited handle resolving with a value, after the peer has done
that work. muxws negotiates nothing and still holds per-connection state.

### Versioning lives in the subprotocol

There is exactly one version on the wire: the generation in the
subprotocol name, `muxws.v1.<codec>`. It changes only for a change that breaks the wire,
and a v1 acceptor rejects a `muxws.v2.<codec>` offer at the handshake, before either side has sent a
byte of protocol.

There is no counter for additive revisions within a generation. Additive changes are new frame types
and new fields, which a receiver ignores, so peers of different additive revisions interoperate
without announcing them.

A frame type that must be acted on rather than ignored, such as `window_update`, requires the sender
to know that the receiver supports it. muxws has no capability advertisement, so **flow control,
when built, is a new generation**, `muxws.v2.<codec>`, not an extension within v1. A generation
change is inexpensive because the two peers of a deployment are deployed together.

### Stream state machine

Per stream, per peer - each peer independently tracks the same five states. `local` means "this
peer"; `remote` means the other one.

| State | How it is entered | Frames this peer may send | Frames it may receive |
|---|---|---|---|
| `idle` | id allocated, nothing sent | `open` | `open` (from the other parity) |
| `open` | `open` sent or received without `end` | `data`, `reset` | `data`, `reset` |
| `half_closed_local` | this peer sent `end: true` | `reset` only | `data`, `reset` |
| `half_closed_remote` | received `end: true` | `data`, `reset` | `reset` only |
| `closed` | both ends sent `end`, or either sent `reset`, or the connection died | nothing | nothing (ignored) |

A stream that is `half_closed_local` on one peer is `half_closed_remote` on the other. Both ends
half-closed means closed. The unary shape is `open(end=true)` → `half_closed_local`
immediately, one `data(end=true)` back → `closed`.

**Frames for streams that are gone.** This race is common on a multiplexed connection: peer A
cancels stream 7 while peer B's `data(7)`, sent earlier, is already on the wire. Neither side treats
it as an error:

> **Any stream-level frame naming an id that is not currently live, but does not exceed the highest
> id that peer has opened, is ignored without an error** - the stream is closed, was reset, or was
> refused. No reset, no connection error, at most a counter. **Any stream-level frame other than a
> valid `open` that names an id *above* the highest id that peer has opened is a
> connection-level protocol error**, because that is not a race: the peers disagree about the id
> space.

The second rule limits the first to ids that were opened.

**Retention.** Ignoring frames for closed streams requires telling a closed id from one that was
never opened; storing every closed id would grow without bound on a long-lived socket. Instead, a
peer keeps exactly **`highest_open_seen` per parity plus the map of live streams**. An incoming id
is live (handle it), or below the high-water mark and not live (ignore it), or above the high-water
mark (connection error). The state is constant, the decision is O(1), and nothing is stored per
closed stream.

**Illegal frames.** A frame that is illegal *for a stream* - `data` after receiving `end`, a
non-fragment frame mid-reassembly, a frame the receiver declines as over-size - resets **that
stream** with `PROTOCOL_ERROR` (or `PAYLOAD_TOO_LARGE`) and leaves the connection alone. An `open`
beyond the receiver's own concurrency limit is likewise stream-level, and its code is `REFUSED`:
nothing ran, so the opener may retry it, and because the limit is not announced, `REFUSED` is all
the opener receives. A frame that is illegal *for the connection* - a message the codec cannot
decode, a missing `type`, a stream id with the wrong parity, an `open` whose id is not greater than
that peer's highest previous open, a stream-level frame naming an id nobody has opened - ends the
connection with `goaway(PROTOCOL_ERROR)` followed by a socket close. The rule of thumb: if the peers
can still agree about the state of every *other* stream, keep the connection.

### Error / reset codes

Numeric on the wire, named in both APIs. The same table is used by `reset` and `goaway`. The last
column is normative: two codes exist separately only when they require different behaviour from the
receiving side, so that behaviour is part of the specification.

| Code | Name | Meaning | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, an orderly shutdown. On `reset`, "I am done and no longer interested" without failure semantics. | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` on the reset frame carries a structured error object. | Surface to the caller. Retrying is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted, and definitively not processed - safe to retry. Used for no registered handler, for post-`goaway` opens, and for an `open` beyond the receiver's own concurrency limit. | Retry: elsewhere if another connection is available, otherwise after a delay - the receiver may be at its concurrency limit. |
| 5 | - | **Retired.** Was `STREAM_LIMIT`, when the concurrency limit was announced by the receiver and enforced by the sender. The number is not reused. | - |
| 6 | `TIMEOUT` | A deadline expired locally; the reset informs the remote so it can stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts, or a reassembled payload exceeded the receiver's `max_payload_bytes`. | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream that was live at that instant. **Never appears on the wire.** Surfaces to the local caller as `ConnectionLost`. | Do not retry on this peer now - it has none. Rebuild from `on_reconnect`. |

---

## The codec is a deployment decision

Everything above describes a *logical* frame. Turning one into a WebSocket message, and back, is the
job of exactly one small port:

```python
class Codec(Protocol):
    name: str                                  # "json", "msgpack", ... - the wire-visible name
    binary: bool                               # True -> binary WS messages, False -> text

    def encode(self, frame: Frame) -> str | bytes: ...
    def decode(self, message: str | bytes) -> Frame: ...
```

```ts
interface Codec {
  readonly name: string;
  readonly binary: boolean;
  encode(frame: Frame): string | ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
}
```

**One codec per peer**, for the life of the connection. `binary` is declared rather than inferred so
the peer knows which WebSocket send method to call and which message type to expect on receipt,
without sniffing every incoming message. JSON ships with the library, is registered by the library,
and is the default: it is two lines over the standard library in both languages and adds no
dependency to anybody.

### Selecting it is deployment configuration, not a call argument

The codec is not a `connect()` argument such as `codec=MsgPackCodec()`, for the reason the user
gave:

> "jaz bi sicer nastavitev kodeka dal v [.env] ... ne v function signature. ker potem ni spremenljivo z
> deploymentom, ampak je treba prav if v kodo dati. če pa je nastavitev encoderja v bistvu konstanta iz
> neke sistemske 'env' datoteke, je pa stvar transparentna med dev in prod"

A codec passed at the call site is a code-level constant: changing it between development and
production means an `if` in application code, keyed on something the application also has to invent
a way to know. A codec read from the environment is transparent - the same source file runs in both
places, and which encoding it uses is a fact about the deployment, exactly like a database URL. The
codec is also not a per-call choice: the connections of one process normally use one codec.

**Python** reads it into a `settings` singleton, mirroring `fastapi_viewsets/conf.py` in
fastapi-viewsets - with the same structure:

```python
# muxws/conf.py
class Settings:
    def __init__(self):
        self.codec: str = os.environ.get("MUXWS_CODEC", "json")

settings = Settings()
```

```python
from muxws.conf import settings
settings.codec = "msgpack"     # an application may also set it, before connecting
```

**TypeScript** reads `import.meta.env.VITE_MUXWS_CODEC`, which `vite build` substitutes as a string
literal - the value is baked into the bundle rather than looked up at runtime:

```
# .env.development
VITE_MUXWS_CODEC=json

# .env.production
VITE_MUXWS_CODEC=msgpack
```

Development uses readable JSON and production a compact codec, and no source file names either. JSON
is the default because it is readable during development.

A `codec=` argument on `connect()` and on the peer constructor exists as an **override for tests**
and for a process whose connections need different codecs (a bridge between two deployments). It is
explicitly not the documented path, and no example outside the test suite should use it.

### Registration is explicit, and it is the only mechanism

```python
muxws.register_codec("msgpack", MsgPackCodec())
```

```ts
registerCodec('msgpack', new MsgPackCodec());
```

**There is no dynamic import, lazy auto-registration or probing for installed modules.** The library
registers `json` itself; anything heavier - `msgpack`, `cbor` - is an optional dependency (a PyPI
extra, an npm subpath import) that the *application* registers in its own bootstrap. The documented
instruction to an application author is one sentence: register the codecs you care about, for dev or
for prod.

The alternatives have these drawbacks. Import-time auto-registration makes the set
of available codecs depend on import order. "Try to import it, register it if it is there" turns a
missing dependency into a behaviour change without an error. A registry populated by entry-point
scanning makes the answer to "which codec is this process using" depend on what else is installed in
the virtualenv. Explicit registration in application startup code is the only version where the
answer is visible in the application's own source.

**Two requirements make the TypeScript side tree-shake:**

1. **A codec module must never register itself at import time.** Self-registration makes the import
   a side effect, and a bundler does not remove an import with side effects, because removing it
   would change behaviour.
2. **The npm package declares `"sideEffects": false`** (at minimum for the codec subpaths), which is
   the assertion a bundler needs before it is allowed to drop an import whose bindings are unused.

Given both, the idiomatic bootstrap tree-shakes cleanly:

```ts
import { registerCodec } from 'muxws';
import { MsgPackCodec } from 'muxws/msgpack';

if (import.meta.env.VITE_MUXWS_CODEC === 'msgpack') {
  registerCodec('msgpack', new MsgPackCodec());
}
```

The mechanism is three steps in this order, and it is worth being accurate about them because each one
depends on the previous - and because the whole chain is a property of `vite build`, not of the dev
server, where `import.meta.env` stays a real object read at runtime and nothing is eliminated at all.
**Constant substitution:** the production build replaces `import.meta.env.VITE_MUXWS_CODEC` with the
literal `"json"` in a JSON build, so the condition is `"json" === 'msgpack'`. **Dead-code
elimination:** that folds to `false`, and the entire `if` body is removed. **Tree-shaking:** with the
body gone, nothing that survives references `MsgPackCodec`, so the binding is unused - and because the
package declares no side effects and the module does not register itself, the bundler is permitted to
conclude that evaluating `muxws/msgpack` at all is unobservable, and drops the import.

Without either requirement, the first two steps still happen and the third does not: the codec
module stays in the bundle.

The last step has a limit.
Dropping muxws's own msgpack module removes its `import` of `@msgpack/msgpack`, but whether *that*
package then leaves the bundle depends on that package's own `sideEffects` declaration: a
dependency that does not claim to be side-effect-free is retained by a conforming bundler even when
none of its exports are used. muxws controls only its own modules, and open question 2 asks for the
claim to be checked against a production build.

The codecs other than JSON are opt-in because they are **separate dependencies that may not be
installed**, and the library cannot register a codec that is not installed. The `if` also removes
unused codecs from the bundle; an application that registers every installed codec works correctly
and carries a few unused kilobytes.

**An unregistered codec name raises an error at startup**, on the first connection attempt, before
any socket is opened:

```
muxws: codec "msgpack" is not registered.
  MUXWS_CODEC=msgpack (from the environment)
  registered: json
  Install it (pip install muxws[msgpack]) and register it during startup:
      muxws.register_codec("msgpack", MsgPackCodec())
```

There is no fallback to JSON. With a fallback, a deployment configured for msgpack would run JSON
without an error, on one end or on both.

### A mismatch guard, not a negotiation

The configured codec name is offered as a **WebSocket subprotocol**: `muxws.v1.json`,
`muxws.v1.msgpack`. The acceptor accepts the connection only if the offered name matches its own
configured codec; otherwise **it refuses the WebSocket handshake** - it selects no
subprotocol and answers the upgrade with `400 Bad Request`. There is no half-open state to clean up
afterwards, because the socket never existed.

**This is an assertion, not a negotiation.** There is no fallback to a common encoding, no
per-connection multi-codec support, no list of acceptable alternatives, and no runtime branching
anywhere in the peer. Each side states its configured codec, and the connection is established only
when they match.

With the codec set in configuration, the frontend and backend `.env` files can differ. Without the
check, msgpack bytes reach a JSON decoder and fail at an arbitrary moment on an established
connection; with it, the upgrade is rejected at connect time.

**Both ends report the mismatch, because a browser hides the rejected upgrade's response from
JavaScript:** the browser reports a failed connection and never hands the page the response body, so
anything the server writes there is readable in a server log and in a developer's network panel and
nowhere else. Each side reports what it knows. The **client** raises `CodecMismatch` built from what
it *offered*, and names both environment variables so the reader knows where to look on each side:

```
muxws: the server refused the muxws.v1.msgpack subprotocol.
  This peer offered:  msgpack   (VITE_MUXWS_CODEC=msgpack)
  The server is configured by MUXWS_CODEC and did not accept it.
  Both ends must name the same codec.
```

The **server** logs the same failure from its own half, where it can state both values because it saw
the offer:

```
muxws: refusing upgrade - codec mismatch. offered=msgpack configured=json
  (client VITE_MUXWS_CODEC, server MUXWS_CODEC)
```

Both messages name the two variables, so either one identifies the settings to compare.

The subprotocol also carries the protocol generation, so a `muxws.v2.json` peer is rejected by a v1
peer at no extra cost, and by exactly the same mechanism - see "Versioning lives in the subprotocol"
for why that is now the only version on the wire.

### Binary payloads require a binary codec

Under a binary codec, raw bytes are a payload type: a `bytes` in Python or an `ArrayBuffer` in
TypeScript is carried as is, without an encoding step or size overhead. Under JSON a byte string is
not a value, so an application base64-encodes it on both sides (33% larger) or does not send it.

The codec choice therefore also determines whether bytes can be sent. An application that streams
image tiles, audio frames or an already-compressed export has a *capability* under msgpack that it
does not have under JSON, and one that needs that capability is choosing a codec rather than tuning
one. muxws does not base64-encode bytes under the JSON codec, because a 33% expansion applied by one
codec only would skew a deployment's size measurements. Under JSON, the application encodes bytes.

---

## Public API

### `open()` is synchronous, and the handle it returns is itself awaitable

Because the library allocates the stream id rather than the caller, the call that starts a stream has
to hand something back - at minimum the id, so the caller knows which progress pushes and results
belong to it, and an awaitable, so the code that started the operation can wait for its result. In the
user's words:

> "če se stream id assigna samostojno, potem rezultat api funkcije verjetno mora biti nek objekt? saj
> mora vrniti najmanj stream id ter awaitable. stream id zato, da koda sploh ve, katere progress pushe
> in rezultate brati, awaitable pa zato, da konkretna koda, ki je sprožila request, lahko async awaita
> rezultat... v bistvu bi bila še lepša sintaksa samo await (api call)"

One decision provides both. **`peer.open(...)` is a synchronous call returning a `Stream`,
and `Stream` is itself awaitable.** That yields three spellings of the same primitive, and the caller
picks by writing the one they mean:

```python
stream = peer.open(payload)               # the handle; stream.id is readable on the next line
result = await peer.open(payload)         # await the handle: the single result payload
async for chunk in peer.open(payload):    # iterate the handle: every payload, until the remote ends
    ...
```

`await peer.open(p)` yields the first payload, not the `Stream`, so the shortest spelling returns
the result without a second await on `.result()`.

**`open()` is synchronous** because sending on a WebSocket is a queue-and-return operation in both
languages. Neither the browser's `WebSocket.send` nor an append to the Python peer's send queue
waits for anything - the bytes leave when the writer reaches them - so an `await` on `open()` would
wait for neither delivery, transmission nor a reply. `open()` allocates the id and registers the
stream in the peer's live-stream map, an arithmetic step and a dict write, and neither suspends.

**The id is allocated and the `open` frame enqueued in the same synchronous step**, and this is safe
because nothing can run between the two steps. An `async open()` could suspend between them, so two
concurrent calls could put `open` frames on the wire out of id order, which is a connection-level
protocol error. A synchronous `open()` has no suspension point between them, so on a single-threaded
event loop the wire order is the allocation order.

**The two ways `open()` can fail are raised at the call site, synchronously**: `ConnectionGoingAway`
after a `goaway` has been received, and `ConnectionLost` while the peer is between sockets. Both are
facts the peer already knows before it does anything, and a raised exception, unlike a rejected
future that is not awaited, cannot go unobserved.

`open()` does not fail for the concurrency limit, because
the limit on how many streams may be open is the *receiver's*, it is local to the receiver, and it
is not announced (see "Limits are local"). A sender learns that it reached the limit from the
remote: a `reset(REFUSED)` arrives on that stream, and the pending await raises `StreamRefused`. So
saturation is an asynchronous stream failure like a refused handler, not a synchronous local one,
and `StreamLimit` is not an exception this library has.

**Awaiting and iterating the same stream is a caller error**, and it has exactly one rule: **the
first of the two shapes to be used claims the stream, and the other raises
`StreamAlreadyConsumed`.** A stream's payloads are consumed once. Splitting them between an `await`
and an `async for` would give each consumer part of an export without an error, so the second use
raises, with a message naming both uses. The same rule applies to two `async for` loops over one
stream.

The rule concerns *shapes*, not call counts. **Awaiting an already-awaited stream is not an error
and returns the same value again**, because `await` reads a value that the stream has already
finished producing - there is nothing left to split, and the memoized future described below is what
makes the second read cheap and identical. Iterating twice *is* an error, because the second loop
would be asking for payloads the first one already took. A stream allows any number of awaits or one
iteration, not both.

**`request()` exists next to `await peer.open(payload, end=True)`** because it adds two things. It
sets the end flag, so the call site shows that the caller sends nothing more. It enforces the unary
contract: a remote that sends more than one payload raises at the call site instead of the extra
payloads being dropped. `request(p)` is `await open(p, end=True)` plus that check.

The check is in `request()`, not in the handle. `await stream` resolves when the *first* payload
arrives, so a bidirectional caller can read it and continue sending; a second payload has not
arrived at that point. Only a call that waits for the stream to *end* knows the payload count, which
a unary call does and a general handle does not. A strict `await stream` would make the
bidirectional shape unusable.

**`peer.notify()` stays `async` and returns nothing** - `None` in Python, `void` in TypeScript. So
do `muxws.connect()` and `muxws.accept()`, which wait for the dial and for the WebSocket handshake.
A call is async when it waits for I/O.

### Python

```python
import muxws

peer = await muxws.connect("wss://host/ws", headers={"Authorization": f"Bearer {token}"})

# 1. unary: open, end immediately, await the single reply
reply = await peer.request({"action": "list", "model": "item"})

# 1b. the same thing spelled with the handle - `request` additionally rejects a >1-payload reply
total = await peer.open({"action": "count", "model": "item"}, end=True)

# 2. streaming response: open, iterate until the remote ends the stream
async for chunk in peer.open({"action": "export", "model": "item"}):
    render(chunk)

# 2b. keep the handle when the id or the trailers are wanted - note: no await on open()
stream = peer.open({"action": "export", "model": "item"})
log.info("export running on stream %s", stream.id)     # readable immediately
async for chunk in stream:
    render(chunk)
print(stream.trailers)          # available once the stream ended

# 3. bidirectional: keep sending while reading
stream = peer.open({"action": "subscribe"}, headers={"viewset": "item"})
await stream.send({"filter": "EUR"})
async for tick in stream:
    ...
await stream.cancel()           # reset(CANCELLED), stops the remote

# 4. one-shot push, no stream object to leak
await peer.notify({"kind": "progress", "done": 0.4})

await peer.close()              # goaway(NO_ERROR), drain, close
```

Incoming streams - from a server push, or because this peer is the acceptor - go to one handler,
which receives the opening payload as its first argument:

```python
@peer.on_stream
async def handle(payload: Any, stream: muxws.Stream) -> None:
    if payload["action"] == "list":
        await stream.reply({"items": [...]})  # send + end, the unary answer
    elif payload["action"] == "export":
        async for batch in produce(payload):
            await stream.send(batch)
        await stream.end(trailers={"rows": 12000})
```

The handler is invoked **only once the opening payload is fully reassembled** - a fragmented `open`
does not reach the application in pieces. The peer's dispatch code waits for reassembly, so a
handler does not. The payload is also available as `stream.payload` for handlers that stash the
stream and read it later.

**Peer**

| Member | Signature | Notes |
|---|---|---|
| `open` | `open(payload=None, *, headers=None, end=False) -> Stream` | **Not a coroutine.** Zero mandatory arguments. No `timeout`: `open()` returns immediately, so there is nothing for a timeout on it to bound - the deadlines live on the awaits, `stream.result(timeout=)` and `peer.request(timeout=)`. Raises `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between sockets. |
| `notify` | `async notify(payload=None, *, headers=None) -> None` | One-shot push: `open(payload, end=True)` with no `Stream` returned - it returns nothing to await or keep. |
| `request` | `async request(payload=None, *, headers=None, timeout=None) -> Any` | `open(payload, end=True)` awaited to the stream's end, plus the "exactly one payload" check that waiting makes possible. |
| `on_stream` | `on_stream(handler)` | Decorator or plain call. Handler is `(payload, stream)`. One handler per peer; registering a second replaces it (and logs). |
| `on_close` / `on_reconnect` / `on_frame` | `(handler)` | Lifecycle and observability hooks, registered as methods in both languages. `on_close(reason)` fires on every socket loss with a `CloseReason` carrying `code`, `reason`, `was_clean` and `will_retry` - the same four fields in both languages - `will_retry` distinguishing "reconnecting" from "offline"; `on_reconnect(attempt, peer)` fires once per re-established connection, after the hello has been acknowledged. |
| `ping` | `async ping(timeout=5.0) -> float` | Round-trip seconds. |
| `close` | `async close(code=NO_ERROR, reason=None, drain=10.0)` | `goaway`, drain, close. |
| `serve` | `async serve()` | Runs the read loop until the socket closes. Only needed on the acceptor side (see FastAPI wiring); `connect()` starts it for you. |
| `id` | `str` | Connection id for logs: a short random per-process prefix plus a monotonic per-connection counter, e.g. `a3f-17`. See "Connection ids are never reused". |
| `tags` | `dict[str, Any]` | Application data attached to the connection (a session id, a user, a map of what this one socket does and does not want sent to it). An ordinary dict with ordinary dict semantics: any key may be written, overwritten and overwritten again for as long as the socket lives, last write wins, and a rewrite has no cost. muxws does not read, persist or restore it; the dict is created with the peer and discarded when the socket closes. A direct read sees the newest value immediately; `PeerRegistry` answers from the values that were there at the last `register()`. See "`tags` is connection-scoped state". |
| `streams` | `Mapping[int, Stream]` | Live streams, read-only, for introspection. |
| `is_open` | `bool` | `False` for the whole window between a socket loss and the next established connection - which is exactly the window in which `open()` raises `ConnectionLost` rather than queueing. |

**Stream**

| Member | Signature | Notes |
|---|---|---|
| `id` | `int` | Read-only. Never an argument anywhere. |
| `headers` | `dict` | The `open` frame's headers; empty for locally opened streams. |
| `payload` | `Any` | The opening payload, already reassembled. Same value the handler was called with. |
| `send` | `async send(payload, *, end=False)` | Fragments automatically above the cap. |
| `end` | `async end(payload=None, *, trailers=None)` | |
| `reply` | `async reply(payload, *, trailers=None)` | `send` + `end`, the common answer shape. |
| `__await__` | `await stream -> Any` | The result payload: the first one the remote sends. Delegates to the same memoized future `result()` uses, so awaiting twice returns the same value. Claims the stream against iteration. |
| `result` | `async result(timeout=None) -> Any` | What `await stream` delegates to, with a timeout argument. Resolves on the first payload; raises `StreamReset` if the stream is reset first, and raises if it ends without producing one. It does **not** wait for the end, so it does not check for a second payload - that check is `request()`'s, see below. |
| `cancel` | `async cancel(reason=None)` | `reset(CANCELLED)`. |
| `reset` | `async reset(code, reason=None)` | The general form. |
| `__aiter__` | `async for payload in stream` | Yields reassembled payloads until end; raises `StreamReset` if reset. Claims the stream against `await`. |
| `trailers` | `dict \| None` | Populated when the stream ends. |
| `closed` | `asyncio.Event` | Set on close, including on socket death. TypeScript has no Event primitive, so there it is a `Promise<void>` that resolves at the same moment - which can be awaited and passed to `Promise.race`. |

`send()`, `end()` and `reply()` on a stream that is no longer open raise, and the exception class
identifies the cause, so the three cases are three types. A stream that closed normally - both ends
ended - raises **`StreamClosed`**. A stream that was reset raises its own `StreamReset`
(`RemoteError`, `StreamRefused`, whatever the code was). A stream whose socket closed raises
`ConnectionLost`. Callers react differently to each: a normal close that races a last `send()` is an
expected outcome and is usually ignored, a reset is usually reported, and a lost connection means
waiting for `on_reconnect` and reopening streams. A normal close is not a failure, and reporting it
as one would make callers ignore that exception type.

**How `Stream` is awaitable, concretely.** `Stream.__await__` delegates to the same memoized future
that `result()` uses: one future per stream, created lazily on the first await, resolved with the
stream's first payload when it arrives, or rejected with the stream's `StreamReset`. The future is
memoized, so a second `await` on a stream that has produced its value returns that value again
instead of raising or waiting indefinitely. `result(timeout=...)` waits on the same future with a
deadline, so both return the same value.

fastapi-viewsets uses the same idiom for the same reason: `LazyObject._ensure_future()` in
`fastapi_viewsets/context/__init__.py` creates the future on first `__await__` and hands the same
one to every subsequent await, so `await context.user` works from any number of places. The
underlying rule is the same in both cases: an awaitable that is a *value holder* rather than a
one-shot coroutine must support being awaited twice, because two pieces of code can hold the same
handle. Implement `Stream.__await__` against the same pattern.

`__aiter__` reads payloads from the stream's own queue as they arrive and does not use that future,
which is why the two shapes cannot be mixed and why the claim is recorded on the stream rather than
on the future. Creating the future lazily is what keeps the claim accurate - a stream nobody ever
awaits has no future to resolve, and a stream that is never iterated has no queue consumer.

Because `open()` is not a coroutine, the entire "coroutine was never awaited"
class of `RuntimeWarning` cannot arise for a handle the caller deliberately does not await. Under an
`async open()`, fire-and-forget written as a bare `peer.open(...)` produced that warning and a stream
that was never actually opened; now it produces an ordinary object that the caller may ignore. (It
should still be `notify()` - see "Three call shapes, one primitive" for why ignoring a handle is not
the same guarantee as not being handed one.)

**PeerRegistry** - the one piece of state muxws keeps *about* connections rather than inside one.
`peer.tags` is the single source of truth; the registry is a pure index over it, so nothing is ever
passed twice.

| Member | Signature | Notes |
|---|---|---|
| `register` | `register(peer) -> None` | Indexes the peer under **every key its `tags` holds at that moment** - it indexes every key and skips without an error a value that cannot be a lookup key (a dict, a list). Idempotent and **re-indexing**: calling it again after `tags` changed replaces the peer's index entries wholesale, so it is found under the new values and no longer under the old ones. That second call is what a hello handler makes once the client-supplied identity has arrived. Removal on close is automatic, via the peer's own close hook. |
| `registered` | `registered(peer)` | Context manager wrapping `register` plus an explicit deregister, for the usual `with registered(peer): await peer.serve()` shape. |
| `peers_for` | `peers_for(**tags) -> list[Peer]` | Every live peer whose `tags` match all given keys. |

`peers_for` returns a **list, not a set**: one session routinely has several open tabs, fan-out is a
loop over that list, and a stable order makes the logs of a fan-out readable ("pushed to 3 peers:
c7f3, a19b, 4d02") in a way that iteration over a set does not. Callers must treat the list as a
snapshot - a peer in it may already be closing by the time they reach it, which is exactly why
`notify` is best-effort.

**`tags` is connection-scoped state.** The dict is created with the peer, is
readable and writable for as long as that peer's socket is alive, and is gone when it closes.
**Nothing in muxws persists it, snapshots it, or carries it across a reconnect.** On the next socket
the acceptor holds a new peer object with a new empty dict, and only application code writes to it,
in three places: the acceptor's framework, writing what its authentication established (the
session); the handler for the hello the reconnect helper replayed; and the handler for any later
message the client chose to send. muxws writes nothing to it. A tag added during a connection is not
restored on the next socket; the application writes it again if it needs it.

**State that lives only as long as the socket does not carry over a reconnect**, so a client that
fails restarts from the default state. `backchannel` relies on this. What one socket has said it
does and does not want delivered on it is a fact about that socket, and it is kept nowhere but in
that socket's `tags` - not in backchannel's own store, not against the account, and not in the
browser either - so a tab that reconnects starts with none of those declarations and gets whatever
the server would have decided without them. If those declarations were persisted beyond the socket,
a closed tab's muting would apply to its successor, without any error.

**A tag value may be replaced any number of times**; a write is a plain assignment. Direct reads and
the registry see the change at different times. A direct read - `peer.tags["session"]`, or any
application code holding the peer - sees the new value on the next line; there is no copy and no
snapshot anywhere in that path. `PeerRegistry` does not, because it is an index built at
`register()` time: a peer whose tags changed is still found under the values it was registered with
until `register(peer)` is called again, which replaces its entries with the current dict.
Re-indexing is an explicit call, not a watched dict, so an ordinary assignment does no registry
work; identity that changes during a connection is a new connection under the hello rules.

**A key used for lookup is not changed after `register()`, and a key that changes is not used for
lookup.** The registry indexes every key it finds and so cannot tell the two apart; the application
can, and the staleness is observable only through a `peers_for` query naming a key whose value moved
since the last `register()`. Per-connection state that is *read off a peer the application is
already holding* needs no registry call: keep it in `tags` under a key nothing ever looks up,
replace it as often as the client re-declares it, and never call `register()` for it at all.
backchannel's arrangement is exactly that split - `session` is written once at `accept()` from the
framework's own authentication and never touched again, so its index entry cannot go stale, while
its map of per-operation delivery overrides is replaced wholesale on every re-declaration and is
only ever read off a peer the caller is already holding - one out of the list
`peers_for(session=...)` has just returned, never a peer found *by* the map. A consumer that finds
peers by a value it also changes calls `register(peer)` after each write.

**A tag's value need not be usable as a lookup key**: `register()` reads every key, and a value that
is never looked up need not be indexable - backchannel's is a map, which is unhashable in Python and
meaningless as an index entry in either language. So `register()` indexes the entries whose value
can serve as a lookup key and passes over the rest: the peer is not findable by that key. Raising
instead would fail `register()` in a hello handler because of a key another handler wrote.

**Server side, FastAPI/Starlette, in one line of wiring:**

```python
from fastapi import FastAPI, WebSocket
import muxws

app = FastAPI()

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await muxws.serve(websocket, handler=handle_stream)   # returns when the socket closes
```

`muxws.serve(socket, handler)` is `accept()` + `on_stream()` + `serve()` collapsed. When the
application needs the peer object itself - to push, or to register it - use the explicit form:

```python
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, session=Depends(current_session)):
    peer = await muxws.accept(websocket)         # completes the handshake; asserts the codec
    peer.tags["session"] = session.id            # the only place the session is written
    peer.on_stream(handle_stream)
    with muxws.registry.registered(peer):        # indexes whatever is in tags
        await peer.serve()
```

The session is written there because it comes from the framework's own authentication and the client
does not set it. Identity the *client* supplies - a tab id, the project it is looking at - arrives
instead in the hello stream that the dialer's reconnect helper replays on every connection, and
`handle_stream` writes that into `peer.tags` too; see "Connection identity is re-established by the
helper". The registry indexes whatever ends up there either way, so `peers_for(session=...)` and
`peers_for(tab=...)` are the same mechanism reading two differently-sourced facts.

**Note what is no longer in those samples: the application's own `await websocket.accept()`.**
`muxws.accept()` is async and performs the WebSocket accept itself, because it is the only party
that knows which subprotocol to select - it has to read the client's offered `muxws.v1.<codec>`
value, compare it against its own configured codec, and either accept naming that subprotocol or
refuse the handshake outright. An application that accepted the socket first could no longer reject
the offer, and a mismatch would appear later as undecodable bytes. Authentication is unaffected: the
framework's `Depends` chain still runs, and still rejects, before the endpoint body executes at all.

Elsewhere in the application, a push to that session is `peers_for` plus a loop - the whole of what
muxws offers in place of rooms and broadcast:

```python
for peer in muxws.registry.peers_for(session=session_id):
    await peer.notify({"kind": "progress", "done": 0.4})
```

Bare asyncio, using the `websockets` library on both ends (no FastAPI anywhere):

```python
import websockets, muxws

async def main():
    async with websockets.serve(lambda sock: muxws.serve(sock, handler=handle), "0.0.0.0", 8765,
                                select_subprotocol=muxws.select_subprotocol):
        await asyncio.Future()
```

The extra argument is where the two transports genuinely differ, and the difference is stated here
rather than left implicit. Starlette hands over a socket that has *not* been accepted, so
`muxws.accept()` can do the whole handshake including the subprotocol decision. `websockets` completes
the handshake before it ever calls the handler, so the decision has to be handed to it up front -
hence `muxws.select_subprotocol`, a plain callable it can install. Where a transport offers neither
hook, the fallback is to verify the negotiated subprotocol on the already-open socket and close it
with a policy-violation code: the same diagnosis, one round trip later.

The socket type itself is duck-typed through a tiny adapter protocol (`send_text`, `send_bytes`,
`receive`, `close`, plus the handshake hook above) so Starlette's `WebSocket`, a `websockets`
connection, and the in-memory test transport are all acceptable arguments. Text and binary sends are
separate methods rather than one polymorphic `send` because the codec declares which of the two it
needs and the adapter does not inspect the value's type. That adapter protocol is the
only place transport-specific code lives.

A dialing Python peer with the reconnect helper, spelled out so it can be diffed line for line
against the TypeScript sample below:

```python
peer = await muxws.connect(
    "wss://host/ws",
    headers={"Authorization": f"Bearer {token}"},
    hello={"tabId": tab_id, "user": user_id},      # replayed verbatim on every reconnect
    reconnect=muxws.Reconnect(initial_delay=0.25, max_delay=30.0, factor=2, jitter=0.3,
                              max_attempts=None),   # None = unlimited
    ping_interval=20.0, ping_timeout=10.0, hello_timeout=10.0,
)

peer.on_reconnect(lambda attempt, peer: store.resubscribe(peer))
peer.on_close(lambda reason: store.mark_offline(reason))
```

`hello` is the only argument in that call whose value muxws never looks at, and the only one that
applies to every connection, not only the first: it is sent as an ordinary `open(..., end=True)` on
every socket this peer establishes, and `on_reconnect` does not fire until the acceptor's handler
has answered it. "Connection identity is re-established by the helper" specifies the exchange; an
application supplies its identity once.

The Python spelling differs from the TypeScript one in two ways.
**Durations are seconds in Python and milliseconds in TypeScript** (`initial_delay=0.25` versus
`initialDelayMs: 250`), because seconds-as-floats is what `asyncio.sleep`, `asyncio.timeout` and
every Python timeout argument in this repo already use, and milliseconds-as-integers is what
`setTimeout` and every browser API use; forcing one language to carry the other's unit would make
every call site convert. **Hooks are registered as methods in both languages** - `peer.on_reconnect(...)`
and `peer.onReconnect(...)` - and the TypeScript `connect()` options object accepts the same hooks
inline purely as sugar for the common case of registering them before the first connection attempt.
Everything else is the same names in the same order.

### TypeScript

The same shape. Naming rule, following how `route_rest` mirrors `route_viewset` in fastapi-viewsets'
`vue/rest-proxy.ts`: **module-level factories keep the Python name verbatim** (`connect`, `accept`,
`serve`), **classes are PascalCase in both** (`Peer`, `Stream`, `PeerRegistry`), and **methods and
options-object keys are camelCase** (`onStream`, `maxPayloadBytes`). Wire field names stay snake_case
in both languages, because the wire is the shared artefact.

```ts
import { connect, type Stream } from 'muxws';

const peer = await connect('wss://host/ws');

// 1. unary
const reply = await peer.request({ action: 'list', model: 'item' });

// 1b. the same thing spelled with the handle
const total = await peer.open({ action: 'count', model: 'item' }, { end: true });

// 2. streaming response
for await (const chunk of peer.open({ action: 'export', model: 'item' })) render(chunk);

// 2b. keep the handle when the id or the trailers are wanted - note: no await on open()
const stream = peer.open({ action: 'export', model: 'item' });
console.log('export running on stream', stream.id);   // readable immediately
for await (const chunk of stream) render(chunk);
console.log(stream.trailers);

// 3. bidirectional + cancel
const sub = peer.open({ action: 'subscribe' }, { headers: { viewset: 'item' } });
await sub.send({ filter: 'EUR' });
for await (const tick of sub) { /* ... */ }
await sub.cancel();

// 4. one-shot push
await peer.notify({ kind: 'progress', done: 0.4 });
```

```ts
peer.onStream(async (payload: any, stream: Stream) => {
  if (payload.action === 'list') {
    await stream.reply({ items: [] });
  } else {
    for await (const batch of produce(payload)) await stream.send(batch);
    await stream.end({ trailers: { rows: 12000 } });
  }
});
```

Following the repo's existing overload convention (positional arguments *or* an options object, as
`route_rest` does), `open` and `request` accept `(payload?)`, `(payload?, options?)`, or
`(options)`. `open`'s options are `{ payload?, headers?, end? }`; `request`'s are the same plus
`timeoutMs?`, because a deadline belongs on a call that waits and `open()` does not wait for
anything.

**`Stream<T>` implements `PromiseLike<T>`, not `Promise<T>`.** It has `then`, `catch` and `finally`,
each delegating to an internal promise that settles with the stream's first payload or rejects with
its `StreamReset`. A plain thenable is all `await` actually requires - `await` calls `then` on
whatever it is given and never inspects the prototype chain - and muxws implements a thenable. A
`Promise` subclass carries species semantics, so every derived call (`stream.then(...)`,
`stream.catch(...)`) would construct another `Stream` through `Symbol.species`, without a stream id.
A thenable's `then` returns an ordinary `Promise`.

`Stream<T>` is simultaneously `AsyncIterable<T>`, via `Symbol.asyncIterator`. The two interfaces
coexist on one object, and the caller's syntax selects the shape: `await` calls `then` and `for
await` calls the iterator, and the first of them to be used claims the stream as described above.

**Unconsumed handles.** When a stream that was never awaited or iterated is reset, its internal
promise rejects without a handler, and the browser reports an `Unhandled promise rejection`, which
some deployments forward as an error. So the implementation **attaches a default no-op rejection
handler to the internal promise at construction time**, and surfaces the failure through the peer's
`onFrame` / error hook instead, where an application observes failed pushes. Attaching the handler
lazily on the first `then` is too late; by then the rejection may already have been reported.

`notify()` is therefore a separate call rather than `open()` with the handle discarded. `notify()`
returns nothing at all: there is no promise to leave unhandled, no handle to retain by accident, and
no stream left open against the receiver's concurrency limit. Discarding a handle depends on every
call site; returning nothing applies to all of them.

Browser reconnect wrapper - the same call as the Python one above, in TypeScript units:

```ts
const peer = await connect('wss://host/ws', {
  hello: { tabId: tabId(), user: userId },   // tabId() reads sessionStorage - see the reconnect section
  reconnect: { initialDelayMs: 250, maxDelayMs: 30_000, factor: 2, jitter: 0.3, maxAttempts: Infinity },
  pingIntervalMs: 20_000, pingTimeoutMs: 10_000, helloTimeoutMs: 10_000,
  onReconnect: (attempt, peer) => { store.resubscribe(peer); },
  onClose: (reason) => { if (!reason.willRetry) store.markOffline(reason); },
});

// equivalently, and identically to Python, after construction:
peer.onReconnect((attempt, peer) => store.resubscribe(peer));
peer.onClose((reason) => store.markOffline(reason));
```

The Node acceptor lives behind a subpath export (`muxws/node`) so that importing the
browser entry point never pulls the `ws` dependency into a bundle. The peer implementation is shared;
only the socket adapter differs.

---

## Three call shapes, one primitive

Unary and streaming calls are **not two protocols**. There is
one stream primitive and three wrappers around it.

| Shape | Wire | API |
|---|---|---|
| Unary | `open(end)` → `data(end)` | `await peer.request(payload)`, or `await peer.open(payload, end=True)` |
| Streaming response | `open(end)` → `data`, `data`, ..., `data(end)` | `async for chunk in peer.open(payload)` |
| Bidirectional | `open` → interleaved `data` both ways → `data(end)` both ways | `stream = peer.open(payload)`, then `stream.send()` plus iteration |
| One-shot push | `open(end)`, nothing awaited | `await peer.notify(payload)` |

The fourth row is the degenerate case rather than a fourth shape: `notify()` is `open(payload,
end=True)` that returns nothing. It exists because "fire and forget" written with `open()` returns a
`Stream` the caller does not need; a discarded stream that was never ended stays open, counts
against the *receiver's* concurrency limit, and causes later pushes to be refused. A discarded
handle also causes a second problem in the browser: an internal promise that nobody attached a
handler to, whose rejection the runtime reports as unhandled (the TypeScript section describes the
default no-op handler that keeps this from reaching the console, and why it cannot be attached
lazily). Returning nothing prevents both problems; `backchannel`'s push path does not raise, so a
limit reached there would otherwise go unreported. The other half of that guarantee is on the
receiving side: **a handler that returns without having ended its stream ends it implicitly**, so
the receiving handler need not end the stream. One progress push every 250 ms therefore occupies one
stream slot for a round trip, not forever.

`request()` is `open(payload, end=True)` awaited to completion, and it is a separate call for the
two reasons given under "`open()` is synchronous": the `end=True` shows at the call site that this
caller sends nothing more, and it waits for the stream's end so it can raise if the remote sent more
than one payload - because a caller who asked for a single value and got three has a bug that is
reported at the call site instead of the data being dropped. A caller that expects several payloads
uses `open()`; a caller that expects one payload and keeps sending uses `await peer.open(payload)`
without the end flag.

This matters for evolution: a server-side action that starts out unary and later grows a progress
stream does not change protocol or break the wire. Only the caller's choice of wrapper changes.

---

## One registration point, not a router

`peer.on_stream(handler)` takes exactly one handler, and muxws hands it every incoming stream. There
is no path table, no method map, no per-action registration.

muxws has no router because routing depends on what the opening payload *means*
- whether the key is `action`, `type`, `method`, or a path string; whether dispatch is by exact
match or by prefix; whether an unknown key is a 404-shaped reset or a fallback handler. Consumers
answer these differently, and a muxws convention would match at most one of them. fastapi-viewsets'
future WS adapter will dispatch on `{"action": ...}` against its own viewset registry, which is
*its* concern; backchannel will dispatch on its own message kinds; a third consumer will do
something else. One handler with a `match` statement in application code serves all three.

muxws defines the failure mode, based on whether a handler is registered. **No registered handler →
`reset(REFUSED)`**: nothing ran, so the opener may safely retry the operation elsewhere. **A
registered handler that raises → `reset(APPLICATION_ERROR)`, always**, regardless of whether it had
already sent anything. `REFUSED` states that the operation did not happen, so a handler that debits
an account and then raises must not produce it: the client would retry the debit. A peer that ran a
handler assumes the handler did something. And a handler that returns normally without ending the
stream ends it implicitly - the same rule that keeps a one-shot `notify()` from occupying a stream
slot on the receiver for the life of the connection.

---

## Two vocabularies over one socket

In the target deployment, a browser tab holds **one** muxws connection, and two entirely different
closed message vocabularies travel over it:

| | fastapi-viewsets' WS adapter | backchannel |
|---|---|---|
| Who opens the stream | the client | the server (`notify`); the client opens one only for the kinds that originate on its side |
| Shape | request/response, or a streaming response | one-shot push, one envelope per stream; what the client originates is likewise a single message on a stream of its own |
| Vocabulary | `command` / `result` / `error` | a closed set of six: the pushed `progress`, `dialog.open`, `dialog.close` and `cancel`, plus `dialog.reply` and `watch`, which originate on the client and never travel as pushes (`snapshot` is a bundle of the pushed kinds, not a seventh kind) |
| What travels the other way | the reply, on the same stream | a cancel *request* over REST; `dialog.reply` over REST or over a *client*-opened stream; `watch` only ever over a client-opened stream, because it describes the very connection it arrives on - a map of operation token to a boolean that overrides, in either direction, what the server would otherwise have decided to deliver on this socket, re-sent whole whenever it changes, and a deployment with no socket has nothing to override and sends no `watch` at all |
| Correlated by | one stream is one command | an application-level token in the envelope |

Neither vocabulary is known to the other, and neither is known to muxws. A `command` frame and a
`progress` envelope are two application payloads on two independent streams that happen to share a
socket, and the only thing that ties a `progress` envelope to the command that caused it is a token
the *application* minted and put in both - not a stream id, not a muxws correlation of any kind. That
is deliberate on backchannel's side too: its envelopes reach a browser that may never have issued a
command at all (a Celery worker's import started from a cron job still has progress to report), so a
correlation that lived in muxws's stream ids could not express what it needs.

**The stream model lets independent consumers share one connection.** Two consumers at different
layers share one socket, handshake, authentication, reconnect helper and liveness timer without
affecting each other. A progress push cannot delay a command's reply beyond one fragment (the
interleaving rule), a cancelled export does not disturb an open dialog (per-stream reset), and
neither depends on the other. Without multiplexing, both would share one message queue and need a
common discriminator field in every message, a third vocabulary that both would change together.

**muxws defines no message vocabulary**: no `kind` field, no `type` inside `payload`, no reserved
key. A muxws vocabulary would match at most one of the two consumers above and force the other to
nest its own inside it. `payload` is opaque and `headers` are uninterpreted for exactly the same
reason `on_stream` is one handler rather than a router: each consumer defines the meaning of its
opening payloads.

**A change to a consumer's vocabulary requires no change to muxws.** The first time, backchannel's
vocabulary went from five kinds to six when `watch` joined it, so that one connection could say how
much progress traffic it wanted. Twice since, `watch` has kept its name and its slot in the count
and changed shape completely: a choice among preset levels of traffic became a plain declaration of
the operations this connection had muted, and that declaration became a whole-value map of operation
token to a boolean that overrides a server-side delivery decision in either direction - silencing
what would have been sent, reviving what would have been suppressed, and still touching nothing but
non-terminal `progress`. One of the three changes added a kind; the other two replaced what a kind
means. None of the three changes added a frame type, reserved key, setting or version to muxws, or
affected the other consumer.

A `kind` field in muxws would require a muxws release for each new kind or changed meaning, which
the other consumer has to adopt. Without it, such changes stay inside one consumer's payloads.

---

## Auth is a handshake concern

Authentication happens once, at the WebSocket upgrade: cookies are sent automatically, or a token
goes in a header (Python client) / in the subprotocol or a query parameter (browsers cannot set
headers on `WebSocket`). By the time `accept()` is called, the framework's own dependency machinery
has already accepted or rejected the connection - in FastAPI, an ordinary `Depends` on the
`@app.websocket` handler, which runs before the endpoint body and therefore before muxws completes
the handshake.

**A token in the subprotocol list and the codec subprotocol coexist** in the same header. The
WebSocket handshake offers a *list* of subprotocol values, so muxws offers `muxws.v1.<codec>` as its
first entry and the application may append its own - `new WebSocket(url, ['muxws.v1.json',
`bearer.${token}`])`. On the accepting side, muxws matches only the entry with the `muxws.v1.`
prefix and ignores every other value entirely, leaving them for the application's authentication to
read; and the value it selects as the negotiated subprotocol is always its own, because that is the
one the client's peer will check. muxws does not interpret a credential and ignores entries without
its prefix.

Per-stream `headers` exist and an application may put a scoped token in them, but **muxws never
interprets headers**. Re-authenticating per stream would mean muxws understanding credentials, which
is squarely in the non-goals; a connection whose credential expires mid-life should be closed with
`goaway`, and the client's reconnect helper will dial again and re-authenticate at the handshake.

This is also why a credential does not belong in the reconnect helper's `hello` payload, although
both identify the client at connection time. The credential is *proved* at the handshake, once per
socket, by the framework, and a reconnect re-proves it by definition. The hello carries **identity
that the client asserts and the server merely records** - which tab this is, what it is looking
at - and is replayed verbatim because it is not a secret and does not expire. A token expires, so it
cannot be replayed verbatim.

---

## Timeouts and cancellation

**`request(timeout=...)`**: no default timeout. A stream lives until it ends, is reset, or the
connection dies - and "the connection died" raises `ConnectionLost` from the pending call when the
socket closes, which covers the case a default timeout would cover. Actions can take minutes, so a
fixed library default such as 30 seconds would fail them. When a timeout *is* given and expires, the
peer sends `reset(TIMEOUT)` (so the remote stops working; otherwise the remote keeps using resources
for an abandoned request) and raises `StreamTimeout` locally.

**Cancellation is cooperative in the same sense as `asyncio` cancellation.** `stream.cancel()` sends
`reset(CANCELLED)` and closes the stream locally *immediately* - the caller does not wait for
acknowledgement, and any payloads still in flight from the remote are discarded. On the remote side,
the handler task is cancelled: in Python the peer calls `task.cancel()`, so the handler observes
`asyncio.CancelledError` at its next `await`; in TypeScript the handler receives `stream.signal`, an
`AbortSignal`, and any `await stream.send(...)` after the reset rejects with `StreamReset`. A
handler that catches and suppresses cancellation keeps running but cannot send.

**The mapping runs both ways, and must be symmetric:**

- Local `asyncio.CancelledError` propagating out of `await stream.result()` or an `async for` (for
  example because the enclosing `asyncio.timeout()` block fired, or a `TaskGroup` is unwinding)
  → the peer sends `reset(CANCELLED)` and re-raises `CancelledError`. Never swallow it: a coroutine
  that absorbs `CancelledError` breaks structured concurrency.
- Incoming `reset(CANCELLED)` on a stream this peer is *producing* on → cancel the handler task.
- Incoming `reset(APPLICATION_ERROR)` on a stream this peer is *consuming* → raise `RemoteError` out
  of the pending `await` or the `async for`.

---

## Error model

Python exception hierarchy - all raised from the awaiting call site, never swallowed into a callback:

```
MuxwsError
├── ProtocolError            # this peer or the remote violated the spec
├── ConnectionClosed         # socket died; carries .code, .reason, .was_clean
├── ConnectionGoingAway      # open() after goaway - raised synchronously out of open()
├── StreamAlreadyConsumed    # await and iterate, or two iterations, on one stream
├── StreamClosed             # send()/end()/reply() on a stream that closed normally
├── CodecError               # configuration; carries .configured and .available
│   ├── CodecNotRegistered   # the configured name was never registered - raised at startup
│   └── CodecMismatch        # the acceptor's codec differs; the handshake was rejected
└── StreamReset              # carries .code (ResetCode), .reason, .stream_id
    ├── RemoteError          # code == APPLICATION_ERROR; carries .payload
    ├── StreamTimeout        # code == TIMEOUT
    ├── StreamRefused        # code == REFUSED; not processed - retry, elsewhere or later
    └── ConnectionLost       # code == CONNECTION_CLOSED; synthesised locally, never from the wire
```

`StreamClosed` is not a `StreamReset`. A stream that both ends have ended has not failed, and a
`send()` that arrives after the end is an expected race. As a `StreamReset` it would report a reset
that did not happen; as a `ProtocolError` it would report a bug that does not exist. Callers that
genuinely do not care about the difference catch `MuxwsError`.

`StreamRefused` covers every refusal, including concurrency saturation: the limit is not announced,
so the caller's reaction is the same: `REFUSED` means nothing ran, and where to retry is the
application's judgement either way.

`ConnectionLost` requires a different reaction: there is no
connection to retry on, so the correct reaction is neither "retry now" nor "back off and retry here"
but "wait for `on_reconnect` and rebuild". It is also the only member of the family that is never
decoded from a `reset` frame - the peer synthesises it for every stream that was live when the socket
died. It differs from `ConnectionClosed`: `ConnectionClosed` is the *connection*
failing and is what `serve()` and peer-level calls raise; `ConnectionLost` is a *stream* failing
because the connection did. "Reconnect: the socket, not the streams" specifies exactly which call
raises which.

The two `CodecError` subclasses sit outside `StreamReset` because neither is a stream failure and
neither is retryable: both mean the deployment is misconfigured, and both fire before any stream can
exist. `CodecNotRegistered` is raised locally, on the first connection attempt, with the message shown
in "The codec is a deployment decision". `CodecMismatch` is what a rejected subprotocol handshake is
translated into - which is the only translation available in a browser, where the raw handshake failure
is deliberately opaque to JavaScript, so the peer reconstructs the diagnosis from the codec name it
offered and reports that rather than a bare "connection failed".

TypeScript mirrors this with classes of the same names, delivered as promise rejections and as
`throw` inside `for await`. Both languages must set a `name` / `__class__` discriminator so
cross-language tests can assert on error identity.

**Application errors.** A handler that raises produces `reset(APPLICATION_ERROR)` with a `reason`
and an optional structured `payload`. The default serializer sends `{"type": "ValueError",
"message": str(exc)}` - useful in development, and a potential information leak in production, so
the peer takes an `error_serializer` hook and the documentation must say plainly that a
public-facing deployment should redact it. muxws does not attempt to map exceptions to status codes;
it does not know the application's error types.

**The hook is supplied per peer** - an argument to `connect()` and to whatever constructs the
acceptor's peer - rather than a module-level default, and the reason is that one process routinely
holds both kinds of connection. A service that accepts browser sockets on one endpoint and dials a
sibling service on another wants the browser peers redacted to a code and the internal peer carrying
the full traceback, and a process-wide setting would apply one choice to both. A per-peer setting is
chosen where the remote is known.

**Connection death.** Every live stream is closed with a locally synthesised
`StreamReset(CONNECTION_CLOSED)`, delivered to its local caller as `ConnectionLost`; every pending
`request()`, every pending `await stream`, and every `async for` raises it rather than hanging. Then
`on_close(reason)` fires - once per socket loss, with a `CloseReason` carrying `code`, `reason`,
`was_clean` and `will_retry`: the close code and text the socket reported, whether the close was
orderly, and whether the reconnect helper intends to dial again. Those four are the same four in both
languages, so a connection indicator reads the same fields in both. Nothing is retried
automatically at the stream level; the next section specifies what is re-established and what is not.

---

## Reconnect: the socket, not the streams

**v1 does not resume streams across a reconnect.** A dropped connection resets every stream on it with
`CONNECTION_CLOSED`, and the new socket starts with an empty id space.

Resumption belongs to the application. Resuming a stream requires the sender to have retained
everything the receiver might not have received, and to know how much that was - a per-stream
acknowledged byte offset, retained buffers with a retention policy, and a resumption token that
survives process restart. That is a durable-state problem, and durable state is inherently
application-specific: what to retain, for how long, and what a partially-delivered result even means
are questions muxws cannot answer for anyone. Partial resumption, such as buffering the last N
frames, loses data without an error when the gap exceeds N.

`backchannel` is the worked example of doing this correctly one layer up: it *always* persists
progress and dialog state server-side and treats a muxws push purely as an accelerator over a REST
polling baseline. After a reconnect, the client re-reads state and continues; no state is lost,
because no state exists only in a stream. Any consumer that needs continuity should follow that
shape rather than ask muxws for it.

What the client peer *does* ship is a socket-level reconnect helper, and the rest of this section
specifies it in detail.

The helper generalises the WebSocket client and endpoint of the KlubIS application.
`fast_api/main.py` there is a WebSocket endpoint that takes a client-supplied `tabId` in an `init`
message, subscribes the socket to Redis pubsub channels per tab, per user, per project and per
project-user, forwards a `cmd` message to Celery as `send_task(..., args=(ws_tab, cmd), ...)` - the
tab id travelling as the task's first argument, so the worker knows which channel to publish its
answer on - and fires two catch-up tasks (`get_changelog`, `get_init_data`) from
`ws_connect_tasks()` on every `init`, which its own client sends on every connect.
`vue/ws/web-socket-manager.ts` is the client half: exponential backoff `min(1000 * 2 ** attempts,
30000)`, a 20 s application-level `ping` heartbeat that the endpoint answers with a `pong`, the
attempt counter reset on open, and the `init` message re-sent from `onopen`. `tasks/ws_utils.py`
names the channels (`ws_channel_tab:<id>` and its siblings) and holds a small per-tab shared state
in Redis. The helper implements these mechanisms in the transport.

Where this specification deliberately differs from the precedent - jitter on the delay, the counter
resetting on an established rather than an open connection, a deadline on the heartbeat's reply, the
identity replay owned by the helper instead of by the client's `onopen`, and `sessionStorage`
instead of module scope - the difference is called out where it arises, together with the problem it
solves. A library parameterises and guarantees what an application with a known deployment can
hard-code or leave to its own client.

### The backoff algorithm

The helper's entire state is an attempt counter, and the schedule is the usual one plus jitter:

```
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

| Option | Python | TypeScript | Default | Meaning |
|---|---|---|---|---|
| initial delay | `initial_delay` | `initialDelayMs` | 0.25 s / 250 | Delay before the first retry, before jitter. |
| growth factor | `factor` | `factor` | 2 | Multiplier per consecutive failed attempt. |
| cap | `max_delay` | `maxDelayMs` | 30 s / 30 000 | Upper bound on the pre-jitter delay. |
| jitter | `jitter` | `jitter` | 0.3 | Fraction of the delay, applied symmetrically: ±30%. |
| attempt cap | `max_attempts` | `maxAttempts` | unlimited | After this many failed attempts the peer stops reconnecting and closes. |
| heartbeat | `ping_interval` | `pingIntervalMs` | 20 s / 20 000 | How often a `ping` frame goes out on an idle socket. |
| heartbeat deadline | `ping_timeout` | `pingTimeoutMs` | 10 s / 10 000 | How long a `pong` may take before the socket is declared dead. |
| hello deadline | `hello_timeout` | `helloTimeoutMs` | 10 s / 10 000 | How long the hello exchange below may take before the attempt is failed. |

**The attempt counter increments on every failed attempt and resets when the connection is
*established* - and this document defines "established" as two things rather than one:** the socket
is open with the `muxws.v1.<codec>` subprotocol accepted, and the hello (next subsection) has been
acknowledged. Resetting the counter when the socket opens fails when a server accepts sockets while
its backend is failing - a backend that is up before its database is, a deploy that is half rolled
out, a gateway that terminates the upgrade and then cannot reach the application. With a reset on
open, every attempt counts as a success, the counter stays at zero, and retries run at the initial
delay for the whole outage. Resetting only when the connection is *usable* makes the helper back off
from partial failures as well.

**Jitter spreads reconnections out.** KlubIS's schedule has none; without jitter: when the server
restarts, every open tab in every browser sees its socket close within the same few milliseconds, so
every tab computes exactly 1000 ms and retries on the same millisecond. If the server is not ready
yet - and a server that has just restarted usually is not - they all fail together, all compute
exactly 2000 ms, and collide again; and because the schedule is deterministic they keep colliding on
every subsequent doubling. Backoff makes that load periodic without spreading it, and it reaches a
process that is still starting. With ±30% jitter, N tabs spread their first retry over a 175-325 ms
window, and every doubling widens the window proportionally.

Jitter allows the 250 ms initial delay. On a deterministic schedule, a short first delay is a
synchronised loop against a starting server, so a longer first delay such as 1 s is needed. With
jitter, a transient interruption (a Wi-Fi handover, a proxy recycling one connection, a load
balancer draining a node) recovers in 0.25 s, and an outage reaches the 30 s cap in about eight
attempts.

**Reconnection applies to connections that were lost, never to establishing the first one**, and the
first attempt is not retried. `connect()` awaits the first attempt and **raises if it fails**, with
the underlying error, even when unlimited retries are configured. A `connect()` that retried in the
background would report no error for a wrong URL, a host that does not exist, a codec mismatch or an
expired token, none of which a retry fixes. A connection that worked and then dropped had a valid
URL and credentials, so a retry can succeed. The caller handles the first attempt; the helper
handles every attempt after a successful connection.

**The heartbeat detects dead sockets**, which backoff requires: backoff starts only when a socket
loss is detected. The peer sends a `ping` frame every `ping_interval` on an otherwise idle socket;
if no `pong` comes back within `ping_timeout`, the socket is declared dead, closed locally, and the
backoff path runs exactly as it would after a clean close. A half-open TCP connection - the browser
tab that was suspended, the laptop whose lid was closed, the NAT that dropped an idle mapping, the
load balancer that dropped the back end - produces no close event on the client, and TCP detects it
after minutes or, with keepalives off, never. KlubIS ships this heartbeat at exactly this
interval - the client sends `{"type": "ping"}` every 20 s and the endpoint answers `{"type":
"pong"}` - and for exactly the reason the wire format section already gives: it is an
*application-level* ping because browsers do not expose WebSocket control frames to JavaScript.
muxws's `ping`/`pong` frames implement this application-level ping in the protocol.

**muxws adds a deadline to the heartbeat.** KlubIS's client sends the ping and its server answers,
but the client does not time the answer: the ping's detection value there is indirect - writing to a
dead socket eventually makes the browser notice and fire `onclose`, which is a real effect and is
why the heartbeat helps at all. With a deadline on the `pong`, detection takes at most
`ping_interval + ping_timeout` instead of the platform's write timeout, and the reconnect tests
assert this bound without waiting on TCP. It costs one timer.

### Connection identity is re-established by the helper, not by the application

An application supplies a **hello** exactly once, at `connect()`:

```python
peer = await muxws.connect(url, hello={"tabId": tab_id, "user": user_id})
```

and the helper sends it, identical, on every connection this peer makes - the first one and every
reconnect alike - **before `on_reconnect` fires and before any application frame goes out**.

muxws does not interpret it. Mechanically the hello is `open(hello_payload, headers=hello_headers,
end=True)`: an ordinary stream, delivered to the acceptor's own `on_stream` handler like every other
stream, carrying whatever the application decided identity means. Because a handler that returns
without ending its stream ends it implicitly, the acceptor's handler acknowledges the hello by
returning, and no application code sends an acknowledgement. muxws controls the **timing and the
repetition** of the hello, not its content.

Two consequences of it being an ordinary stream. The acceptor recognises a hello the same way it
recognises anything else - by looking at the payload it was handed, in its own vocabulary, in the
same `match` statement that handles every other opening payload; muxws does not mark it, because a
marker would add a muxws vocabulary. And `hello` is optional: a peer given none simply does not send
one, is established as soon as the socket is open with the subprotocol accepted, and has no hello
exchange. Service-to-service peers that authenticate at the handshake and have nothing further to
assert are the normal case for that.

When each client re-sends its identity itself, a client that omits it gets a connected socket the
server cannot associate with a client: it receives nothing and reports no error. Because the helper
replays the hello, every reconnected peer identifies itself.

Three rules keep it that way:

- **The hello is captured at `connect()` and replayed verbatim.** It is not re-read, not recomputed,
  and not a callback. Identity that changes is not a reconnect, it is a different connection: an
  application whose identity changes closes the peer and connects a new one. Note that a *credential*
  is not identity and does not belong in the hello - authentication is a handshake concern, so a
  reconnect re-authenticates by dialling again, and an expired token surfaces as a rejected
  handshake rather than as a stale value replayed forever.
- **`on_reconnect` fires after the hello is acknowledged, never before.** An application that
  re-subscribes from `on_reconnect` before the hello is acknowledged would subscribe on a socket the
  server has not yet associated with a client.
- **A failed hello is a failed connection attempt.** If the hello stream is reset, or does not
  complete within `hello_timeout`, the peer closes the socket, does *not* fire `on_reconnect`,
  increments the attempt counter and backs off. The helper backs off from a server that accepts
  sockets but cannot yet process the hello, instead of keeping the connection.

On the acceptor side the hello is the natural place where `peer.tags` gets written from
client-supplied data, and the only place *indexed identity* is written that way, which is what makes
`PeerRegistry.peers_for(tab=...)` findable at all. Later messages may write tags too - backchannel's
per-operation delivery overrides are one, replaced wholesale by an ordinary stream handler every
time the client re-declares them, and never registered, because nothing ever looks a peer up by
them - but identity belongs in the hello specifically because the hello is the one message whose
replay the helper guarantees, and a tag that is not written again after a reconnect is absent.

KlubIS does the same thing one layer further out: the tab id from `init` becomes both the pubsub
channel name (`ws_channel_tab:<id>`, in `tasks/ws_utils.py`) and the key of a small per-tab shared
state in Redis, so that a Celery worker holding no socket can still publish to the right tab. `tags`
plus `PeerRegistry` is the in-process half of that arrangement; the cross-process half is a
backplane, and muxws has no backplane; backchannel has one.

### The identity survives a page reload as well as a socket drop

KlubIS mints its tab identity with `const tabId = crypto.randomUUID()` at module scope in
`vue/ws/main-web-socket.ts`. That survives every socket drop, which is what it was written for, and
a page reload re-evaluates the module and mints a fresh one. For a consumer that attaches
recoverable state to the identity, a reload during a long import makes the tab a new client to the
server.

The recommended lifetime is `sessionStorage`:

```ts
function tabId(): string {
  let id = sessionStorage.getItem('muxws.tab');
  if (!id) sessionStorage.setItem('muxws.tab', (id = crypto.randomUUID()));
  return id;
}

const peer = await connect('wss://host/ws', { hello: { tabId: tabId() } });
```

`sessionStorage` is per-tab and survives reload and in-tab navigation, which is the lifetime of a
tab identity. `localStorage` is not suitable: it is shared across every tab of the origin, so two
tabs would claim one identity and the server would fan one tab's traffic to both. Module scope is
not suitable either: a reload creates a new identity.

The identity ends when the tab closes. What should outlive a tab
is not the connection identity but the *application-level state* the tab was watching - a running
import's progress, an open dialog, a half-answered question - and that belongs one layer up, to
something that persists it server-side and hands it back on request. See the backchannel document,
where the server-side store is authoritative and push only reduces latency: a brand-new tab with a
brand-new identity fetches one snapshot per token it cares about and is fully caught up, and nothing
about the old tab's identity needed to survive for that to work.

muxws recommends this and does not implement it. Minting and storing an identity is three lines
whose shape depends entirely on what the application considers an identity, and a library that
generated one would be defining a vocabulary - see "Two vocabularies over one socket" for why muxws
defines no vocabulary.

### `on_reconnect` is where the layer above rebuilds

**`on_reconnect(attempt, peer)` guarantees two things: a live socket, and an
identity the acceptor has already accepted on it** - it fires only after the socket is up with the
subprotocol accepted *and* the hello has been acknowledged, which is what makes the second half of
that promise true. That is all muxws restores. **Every stream is gone, nothing
is replayed, no frame that was in flight is re-sent**, and the new socket's id space starts empty.
On the acceptor side the peer is a new object with a fresh, empty `tags`, filled only by the
framework's own authentication, by the replayed hello, and by whatever the client goes on to say on
this socket: per-connection state a handler wrote to the old peer is gone, which is the guarantee
"`tags` is connection-scoped state" describes from the other end.

Everything past those two things is the layer above's, and `on_reconnect` is the single point at
which it does that work: re-subscribing to what it was watching, re-fetching the snapshots it was
displaying, re-issuing any request whose answer never arrived and which is safe to re-issue. For
backchannel that obligation is one line - read the register once - and its own document says so in
those terms; the two statements are the same contract read from the two sides. Note what is
deliberately *not* on that list: the per-connection delivery overrides that client had declared on
the old socket are not re-declared, and nothing anywhere remembers them. They were in `tags` and are
gone, which is why backchannel keeps them there; the single read it makes returns what the server
decides without them, so the layer above restores only state it can read again.

The precedent is the same shape. KlubIS's endpoint fires `ws_connect_tasks()` - `get_changelog` and
`get_init_data` - from its `init` handler, which is to say on every connect, the first and every
reconnection alike, and unconditionally: a catch-up snapshot rather than a diff. An unconditional
re-fetch does not require the client to know what it missed, which would need durable state. A full
re-fetch needs no per-client history, and it is available to any consumer that kept a REST endpoint
capable of answering "what is the state now" - which is exactly the shape backchannel is built
around.

The API makes the boundary explicit: `Peer` survives the reconnect, `Stream` objects do not, and any
stream held across one is already closed. `on_close(reason)` fires on **every** socket loss, not only
the final one, and `reason.will_retry` distinguishes "reconnecting" from "offline" - the helper sets
it `False` only when `max_attempts` has been exhausted or `close()` was called deliberately. The other
three fields on `CloseReason` (`code`, `reason`, `was_clean`) say what the socket reported, which is
what an indicator needs to distinguish a close by the server from a network failure. An
application that wants a connection indicator needs no state of its own beyond those two hooks.

### What happens to the streams that were live

Without this rule, an `await` on a handle after socket death would wait indefinitely, without an
error or a log line.

**When the socket dies, every live stream on it transitions to `closed` and is failed locally with a
synthesised `reset(CONNECTION_CLOSED)`** - reset code 9, the one code that never appears on the wire.
Concretely, at that instant and before `on_close` fires:

- A pending `await stream` or `await stream.result()` **rejects** with `ConnectionLost`. The
  memoized future is resolved once, with that error, so a second await gets the same error rather
  than waiting indefinitely.
- An `async for` over the stream **raises** `ConnectionLost` out of the loop at the next iteration
  rather than terminating normally, because a consumer that saw the loop end cleanly would conclude
  the export finished.
- An in-flight `request()` **raises `ConnectionLost`**. It never returns a partial value and never
  hangs. `request()` has no default timeout (see "Timeouts and cancellation"), so connection death
  ends a request that receives no answer.
- `stream.send()`, `end()` and `reply()` raise `ConnectionLost`; `cancel()` and `reset()` are no-ops,
  because the stream is already closed and there is nothing left to tell the remote.
- `stream.closed` is set, so code that waited on the event rather than on a value also wakes.

**While the peer is disconnected, starting new work raises instead of queueing.** `open()`
raises `ConnectionLost` synchronously, exactly as it raises `ConnectionGoingAway`; `notify()` and
`request()` reject with it. Nothing is buffered for the next socket. muxws keeps no stream state
across a reconnect, and a queued frame sent to a server that no longer knows the sender would fail
without reaching a call site. `peer.is_open` is `False` for that whole window for applications that
would rather defer than fail - though the better answer is almost always to do the re-issuing from
`on_reconnect`, where the socket is open and the identity is established.

`ConnectionLost` is a `StreamReset` subclass and `ConnectionClosed` is not, and they have different
scopes. `ConnectionLost` says *this stream* failed because the connection did, and is what every
stream-shaped call raises. `ConnectionClosed` says *the connection* ended, and is what `serve()`
returns through and what peer-level calls raise. A caller that only wants to know "it broke" catches
`StreamReset`; a caller that chooses between retrying now, retrying from `on_reconnect` and giving
up needs the distinction.

The acceptor side has no reconnect helper. An acceptor peer whose socket closed is removed from
`PeerRegistry` by its close hook; a `notify()` on it before that removal raises `ConnectionLost` in
the pushing code, so a push is best-effort and backchannel's transport catches the error.

---

## Backpressure

v1 has **no per-stream flow control.** The only backpressure is the WebSocket send buffer, and that is
connection-wide: a peer that stops reading eventually stalls *every* stream, and a producer that
outruns its consumer on one stream buffers in the OS and in the WebSocket library.
What v1 does provide is the receiver's own concurrency limit (bounding how many producers can exist at
once) and the fragmentation constant (bounding how long one of them can occupy the socket), which
together are sufficient for the traffic muxws is designed for: control messages, progress updates, and
result sets in the kilobytes-to-low-megabytes range. Both are local defences rather than agreements -
see "Limits are local" - and that is exactly what distinguishes them from flow control, which is an
agreement by definition.

`window_update` is reserved as a frame type for when these limits are insufficient. The concrete
trigger for building it: **a real producer/consumer speed mismatch on a single stream** - a server
exporting rows faster than a browser can render them, with observed unbounded memory growth on
either end. Until such a mismatch is measured, per-stream credit windows would be a large amount of
state-machine complexity (an initial window both peers must agree on, which this connection has no
place to carry and would need a new generation to introduce, per-stream and connection-level
accounting, window-exhaustion stalls, the classic deadlock when both peers wait for credit) serving
no demonstrated need. Applications that need it before muxws implements it can use application-level
pacing, where the consumer sends an ack payload every N items and the producer waits for it - the
same mechanism, at the layer that knows what N should be.

---

## Observability

Every frame must be loggable in one line, and stream ids make correlation trivial. The suggested
shape, emitted at `DEBUG` under the `muxws.frames` logger:

```
muxws conn=a3f-17 dir=tx type=open   stream=7  end=0 bytes=214  headers=1
muxws conn=a3f-17 dir=rx type=data   stream=7  end=0 bytes=8192 frag=1/4
muxws conn=a3f-17 dir=rx type=data   stream=7  end=1 bytes=1904
muxws conn=a3f-17 dir=tx type=reset  stream=9  code=1 reason="user navigated away"
muxws conn=a3f-17 dir=rx type=goaway last=7 code=0
```

`peer.on_frame(handler)` receives `(direction, frame, byte_length)` before encode / after decode, for
applications that want metrics rather than logs: frames per second by type, open streams gauge, reset
counts by code, fragmentation ratio. The peer itself must never log payload *contents* at any level -
they are application data and routinely contain secrets.

### Connection ids are never reused

`peer.id` is what makes that log readable, so its shape is a decision rather than an implementation
detail: **a short random prefix minted once per process, plus a counter incremented once per
connection** - `a3f-17`, the seventeenth connection of the process that drew `a3f`. Never reused
within a process, never duplicated within a process, and across processes only the prefix can coincide.

The id guarantees no reuse within a process, not global uniqueness. A reused id would make two
connections appear as one connection in a log. A collision between two *processes* is
distinguishable by the process in the same log line, and a monotonic counter prevents reuse without
coordination.

---

## How fastapi-viewsets will consume this

Not part of muxws, and it will live in the fastapi-viewsets repo. It is described here only to show
that the API above is sufficient, and because two types in fastapi-viewsets were shaped for it in
advance.

`Context.clone_for_command()` already exists, and its docstring says it produces "an isolated copy of
this context for one command on a long-lived connection (e.g. a WS message)". `docs/guide/architecture.md`
states that `route_viewset` is "the HTTP **transport adapter**" and that `clone_for_command()` exists
"specifically so a future WS adapter can run this same pipeline once per message on one connection".
`ViewSetResult`'s docstring anticipates the mapping: "a future WS adapter would fold all three into
the outgoing message payload as plain JSON keys instead (no real headers/cookies/status line exist
over WS)".

The shape is simple - **one muxws stream is one command** - and the adapter has four prerequisites
in the current fastapi-viewsets code.

**Context cannot simply be built once per connection and cloned.** `clone_for_command(action_name)`
carries `self._action_configuration` over unchanged, and that dict is the output of
`resolve_action_configuration(cls, action_name)` for one *specific viewset class*. Only the action
name varies across a clone; the class does not. A connection whose Context was built while serving
viewset A, then serving a stream for viewset B, would hand B's middleware A's class-level and
method-level `@action_configuration`. So the adapter must rebuild the configuration per stream, via
`resolve_action_configuration(target_cls, action_name)`, and pass it into the clone - which means
either `clone_for_command()` grows an `action_configuration` parameter, or the adapter constructs the
`Context` itself from the cloned data dict.

**Context processors receive a `viewset` argument that does not exist at handshake time.**
`build_context(request, viewset, ...)` passes it to every processor, and a processor that branches
on the viewset or the action would be evaluated once per connection, for one viewset, rather than
once per command. Two options, and the adapter must pick one explicitly rather than by accident:
rebuild the whole Context per stream once the opening payload has named the viewset (correct, and
pays the processor cost per command), or keep the per-connection Context but restrict it to
viewset-independent processors, documenting that constraint for anyone deploying over WS.

**Every context value must survive a serialize/deserialize round trip.** `clone_for_command` goes
through `serialize_context()` / `json.dumps` / `deserialize_context()`, and that path is *never*
exercised over HTTP - a processor that puts a plain domain object into context, or a `LazyObject`
whose resolved value is one, works over HTTP and raises when a WS stream clones the context. This
constrains context processors: it is the same constraint the Celery path already imposes, which is
why `SerializableObject` and `settings.viewsets_context_json_encoder` exist, and the WS adapter's
documentation must say so.

**`lifecycle_runner` has to grow a way to hand back the `ViewSetResult`.** Today `execute()` runs
`run_command_chain` only when `response is not None`, immediately writes `result.headers` /
`result.cookies` / `result.status_code` onto that live Starlette `Response`, and returns
`result.body`; the `ViewSetResult` is consumed and discarded. A WS adapter therefore cannot get one
out of the existing pipeline at all - it would need a `Response`-shaped object to read from, or its
own runner. Note that `ViewSetResult`'s own docstring already promises the WS mapping ("a future WS
adapter would fold all three into the outgoing message payload as plain JSON keys"), so the
docstring and the runner currently disagree about whether that is reachable. **This is a
prerequisite in fastapi-viewsets, not work inside the adapter**: `lifecycle_runner` needs an
explicit transport sink - an object the chain's result is handed to, with `route_viewset` supplying
the HTTP one that writes headers/cookies/status onto the `Response` and the WS adapter supplying one
that folds `body` / `headers` / `cookies` / `status_code` into a single JSON payload. Once that
exists, the command middleware chain really does run exactly as it does over HTTP.

With those four in place: the opening payload names
the viewset and action, the chain runs, and the folded result goes back as the single reply. An
action that wants to stream (a paginated list, a long export) sends several `data` frames instead of
one - the same pipeline with a different call shape; see "Three call shapes, one primitive".

### What is being extracted is a protocol, not "the WS transport"

The HTTP transport stays in fastapi-viewsets; the two transports are not symmetric.

What is leaving is a **general-purpose WebSocket protocol**: framing, multiplexing, stream lifecycle,
cancellation, connection lifecycle, with no idea that viewsets exist. What is *not* leaving is the WS
transport **adapter** for viewsets - the thing that is actually analogous to `route_viewset`, that
reads `{"action": ..., "model": ...}` off an opening payload, resolves a viewset class, builds a
`Context`, runs the command middleware chain and folds a `ViewSetResult` into a reply. That adapter
stays in fastapi-viewsets, taking an optional dependency on muxws. So the end state is an HTTP adapter
and a WS adapter sitting side by side in one library, with a protocol package underneath the second of
them - not two adapter packages around a core.

**The test for a split like this one is whether someone who does not use viewsets would install the
package.** For muxws, yes - multiplexed streams over a WebSocket is a thing people want on its own,
which is the entire premise of this document. For backchannel, yes - progress and dialogs for
long-running work is a thing people want on its own too. For an HTTP adapter for viewsets, no. That
is not a package; it is a feature of the library it belongs to, and giving it a version number and a
release stream would be inventing a distribution boundary where no consumer boundary exists.

Removing HTTP from fastapi-viewsets would take `build_schema` and OpenAPI generation with it, since
a schema describes HTTP routes; it would take `response_classes` (`NotFoundError` is an
`HTTPException`, `NOT_FOUND_RESPONSE` is an OpenAPI responses fragment); and it would break the
frontend `route_rest`, which validates itself against the backend's `/schema` endpoint at runtime.
The remaining package would not work with FastAPI.

**The transport adapter boundary is documented here so that code does not cross it.** Two modules
are HTTP-specific: `route_viewset.py`, which imports `fastapi.Request`/`Response` at runtime and
synthesises the endpoint signatures, and `build_schema.py`, which reads FastAPI's route table
(`APIRouter`, `APIRoute`) to produce the schema the frontend validates itself against. Everything
the pipeline is actually made of is already transport-agnostic by construction: `Context`,
`ViewSetResult`, the command middleware chain, `clone_for_command()`.

Two parts lie on the boundary.
`lifecycle_runner.py` writes `result.headers` / `result.cookies` / `result.status_code` onto a live
`Response` - which is the fourth prerequisite above, and the reason it is phrased as "an explicit
transport sink" rather than as adapter-internal work: the fix moves that write into the HTTP
adapter. And `Request` appears as a `TYPE_CHECKING`-only annotation on context processors,
middleware and the auth adapters, which is the surface a real extraction would have to generalise
into a transport-neutral request object.

While the boundary stays where it is (the transport sink restores it), extracting a
transport-agnostic core means generalising one annotation and moving two modules. Extracting it now
would add two packages, two release streams, two changelogs and a version-compatibility matrix with
no current consumer, so the boundary is documented and not split.

---

## What backchannel needs from this

This list is the contract; it is short because backchannel's transport port is one method,
`notify()`, and backchannel's *envelope* traffic is push-only: there is no envelope in the client →
server direction at all. What the client originates travels some other way - a dialog reply over
REST or over a stream the *client* opens, a cancel request over REST, a `watch` over a stream the
client opens, none of them an envelope or a call on the transport port. Three
capabilities, and the parallel `backchannel` document lists the same three - each from its own side;
the wording differs and the content is the same. Only two of the three are calls backchannel makes:
`peer.notify()` and `PeerRegistry.peers_for()`. The first is a property of the protocol that those
two calls rely on, not a third method.

1. **Symmetric `open`** - the acceptor can start a stream to a connected client without that client
   having asked for anything. This is how a progress update or a dialog prompt reaches the browser,
   and it needs no push-specific machinery at all.
2. **A one-shot push** - `peer.notify(payload)`, a stream that is opened and ended in a single call
   and returns no `Stream` object. Every backchannel push is one envelope, so this
   is the only send shape it uses; and because it cannot leave a stream open - or, in the browser,
   an unobserved rejected promise from a handle nobody kept - a push can never accumulate open
   streams against the receiving peer's concurrency limit, or unhandled rejections in the console.
   Discarding the handle `peer.open()` returns would leave both, so backchannel uses `notify()`.
3. **Peer lookup by session tag** - `peer.tags["session"]` plus `PeerRegistry.peers_for(session=...)
   -> list[Peer]`, with removal on close handled by muxws. A session may have several open tabs, so
   this returns a list and backchannel fans out over it. The same dict is also where backchannel keeps
   each connection's per-operation delivery overrides - a map value, read off a peer that list has
   already handed back rather than looked up by, replaced wholesale whenever that client re-declares
   it, and discarded with the socket, not restored - see "`tags` is
   connection-scoped state".

backchannel needs nothing else. In particular backchannel does not need stream resumption, ordering
guarantees across streams, delivery acknowledgement, or persistence from muxws - it provides its
own, precisely because muxws does not. **backchannel must not require any muxws API beyond these
three.** It also does not need a socket of its own: in the deployment both consumers are built for,
backchannel's envelopes and the viewsets adapter's commands share one connection and are independent
of each other; see "Two vocabularies over one socket".

**Why the registry lives here and not in `MuxwsTransport`.** The registry is in muxws because an
index over connections has to be pruned when a connection dies, which means hooking `peer.on_close`
and getting the ordering right against `goaway`, drain, and socket death. muxws already owns all
three events; a `MuxwsTransport` that kept its own `session -> peers` map would be re-deriving that
close-hook logic from outside, and would have to handle the half-open socket and drain timeout cases
that muxws already defines. So `PeerRegistry` ships in muxws, `MuxwsTransport` calls
`peers_for(session=...)` and holds no registry of its own. The registry is per-process either
way - which is why backchannel has a Redis backplane, and is unaffected by this choice.

**`request`, `stream.send` and `stream.cancel` are not in that list**, because their consumer is the
fastapi-viewsets WS adapter described above. That adapter does unary request/response (one stream is
one command, `request()` on the client side, `reply()` on the server side), genuinely streams
multiple payloads on one stream (a paginated list, a long export via `stream.send`), and genuinely
cancels mid-flight when a user navigates away (`stream.cancel()`). Each capability has a named
consumer.

The requirement above limits what backchannel *requires*. Two things travel from
backchannel's dialing side, and neither widens the contract. The first is an optional
accelerator - sending a dialog reply over a client-opened stream instead of over REST - which is a
`request()` from the dialer; it is opt-in, every backchannel deployment works with it switched off,
and nothing on the acceptor side changes either way. The second is `watch`, which is not optional in
the same sense but needs nothing new either: it is a one-shot client-opened stream, and on the
acceptor side it is an `on_stream` handler assigning one map into `peer.tags` - the same two
mechanisms items 2 and 3 already name, used from the other end and by an ordinary application
handler rather than by any new muxws API. That `watch` carries a whole value and is idempotent and
last-writer-wins is what keeps it that cheap: the handler overwrites one key and returns, so a
client re-declaring its overrides ten times in a minute costs ten assignments and no registry work
at all. Both are listed so that the two documents agree.

---

## Repository and package layout

One repository shipping two packages on a single version stream, mirroring how fastapi-viewsets ships
`fastapi_viewsets/` to PyPI and `vue/` to npm and bumps both together.

```
muxws/
├── muxws/                     # PyPI: muxws
│   ├── __init__.py            # connect / accept / serve / register_codec / registry re-exports
│   ├── conf.py                # settings singleton; reads MUXWS_CODEC from the environment
│   ├── frames.py              # frame dataclasses, validation
│   ├── codecs/
│   │   ├── __init__.py        # Codec protocol, the codec registry, register_codec
│   │   ├── json_.py           # JsonCodec - always registered by the library
│   │   └── msgpack_.py        # under the [msgpack] extra; NEVER registers itself
│   ├── peer.py                # Peer: read loop, dispatch, local limits, ping, goaway
│   ├── stream.py              # Stream: state machine, fragmentation, iteration, __await__
│   ├── errors.py              # exception hierarchy + ResetCode enum
│   ├── registry.py            # PeerRegistry
│   ├── reconnect.py           # backoff policy, heartbeat, hello replay, reconnecting client peer
│   ├── transports/            # starlette.py, websockets_.py, memory.py (tests)
│   └── *_test.py              # pytest, colocated
├── ts/                        # npm: muxws
│   ├── index.ts               # browser entry: connect, Peer, Stream, errors, registerCodec
│   ├── codec.ts               # Codec interface, registry, JsonCodec
│   ├── msgpack.ts             # subpath export /msgpack; NEVER registers itself
│   ├── node.ts                # subpath export: accept/serve over `ws`
│   ├── frames.ts stream.ts peer.ts errors.ts reconnect.ts registry.ts
│   └── *.spec.ts              # vitest, colocated
├── conformance/               # language-neutral JSON fixtures, read by BOTH suites
│   ├── frames/*.json          # logical frames + their expected JSON wire form
│   ├── sequences/*.json
│   └── invalid/*.json
├── SPEC.md                    # the normative wire format (this doc's wire section, promoted)
├── pyproject.toml             # version = X.Y.Z
└── package.json               # version = X.Y.Z, kept identical; "sideEffects": false
```

`SPEC.md` is normative and versioned separately from the implementations by the generation integer
in the subprotocol name (v1 today; see "Versioning lives in the subprotocol"); the packages' semver
tracks the implementations. **One versioning convention covers both libraries in this pair**: a
single monotonically increasing integer, bumped only for a breaking wire change, carried in the
lowest-level place that can carry it - the `muxws.v1.<codec>` subprotocol for muxws, since it has no
frame that both peers are guaranteed to exchange, and `v` in the envelope for backchannel, which has
no handshake of its own. Neither uses semver on the wire; semver is for the packages. Python has
**zero required runtime dependencies** (stdlib `json` and `asyncio` are enough for the default codec
and the peer); `starlette` and `websockets` are optional extras selected by which transport you
import, and `msgpack` is an optional extra selected by which codec you register - `pip install
muxws[msgpack]`. The TS package has zero runtime dependencies in the browser entry point, `ws` as an
optional peer dependency for `node.ts`, and `@msgpack/msgpack` as an optional peer dependency
reachable only through the `/msgpack` subpath. `"sideEffects": false` in `package.json` is required:
it permits a bundler to drop the msgpack subpath from a JSON build, as "Registration is explicit"
sets out.

---

## Decisions that are fixed

These decisions are fixed; changing one requires the user's agreement.

1. One symmetric `Peer` per language. No separate client and server implementations.
2. Server push is the acceptor calling `open()`, and its fire-and-forget spelling is
   `peer.notify(payload)` - `open(payload, end=True)` returning no `Stream` and no awaitable at all.
   No push-specific protocol machinery beyond that.
3. Stream ids are allocated by the library, never by the caller. Dialer odd, acceptor even, id 0 for
   connection-level, **open ids monotonic per peer** (`data`/`reset` may name any live id in any
   order), never reused, `goaway` on exhaustion. The id is taken and the `open` frame enqueued in one
   synchronous step, so ids reach the wire in allocation order.
4. One frame per WebSocket message, spelled-out field names, and the frame-to-message mapping is a
   pluggable `Codec` - JSON the default, the shipped-by-default registration, and the byte-for-byte
   interoperability baseline both languages must reproduce exactly. Every size limit counts bytes of
   the whole **codec-encoded** message, in both languages.
5. Fragmentation above `MAX_FRAME_BYTES` is mandatory, not optional, and slices are cut at codepoint
   boundaries with the envelope and escaping budgeted for by the sender. The cap is a **protocol
   constant of 65536 (64 KiB)**, not a negotiated value: it bounds how long one stream may hold a
   socket with a single global message order, so a larger frame is worse and not better, and a
   negotiated cap could only lengthen that time. A receiver may accept larger frames; a sender
   always fragments at the constant.
6. Interleaving is enforced: **at most one unsent fragment queued per stream, and the
   writer picks the next frame round-robin over streams with queued work.** No FIFO send queue.
7. End of stream is a flag on `open`/`data`; trailers are a field on the end-carrying frame. Neither
   is its own frame type.
8. v1 frame set: `open`, `data`, `reset`, `ping`, `pong`, `goaway`. `window_update` is
   reserved and unimplemented. **There is no `settings` frame and no negotiation of any kind:** every
   limit is either a protocol constant or a local receiver-side defence, and the only version on the
   wire is the generation in the subprotocol name.
9. Flow control in v1 is the receiver's own concurrency limit (default 100 streams, enforced locally,
   over-limit opens answered with `REFUSED`) plus the fragmentation constant. `max_payload_bytes` is
   likewise local, defaults to 67108864 (64 MiB), and is enforced **as fragments accumulate** rather
   than after reassembly. None of the three is announced. No credit windows.
10. Stream-level violations reset the stream; connection-level violations `goaway` the connection.
    Frames for closed or never-opened ids below the peer's high-water mark are ignored, not errors.
11. `REFUSED` covers no-handler, post-`goaway` opens, and an `open` beyond the receiver's own
    concurrency limit. There is no separate concurrency code and no `StreamLimit` exception.
12. Unknown frame types and unknown fields are ignored. A frame type that must be *acted on* rather
    than tolerated needs a new subprotocol generation; there is no extension advertisement.
13. One `on_stream` handler, called as `(payload, stream)` after the opening payload is reassembled;
    a handler that returns without ending its stream ends it implicitly. No routing, no dispatch
    table, in muxws - and **no message vocabulary either**: no `kind` field, no reserved key inside
    `payload`. Two independent consumers define their own closed sets over one socket, and a muxws
    vocabulary could match at most one of them. A change to a consumer's vocabulary requires no
    change in muxws and does not affect the other consumer.
14. Auth at the handshake. Per-stream headers exist but are never interpreted.
15. No stream resumption across reconnects in v1. The reconnect helper restores only the socket and
    the connection identity. Concretely: exponential backoff `min(initial * factor ** attempts,
    cap)` with symmetric jitter (250 ms, ×2, 30 s, ±30%, unlimited attempts); the attempt counter
    reset only when a connection is **established**, meaning socket open with the subprotocol
    accepted *and* hello acknowledged; a `ping`/`pong` heartbeat every 20 s with a 10 s deadline, so
    half-open sockets are detected; the application's `hello` payload captured once at `connect()`
    and replayed verbatim on every connection, before `on_reconnect` fires; and every stream that
    was live when the socket died failed locally with `CONNECTION_CLOSED`, surfaced as
    `ConnectionLost`, never left waiting. `open()`, `notify()` and `request()` fail fast while the
    peer is between sockets; nothing is queued for the next one. **Reconnection covers lost
    connections only** - `connect()` raises if the *first* attempt fails, whatever the retry
    configuration says, so a wrong URL surfaces instead of being retried forever.
16. `request()` has no default timeout.
17. `PeerRegistry` lives in muxws, indexes every key `peer.tags` held as of the last `register()`
    call - passing over values that cannot serve as a lookup key rather than raising on them, since
    a mutate-only key is under no obligation to be indexable - and returns `list[Peer]`. `tags` is
    per-connection application state that muxws neither reads nor persists: any key may be
    overwritten as often as the application likes for the life of the socket, whatever its value is,
    a direct read sees the newest value immediately, `peers_for` keeps answering from the last
    `register()` until another one replaces the peer's entries wholesale, and the dict is discarded
    with the connection. A key used for lookup is not changed, and a key that changes is not used
    for lookup; a consumer that needs both on one key calls `register(peer)` after each write. A
    reconnect starts a fresh dict, filled only by the acceptor's framework, the replayed hello, and
    whatever later messages that client sends.
18. Both packages ship from one repo on one version stream, with a shared JSON conformance corpus.
19. The codec is chosen from the environment - `MUXWS_CODEC` via `muxws.conf.settings`,
    `VITE_MUXWS_CODEC` in the browser - and never from a call argument, `codec=` surviving only as a
    test override. Codecs are registered explicitly, never register themselves at import time, and
    the npm package declares `"sideEffects": false`. An unregistered name raises at startup; there
    is no fallback to JSON.
20. The configured codec is carried in the WebSocket subprotocol as `muxws.v1.<codec>` and the
    acceptor **asserts** it - on a mismatch it selects no subprotocol and refuses the upgrade
    with 400. No negotiation, no fallback, no per-connection multi-codec support. The client
    composes its own `CodecMismatch` diagnostic from what it offered, naming both `VITE_MUXWS_CODEC`
    and `MUXWS_CODEC`, because a browser cannot read the rejection body; the server logs the same
    thing where the body *is* readable. The subprotocol's version component is the breaking-change
    generation, and it is the only version on the wire; additive revisions are announced nowhere,
    because unknown frame types and fields are ignored and so need no announcement.
21. `peer.open()` is synchronous and returns a `Stream`. `Stream` is both awaitable - yielding the
    result payload (the remote's first) from a memoized, re-awaitable future, a `PromiseLike` rather
    than a `Promise` subclass in TypeScript - and async-iterable, and the first of the two shapes
    used claims the stream while the other raises `StreamAlreadyConsumed`. Awaiting twice is not
    that error: the future is memoized and hands back the same value. `open()` takes no `timeout`,
    because it returns immediately and there is nothing for one to bound; deadlines live on
    `stream.result(timeout=)` and `peer.request(timeout=)`. `notify()` returns nothing; `connect()`,
    `accept()` and `request()` stay async, because each of them waits for I/O. Sending on a stream
    that closed normally raises `StreamClosed`, distinct from `StreamReset` (the stream was reset)
    and `ConnectionLost` (the socket died).
22. muxws sits at the bottom of the stack and **never depends on any package above it** - not
    backchannel, not fastapi-viewsets, not the frontend kit; not as an import, an optional extra, or
    a `TYPE_CHECKING` annotation. fastapi-viewsets depends on muxws directly for its own WS transport
    adapter, in parallel with backchannel's dependency, not through it.

## Decisions left to the implementer

- The exact internal decomposition of `Peer` (one read loop task plus a writer task, versus a single
  task with a send queue). A send queue is recommended as the place to enforce the round-robin
  fairness of fixed decision 6. The data structure (a deque of per-stream queues, a ready-set with a
  rotating cursor) is not specified; the policy is.
- Whether `Stream` iteration buffers unbounded payloads when the consumer is slow, or applies a local
  high-water mark and stops reading the socket. Prefer the latter; the threshold is yours.
- The Python WebSocket client dependency for `muxws.connect` (`websockets` versus `aiohttp`), and
  whether both are supported as extras.
- Logging library integration details, metric names, and whether `on_frame` is sync or async.
- Whether `PeerRegistry` is a module-level singleton, an injectable instance, or both. (Both, with
  the singleton as a convenience over an instance, is the safe answer.)
- Which msgpack library each language binds to, and whether `cbor` ships at all. Both are optional
  extras behind the same `Codec` port, so the choice is reversible and affects only applications
  that register one.
- Whether the codec registry is a module-level dict or an injectable object. The environment-driven
  path only ever needs the former; the `codec=` test override only ever needs an instance. Nothing
  else is needed.
- How the `.env` value reaches a Python process that is not a web server (a worker, a script). Reading
  `os.environ` in `muxws.conf` is the specified floor; whether the application layers `python-dotenv`
  or its own config loader on top of it is the application's business, and `settings.codec` is
  writable precisely so it can.
- Python 3.10 support versus requiring 3.11 for `asyncio.TaskGroup` / `asyncio.timeout`. This repo
  targets `>=3.10`; matching that costs a small compatibility shim.
- Test-suite mechanics for the live cross-language run (who spawns whom, port allocation, CI matrix).

---

## Implementation milestones

Each milestone is independently shippable and independently testable. Do not start the next one until
the previous one's tests pass in both languages where both are in scope.

**M1 - Frames and the codec seam, both languages, no sockets.** Frame model, validation, `ResetCode`
enum, the error hierarchy, the `Codec` port with `JsonCodec` behind it, `register_codec` /
`registerCodec` and the registry, and the fragmentation splitter/assembler as a pure function of
(payload, cap, codec). Ships as a usable release for anyone wanting to speak the protocol by hand.
The `conformance/frames/` and `conformance/invalid/` corpora are written *here* and both languages
read them from M1, so both languages are checked against one corpus.

**The codec seam belongs in M1 and not later**, even though nothing but JSON will exist until well
after M3. It is the boundary that every send path and every receive path crosses, so retrofitting it
means touching all of them in both languages, in a codebase that by then has a state machine and two
transports layered on top - and it means rewriting the corpus M1 produced, because a corpus written
against "the JSON bytes" rather than against "a logical frame plus what a named codec makes of it"
depends on JSON. The codec interface in M1 costs an interface and an indirection; adding it later
would require rewriting the corpus.

**M2 - Python peer core over an in-memory transport.** `Peer`, `Stream`, the state machine, id
allocation with parity in the same synchronous step as the enqueue, `on_stream`,
`open`/`notify`/`send`/`end`/`reset`, the awaitable handle (`Stream.__await__` over the memoized
future, re-awaitable, claiming the stream against iteration), `request` on top of it, async iteration,
cancellation mapping in both directions. Tested with two peers wired to each other in memory, no
WebSocket anywhere. Every row of the state table gets a test, including the illegal transitions.

**M3 - Real transports.** Starlette/FastAPI acceptor, `websockets` dialer (Python); browser
`WebSocket` dialer and `ws` acceptor (TypeScript, which also means porting M2 to TS, including
`Stream` as a `PromiseLike` with its default rejection handler). This is also where the codec stops
being purely local: environment selection (`MUXWS_CODEC` / `VITE_MUXWS_CODEC`), the loud startup
failure on an unregistered name, and the `muxws.v1.<codec>` subprotocol offered by the dialer and
asserted by the acceptor - none of which a transport-free milestone can exercise, because none of
them exists without a handshake. At the end of M3 a Python peer and a TS peer exchange frames over a
real socket, the first live cross-language test.

**M4 - Connection liveness and shutdown.** `ping`/`pong` with liveness timers, `goaway` with drain
semantics, graceful `close()`. This is the milestone where "what happens when things go wrong at the
connection level" is specified, and the milestone at which open question 1 (whether `headers` stays)
is answered by looking at whether anything actually uses it.

**M4 contains the liveness timer and the shutdown handshake.** It used to be "connection lifecycle",
and the majority of it was the `settings` exchange - the frame, the defaults-until-ack window, the
ack-as-ordering-point rule, `protocol_version` mismatch handling - all of which is gone with the
frame. The concurrency limit went with it in a different direction: it is now a receiver-side cap
enforced locally, so it belongs with the other receive-side caps in M5 and is built there. What is
left is the liveness timer and the shutdown handshake, and those two stay together and stay separate
from M3 for one reason: `goaway` drain ordering against in-flight streams is the part of this
protocol most likely to be subtly wrong, and it needs a milestone where it is the thing being tested
rather than a detail alongside two transports. It is the smallest of the middle milestones.

**M5 - Robustness and convenience.** Fragmentation wired into the send path with the one-unsent-
fragment rule and round-robin writer selection, the receive-side caps together - the frame cap, the
`max_payload_bytes` bound enforced as fragments accumulate, and the concurrency limit answering
over-limit opens with `REFUSED` - `PeerRegistry` (`register` / `registered` / `peers_for`,
re-indexing on an explicit re-`register` after a tag change, with no cost for a tag that is only
read from a peer and never looked up, and pruned by the peer's own close hook), `on_frame`
observability - and the reconnect helper on both clients, which is the largest single item here and
is a specified mechanism: jittered exponential backoff, the attempt counter reset only on an
*established* connection, the heartbeat wired to the `ping`/`pong` frames M4 built, the `hello`
payload replayed verbatim before `on_reconnect` fires and the attempt failed if it does not
complete, `on_close(reason.will_retry)`, and `ConnectionLost` raised out of every stream-shaped
call - both the ones that were in flight when the socket died and the ones attempted while the peer
is between sockets. First release usable in production, and the first release `backchannel` can be
built against - its three contract items are symmetric `open` (M2), `notify` (M2), and the registry
(here).

**M6 - Conformance, documentation, wire freeze.** The full `conformance/sequences/` corpus, the live
Python↔TS matrix in CI in both role assignments, `SPEC.md` promoted to normative under the v1
generation, and an explicit test that the forward-compatibility rule holds (an unknown frame type is
ignored and logged once, and the connection survives it). A second codec tests the codec interface
built in M1: ship `msgpack` as an optional extra in both languages, run the whole sequence corpus
through it by round-trip, and add the cross-language pair (Python peer and TS peer, both on msgpack)
to the CI matrix. It requires a codec module and a CI matrix row, and a defect in the interface is
found before 1.0. Tag 1.0; the wire format is frozen from here - the JSON wire form is what is
frozen, since it is the baseline every peer must reproduce.

**M7 - Developer documentation.** The VitePress site in a `docs/` workspace: a rationale and
architecture narrative, a quick start that works when copy-pasted, and an API reference covering every
public symbol in both languages with signature, parameters, return, raises and one runnable example -
all of it checked by tests that execute the examples and fail when a symbol is documented nowhere.

---

## Testing strategy

**Per language, unit level.** Codec round-trips, for every registered codec. Every state-table
transition, legal and illegal. Fragment splitting and assembly, including the pathological cases
that the fragmentation rules exist for: a payload where the byte-count slice point falls *inside* a
multi-byte codepoint (the splitter must move the boundary back, and in TypeScript must not split a
surrogate pair either), a payload of control characters whose escaping triples its encoded size
under JSON (the splitter must re-split rather than emit an over-cap frame), a slice-point sweep
asserting that no produced message exceeds the cap in bytes *of that codec's output*, a fragment
stream interrupted by a reset, and an over-cap payload with fragmentation disabled. The same
payloads live in the shared corpus, so both ports must produce the *same* fragment boundaries under
the same codec. Cancellation races - reset arriving after the stream already ended, reset arriving
while a fragment is half-assembled, cancel called twice. Frames for a closed id and for an id above
the peer's high-water mark, asserting silence in the first case and `goaway(PROTOCOL_ERROR)` in the
second. Two concurrent `open()` calls from different tasks, asserting that the ids on the wire are
increasing - which verifies that no suspension point exists between allocation and enqueue.
Connection death with N streams open. The receiver's concurrency limit at the boundary, asserting
that the limit-plus-one `open` is answered with `reset(REFUSED)`, that the opener's pending await
raises `StreamRefused`, that the streams already open are undisturbed, and - an assertion that fails
if the sender enforces a quota - that the *opener* raised nothing synchronously and put the `open`
on the wire like any other. `max_payload_bytes` at the boundary too, asserting the reset goes out on
the fragment that crosses the limit rather than after the last one, which fails for an
implementation that buffers the whole payload first.

**The handle's contract has its own tests.** `stream.id` readable on the line after `open()` with
nothing awaited in between. `await stream` twice, returning the same value the second time rather
than raising or hanging - the memoized future. `await` then `async for` on one stream, and `async
for` twice, both asserting `StreamAlreadyConsumed` and asserting that the *first* consumer still
received everything. A stream that is reset with nobody having awaited or iterated it, asserting in
TypeScript that no unhandled rejection is reported and that the failure did reach the peer's error
hook - the test for the default no-op rejection handler, which no other test exercises, because
every other test consumes its handle. And in Python, that `peer.open(...)` on a line by itself emits
no `RuntimeWarning`. One pair tests the division between the handle and `request()`: against a
remote that sends two payloads and then ends, `await peer.open(p)` resolves with the first and does
not raise, while `await peer.request(p)` raises - which fails if the count check moves to the handle
or `request()` stops waiting for the end.

**The reconnect helper needs tests of its own, and they are unit tests rather than conformance
fixtures**, because none of this is on the wire: it is entirely a property of the dialing peer, so
the shared corpus does not cover it and both ports must test it separately against the in-memory
transport, with an injected clock and an injected random source. The schedule is a pure function of
(attempts, options, random draw) and is tested as one: assert `min(initial * factor ** attempts,
cap)` before jitter, assert every jittered delay lands inside ±`jitter` of it, and assert that the
cap limits the delay. Two failure modes have their own tests. **The counter must reset only on an
established connection**: a server that accepts the socket and then drops it before the hello
completes must produce a growing delay sequence, not a fixed interval at the initial delay - which
is the test that fails under the obvious "reset in `onopen`" implementation. **And the first attempt
is not retried at all**: a `connect()` against a URL nothing is listening on must raise out of the
call, with unlimited retries configured, rather than returning a peer and dialling forever. **Jitter
must spread retries**: run N peers whose sockets all die at the same simulated instant and assert
their first retry instants are distinct and spread across the expected window, which fails if the
jitter parameter is stored but not applied.

The identity replay has its own tests, which detect a reconnected peer that did not identify itself.
Drop and restore the socket three times and assert the acceptor received three byte-identical
hellos, that `on_reconnect` fired after each acknowledgement and never before it, and that an
application which registered no `on_reconnect` handler at all still ends up with a peer the server
can find in its registry. A hello that the acceptor resets, and a hello whose acknowledgement never
arrives, must both leave `on_reconnect` unfired, increment the attempt counter and produce a
backoff - not a peer on an open socket the server cannot associate with a client.

**`tags` and the registry have tests for the guarantees consumers rely on**, all for a dict written
more than once. A tag written after `register()` must be visible on `peer.tags` on the next line,
must *not* be found by `peers_for` until `register(peer)` is called again, and must be found by it
immediately afterwards - under the new value and no longer under the old one. The three assertions
detect different defects: a registry that watched the dict and re-indexed on every write fails the
second, and one that never re-indexes at all fails the third. Then the pattern backchannel uses,
which is the two halves of that rule exercised together: register a peer under `session`, overwrite
a *different*, never-looked-up key - whose value is a whole map replaced outright each time, as
backchannel's overrides are - a hundred times without re-registering, and assert that every read
sees the newest value, that `peers_for(session=...)` returns the peer unchanged throughout, and that
the registry's index did not grow by a single entry. The last assertion fails if the registry
re-indexes on assignment. One more covers a map-valued tag: `register(peer)` called while that key
is present must neither raise on the unhashable value nor index it. And across a drop and restore,
the acceptor's new peer must start with a `tags` containing only what the framework and the replayed
hello put there: a tag the previous connection's handler added is gone, and the peer is not findable
by it. This test covers backchannel's delivery overrides and fails if tags are carried over a
reconnect.

Liveness and the streams that were live are the rest. A transport that drops `pong` frames must be
detected within `ping_interval + ping_timeout` and enter the same backoff path as a clean close,
without the test waiting on anything resembling a TCP timeout. With N streams open in every shape at
once - one awaited handle, one `async for`, one in-flight `request()`, one bidirectional stream
mid-send - killing the socket must make every one of them raise `ConnectionLost`, and the test needs
no timeout of its own: an `await` that does not complete is the failure under test, so the test
detects it rather than waiting for an error. Assert also that `stream.closed` is set, that
`on_close` fired exactly once with `will_retry` true, that a second `await` on an already-failed
handle raises the same error rather than blocking, that `open()` during the disconnected window
raises synchronously, and that nothing attempted in that window appears on the wire after the new
socket comes up. Finally, `max_attempts` exhausted fires `on_close` once with `will_retry` false and
never dials again.

**Codec configuration and the mismatch guard.** A configured name that was never registered,
asserting `CodecNotRegistered` at startup, that the message names the environment variable, the
value it found and the registered set, and that no socket was opened. A dialer and acceptor on
different codecs, asserting the acceptor selected no subprotocol and answered 400, that the dialer
surfaced `CodecMismatch` rather than a decode failure after connect or a bare connection error, that
its message names both `VITE_MUXWS_CODEC` and `MUXWS_CODEC`, that the acceptor logged both values,
and that *no* frame was exchanged. A peer offering `muxws.v2.json` against a v1 acceptor, asserting
the same rejection. A negative test checks that the guard does not reject matching codecs: a dialer
and acceptor on the same non-default codec, asserting they interoperate normally.

**Cross-language conformance is mandatory.** Without a shared corpus, two implementations of one
protocol can diverge without an error. The corpus lives in `conformance/`, is plain JSON, and is
read verbatim by both `pytest` and `vitest`. Three kinds:

`conformance/frames/*.json` - a list of `{"name", "frame", "json_wire"}` triples: a *logical* frame,
plus the JSON wire form that frame must take. The corpus is deliberately not "logical frame → expected
bytes", because the bytes depend on the codec. It is a corpus of logical frames,
with the JSON rendering pinned alongside them, and the two are checked differently:

- **The JSON codec is checked against `json_wire`.** Both suites assert `jsonCodec.decode(json_wire)
  == frame` and `jsonCodec.decode(jsonCodec.encode(frame)) == frame`. JSON is the interoperability
  baseline - the one encoding a Python peer and a TypeScript peer must agree on byte-for-byte, since
  it is the default and readable codec - so it is the codec that gets a pinned wire form at all.
  However, the round-trip test compares *parsed objects*, never byte-identical output, so neither
  implementation is forced into a particular JSON key order or whitespace. One separate test does
  pin a canonical key order (`type`, `stream`, then remaining keys alphabetically) for the benefit
  of anyone diffing logs.
- **Every other codec is checked by round-trip over the same logical frames.** `codec.decode(
  codec.encode(frame)) == frame`, for every frame in the corpus, for every registered codec. No wire
  form is pinned, because pinning msgpack bytes in a JSON fixture file would be unreadable and would
  freeze an encoder version rather than a protocol. What must hold is that the logical frame survives,
  which is the actual contract the `Codec` port makes.
- **Cross-language, per codec.** A Python peer and a TypeScript peer configured with the same
  non-JSON codec must understand each other over the sequence corpus. That is the test the
  round-trip cannot replace: a codec can round-trip perfectly within one language while two language
  bindings of the "same" format disagree about how they represent, say, an integer key or a binary
  payload. Every shipped codec has this pair in CI.

`conformance/sequences/*.json` - scripted exchanges, replayed by both implementations against the
in-memory transport:

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

Note `stream_ref`, not a raw id: the *fixture* refers to streams by ordinal, and the runner resolves
it - so the fixture does not choose ids. The top-level
`max_frame_bytes` is not a wire value: the cap is a protocol
constant, and the fixture tells the runner to construct both peers with a lower cap, which is
a test-only construction argument and the only reason any value other than 65536 exists.

The interleaving rule needs its own fixture, because an implementation can violate it while passing
every other test:

```json
{
  "name": "small-frame-overtakes-a-fragmented-payload",
  "max_frame_bytes": 1024,
  "steps": [
    {"peer": "dialer", "call": "open", "payload": {"blob": "<16 fragments worth>"}, "as": "big"},
    {"peer": "dialer", "call": "open", "payload": {"ping": 1}, "as": "small"},
    {"expect_frame_order": {"stream": 3, "no_later_than_frames_on": 1, "count": 2}}
  ]
}
```

The assertion is exactly the guarantee: the single small frame on stream 3 appears on the wire no
later than after the second fragment of stream 1. A FIFO writer, or a producer that enqueues all
sixteen fragments at once, fails this fixture.

`conformance/invalid/*.json` - malformed and illegal input with the required reaction, asserted as
"which frame goes out and does the connection survive": wrong parity, an `open` with an id not
greater than that peer's highest previous open, `data` after `end`, an over-cap encoded message,
garbage JSON, a fragment sequence interrupted by a non-fragment frame, a stream-level frame naming an
id above the peer's high-water mark (connection dies), and a `data` frame for an already-closed id
(connection survives, nothing goes out). "Garbage JSON" generalises to "a message the configured codec
refuses to decode", and the fixture says which - so the same case can be replayed under a binary codec
by substituting a byte sequence that codec rejects, rather than being silently skipped there.

**Live integration, both role assignments.** In CI: a Python acceptor with a TypeScript dialer,
*and* a TypeScript acceptor with a Python dialer, running the same scenario script - concurrent
unary requests interleaved with a streaming export and a server push, one of them cancelled
mid-flight, and a `goaway` shutdown at the end. Both directions must pass with the same script; a
failure in one direction indicates direction-specific behaviour.

The same matrix gets one reconnect scenario, because the reconnect helper is the one component whose
correctness depends on a real socket dying in a way the in-memory transport cannot fully imitate: kill
the acceptor process with streams open, restart it, and assert that the dialer re-dials on a jittered
delay, replays a byte-identical hello that the *other language's* acceptor accepts, fires
`on_reconnect` exactly once after it, and that every stream that was open raised `ConnectionLost` in
the meantime. The last assertion detects a caller left waiting across the reconnect.

**Property-based tests, where inexpensive.** Generate random legal frame sequences, assert both
decoders agree and that the state machine never reaches an undefined transition. Hypothesis (Python)
and fast-check (TS) can share generated corpora via the same fixture directory.

---

## Open questions

Each open question carries a recommendation and the condition that would change it. None of them
blocks M1.

1. **Should `headers` exist in v1 at all?** The application can put everything in the payload, and
   `headers` has one justification: a dispatcher can read routing
   metadata before a possibly-fragmented body has fully arrived. **Recommendation: keep it, justified
   solely by that, and delete it at M4 if no consumer uses it** - the wire freezes at M6, so M4 is the
   last moment to remove a field. Note that the one plausible consumer is the fastapi-viewsets
   dispatcher reading the viewset name off `headers` to pick a handler while the command payload is
   still being reassembled, which is why the samples above use `headers={"viewset": "item"}` rather
   than an invented `topic`. backchannel does not use headers at all, and neither does the reconnect
   helper's hello - it accepts `hello_headers` for symmetry with `open()`, but a hello payload is
   small, arrives in one frame, and has nothing to dispatch on. So the count of real consumers is
   still one, and M4 checks whether that is still the case.

2. **Subpath packaging, for the Node acceptor and for the codecs.** The brief takes the position
   that `ws` belongs behind `muxws/node` and the heavy codecs behind `muxws/msgpack`, so that
   neither reaches a browser bundle that did not ask for it. The codec half of that claim carries a
   specific mechanism - constant substitution, then dead-code elimination, then tree-shaking, given
   no self-registration and `"sideEffects": false` - and the mechanism is unverified until a
   production build confirms it. **Recommendation: verify it at M3 with an actual production build**
   (a JSON build and a msgpack build of the same app, comparing bundle contents, not just sizes)
   before either subpath becomes public API surface. If it does not hold, the bundle carries a few
   unused kilobytes - but the packaging shape should be settled before the API is.

3. **Should `request()` grow a configurable default timeout at the peer level?** Fixed decision
   today is "no default at all". **Recommendation: ship without it.** `Peer(default_timeout=...)` is
   a purely additive, non-breaking change, so it can be added when a consumer has a concrete case. A
   wrong default timeout would fail long actions in production without an obvious cause. The
   reconnect rules strengthen the recommendation rather than weakening it: the failure a default
   timeout is really guarding against is a request that will never be answered, and "What happens to
   the streams that were live" makes connection death end exactly those, promptly, with a
   distinguishable error. A timeout then only covers a remote that is alive and slow, and only the
   caller can choose that deadline.

4. **What exactly forces `window_update` to be built?** Written above as "a real producer/consumer
   speed mismatch with observed unbounded memory growth". **Recommendation: turn that into a metric
   with a threshold during M5** - the `Stream` iteration high-water mark being hit on a real
   deployment, counted and exported - so the decision is based on measurements. Until the counter is
   non-zero in production, it is not built. Note that the answer is now also more expensive: with no
   capability advertisement on the wire, building it means a `muxws.v2` generation rather than an
   extension inside v1 (see "Versioning lives in the subprotocol"), so the required evidence is
   higher.

5. **Name.** `muxws` is fixed. The name is also used by a few small unrelated projects in other
   ecosystems, none on PyPI or npm.
