---
title: muxws - multiplexed WebSocket transport
sidebar: false
search: false
outline: deep
---

# muxws

> **Status: Design - not implemented.** This document is the implementation brief for a *separate*
> library (`muxws` on PyPI, `muxws` on npm, its own repository). It lives
> in this repo only because muxws was designed while working on fastapi-viewsets, and because two
> types in this library (`Context`, `ViewSetResult`) were shaped in anticipation of it. It is
> deliberately **not** in the VitePress nav: it is not user documentation for fastapi-viewsets.
> The audience is the coding agent (or person) who will implement muxws from scratch.

muxws is a WebSocket protocol and reference implementation that mimics the *semantics* of HTTP/3:
many independent, concurrently interleaved streams over one socket, either peer able to open a
stream, per-stream cancellation, request/response and streaming-response shapes - plus unsolicited
pushes from the acceptor, which fall out of the symmetry for free.

muxws knows nothing about viewsets, progress reporting, dialogs, tasks or Celery. The dependency
direction is strict and never inverted:

```
fastapi-viewsets  --optional-->  backchannel  --optional-->  muxws
        \                                                      ^
         \--------------------- optional ---------------------/
```

muxws depends on none of the packages above it and must be usable standalone by anyone who wants
multiplexed streams over a WebSocket. The second arrow is not a redundancy: fastapi-viewsets' own
WebSocket transport adapter talks to muxws directly rather than through backchannel, because the two
consumers want different things from it - see "Where muxws sits in the stack" for the whole graph and
"Two vocabularies over one socket" for what happens when both of them are on one connection at once.

---

## Purpose and scope

A browser and a server that talk over one WebSocket normally end up inventing the same three things,
badly: a correlation id so replies can be matched to requests, a convention for "this reply has more
parts coming", and a way to say "never mind, stop sending". muxws is those three things, specified
once, implemented once per language, with a state machine behind them so the edge cases (cancel
racing with completion, socket death mid-stream, a peer that stops reading) have defined answers
instead of ad-hoc ones.

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

The rule that graph encodes is one line and it is not negotiable: **muxws must never grow a
dependency on any package above it.** Not an import, not an optional extra, not a "just for the type
annotation" `TYPE_CHECKING` block. The moment muxws knows what a viewset or a progress token is, the
claim that opens this document - that anyone wanting multiplexed streams over a WebSocket can install
it and use it - stops being true, and the test in "How fastapi-viewsets will consume this" ("would
someone who has never heard of viewsets install this package?") starts failing for the one package it
must always pass for.

The two arrows into muxws carry different traffic, which is why neither can be folded into the other:
backchannel uses exactly three things (`notify`, `peers_for`, and symmetric `open` as the property
those stand on), while the fastapi-viewsets WS adapter uses `request`, `stream.send` and
`stream.cancel` and never touches the registry. The detailed composition story - which package owns
which decision when all of them are deployed together, and how a dialog gets from a Celery worker to a
Vuetify modal - lives in the backchannel document rather than here; this section exists only to fix
the direction of the arrows.

---

## One implementation, not two

The central architectural claim, and the first question the design has to answer, in the user's own
words:

> "ne vem, če je sploh treba ločiti med 'serversko' in 'front-end' implementacijo. Po mojem je samo
> implementacija protokola ter API, ki se kliče, da se nek ukaz pošlje na drugo stran"

Yes - that is exactly right, and the design commits to it. There is **one symmetric `Peer` type per
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

Two consequences worth stating explicitly, because they are what this symmetry buys:

**Server push costs zero extra protocol machinery.** There is no push frame, no subscription
mechanism, no "server-initiated message" special case. The acceptor calls `peer.open(payload)` (or
`peer.notify(payload)`, which is the same thing ended immediately) with an even id, and the dialer's
`on_stream` handler fires. The dialer can answer on that stream, stream back, or cancel it - all the
same code paths as any other stream, in both directions. A protocol that treated the server as
reply-only would need a second, parallel mechanism for push, with its own correlation scheme and its
own cancellation story. Symmetry deletes that entire second mechanism.

**A Python peer may be the dialer and a TypeScript peer may be the acceptor.** Service-to-service
Python, a sidecar process that dials into a hub, a Node process accepting connections - all of these
are supported by construction, not by writing new code. The two language ports are ports of the
*same* peer, which is also what makes the cross-language conformance suite (see Testing) meaningful:
either port must be able to sit on either end.

---

## Problem: one socket, many concurrent operations

Start with the naive shape: JSON in, JSON out, one message at a time. It survives exactly until the
second concurrent operation. The client asks for a report, then the user clicks something else while
the report is still generating; now two replies are in flight and there is nothing on the wire that
says which is which. The universal fix is a correlation id in every message.

Once you have a correlation id you immediately need three more things. The report reply arrives in
pieces, so a message needs "more coming" versus "that was the last one" - a *stream*, not a reply.
The user navigates away, so you need "stop generating, I do not want it" - *cancellation* addressed
to one operation and not the socket. And the socket dies, so every in-flight operation needs to be
failed *locally* with a distinguishable error rather than hanging forever.

A correlation id that has a lifecycle, an end marker, a cancel signal and a defined death is not a
correlation id any more. It is a stream, and this is precisely the model HTTP/2 and HTTP/3 already
formalised. muxws takes that model - stream ids, frames, per-stream state, connection-level control
frames - and puts it on a WebSocket.

**Why not just open several WebSockets, one per operation?** Because each one costs a full HTTP
upgrade (TLS session reuse helps, the round trip does not disappear), browsers cap concurrent
connections per origin, every socket needs its own auth and its own reconnect logic, and the server
pays a connection's worth of memory per in-flight operation. Multiplexing over one socket is the
whole reason HTTP/2 exists; re-deriving "one connection per request" on top of WebSockets is moving
backwards.

**Why not Socket.IO / a generic pub-sub library?** Those give you named events and rooms - a
broadcast model. They do not give you a request/response correlation with a typed end, per-operation
cancellation, or a streaming reply that terminates. You end up rebuilding all four on top, which is
this document.

---

## Problem: who invents the stream id

The user's first guess at the mandatory API surface:

> "Pazi, da bo API čist ter maksimalno preprost za uporabo, z minimalnim številom obveznih parametrov
> (na hitro se mi zdi stream id ter payload edino, kar bi bilo res obvezno?)"

The mandatory count is lower than that. **The stream id is never a caller argument.** It is allocated
by the library, appears on the wire, and is exposed read-only as `stream.id` for logging and
correlation - but no caller ever invents one.

If callers picked ids, two things break. First, both peers can open streams (that is the whole point
of the symmetry above), so two independently chosen ids collide the moment both ends pick `1` at the
same time - and the collision is silent, producing two logical operations merged into one stream.
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
  safe recovery. `data` and `reset` may name any previously-opened id in any order - interleaving is
  the entire point of the design, and once stream 9 is open, every frame on the still-live stream 7
  names a lower id.
- **Exhaustion** at 2^31-1: the exhausting peer sends `goaway` with `last_stream` set to the highest
  id it has processed, stops opening new streams, lets in-flight ones drain, and closes. A client
  peer with the reconnect helper enabled then dials a fresh socket with a fresh id space. Nobody will
  ever hit this at one stream per user action, but "undefined behaviour at 2 billion" is not a
  specification.

So the required surface for starting a stream is `open(payload)` - and `payload` itself has a default
of `null`, because a stream whose meaning is entirely in its headers is legitimate. Zero mandatory
arguments. Continuing a stream is `stream.send(payload)`. Everything else - headers, the end flag -
is a keyword argument with a default.

Taking the id away from the caller does put an obligation back on the call: since the caller cannot
name the stream it just started, `open()` has to hand back an object that names it - and that object
has to be usable both as a handle (`stream.id`, `stream.send`, `stream.cancel`) and as the awaitable
the calling code is waiting on. "`open()` is synchronous, and the handle it returns is itself
awaitable" is where that obligation is discharged.

---

## Honest about what is and is not mimicked

"Mimics HTTP/3" is a claim about the *semantic* model only, and the difference matters enough to
state plainly rather than let a reader assume otherwise.

**What is not mimicked, and cannot be.** A WebSocket runs over one TCP connection (or one HTTP/2
stream, if the WS runs over h2 - which only makes this worse). Therefore:

- **No independent per-stream loss recovery.** QUIC's headline feature is that a lost packet stalls
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

**The engineering rule this honesty produces.** Because ordering is global and there is one send
queue, *a single large frame blocks every other stream for as long as it takes to transmit*. A 1 MB
DATA frame on a stream doing a bulk export, on a link with 1 MB/s of usable uplink, adds a full
second of latency to a 200-byte progress update on another stream. That is not a theoretical concern;
it is the default outcome if you naively send whatever the application handed you.

Therefore: **the implementation MUST fragment any logical payload whose encoded frame would exceed
the protocol constant `MAX_FRAME_BYTES`, 65536 bytes (64 KiB)** - the exact slicing rule is in
"Fragmentation" below - **and MUST interleave frames from other streams between the fragments.**
Interleaving is not a hope pinned on a well-behaved send loop; it is two concrete rules, stated here
because they are what makes the MUST above true rather than aspirational:

- **A stream may have at most one unsent fragment queued at any moment.** The producing task
  encodes fragment *n+1* only after fragment *n* has been handed to the socket. A sender that
  enqueues all sixteen fragments of a 1 MB payload in one go has already lost, whatever the writer
  does afterwards.
- **The writer selects the next frame by round-robin over the streams that have queued work.** Not
  FIFO: a FIFO queue with sixteen fragments already in it cannot interleave anything, because the
  ordering decision was made at enqueue time.

At 1 MB/s those two rules together bound the blocking window at roughly 64 ms - one fragment - for a
progress frame on another stream; at typical LAN speeds it is invisible. **The cap is a constant, not
a negotiated value**, and that is the point rather than a simplification: the cap exists to bound how
long one stream may monopolise a socket that has a single global message order, so a *larger* frame is
worse and not better. A negotiation would have optimised in the wrong direction - a machine-to-machine
link raising the cap because it can afford the bytes would be buying throughput it did not need by
lengthening exactly the window this rule exists to shorten. Fragmentation is therefore not optional
and not tunable, and a receiver that declines an over-constant frame resets that stream with
`PAYLOAD_TOO_LARGE` rather than silently accepting it. This rule is what makes the "HTTP/3 semantics"
framing an engineering position rather than a slogan: the one property WS cannot give us is precisely
the one we have to compensate for in the sender.

---

## Wire format

**One frame per WebSocket message.** How a logical frame becomes that message is the job of the
connection's **codec**, which is a deployment decision rather than a wire constant - the port, how it
is chosen, and how the two peers are prevented from disagreeing about it are all in "The codec is a
deployment decision" below.

**JSON text is the default codec, and the default is deliberate.** Not because JSON is efficient - it
is not - but because a developer with the browser devtools Network tab open can read the entire
protocol without tooling, and because "what is actually on the wire" is the first question every
debugging session asks. For the traffic shapes muxws targets (control messages, progress updates,
moderate result sets), byte-shaving is worth less than that, so a deployment that has not measured an
encoding problem should stay on JSON and lose nothing. A deployment that *has* measured one changes a
line in its `.env`, and nothing in this section changes except which bytes carry it.

JSON is also the **interoperability baseline**: it is the one encoding every muxws peer in either
language must implement, and must produce identically, which is what lets the conformance corpus in
"Testing strategy" assert on exact wire bytes at all. Everything below - the envelope fields, the size
accounting, the fragmentation rule, the frame types, the state machine - is specified over the
*logical* frame and holds under any codec. The JSON rendering is shown throughout because it is the
readable one, not because it is the only one.

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

Unknown fields are ignored, and unknown *frame types* are ignored too (logged once). That is a
forward-compatibility rule and nothing more: a v1 peer never sends a frame type this document does not
define, so nothing on a v1 connection can rely on the other side doing something with one. A frame
type that must be *acted on* rather than tolerated needs both peers to be known to understand it, and
the only place this design has to say that is the subprotocol generation - see "Versioning lives in
the subprotocol".

### How sizes are counted

**Every size limit in this specification is the byte length of the fully encoded WebSocket message** -
the complete codec output for the frame, envelope included, exactly as it goes on the wire. Not the
length of the payload before encoding, not the length of a `fragment` field, and - for a text codec in
particular - not a JavaScript string's `.length`, which counts UTF-16 code units and therefore
disagrees with Python's byte count on every non-BMP character. Under the JSON codec the TypeScript
port measures with `TextEncoder().encode(text).length` (or the equivalent incremental count) and the
Python port measures `len(text.encode("utf-8"))`; under a binary codec both simply take the length of
the produced buffer. In every case the number compared against `MAX_FRAME_BYTES` is the length of what
the socket is about to send.

Stating this once, and stating it in terms of the *codec's* output rather than "the JSON text", is what
keeps the two ports fragmenting the same payload the same way - the only reason a cross-language
conformance corpus can assert on frame boundaries at all - and what keeps that true when the codec
changes. A sender that budgeted against JSON text while msgpack bytes went on the wire would be
fragmenting against a size nobody is sending, and would produce over-cap frames on exactly the
deployments that chose the compact codec to make frames smaller.

### Fragmentation

Any frame that may carry `payload` may instead carry `fragment` + `more`. The receiver concatenates
the `fragment` values and hands the result back to the codec for decoding when a fragment arrives
without `more: true`. The interesting half is the sender, and it has to be specified precisely,
because the naive rule ("slice the payload every `MAX_FRAME_BYTES` bytes") produces frames that no
conforming receiver will accept and slices that cannot be encoded at all.

The shape of the rule is codec-independent: **the sender encodes the logical payload with the
connection's codec, slices that encoded form, and puts each slice into a frame that the codec then
encodes again.** Both facts that make the naive rule wrong survive that generalisation, and the JSON
codec is the worked example of each.

First, a slice has to land on a boundary the codec can represent. Under JSON, `fragment` is a
*string*: a cut at an arbitrary byte offset can land in the middle of a multi-byte UTF-8 sequence, and
a half sequence is not representable as a JSON string value - the frame is unencodable, not merely
ugly. A binary codec has the easy version of the same problem, since every byte offset is
representable, but it is the same rule, and the splitter is written once against it.

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
  receiver only as a floor of what it must accept - because nothing breaks when one peer is generous
  and a strict receiver is an ordinary defence rather than a protocol requirement.
- **Slices are cut at a boundary the codec can encode** - Unicode codepoint boundaries of the encoded
  text for a text codec, byte boundaries for a binary one. Under JSON in TypeScript that additionally
  means never splitting a surrogate pair, since a lone surrogate is not a codepoint. A splitter that
  would land mid-sequence moves the boundary backwards.
- **The sender budgets for the envelope and for expansion, then verifies.** The honest, boring rule:
  slice to `MAX_FRAME_BYTES - 512` bytes of encoded payload, encode the frame, and if the encoded
  message is still over the cap (under JSON, a payload of nothing but control characters can triple in
  size), re-split that slice and try again. The 512-byte reservation covers the envelope keys, the
  stream id, and the ordinary expansion of well-behaved content; the verify-and-re-split loop covers
  everything else. On a deliberately tiny cap - the splitter takes the cap as an argument, and the
  conformance fixtures drive it with 64 and 1024 bytes precisely to exercise the arithmetic, which is
  the only reason a value other than the constant exists anywhere - the reservation is
  `min(512, cap // 2)`, and the loop, not
  the reservation, is what guarantees the result fits. A cap too small to hold the envelope plus one
  indivisible unit is a configuration error raised when the peer is constructed, rather than
  discovered later as an infinite split loop. The splitter is a pure function of (payload, cap, codec)
  and is tested as one - see M1.

The reservation is a per-codec hint; the verify-and-re-split loop is the guarantee. That division is
deliberate, and it is the reason adding a codec does not mean re-deriving this section: a codec whose
expansion behaves differently simply converges in a different number of iterations, and a codec author
who picks a bad hint pays in wasted encodes rather than in over-cap frames on the wire.

The remaining rules are about ordering. Fragments of one logical payload must be contiguous *on that
stream* - frames of *other* streams may and should interleave between them, which is the entire
point, and at most one unsent fragment may be queued at a time (see "The engineering rule this
honesty produces"). Receiving a non-fragment frame on a stream with a fragment assembly in progress
is a stream-level protocol error. `end: true` may appear only on the final fragment. `headers` are
never fragmented: an `open` whose headers alone push the frame over the cap is a `PAYLOAD_TOO_LARGE`
reset - headers are metadata, and if they are 64 KiB they are a body.

### Frame types

**`open`** - opens a stream. Sender: either peer, using its own parity. Fields: `stream` (required),
`headers`, `payload`/`fragment`, `end`. `end: true` on the `open` is the unary request shape: "here
is the whole request, I will send nothing more". Receiving an `open` for an id with the wrong parity,
or for an id not greater than the highest id that peer has previously *opened*, is a connection-level
protocol error.

**`data`** - a payload chunk on an existing stream. Sender: either peer, on a stream where its own
side is not yet half-closed. Fields: `stream`, `payload`/`fragment`/`more`, `end`, `trailers`.

**End of stream is a flag, not a frame.** There is no separate `end` frame type: a peer with nothing
left to say sends `{"type": "data", "stream": 7, "end": true}` with no payload. One fewer frame type,
one fewer state transition, and no ambiguity about whether the end frame's payload counts as data.
The API exposes this as `stream.end()`.

**Trailers are a field, not a frame.** They ride on the frame that carries `end: true`. HTTP/2 needs
a separate frame because its body is an opaque octet stream that cannot carry structured metadata;
ours is a structured value the codec can encode alongside the body, whatever that codec is. This
keeps "the stream ended" a single event carrying everything known at the
end, rather than two events that a receiver has to correlate.

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
site - it is not an async call, so the failure arrives as a raised exception rather than as a future
somebody has to remember to await - and any of the receiver's own streams with an id greater than
`last_stream` are reset locally with `REFUSED` - they were never processed, so they are safe to retry
on a new connection. Streams at or below `last_stream` are allowed to finish until a drain timeout
(default 10 s) elapses, then the socket closes.

**`window_update`** - **reserved, not implemented in v1.** See "Backpressure".

### Limits are local, and the frame cap is a constant

**There is no `settings` frame and no negotiation of any kind.** An earlier draft opened every
connection with one, both peers stating their receive-side limits and acknowledging the other's, with
a defaults-until-ack window and an ack-as-ordering-point rule to keep a perfectly ordinary crossing of
two frames from turning into a `PAYLOAD_TOO_LARGE` reset nobody earned. All of that machinery is gone,
and it is worth saying what happened to each value it carried, because the frame was deleted one row
at a time and each row had a different reason.

`max_frame_bytes` is now the protocol constant `MAX_FRAME_BYTES` = 65536. It was the last row with a
plausible claim to being negotiable, and the claim inverts on inspection: **a bigger frame is worse,
not better.** The cap does not exist to protect a small receiver's memory - `max_payload_bytes` does
that - it exists to bound how long one stream may hold a socket that has exactly one global message
order. Raising it lengthens the window in which a progress update waits behind an export; lowering it
below 64 KiB buys nothing that the interleaving rules do not already buy. A negotiated cap therefore
offered exactly one direction of movement, and it was the wrong one. Making it a constant also deletes
the entire class of bugs where two peers disagree about which value is in force, which is what the
defaults-until-ack rules existed to paper over.

`max_payload_bytes` - the largest reassembled payload a receiver will accept - is a **local receiver
setting, default 67108864 (64 MiB)**, and it never appears on the wire. It is a memory bound, and a
memory bound is nobody's business but the receiver's; announcing it would only have let a sender
pre-empt a reset it was going to get anyway. Because bounding memory is the whole purpose, the reset
goes out **as soon as the accumulated fragments exceed the limit**, not after reassembly completes: a
receiver that dutifully assembles 900 MB in order to discover it exceeds 64 MiB has already lost the
thing the limit was protecting.

`max_concurrent_streams` is likewise **local and receiver-side**, default 100, and a receiver resets
opens beyond it with `REFUSED`. The announced version of it was mechanism without a purpose: a
negotiated stream quota is part of flow control, and this design explicitly defers flow control
(`window_update` stays reserved). What is genuinely needed is only the same class of protection as
`max_payload_bytes` - a bound on how much work one remote peer can make this one hold at once - and a
local limit is the whole of that. The consequence at the API is that `open()` never raises
`StreamLimit`, and the exception leaves the public surface entirely: a sender no longer knows a limit
it could fail against, and a stream refused for saturation is a `REFUSED` reset like any other
refusal.

`protocol_version` and `extensions` are the subject of the next subsection.

**So what is muxws, if it negotiates nothing?** Not a codec, and it is worth resisting the pull of
that description, because a library that only turns frames into messages and back could be a pure
function with no connection state at all - and this one cannot. **muxws is a connection manager.**
Multiplexing requires tracking which streams are live and in which state, reassembling fragments that
arrive interleaved with other streams' frames, resolving the awaitable handle that `open()` handed
back when the matching payload arrives, and keeping a liveness timer so a half-open socket is
discovered rather than waited on. None of that can be pushed up a layer, and the reason is structural:
**the application never sees a stream open.** It sees a handler invoked with a fully reassembled
payload, or an awaited handle resolving with a value - both of which are the *end* of work the peer
had to do while holding state the application was deliberately not shown. Deleting the negotiation
made muxws smaller; it did not make it stateless, and a design that claimed otherwise would be
describing something that cannot dispatch its own incoming streams.

### Versioning lives in the subprotocol

With the `settings` frame gone there is exactly one version on the wire: the generation in the
subprotocol name, `muxws.v1.<codec>`. It is bumped only for a change that genuinely breaks the wire,
and a v1 acceptor rejects a `muxws.v2.<codec>` offer at the handshake, before either side has sent a
byte of protocol.

The earlier draft carried a second, finer counter (`settings.protocol_version`) for the *additive*
revisions inside a generation - a peer that had grown a frame type announced 2 while still offering
`muxws.v1`. That counter had nothing to do: this specification's additive changes are new frame types
and new fields, both of which the receiver rule above says to ignore, so a v1 peer and a v1-plus-a-bit
peer already interoperate correctly without either knowing which one the other is. A version number
whose only use is to tell a peer something it does not need to act on is a version number that will
eventually be wrong in a log.

The one thing the counter *could* have supported, and the reason `extensions` had a row, is a
capability gate: a frame type that must be acted on rather than tolerated needs the sender to know, in
advance, that the other side understands it. `window_update` was the named candidate. muxws does not
have that gate any more, and the honest consequence is that **the day flow control is built, it is a
new generation** - `muxws.v2.<codec>` - rather than an extension advertised inside v1. That is a real
cost, paid deliberately: an advertisement mechanism with one hypothetical consumer is exactly the kind
of machinery this design keeps deleting, and a generation bump on a protocol whose two peers are
deployed together is cheap.

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
half-closed means closed. The unary shape is simply `open(end=true)` → `half_closed_local`
immediately, one `data(end=true)` back → `closed`.

**Frames for streams that are gone.** This is the single most common race on a multiplexed
connection and it needs one rule, not three. Peer A cancels stream 7; peer B's `data(7)`, sent a
millisecond earlier, is already on the wire. Nothing is wrong, and neither side may treat it as an
error:

> **Any stream-level frame naming an id that is not currently live, but does not exceed the highest
> id that peer has opened, is silently ignored** - the stream is closed, or was reset, or never got
> past the race. No reset, no connection error, at most a counter. **Any stream-level frame other
> than a valid `open` that names an id *above* the highest id that peer has opened is a
> connection-level protocol error**, because that is not a race: the peers disagree about the id
> space.

That second half is what keeps "ignore late frames" from degenerating into "ignore everything".

**And the retention rule that makes it decidable.** "Ignore frames for closed streams" is
unimplementable unless a peer can tell a closed id from an id that never existed, and remembering
every closed id forever is a per-connection memory leak on a socket that lives for hours. It does not
need to: a peer keeps exactly **`highest_open_seen` per parity plus the map of live streams**. An
incoming id is live (handle it), or below the high-water mark and not live (ignore it), or above the
high-water mark (connection error). Constant state, O(1) decision, no per-closed-stream bookkeeping.

**Illegal frames.** A frame that is illegal *for a stream* - `data` after receiving `end`, a
non-fragment frame mid-reassembly, a frame the receiver declines as over-size - resets **that stream**
with `PROTOCOL_ERROR` (or `PAYLOAD_TOO_LARGE`) and leaves the connection alone. An `open` beyond the
receiver's own concurrency limit is likewise stream-level, and its code is `REFUSED`: nothing ran, so
the opener may retry it, and since the limit is the receiver's own and unannounced there is nothing
finer for the opener to be told. A frame that is illegal *for the connection* - a message the codec
cannot decode, a missing `type`, a stream id with the wrong parity, an `open` whose id is not
greater than that peer's highest previous open, a stream-level frame naming an id nobody has
opened - ends the connection with
`goaway(PROTOCOL_ERROR)` followed by a socket close. The rule of thumb: if the peers can still agree
about the state of every *other* stream, keep the connection.

### Error / reset codes

Numeric on the wire, named in both APIs. The same table is used by `reset` and `goaway`. The last
column is not advisory: two codes exist separately only when they demand different behaviour from the
receiving side, so that behaviour is part of the specification.

| Code | Name | Meaning | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, an orderly shutdown. On `reset`, "I am done and no longer interested" without failure semantics. | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` on the reset frame carries a structured error object. | Surface to the caller. Retrying is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted, and definitively not processed - safe to retry. Used for no registered handler, for post-`goaway` opens, and for an `open` beyond the receiver's own concurrency limit. | Retry: elsewhere if another connection is available, otherwise after a delay - the receiver may simply be saturated. |
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

The obvious API would have been `connect(url, codec=MsgPackCodec())`. It is the wrong shape, and the
user's reasoning is the whole argument:

> "jaz bi sicer nastavitev kodeka dal v [.env] ... ne v function signature. ker potem ni spremenljivo z
> deploymentom, ampak je treba prav if v kodo dati. če pa je nastavitev encoderja v bistvu konstanta iz
> neke sistemske 'env' datoteke, je pa stvar transparentna med dev in prod"

A codec passed at the call site is a code-level constant: changing it between development and
production means an `if` in application code, keyed on something the application also has to invent a
way to know. A codec read from the environment is transparent - the same source file runs in both
places, and which encoding it uses is a fact about the deployment, exactly like a database URL. It
also stops being a per-call decision, which it never was: two connections in one process encoding
differently is not a feature anybody asked for.

**Python** reads it into a `settings` singleton, mirroring `fastapi_viewsets/conf.py` in this
repository - same shape, same reason, and an application that already knows one knows the other:

```python
# muxws/conf.py
class Settings:
    def __init__(self):
        self.codec: str = os.environ.get("MUXWS_CODEC", "json")

settings = Settings()
```

```python
from muxws.conf import settings
settings.codec = "msgpack"     # an application may also just set it, before connecting
```

**TypeScript** reads `import.meta.env.VITE_MUXWS_CODEC`, which `vite build` substitutes as a string
literal - the value is baked into the bundle rather than looked up at runtime:

```
# .env.development
VITE_MUXWS_CODEC=json

# .env.production
VITE_MUXWS_CODEC=msgpack
```

Development gets readable devtools; production gets the compact bytes; no source file mentions either.
That is the entire intended workflow, and it is why the default is JSON rather than the fastest thing
available: the readable encoding is the one you want in the environment where you are reading.

A `codec=` argument on `connect()` and on the peer constructor does survive, and it is documented as
what it is: an **override for tests**, and an escape hatch for the rare process holding two
connections that genuinely need different codecs (a bridge between two deployments, say). It is
explicitly not the documented path, and no example outside the test suite should use it.

### Registration is explicit, and it is the only mechanism

```python
muxws.register_codec("msgpack", MsgPackCodec())
```

```ts
registerCodec('msgpack', new MsgPackCodec());
```

That is all there is. **No dynamic imports, no lazy auto-registration, no probing whether a module
happens to be installed.** The library registers `json` itself; anything heavier - `msgpack`, `cbor` -
is an optional dependency (a PyPI extra, an npm subpath import) that the *application* registers in
its own bootstrap. The documented instruction to an application author is one sentence: register the
codecs you care about, for dev or for prod.

Every alternative is worse in a way that shows up late. Import-time auto-registration makes the set of
available codecs depend on import order. "Try to import it, register it if it is there" makes a
missing dependency into a silent behaviour change rather than an error. A registry populated by
entry-point scanning makes the answer to "which codec is this process using" depend on what else is
installed in the virtualenv. Explicit registration in application startup code is the only version
where the answer is visible in the application's own source.

**Two hard requirements make the TypeScript side tree-shake**, and they are requirements rather than
suggestions:

1. **A codec module must never register itself at import time.** Self-registration makes the import a
   side effect, and a side-effecting import can never be shaken out - a bundler that removed it would
   be changing behaviour, so it correctly refuses to.
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

Remove either requirement and the chain breaks at that last step, and only there: the constant still
folds, the branch still disappears, and the codec still ships. That is the precise claim - the first
two steps are unconditional, the third is the one the two requirements buy.

There is a limit on how far that last step reaches, and it should be named rather than glossed.
Dropping muxws's own msgpack module removes its `import` of `@msgpack/msgpack`, but whether *that*
package then leaves the bundle is its own `sideEffects` declaration's business, not ours: a dependency
that does not claim to be side-effect-free is retained by a conforming bundler even when none of its
exports are used. muxws can guarantee its half of the graph and nothing past it, which is one more
reason open question 2 asks for the claim to be checked against a real production build rather than
believed because it is written here.

**The honest counterpoint**, which the user raised and which the reasoning above should not be allowed
to obscure: bundle size on its own would be a weak argument for any of this. A JSON codec is trivial,
and three registered codecs are noise next to any real application. The reason the heavy codecs are
opt-in is that they are **separate dependencies that may simply not be installed** - the library cannot
register what is not there, and must not pretend otherwise. The `if` is what additionally buys the lean
bundle. An application that omits it and registers every codec it has installed is doing nothing wrong
and will work correctly; it just carries a few kilobytes it is not using.

**An unregistered codec name is a loud failure at startup**, on the first connection attempt, before
any socket is opened:

```
muxws: codec "msgpack" is not registered.
  MUXWS_CODEC=msgpack (from the environment)
  registered: json
  Install it (pip install muxws[msgpack]) and register it during startup:
      muxws.register_codec("msgpack", MsgPackCodec())
```

Never a silent fallback to JSON. A fallback would produce the single most confusing outcome available:
a deployment that believes it is running msgpack, is not, and only discovers it when the other end
disagrees - or, worse, never discovers it because both ends fell back and everything works while the
configuration is a lie.

### A mismatch guard, not a negotiation

The configured codec name is offered as a **WebSocket subprotocol**: `muxws.v1.json`,
`muxws.v1.msgpack`. The acceptor accepts the connection only if the offered name matches its own
configured codec; otherwise **it refuses the WebSocket handshake outright** - it selects no
subprotocol and answers the upgrade with `400 Bad Request`. There is no half-open state to clean up
afterwards, because the socket never existed.

State this plainly, because the shape is deliberately weaker than it looks: **this is an assertion,
not a negotiation.** There is no fallback to a common encoding, no per-connection multi-codec support,
no list of acceptable alternatives, and no runtime branching anywhere in the peer. Each side declares
what its `.env` says and the connection either happens or does not.

Its value is proportional to the risk the previous section introduced. With the codec now living in
configuration, the single most likely production failure is a `.env` that drifted between frontend and
backend - one deployed, the other not. Without the guard that failure is msgpack bytes arriving at a
JSON decoder: an unintelligible error, at an arbitrary moment, on a connection that appeared to
establish successfully. With it, the failure is a rejected upgrade at connect time. That is the entire
justification, and it is enough.

**The diagnosis has to be written twice, and that is a property of browsers rather than a
redundancy.** A rejected WebSocket upgrade is deliberately opaque to JavaScript: the browser reports a
failed connection and never hands the page the response body, so anything the server writes there is
readable in a server log and in a developer's network panel and nowhere else. So each side composes
the message it is able to compose. The **client** raises `CodecMismatch` built from what it *offered*,
which is the half it knows, and names both environment variables so the reader knows where to look on
each side:

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

Neither message is complete on its own and both name the two variables, so whichever one a developer
reaches first tells them what to compare.

The subprotocol also carries the protocol generation, so a `muxws.v2.json` peer is rejected by a v1
peer at no extra cost, and by exactly the same mechanism - see "Versioning lives in the subprotocol"
for why that is now the only version on the wire.

### Binary payloads make this a functional difference, not just a fast one

Under a binary codec, raw bytes are a payload type: a `bytes` in Python or an `ArrayBuffer` in
TypeScript rides natively, with no encoding step and no size penalty. Under JSON it cannot - a byte
string is not a JSON value, so an application must base64 it (paying 33% and doing the encoding on both
sides itself) or simply not send it.

So the codec choice is not only about speed. An application that streams image tiles, audio frames or
an already-compressed export has a *capability* under msgpack that it does not have under JSON, and one
that needs that capability is choosing a codec rather than tuning one. This is worth knowing before
picking a default, and it cuts both ways: it is also the reason muxws does not quietly base64 bytes
under the JSON codec on the application's behalf. A transparent 33% expansion that only some codecs
apply is exactly the kind of invisible behaviour difference that makes a deployment's own measurements
lie to it. Under JSON, bytes are the application's problem, explicitly.

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

Both halves fall out of one decision. **`peer.open(...)` is a synchronous call returning a `Stream`,
and `Stream` is itself awaitable.** That yields three spellings of the same primitive, and the caller
picks by writing the one they mean:

```python
stream = peer.open(payload)               # the handle; stream.id is readable on the very next line
result = await peer.open(payload)         # await the handle: the single result payload
async for chunk in peer.open(payload):    # iterate the handle: every payload, until the remote ends
    ...
```

An earlier draft had `stream = await peer.open(...)`, and it fails the second line of that list.
There, `await peer.open(p)` yielded the *Stream* - so the shortest and most obvious spelling in the
language produced the object you still had to do work with, and getting the actual result took a
second await on `.result()`. Awaiting a call should give you what the call is for.

**Why `open()` may be synchronous at all**, since "sends are async" is a strong enough convention that
breaking it needs an argument rather than a preference: sending on a WebSocket is a queue-and-return
operation in both languages. Neither the browser's `WebSocket.send` nor an append to the Python peer's
send queue waits for anything - the bytes leave when the writer reaches them - so the `await` in
`await peer.open(...)` was never waiting for delivery, for transmission, or for a reply. It was
waiting for nothing, by convention. What `open()` genuinely must do eagerly is allocate the id and
register the stream in the peer's live-stream map, and both of those are arithmetic and a dict write.
Once the convention is set aside there is nothing left in `open()` that could suspend.

**The id is allocated and the `open` frame enqueued in the same synchronous step**, and the
synchronicity is what makes that safe rather than merely convenient. Under an `async open()` the two
could be separated by a suspension point, and two coroutines calling `open()` concurrently could reach
the send queue in the opposite order, putting a non-monotonic `open` sequence on the wire - which, by
this document's own rule, is a connection-level protocol error a peer would be committing against
itself. That is why an earlier draft specified allocation *at enqueue time* rather than at call time.
A synchronous `open()` has no suspension point between the two, so on a single-threaded event loop the
pair is indivisible and the wire order is the allocation order by construction. "Allocate at call
time" and "allocate at enqueue time" are now the same instant, and the race stopped being something an
implementation has to be careful about.

**The two ways `open()` can fail are raised at the call site, synchronously**: `ConnectionGoingAway`
after a `goaway` has been received, and `ConnectionLost` while the peer is between sockets. Both are
facts the peer already knows before it does anything, and a raised exception is a better home for them
than a rejected future was - a fast failure delivered as a raise is one the caller cannot leave
unobserved by forgetting to await something.

Note what is *not* in that list. There is no concurrency limit for `open()` to fail against, because
the limit on how many streams may be open is the *receiver's*, it is local to the receiver, and it is
not announced (see "Limits are local"). A sender learns it has hit one the same way it learns anything
else the remote decided: a `reset(REFUSED)` arrives on that stream, and the pending await raises
`StreamRefused`. So saturation is an asynchronous stream failure like a refused handler, not a
synchronous local one, and `StreamLimit` is not an exception this library has.

**Awaiting and iterating the same stream is a caller error**, and it has exactly one rule: **the first
of the two shapes to be used claims the stream, and the other raises `StreamAlreadyConsumed`.** A
stream's payloads are consumed once. Splitting them silently between an `await` and an `async for`
would hand half an export to each consumer, and neither would look wrong locally - the worst available
failure mode, so the losing shape is refused loudly instead, with an error naming both uses. The same
rule covers the degenerate case of two `async for` loops over one stream.

The rule is about *shapes*, not about call counts, and the distinction has to be stated because the
two halves look contradictory otherwise. **Awaiting an already-awaited stream is not an error and
returns the same value again**, because `await` reads a value that the stream has already finished
producing - there is nothing left to split, and the memoized future described below is what makes the
second read cheap and identical. Iterating twice *is* an error, because the second loop would be
asking for payloads the first one already took. So: any number of awaits, or exactly one iteration,
and never both.

**`request()` survives unchanged in intent**, even though `await peer.open(payload, end=True)` is now
one keyword away from it, because it adds two things that are not sugar. It sets the end flag, so the
call site *states* "I am done talking" - a reader does not have to scan the rest of the function to
rule out a later `stream.send`. And it enforces the unary contract: a remote that sends more than one
payload raises, because a caller who asked for a single value and got three has a bug that should
surface at the call site rather than silently drop data. `request(p)` is `await open(p, end=True)`
plus that check, and the check is a real behavioural difference.

It has to live in `request()` rather than in the handle, and the reason is a timing one worth stating
because it also explains what `await stream` means precisely. `await stream` resolves the moment the
*first* payload arrives - that is what makes it the natural spelling for a bidirectional stream whose
caller wants the first answer and keeps talking - and at that instant there is nothing yet to object
to, since a second payload, if one is coming, comes later. Only a call that waits for the stream to
*end* can know how many payloads there were, and waiting for the end is exactly what a unary call
should do and what a general-purpose handle must not. Making `await stream` strict instead would buy
the check by making the bidirectional shape unusable.

**`peer.notify()` stays `async` and returns nothing** - `None` in Python, `void` in TypeScript. So do
`muxws.connect()` and `muxws.accept()`, which dial and complete a WebSocket handshake respectively and
therefore genuinely wait. The rule is not "nothing is async any more"; it is that a call is async when
it waits for something real.

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
does not reach the application in pieces. That is the real reason an earlier draft of this API made
every handler start with `await stream.first()`; the reassembly wait is genuine, but it belongs in
the peer's dispatch code, not in the first line of every handler anyone ever writes. The payload is
also available as `stream.payload` for handlers that stash the stream and read it later.

**Peer**

| Member | Signature | Notes |
|---|---|---|
| `open` | `open(payload=None, *, headers=None, end=False) -> Stream` | **Not a coroutine.** Zero mandatory arguments. No `timeout`: `open()` returns immediately, so there is nothing for a timeout on it to bound - the deadlines live on the awaits, `stream.result(timeout=)` and `peer.request(timeout=)`. Raises `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between sockets. |
| `notify` | `async notify(payload=None, *, headers=None) -> None` | One-shot push: `open(payload, end=True)` with no `Stream` returned - nothing to await, nothing to leak. |
| `request` | `async request(payload=None, *, headers=None, timeout=None) -> Any` | `open(payload, end=True)` awaited to the stream's end, plus the "exactly one payload" check that waiting makes possible. |
| `on_stream` | `on_stream(handler)` | Decorator or plain call. Handler is `(payload, stream)`. One handler per peer; registering a second replaces it (and logs). |
| `on_close` / `on_reconnect` / `on_frame` | `(handler)` | Lifecycle and observability hooks, registered as methods in both languages. `on_close(reason)` fires on every socket loss with a `CloseReason` carrying `code`, `reason`, `was_clean` and `will_retry` - the same four fields in both languages - `will_retry` distinguishing "reconnecting" from "offline"; `on_reconnect(attempt, peer)` fires once per re-established connection, after the hello has been acknowledged. |
| `ping` | `async ping(timeout=5.0) -> float` | Round-trip seconds. |
| `close` | `async close(code=NO_ERROR, reason=None, drain=10.0)` | `goaway`, drain, close. |
| `serve` | `async serve()` | Runs the read loop until the socket closes. Only needed on the acceptor side (see FastAPI wiring); `connect()` starts it for you. |
| `id` | `str` | Connection id for logs: a short random per-process prefix plus a monotonic per-connection counter, e.g. `a3f-17`. See "Connection ids are never reused". |
| `tags` | `dict[str, Any]` | Free-form application data hung on the connection (a session id, a user, a map of what this one socket does and does not want sent to it). An ordinary dict with ordinary dict semantics: any key may be written, overwritten and overwritten again for as long as the socket lives, last write wins, and there is no bookkeeping to pay for a rewrite. muxws never reads it, never persists it and never restores it - the dict is created with the peer and dies with the socket. A direct read sees the newest value immediately; `PeerRegistry` answers from the values that were there at the last `register()`. See "`tags` is connection-scoped state, and that is the point". |
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
| `result` | `async result(timeout=None) -> Any` | What `await stream` delegates to, with a timeout argument. Resolves on the first payload; raises `StreamReset` if the stream is reset first, and raises if it ends without producing one. It does **not** wait for the end, so it cannot and does not police a second payload - that check is `request()`'s, see below. |
| `cancel` | `async cancel(reason=None)` | `reset(CANCELLED)`. |
| `reset` | `async reset(code, reason=None)` | The general form. |
| `__aiter__` | `async for payload in stream` | Yields reassembled payloads until end; raises `StreamReset` if reset. Claims the stream against `await`. |
| `trailers` | `dict \| None` | Populated when the stream ends. |
| `closed` | `asyncio.Event` | Set on close, including on socket death. TypeScript has no Event primitive, so there it is a `Promise<void>` that resolves at the same moment - which composes with `await` and `Promise.race` the way an application actually wants to use it. |

`send()`, `end()` and `reply()` on a stream that is no longer open raise, and *which* exception they
raise is the caller's whole diagnosis, so the three cases are three types. A stream that closed
normally - both ends ended - raises **`StreamClosed`**. A stream the remote objected to raises that
stream's own `StreamReset` (`RemoteError`, `StreamRefused`, whatever the code was). A stream that died
with the socket raises `ConnectionLost`. They are separated because callers do genuinely different
things: a normal close racing a last `send()` is an expected outcome of a bidirectional conversation
and is usually swallowed, a reset means the remote said no and may deserve surfacing, and a lost
connection means wait for `on_reconnect` and rebuild. Collapsing the first into the other two would
also mislabel it - nothing failed, the conversation simply ended - and a programming-error exception
for an ordinary race is the kind of thing that gets caught and ignored everywhere until it hides a
real failure.

**How `Stream` is awaitable, concretely.** `Stream.__await__` delegates to the same memoized future
that `result()` uses: one future per stream, created lazily on the first await, resolved with the
stream's first payload when it arrives, or rejected with the stream's `StreamReset`. Memoized rather
than recreated, which is the whole point - `await stream` is re-awaitable, and a second `await` on a
stream that has already produced its value returns that value again rather than raising or hanging
forever on a consumed awaitable. `result(timeout=...)` is the same future with a deadline wrapped
around the wait, not a second source of the value, so the two can never disagree about what the
stream produced.

This repository already establishes exactly that idiom, for exactly that reason:
`LazyObject._ensure_future()` in `fastapi_viewsets/context/__init__.py` creates the future on first
`__await__` and hands the same one to every subsequent await, so `await context.user` works from any
number of places. The underlying rule is the same in both cases: an awaitable that is a *value holder*
rather than a one-shot coroutine has to survive being awaited twice, because nothing stops two pieces
of code from holding the same handle. Implement `Stream.__await__` against the same pattern.

Iteration takes the other route deliberately: `__aiter__` reads payloads off the stream's own queue as
they arrive and never touches that future, which is why the two shapes cannot be mixed and why the
claim is recorded on the stream rather than on the future. Creating the future lazily is what keeps
the claim honest - a stream nobody ever awaits has no future to resolve, and a stream nobody ever
iterates has no queue consumer to starve.

A pleasant side effect of `open()` not being a coroutine: the entire "coroutine was never awaited"
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
| `register` | `register(peer) -> None` | Indexes the peer under **every key its `tags` holds at that moment** - the registry has no notion of which keys matter, so it takes them all, except that a value which cannot serve as a lookup key (a dict, a list) is passed over rather than raising on. Idempotent and **re-indexing**: calling it again after `tags` changed replaces the peer's index entries wholesale, so it is found under the new values and no longer under the old ones. That second call is what a hello handler makes once the client-supplied identity has arrived. Removal on close is automatic, via the peer's own close hook. |
| `registered` | `registered(peer)` | Context manager wrapping `register` plus an explicit deregister, for the usual `with registered(peer): await peer.serve()` shape. |
| `peers_for` | `peers_for(**tags) -> list[Peer]` | Every live peer whose `tags` match all given keys. |

`peers_for` returns a **list, not a set**: one session routinely has several open tabs, fan-out is a
loop over that list, and a stable order makes the logs of a fan-out readable ("pushed to 3 peers:
c7f3, a19b, 4d02") in a way that iteration over a set does not. Callers must treat the list as a
snapshot - a peer in it may already be closing by the time they reach it, which is exactly why
`notify` is best-effort.

**`tags` is connection-scoped state, and that is the point.** The dict is created with the peer, is
readable and writable for as long as that peer's socket is alive, and is gone when it closes.
**Nothing in muxws persists it, snapshots it, or carries it across a reconnect.** On the next socket
the acceptor holds a new peer object with a new empty dict, and exactly three writers ever put
anything into it, all three of them the application's own: the acceptor's framework, writing what its
authentication established (the session); the handler for the hello the reconnect helper replayed;
and the handler for any later message the client chose to send. muxws is not a fourth, and there is
no other path in. A tag some handler added mid-connection is therefore not re-added by anybody - if
it matters on the next socket, the application states it again on the next socket, and stating it is
the only way it can get there.

That is a guarantee rather than a gap, and a consumer may lean on it: **state that lives only as long
as the socket cannot go stale across a reconnect**, so a client that crashes in some unusual state
recovers into the default one rather than into a remembered state that nobody is left to correct.
`backchannel` leans on exactly that, and its self-healing argument rests on nothing else. What one
socket has said it does and does not want delivered on it is a fact about that socket, and it is kept
nowhere but in that socket's `tags` - not in backchannel's own store, not against the account, and
not in the browser either - so a tab that reconnects starts with none of those declarations and gets
whatever the server would have decided without them. The alternative shape, the same declarations
persisted anywhere that outlives the socket, has a failure mode with no recovery path: a tab that
silenced something and then died stays silent for a successor that never asked to be, and nothing
about the resulting silence looks like an error.

**Replacing a tag value is legal, and replacing the same one repeatedly is equally legal** - the dict
is not a registration, and a write to it is an assignment and nothing more. What differs is when the
change becomes visible, and that is worth stating precisely because only one of the two readers
updates itself. A direct read - `peer.tags["session"]`, or any application code holding the peer -
sees the new value on the next line; there is no copy and no snapshot anywhere in that path.
`PeerRegistry` does not, because it is an index built at `register()` time: a peer whose tags changed
is still found under the values it was registered with until `register(peer)` is called again, which
replaces its entries with the current dict. That is deliberately an explicit call rather than a
watched dict - re-indexing on every write would put the registry's bookkeeping on the path of every
ordinary assignment, to serve a case (identity that changes mid-connection) the hello rules already
classify as a different connection.

**The rule that falls out of that is one line: look up on keys you do not mutate, and mutate keys you
do not look up.** The registry indexes every key it finds and so cannot tell the two apart; the
application can, and the staleness is observable only through a `peers_for` query naming a key whose
value moved since the last `register()`. Per-connection state that is *read off a peer the
application is already holding* is therefore free of the registry entirely: keep it in `tags` under a
key nothing ever looks up, replace it as often as the client re-declares it, and never call
`register()` for it at all. backchannel's arrangement is exactly that split - `session` is written
once at `accept()` from the framework's own authentication and never touched again, so its index
entry cannot go stale, while its map of per-operation delivery overrides is replaced wholesale on
every re-declaration and is only ever read off a peer the caller is already holding - one out of the
list `peers_for(session=...)` has just returned, never a peer found *by* the map. A consumer that
genuinely needs to find peers by a value it also mutates has exactly one obligation, and it is not a
hidden one: call `register(peer)` after each write.

**A tag's value is not required to be usable as a lookup key**, and that has to be stated because
`register()` takes every key it finds and a value on the mutate-only side of that rule is under no
obligation to be indexable - backchannel's is a map, which is unhashable in Python and meaningless as
an index entry in either language. So `register()` indexes the entries whose value can serve as a
lookup key and passes over the rest: the peer is simply not findable by that key, which is exactly
the arrangement such a key was chosen for. An implementation that raised instead would make "mutate
keys you do not look up" false for precisely the values a consumer is most likely to hang off a
connection, and would do it at the one call site - a hello handler re-registering after identity
arrived - that has no idea what other keys some other handler put in the dict.

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
never gets a say in it. Identity the *client* supplies - a tab id, the project it is looking at -
arrives instead in the hello stream that the dialer's reconnect helper replays on every connection,
and `handle_stream` writes that into `peer.tags` too; see "Connection identity is re-established by
the helper". The registry indexes whatever ends up there either way, so `peers_for(session=...)` and
`peers_for(tab=...)` are the same mechanism reading two differently-sourced facts.

**Note what is no longer in those samples: the application's own `await websocket.accept()`.**
`muxws.accept()` is async and performs the WebSocket accept itself, because it is the only party that
knows which subprotocol to select - it has to read the client's offered `muxws.v1.<codec>` value,
compare it against its own configured codec, and either accept naming that subprotocol or refuse the
handshake outright. An application that accepted the socket first would already have thrown away both
the offer and the ability to reject it, and the mismatch would resurface later as undecodable bytes -
exactly the failure the guard exists to convert into a legible one. Authentication is unaffected: the
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

The extra argument is where the two transports genuinely differ, and it is worth one honest sentence
rather than a silent inconsistency. Starlette hands over a socket that has *not* been accepted, so
`muxws.accept()` can do the whole handshake including the subprotocol decision. `websockets` completes
the handshake before it ever calls the handler, so the decision has to be handed to it up front -
hence `muxws.select_subprotocol`, a plain callable it can install. Where a transport offers neither
hook, the fallback is to verify the negotiated subprotocol on the already-open socket and close it
with a policy-violation code: the same diagnosis, one round trip later.

The socket type itself is duck-typed through a tiny adapter protocol (`send_text`, `send_bytes`,
`receive`, `close`, plus the handshake hook above) so Starlette's `WebSocket`, a `websockets`
connection, and the in-memory test transport are all acceptable arguments. Text and binary sends are
separate methods rather than one polymorphic `send` because the codec declares which of the two it
needs and the adapter should not have to guess from the value's type. That adapter protocol is the
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

`hello` is the only argument in that call whose value muxws never looks at, and the only one whose
effect outlives the first connection: it is sent as an ordinary `open(..., end=True)` on every socket
this peer establishes, and `on_reconnect` does not fire until the acceptor's handler has answered it.
"Connection identity is re-established by the helper" specifies the exchange; the short version is
that an application states who it is once and never has to remember to state it again.

Note the two deliberate differences from the TypeScript spelling, and there are only two.
**Durations are seconds in Python and milliseconds in TypeScript** (`initial_delay=0.25` versus
`initialDelayMs: 250`), because seconds-as-floats is what `asyncio.sleep`, `asyncio.timeout` and
every Python timeout argument in this repo already use, and milliseconds-as-integers is what
`setTimeout` and every browser API use; forcing one language to carry the other's unit would make
every call site convert. **Hooks are registered as methods in both languages** - `peer.on_reconnect(...)`
and `peer.onReconnect(...)` - and the TypeScript `connect()` options object accepts the same hooks
inline purely as sugar for the common case of registering them before the first connection attempt.
Everything else is the same names in the same order.

### TypeScript

The same shape. Naming rule, following how `route_rest` mirrors `route_viewset` in this repo's
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
whatever it is given and never inspects the prototype chain - and it is deliberately what muxws
implements. Subclassing `Promise` is the obvious move and the wrong one: `Promise` subclasses carry
species semantics, so every derived call (`stream.then(...)`, `stream.catch(...)`) constructs another
`Stream` through `Symbol.species` and hands back something typed as a stream that is nothing of the
kind, with a constructor that never ran against a real stream id. A thenable has no derived-type
problem at all, because `then` simply returns an ordinary `Promise`.

`Stream<T>` is simultaneously `AsyncIterable<T>`, via `Symbol.asyncIterator`. The two interfaces
coexist on one object, and which one the caller reaches for is what selects the shape - `await` picks
`then`, `for await` picks the iterator, and the first of them to be used claims the stream as
described above.

**The handle nobody consumed.** A stream that resets when nobody ever awaited or iterated its handle
is a real hazard in the browser: the internal promise rejects with no handler attached, and a naive
implementation produces an `Unhandled promise rejection` in the console - noise for a failure the
application deliberately chose not to look at, and in some deployments a reported, paged-on error. So
the implementation **attaches a default no-op rejection handler to the internal promise at
construction time**, and surfaces the failure through the peer's `onFrame` / error hook instead, where
an application that cares about failed pushes is already looking. Attaching the handler lazily on the
first `then` is too late; by then the rejection may already have been reported.

That mechanism also sharpens the argument for keeping `notify()` as its own primitive rather than
telling callers to "call `open` and ignore the handle". `notify()` returns nothing at all: there is no
promise to leave unhandled, no handle to retain by accident, and no stream left open against the
receiver's concurrency limit. "Ignore the handle" is a convention that has to be honoured at every call
site; handing back nothing is a guarantee that holds at all of them.

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

The single most important ergonomic property: unary and streaming are **not two protocols**. There is
one stream primitive and three wrappers around it.

| Shape | Wire | API |
|---|---|---|
| Unary | `open(end)` → `data(end)` | `await peer.request(payload)`, or `await peer.open(payload, end=True)` |
| Streaming response | `open(end)` → `data`, `data`, ..., `data(end)` | `async for chunk in peer.open(payload)` |
| Bidirectional | `open` → interleaved `data` both ways → `data(end)` both ways | `stream = peer.open(payload)`, then `stream.send()` plus iteration |
| One-shot push | `open(end)`, nothing awaited | `await peer.notify(payload)` |

The fourth row is the degenerate case rather than a fourth shape: `notify()` is `open(payload,
end=True)` that returns nothing. It exists because "fire and forget" written with `open()` returns a
`Stream` the caller has no reason to keep and every reason to forget - and a forgotten stream that
was never ended stays open, counts against the *receiver's* concurrency limit, and gets the next few
dozen pushes refused. Now that the handle is also a thenable, forgetting it costs a second thing in the
browser: an internal promise that nobody attached a handler to, whose rejection the runtime reports as
unhandled (the TypeScript section describes the default no-op handler that keeps this from reaching
the console, and why it cannot be attached lazily). Handing back nothing makes both problems
unrepresentable at once, which matters most exactly where it is least visible: `backchannel`'s push
path is contractually forbidden to raise, so a limit hit there would be silent. The other half of that
guarantee is on the receiving side: **a handler that returns without having ended its stream ends it
implicitly**, so the notified peer does not have to remember to close a stream it never wanted to talk
on. One progress push every 250 ms therefore occupies one stream slot for a round trip, not forever.

`request()` is `open(payload, end=True)` awaited to completion, and it earns a name of
its own for the two reasons given under "`open()` is synchronous": the `end=True` states at the call
site that this caller is finished talking, and it waits for the stream's end so it can raise if the
remote sent more than one payload - because a caller who asked for a single value and got three has a
bug that should surface at the call site, not silently drop data. If a stream might return more than
one payload, that caller wanted `open()`, and if it might return exactly one but the caller wants to
keep sending, that caller wanted `await peer.open(payload)` without the end flag.

This matters for evolution: a server-side action that starts out unary and later grows a progress
stream does not change protocol or break the wire. Only the caller's choice of wrapper changes.

---

## One registration point, not a router

`peer.on_stream(handler)` takes exactly one handler, and muxws hands it every incoming stream. There
is no path table, no method map, no per-action registration.

This is a deliberate refusal, not an omission. Routing needs to know what the opening payload *means*
- whether the key is `action`, `type`, `method`, or a path string; whether dispatch is by exact match
or by prefix; whether an unknown key is a 404-shaped reset or a fallback handler. Every consumer
answers those differently, and muxws having an opinion would mean every consumer either fights it or
ignores it. fastapi-viewsets' future WS adapter will dispatch on `{"action": ...}` against its own
viewset registry, which is *its* concern; backchannel will dispatch on its own message kinds; a third
consumer will do something else. One handler, and a `match` statement in application code, composes
with all three.

The one thing muxws does supply is the failure mode, and it is keyed on a fact the peer actually
knows rather than on a judgement call. **No registered handler → `reset(REFUSED)`**: nothing ran, so
the opener may safely retry the operation elsewhere. **A registered handler that raises →
`reset(APPLICATION_ERROR)`, always**, regardless of whether it had already sent anything. An earlier
draft said "raises before touching the stream → REFUSED", which is both undecidable (what counts as
touching?) and dangerous: `REFUSED` promises the operation definitively did not happen, so a handler
that debits an account and then raises would, under that rule, invite the client to retry the debit.
Either the peer knows there was no handler, or it must assume the handler did something. And a
handler that returns normally without ending the stream ends it implicitly - the same rule that keeps
a one-shot `notify()` from occupying a stream slot on the receiver for the life of the connection.

---

## Two vocabularies over one socket

The refusal above is easiest to see from the other side, in the deployment this library is actually
being built for. A browser tab there holds **one** muxws connection, and two entirely different closed
message vocabularies travel over it:

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

**This is the clearest concrete argument for why muxws's stream model earns its complexity.** Two
independent consumers, written by different concerns at different layers, share one socket, one
handshake, one auth, one reconnect helper and one liveness timer - and they do not interfere. A
progress push cannot delay a command's reply beyond one fragment (the interleaving rule), a cancelled
export does not disturb an open dialog (per-stream reset), and neither consumer had to know the other
existed in order to be written. Without multiplexing, the two would be sharing a single message queue
and would need a jointly-agreed discriminator field at the top of every message - which is to say a
third vocabulary, owned by nobody, that both would have to be changed together to extend.

And the deliberate refusal follows directly: **muxws itself defines no message vocabulary at all.**
Not a `kind` field, not a `type` inside `payload`, not a reserved key. The table above is what would
have gone wrong if it did - muxws would have had to pick one of those two column headings as the
winner and make the other consumer either fight it or nest its own vocabulary inside the imposed one.
`payload` is opaque and `headers` are uninterpreted for exactly the same reason `on_stream` is one
handler rather than a router: the meaning of an opening payload is the one thing a general-purpose
protocol cannot know and every consumer must be free to decide.

**And the property that refusal was buying is no longer an argument from principle. It has been
tested three times, in the right-hand column, in three consecutive revisions of this design.** The
first time, backchannel's vocabulary went from five kinds to six when `watch` joined it, so that one
connection could say how much progress traffic it wanted. Twice since, `watch` has kept its name and
its slot in the count and changed shape completely: a choice among preset levels of traffic became a
plain declaration of the operations this connection had muted, and that declaration became a
whole-value map of operation token to a boolean that overrides a server-side delivery decision in
either direction - silencing what would have been sent, reviving what would have been suppressed, and
still touching nothing but non-terminal `progress`. One of the three changes added a kind; the other
two replaced what a kind means. **Not one line of muxws changed for any of them**: no new frame type,
no reserved key, no new setting, no bump of anything muxws versions, and no conversation with the
other consumer, which still knows nothing about any of it and is unaffected by all of it.

That is the difference between a property claimed and a property observed, and it is worth having the
observation on record, because the cost of the alternative is now concrete rather than imagined. A
protocol that had picked a `kind` field would have had to be *asked* three times - once to admit the
new kind, twice more to change what it meant - and each ask is a muxws release, a version the *other*
consumer has to absorb for a change that has nothing to do with it, and one more line in an
enumeration that neither consumer owns and both can be broken by. Instead all three revisions stayed
inside one consumer's own payloads, where they belong - which is the working form of the sentence
above it: a consumer being free to decide what its payloads mean is worth little unless it is also
free to decide again, and again.

---

## Auth is a handshake concern

Authentication happens once, at the WebSocket upgrade: cookies ride along automatically, or a token
goes in a header (Python client) / in the subprotocol or a query parameter (browsers cannot set
headers on `WebSocket`). By the time `accept()` is called, the framework's own dependency machinery
has already accepted or rejected the connection - in FastAPI, an ordinary `Depends` on the
`@app.websocket` handler, which runs before the endpoint body and therefore before muxws completes
the handshake.

**The token-in-subprotocol trick and the codec subprotocol coexist**, and the rule has to be written
down because both want the same field. The WebSocket handshake offers a *list* of subprotocol values,
so muxws offers `muxws.v1.<codec>` as its first entry and the application may append its own -
`new WebSocket(url, ['muxws.v1.json', `bearer.${token}`])`. On the accepting side, muxws matches only
the entry with the `muxws.v1.` prefix and ignores every other value entirely, leaving them for the
application's authentication to read; and the value it selects as the negotiated subprotocol is always
its own, because that is the one the client's peer will check. muxws still never interprets a
credential - it just has to promise not to trip over one.

Per-stream `headers` exist and an application may put a scoped token in them, but **muxws never
interprets headers**. Re-authenticating per stream would mean muxws understanding credentials, which
is squarely in the non-goals; a connection whose credential expires mid-life should be closed with
`goaway`, and the client's reconnect helper will dial again and re-authenticate at the handshake.

This is also why a credential does not belong in the reconnect helper's `hello` payload, and the
distinction is worth keeping straight because both are "who am I" values sent at connection time. The
credential is *proved* at the handshake, once per socket, by the framework, and a reconnect re-proves
it by definition. The hello carries **identity that the client asserts and the server merely
records** - which tab this is, what it is looking at - and is replayed verbatim precisely because it
is not a secret and cannot go stale. A token replayed verbatim forever is the one thing that shape
would get wrong.

---

## Timeouts and cancellation

**`request(timeout=...)`**: no default timeout. A stream lives until it ends, is reset, or the
connection dies - and "the connection died" is already a prompt, distinguishable failure
(`ConnectionLost`, raised out of the pending call the moment the socket goes, never a hang), which is
the failure mode a default timeout would otherwise be guarding
against. Actions legitimately take minutes; a library-imposed 30 seconds would be wrong more often
than right. When a timeout *is* given and expires, the peer sends `reset(TIMEOUT)` (so the remote
stops working - a timeout that leaves the server grinding is a resource leak) and raises
`StreamTimeout` locally.

**Cancellation is cooperative in the same sense as `asyncio` cancellation.** `stream.cancel()` sends
`reset(CANCELLED)` and closes the stream locally *immediately* - the caller does not wait for
acknowledgement, and any payloads still in flight from the remote are discarded. On the remote side,
the handler task is cancelled: in Python the peer calls `task.cancel()`, so the handler observes
`asyncio.CancelledError` at its next `await`; in TypeScript the handler receives `stream.signal`, an
`AbortSignal`, and any `await stream.send(...)` after the reset rejects with `StreamReset`. A handler
that catches and swallows cancellation keeps running, exactly as with any cooperative scheme - it
just cannot send anything more.

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

`StreamClosed` sits outside `StreamReset` and that placement is the point of having it. Both ends
having ended a stream is not a failure of anything: it is the ordinary conclusion of a conversation,
and a `send()` that loses the race against it is an expected outcome rather than a bug. Filing it
under `StreamReset` would tell a caller the remote objected, when the remote did no such thing; making
it a `ProtocolError` would tell the caller its own code is wrong, when nothing was. Callers that
genuinely do not care about the difference catch `MuxwsError`.

`StreamRefused` covers every refusal, and the earlier draft's split - a separate `StreamLimit` for
concurrency saturation - went with the announced stream quota that produced it. There is no longer a
limit a sender can be told about in advance, so there is no longer a second thing for a caller to do:
`REFUSED` means nothing ran, and where to retry is the application's judgement either way.

`ConnectionLost` earns its place under the same test and answers it differently again: there is no
connection to retry on, so the correct reaction is neither "retry now" nor "back off and retry here"
but "wait for `on_reconnect` and rebuild". It is also the only member of the family that is never
decoded from a `reset` frame - the peer synthesises it for every stream that was live when the socket
died. Do not confuse it with its near-namesake one level up: `ConnectionClosed` is the *connection*
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

**Application errors.** A handler that raises produces `reset(APPLICATION_ERROR)` with a `reason` and
an optional structured `payload`. The default serializer sends `{"type": "ValueError", "message":
str(exc)}` - useful in development, and a potential information leak in production, so the peer takes
an `error_serializer` hook and the documentation must say plainly that a public-facing deployment
should redact it. muxws does not attempt to map exceptions to status codes; it has no idea what the
application's error taxonomy is.

**The hook is supplied per peer** - an argument to `connect()` and to whatever constructs the
acceptor's peer - rather than a module-level default, and the reason is that one process routinely
holds both kinds of connection. A service that accepts browser sockets on one endpoint and dials a
sibling service on another wants the browser peers redacted to a code and the internal peer carrying
the full traceback, and a process-wide setting forces it to pick the wrong answer for one of them. Per
peer, the decision is made where the audience is known, which is the same reason a public endpoint and
an internal one do not share a log level.

**Connection death.** Every live stream is closed with a locally synthesised
`StreamReset(CONNECTION_CLOSED)`, delivered to its local caller as `ConnectionLost`; every pending
`request()`, every pending `await stream`, and every `async for` raises it rather than hanging. Then
`on_close(reason)` fires - once per socket loss, with a `CloseReason` carrying `code`, `reason`,
`was_clean` and `will_retry`: the close code and text the socket reported, whether the close was
orderly, and whether the reconnect helper intends to dial again. Those four are the same four in both
languages, so a connection indicator written once is written for both. Nothing is retried
automatically at the stream level; the next section specifies what is re-established and what is not.

---

## Reconnect: the socket, not the streams

**v1 does not resume streams across a reconnect.** A dropped connection resets every stream on it with
`CONNECTION_CLOSED`, and the new socket starts with an empty id space.

This is a layering decision, not a shortcut. Resuming a stream requires the sender to have retained
everything the receiver might not have received, and to know how much that was - a per-stream
acknowledged byte offset, retained buffers with a retention policy, and a resumption token that
survives process restart. That is a durable-state problem, and durable state is inherently
application-specific: what to retain, for how long, and what a partially-delivered result even means
are questions muxws cannot answer for anyone. Building a half-version of it - buffer the last N
frames, hope the gap is small - produces a system that works in testing and silently loses data in
production.

`backchannel` is the worked example of doing this correctly one layer up: it *always* persists
progress and dialog state server-side and treats a muxws push purely as an accelerator over a REST
polling baseline. After a reconnect, the client re-reads state and continues; nothing was lost,
because nothing important lived only in a stream. Any consumer that needs continuity should follow
that shape rather than ask muxws for it.

What the client peer *does* ship is a socket-level reconnect helper, and the rest of this section
specifies it, because "reconnects automatically" is the kind of promise that is easy to write down and
easy to get subtly wrong.

None of what follows is speculative. The author has already solved this by hand, in production, in the
KlubIS application, and the design below is that implementation generalised. It is worth reading first
as a precedent that works rather than as code to improve on. `fast_api/main.py`
there is a WebSocket endpoint that takes a client-supplied `tabId` in an `init` message, subscribes
the socket to Redis pubsub channels per tab, per user, per project and per project-user, forwards a
`cmd` message to Celery as `send_task(..., args=(ws_tab, cmd), ...)` - the tab id travelling as the
task's first argument, so the worker knows which channel to publish its answer on - and fires two
catch-up tasks (`get_changelog`, `get_init_data`) from `ws_connect_tasks()` on every `init`, which
its own client sends on every connect. `vue/ws/web-socket-manager.ts` is the client half: exponential
backoff `min(1000 * 2 ** attempts, 30000)`, a 20 s application-level `ping` heartbeat that the
endpoint answers with a `pong`, the attempt counter reset on open, and the `init` message re-sent
from `onopen`. `tasks/ws_utils.py` names the channels (`ws_channel_tab:<id>` and its siblings) and
holds a small per-tab shared state in Redis. Every load-bearing choice below is one of those,
promoted from an application into a transport.

Where this specification deliberately differs from the precedent - jitter on the delay, the counter
resetting on an established rather than an open connection, a deadline on the heartbeat's reply, the
identity replay owned by the helper instead of by the client's `onopen`, and `sessionStorage` instead
of module scope - the difference is called out where it arises, together with the problem it solves.
None of the five is a criticism of the precedent. An application that knows its own deployment can
reasonably hard-code what a library has to parameterise, can reasonably leave out what its own scale
does not need, and can reasonably rely on its own client remembering to do something that a library
has to guarantee for clients it will never see.

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
| attempt cap | `max_attempts` | `maxAttempts` | unlimited | After which the peer gives up and closes for good. |
| heartbeat | `ping_interval` | `pingIntervalMs` | 20 s / 20 000 | How often a `ping` frame goes out on an idle socket. |
| heartbeat deadline | `ping_timeout` | `pingTimeoutMs` | 10 s / 10 000 | How long a `pong` may take before the socket is declared dead. |
| hello deadline | `hello_timeout` | `helloTimeoutMs` | 10 s / 10 000 | How long the hello exchange below may take before the attempt is failed. |

**The attempt counter increments on every failed attempt and resets when the connection is
*established* - and this document defines "established" as two things rather than one:** the socket is
open with the `muxws.v1.<codec>` subprotocol accepted, and the hello (next subsection) has been
acknowledged.
KlubIS resets the counter in `onopen`, which is the obvious place and is right until a server accepts
sockets while everything behind them is still failing - a backend that is up before its database is, a
deploy that is half rolled out, a gateway that terminates the upgrade and then cannot reach the
application. Under an on-open reset every attempt looks successful, the counter never leaves zero, and
the "exponential" backoff degenerates into a fixed-interval hammer at the initial delay for as long as
the outage lasts. Resetting only when the connection is *usable* makes the helper back off from
partial failures too, which are the common ones.

**Jitter is one line of code and it is the difference between a staggered reconnection and a
synchronised stampede against a server that has just come back up.** It deserves the paragraph rather
than a mention in a defaults list. KlubIS's schedule has none - a defensible omission for one
application whose tab count it knows, and one whose consequence is structural rather than unlucky
once the same code becomes a library running on deployments nobody has seen: when the server
restarts, every open tab in every browser sees its socket close within the
same few milliseconds, so every tab computes exactly 1000 ms and retries on the same millisecond. If
the server is not ready yet - and a server that has just restarted usually is not - they all fail
together, all compute exactly 2000 ms, and collide again; and because the schedule is deterministic
they keep colliding on every subsequent doubling. Backoff does not spread that load out, it only makes
it periodic, and the spike lands precisely on a process that is still warming caches and opening pools.
With ±30% jitter, N tabs spread their first retry over a 175-325 ms window and every doubling widens
the window proportionally, so the herd disperses instead of resonating.

Jitter is also what makes the aggressive 250 ms initial delay safe, and the comparison with KlubIS's
1000 ms is the argument for both numbers. A 1 s floor is partly a *substitute* for jitter: on a
deterministic schedule, a short first delay is a tight synchronised loop against a server that is
probably still starting, so the floor has to absorb what the randomisation would have. With jitter,
the short delay does what it should - a transient blip (a Wi-Fi handover, a proxy recycling one
connection, a load balancer draining a node) recovers in a quarter of a second instead of a full one -
and a genuine outage still reaches the 30 s cap inside about eight attempts.

**Reconnection applies to connections that were lost, never to establishing the first one**, and that
asymmetry is deliberate rather than an omission. `connect()` awaits the first attempt and **raises if
it fails**, with the underlying error, even when unlimited retries are configured. The alternative -
returning a peer that is quietly retrying in the background - makes the most common startup mistake
unreportable: a typo in the URL, a `.env` pointing at a host that no longer exists, a codec mismatch,
an expired token. None of those will ever succeed on retry, and under a background-retry `connect()`
none of them surfaces anywhere except as an application that connected fine and never receives
anything. A connection that was working and then dropped is a different claim entirely: the URL was
right, the credentials were accepted, and the thing that changed is the network - which is exactly the
situation retrying is for. So the first attempt is the caller's to handle, and every attempt after a
successful one is the helper's.

**The heartbeat is the other half of the algorithm, not an add-on**, because backoff only runs once
something notices the socket is gone, and the thing that usually goes wrong is that nothing does. The
peer sends a `ping` frame every `ping_interval` on an otherwise idle socket; if no `pong` comes back
within `ping_timeout`, the socket is declared dead, closed locally, and the backoff path runs exactly
as it would after a clean close. A half-open TCP connection - the browser tab that was suspended, the
laptop whose lid was closed, the NAT that dropped an idle mapping, the load balancer that forgot a
back end - produces no close event at all on the client, and waiting for TCP to work it out can take
minutes or, with keepalives off, never happen. KlubIS ships this heartbeat at exactly this interval -
the client sends `{"type": "ping"}` every 20 s and the endpoint answers `{"type": "pong"}` - and for
exactly the reason the wire format section already gives: it is an *application-level* ping
because browsers do not expose WebSocket control frames to JavaScript. muxws's `ping`/`pong` frames
are that same workaround promoted into the protocol, so that no application has to reinvent it and no
two applications have to invent it differently.

**What muxws adds to the precedent's heartbeat is the deadline**, and it is the third of the five
deliberate divergences. KlubIS's client sends the ping and its server answers, but the client does not
time the answer: the ping's detection value there is indirect - writing to a dead socket eventually
makes the browser notice and fire `onclose`, which is a real effect and is why the heartbeat helps at
all. Making the `pong` an explicit deadline turns "eventually, at whatever the platform's write
timeout happens to be" into `ping_interval + ping_timeout`, a number the reconnect tests can assert
against without waiting on TCP. It costs one timer, and it is the difference between a detection
window a library can state and one it can only hope about.

### Connection identity is re-established by the helper, not by the application

An application supplies a **hello** exactly once, at `connect()`:

```python
peer = await muxws.connect(url, hello={"tabId": tab_id, "user": user_id})
```

and the helper sends it, identical, on every connection this peer ever makes - the first one and every
reconnect alike - **before `on_reconnect` fires and before any application frame goes out**.

muxws does not interpret it. Mechanically the hello is `open(hello_payload, headers=hello_headers,
end=True)`: an ordinary stream, delivered to the acceptor's own `on_stream` handler like every other
stream, carrying whatever the application decided identity means. Because a handler that returns
without ending its stream ends it implicitly, the acknowledgement is free - the server's handler
returning *is* the ack, and no application has to remember to send one. What muxws owns here is not
the content but the **timing and the repetition**, which is the part applications get wrong.

Two consequences of it being an ordinary stream. The acceptor recognises a hello the same way it
recognises anything else - by looking at the payload it was handed, in its own vocabulary, in the same
`match` statement that handles every other opening payload; muxws does not flag it, because a flag
would be the beginning of a vocabulary. And `hello` is optional: a peer given none simply does not
send one, is established as soon as the socket is open with the subprotocol accepted, and behaves
exactly as this document described before the hello existed. Service-to-service peers that authenticate at the
handshake and have nothing further to assert are the normal case for that.

The failure mode this removes is visible in the precedent. In KlubIS the `init` message is re-sent
after a drop because `onopen` happens to send it. That works, and has worked in production for a long
time - but it is a property of that one client rather than of the transport. Every application that
ever speaks that protocol has to remember to do the same thing in the same place, and an application
that forgets gets a socket the server cannot associate with anything: connected, apparently healthy,
subscribed to no channels, receiving nothing, reporting no error. Data simply stops arriving, which is
the worst available shape for a bug. Making the replay the helper's job makes "reconnected but
anonymous" unrepresentable rather than merely unlikely.

Three rules keep it that way:

- **The hello is captured at `connect()` and replayed verbatim.** It is not re-read, not recomputed,
  and not a callback. Identity that changes is not a reconnect, it is a different connection: an
  application whose identity changes closes the peer and connects a new one. Note that a *credential*
  is not identity and does not belong in the hello - authentication is a handshake concern, so a
  reconnect re-authenticates for free by dialling again, and an expired token surfaces as a rejected
  handshake rather than as a stale value replayed forever.
- **`on_reconnect` fires after the hello is acknowledged, never before.** The ordering is the whole
  point: an application that re-subscribes from `on_reconnect` while the hello is still in flight
  would be subscribing on a socket the server has not yet associated with anything - the very failure
  the hello exists to remove, reintroduced by racing it.
- **A failed hello is a failed connection attempt.** If the hello stream is reset, or does not
  complete within `hello_timeout`, the peer closes the socket, does *not* fire `on_reconnect`,
  increments the attempt counter and backs off. A server that accepts sockets but cannot yet establish
  identity on them is a server to back away from, not one to sit on holding a useless connection.

On the acceptor side the hello is the natural place where `peer.tags` gets written from
client-supplied data, and the only place *indexed identity* is written that way, which is what makes
`PeerRegistry.peers_for(tab=...)` findable at all. Later messages may write tags too - backchannel's
per-operation delivery overrides are one, replaced wholesale by an ordinary stream handler every time
the client re-declares them, and never registered, because nothing ever looks a peer up by them - but
identity belongs in the hello specifically because the hello is the one message whose replay the
helper guarantees, and a tag nobody re-states after a reconnect is a tag that is simply not there any
more.

KlubIS does the same thing one layer further out: the tab id from `init` becomes both the pubsub
channel name (`ws_channel_tab:<id>`, in `tasks/ws_utils.py`) and the key of a small per-tab shared
state in Redis, so that a Celery worker holding no socket can still publish to the right tab. `tags`
plus `PeerRegistry` is the in-process half of that arrangement; the cross-process half is a backplane,
and a backplane is emphatically not muxws's job - it is backchannel's, which is why backchannel has
one and this document does not.

### The identity should survive a page reload, not just a socket drop

KlubIS mints its tab identity with `const tabId = crypto.randomUUID()` at module scope in
`vue/ws/main-web-socket.ts`. That survives every socket drop, which is what it was written for, and a
page reload re-evaluates the module and mints a fresh one. There it costs little: the id names pub/sub
channels the reloaded page immediately re-subscribes to, so a new id is simply a new subscription. For
a transport whose consumers may hang recoverable state off that identity, the same lifetime costs
more - a user pressing F5 during a long import becomes, as far as the server is concerned, a different
tab that never asked for anything.

The recommended lifetime is `sessionStorage`:

```ts
function tabId(): string {
  let id = sessionStorage.getItem('muxws.tab');
  if (!id) sessionStorage.setItem('muxws.tab', (id = crypto.randomUUID()));
  return id;
}

const peer = await connect('wss://host/ws', { hello: { tabId: tabId() } });
```

`sessionStorage` is per-tab and survives reload and in-tab navigation, which is exactly the lifetime a
tab identity wants. `localStorage` is the tempting near-miss and is wrong: it is shared across every
tab of the origin, so two tabs would claim one identity and the server would fan one tab's traffic to
both. Module scope is the other near-miss, and it is the one the precedent took.

It still dies with the tab, and that is correct rather than a limitation. What should outlive a tab is
not the connection identity but the *application-level state* the tab was watching - a running
import's progress, an open dialog, a half-answered question - and that belongs one layer up, to
something that persists it server-side and hands it back on request. See the backchannel document,
whose load-bearing decision ("the store is the truth, push is only an accelerator") exists for
precisely this case: a brand-new tab with a brand-new identity fetches one snapshot per token it cares
about and is fully caught up, and nothing about the old tab's identity needed to survive for that to
work.

muxws recommends this and does not implement it. Minting and storing an identity is three lines whose
shape depends entirely on what the application considers an identity, and a library that generated one
would be defining a vocabulary - see "Two vocabularies over one socket" for why that is the one thing
this protocol will not do.

### `on_reconnect` is where the layer above rebuilds

**What `on_reconnect(attempt, peer)` guarantees is exactly two things: a live socket, and an identity
the acceptor has already accepted on it** - it fires only after the socket is up with the subprotocol
accepted *and* the hello has been acknowledged, which is what makes the second half of that promise
true. That
is the entire list of what muxws restores. **Every stream is gone, nothing is replayed, no frame that
was in flight is re-sent**, and the new socket's id space starts empty. On the acceptor side the peer
is a new object with a fresh, empty `tags`, filled only by the framework's own authentication, by the
replayed hello, and by whatever the client goes on to say on this socket: per-connection state that a
handler hung on the old peer died with it, which is the guarantee "`tags` is connection-scoped state"
describes from the other end.

Everything past those two things is the layer above's, and `on_reconnect` is the single point at
which it does that work: re-subscribing to what it was watching, re-fetching the snapshots it was
displaying, re-issuing any request whose answer never arrived and which is safe to re-issue. For
backchannel that obligation is one line - read the register once - and its own document says so in
those terms; the two statements are the same contract read from the two sides. Note what is
deliberately *not* on that list: the per-connection delivery overrides that client had declared on the
old socket are not re-declared, and nothing anywhere remembers them. They died with `tags`, which is
the property backchannel chose a connection-scoped dict for, and the single read it does make comes
back stating what the server decides without them - so the layer above restores state it can see the
origin of, or it restores nothing at all.

The precedent is the same shape. KlubIS's endpoint fires `ws_connect_tasks()` - `get_changelog` and
`get_init_data` - from its `init` handler, which is to say on every connect, the first and every
reconnection alike, and unconditionally: a catch-up snapshot rather than a diff. The *unconditional*
part is the part worth copying: a client that tries to work out what it missed has to know what it
had, which is the durable-state problem this section opened by refusing to solve. Re-fetching
everything is cheap, always correct, and has no edge cases, and it is available to any consumer that
kept a REST endpoint capable of answering "what is the state now" - which is exactly the shape
backchannel is built around.

The API makes the boundary explicit: `Peer` survives the reconnect, `Stream` objects do not, and any
stream held across one is already closed. `on_close(reason)` fires on **every** socket loss, not only
the final one, and `reason.will_retry` distinguishes "reconnecting" from "offline" - the helper sets
it `False` only when `max_attempts` has been exhausted or `close()` was called deliberately. The other
three fields on `CloseReason` (`code`, `reason`, `was_clean`) say what the socket reported, which is
what an indicator needs to distinguish "the server sent us away" from "the network vanished". An
application that wants a connection indicator needs no state of its own beyond those two hooks.

### What happens to the streams that were live

The state machine needs this stated outright, and it matters more now that `open()` hands back an
awaitable handle than it did when every call was a coroutine: a hung `await` is invisible. No error,
no log, no timeout - just a coroutine that never resumes and a spinner that never stops.

**When the socket dies, every live stream on it transitions to `closed` and is failed locally with a
synthesised `reset(CONNECTION_CLOSED)`** - reset code 9, the one code that never appears on the wire.
Concretely, at that instant and before `on_close` fires:

- A pending `await stream` or `await stream.result()` **rejects** with `ConnectionLost`. The memoized
  future is resolved once, with that error, so a second await gets the same error rather than hanging
  on a future nobody will ever resolve again.
- An `async for` over the stream **raises** `ConnectionLost` out of the loop at the next iteration
  rather than terminating normally, because a consumer that saw the loop end cleanly would conclude
  the export finished.
- An in-flight `request()` **raises `ConnectionLost`**. It never returns a partial value and never
  hangs. This is the case that matters most: `request()` has no default timeout by design (see
  "Timeouts and cancellation"), so connection death is the *only* thing that will ever end a request
  whose answer is not coming.
- `stream.send()`, `end()` and `reply()` raise `ConnectionLost`; `cancel()` and `reset()` are no-ops,
  because the stream is already closed and there is nothing left to tell the remote.
- `stream.closed` is set, so code that waited on the event rather than on a value also wakes.

**And while the peer is disconnected, starting new work fails fast rather than queueing.** `open()`
raises `ConnectionLost` synchronously, exactly as it raises `ConnectionGoingAway`;
`notify()` and `request()` reject with it. Nothing is buffered for the next socket. The reasoning is
the same one that produced this section's opening refusal: a queue that survives a reconnect is a
promise of continuity muxws has explicitly declined to make, and flushing it into a server that has
forgotten the sender is worse than failing, because a failure at least reaches a call site.
`peer.is_open` is `False` for that whole window for applications that would rather defer than fail -
though the better answer is almost always to do the re-issuing from `on_reconnect`, where the socket
is known good and the identity is known established.

`ConnectionLost` is a `StreamReset` subclass and `ConnectionClosed` is not, and the two names are
deliberately different scopes rather than synonyms. `ConnectionLost` says *this stream* failed because
the connection did, and is what every stream-shaped call raises. `ConnectionClosed` says *the
connection* ended, and is what `serve()` returns through and what peer-level calls raise. A caller
that only wants to know "it broke" catches `StreamReset` and never learns the difference; a caller
deciding between retrying now, retrying from `on_reconnect`, and giving up needs it - which is the
same test every other entry in the reset-code table has to pass.

The acceptor side has no reconnect helper. It cannot dial. A dead acceptor peer is simply gone: it
drops out of `PeerRegistry` through its own close hook, and a `notify()` that loses the race against
that removal raises `ConnectionLost` at the pushing code, which is precisely why a push is documented
as best-effort and why backchannel's transport swallows it.

---

## Backpressure, honestly

v1 has **no per-stream flow control.** The only backpressure is the WebSocket send buffer, and that is
connection-wide: a peer that stops reading eventually stalls *every* stream, and a producer that
outruns its consumer on one stream buffers in the OS and in the WS library until something gives.
What v1 does provide is the receiver's own concurrency limit (bounding how many producers can exist at
once) and the fragmentation constant (bounding how long any one of them can hold the wire), which
together are sufficient for the traffic muxws is designed for: control messages, progress updates, and
result sets in the kilobytes-to-low-megabytes range. Both are local defences rather than agreements -
see "Limits are local" - and that is exactly what distinguishes them from flow control, which is an
agreement by definition.

`window_update` is reserved as a frame type for the day that is not enough. The concrete trigger for
building it: **a real producer/consumer speed mismatch on a single stream** - a server exporting rows
faster than a browser can render them, with observed unbounded memory growth on either end. Until
that is measured rather than imagined, per-stream credit windows would be a large amount of
state-machine complexity (an initial window both peers must agree on, which this connection has no
place to carry and would need a new generation to introduce, per-stream and connection-level
accounting, window-exhaustion stalls, the classic deadlock when both peers wait for credit) serving no
demonstrated need. Applications that hit the wall before muxws implements it have a workable
stopgap: application-level pacing, where the consumer sends an ack payload every N items and the
producer waits for it - the same mechanism, at the layer that knows what N should be.

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

The property being bought is not uniqueness, which nothing here needs, but the *absence of reuse* -
and the two are worth telling apart, because a design that optimised for uniqueness would happily
recycle ids as connections closed. Reuse is the worse failure. Two different connections appearing
under one name in a log look like *one* connection: a reader following `a3f-17` through a support
ticket sees a session that reconnected, when in fact they are reading two unrelated sockets spliced
together, and every conclusion drawn about the first one is now attributed to the second. A collision
between two *processes* costs far less, because the process is already in the log line next to it -
and a monotonic counter makes reuse structurally impossible without any coordination at all.

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

The shape is simple - **one muxws stream is one command** - but the adapter is not merely a few lines
of glue, and this section is more useful to whoever writes it if it says so. Against the code as it
stands today, four things have to be dealt with.

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
`build_context(request, viewset, ...)` passes it to every processor, and a processor that branches on
the viewset or the action would get it wrong once per connection rather than right once per command.
Two honest options, and the adapter must pick one explicitly rather than by accident: rebuild the
whole Context per stream once the opening payload has named the viewset (correct, and pays the
processor cost per command), or keep the per-connection Context but restrict it to
viewset-independent processors, documenting that constraint for anyone deploying over WS.

**Every context value must survive a serialize/deserialize round trip.** `clone_for_command` goes
through `serialize_context()` / `json.dumps` / `deserialize_context()`, and that path is *never*
exercised over HTTP - a processor that puts a plain domain object into context, or a `LazyObject`
whose resolved value is one, works perfectly today and raises the first time a WS stream clones. That
is a real new constraint on context processors, not a detail: it is the same constraint the Celery
path already imposes, which is why `SerializableObject` and `settings.viewsets_context_json_encoder`
exist, and the WS adapter's documentation must say so.

**`lifecycle_runner` has to grow a way to hand back the `ViewSetResult`.** Today `execute()` runs
`run_command_chain` only when `response is not None`, immediately writes `result.headers` /
`result.cookies` / `result.status_code` onto that live Starlette `Response`, and returns
`result.body`; the `ViewSetResult` is consumed and discarded. A WS adapter therefore cannot get one
out of the existing pipeline at all - it would have to fabricate a `Response`-shaped object to
harvest, or re-implement the runner. Note that `ViewSetResult`'s own docstring already promises the
WS mapping ("a future WS adapter would fold all three into the outgoing message payload as plain JSON
keys"), so the docstring and the runner currently disagree about whether that is reachable. **This is
a prerequisite in this repository, not work inside the adapter**: `lifecycle_runner` needs an explicit
transport sink - an object the chain's result is handed to, with `route_viewset` supplying the HTTP
one that writes headers/cookies/status onto the `Response` and the WS adapter supplying one that
folds `body` / `headers` / `cookies` / `status_code` into a single JSON payload. Once that exists,
the command middleware chain really does run exactly as it does over HTTP.

With those four settled, the rest is what it looked like from the start: the opening payload names
the viewset and action, the chain runs, and the folded result goes back as the single reply. An
action that wants to stream (a paginated list, a long export) sends several `data` frames instead of
one - the same pipeline, a different wrapper, which is the point of "three call shapes, one
primitive".

### What is being extracted is a protocol, not "the WS transport"

Extracting muxws invites an obvious symmetry question - if the WebSocket transport is leaving
fastapi-viewsets, should the HTTP transport leave too, so that the library is left as a pure,
transport-neutral core with two adapter packages beside it? The answer is no, and the reason is that
the symmetry the question assumes does not exist.

What is leaving is a **general-purpose WebSocket protocol**: framing, multiplexing, stream lifecycle,
cancellation, connection lifecycle, with no idea that viewsets exist. What is *not* leaving is the WS
transport **adapter** for viewsets - the thing that is actually analogous to `route_viewset`, that
reads `{"action": ..., "model": ...}` off an opening payload, resolves a viewset class, builds a
`Context`, runs the command middleware chain and folds a `ViewSetResult` into a reply. That adapter
stays in fastapi-viewsets, taking an optional dependency on muxws. So the end state is an HTTP adapter
and a WS adapter sitting side by side in one library, with a protocol package underneath the second of
them - not two adapter packages around a core.

**The test that decides splits like this one is worth stating, because it is reusable and it is the
real argument: would someone who has never heard of viewsets install this package?** For muxws, yes -
multiplexed streams over a WebSocket is a thing people want on its own, which is the entire premise of
this document. For backchannel, yes - progress and dialogs for long-running work is a thing people
want on its own too. For an HTTP-adapter-for-viewsets, nobody, ever. That is not a package; it is a
feature of the library it belongs to, and giving it a version number and a release stream would be
inventing a distribution boundary where no consumer boundary exists.

The concrete consequences make the same point from the other direction. Removing HTTP from
fastapi-viewsets would take `build_schema` and OpenAPI generation with it, since a schema describes
HTTP routes; it would take `response_classes` (`NotFoundError` is an `HTTPException`, `NOT_FOUND_RESPONSE`
is an OpenAPI responses fragment); and it would break the frontend `route_rest`, which validates
itself against the backend's `/schema` endpoint at runtime. What would be left is a package named
fastapi-viewsets that cannot talk to FastAPI - which is a good sign that the line was drawn in the
wrong place.

**The transport adapter boundary is still worth naming, and this is the moment to name it**, because
a seam that is only implicit stops being a seam the first time somebody reaches across it. Two modules
are HTTP by right: `route_viewset.py`, which imports `fastapi.Request`/`Response` at runtime and
synthesises the endpoint signatures, and `build_schema.py`, which reads FastAPI's route table
(`APIRouter`, `APIRoute`) to produce the schema the frontend validates itself against. Everything the
pipeline is actually made of is already transport-agnostic by construction: `Context`,
`ViewSetResult`, the command middleware chain, `clone_for_command()`.

Two things sit on the line rather than on either side of it, and both are named rather than glossed.
`lifecycle_runner.py` writes `result.headers` / `result.cookies` / `result.status_code` onto a live
`Response` - which is the fourth prerequisite above, and the reason it is phrased as "an explicit
transport sink" rather than as adapter-internal work: the fix *is* moving that write back across the
seam to where it belongs. And `Request` appears as a `TYPE_CHECKING`-only annotation on context
processors, middleware and the auth adapters, which is the surface a real extraction would have to
generalise into a transport-neutral request object.

Naming the seam now is cheap and keeps a later extraction of a transport-agnostic core cheap too: as
long as the boundary stays where it is - and the transport sink puts it back where it is - the work
would be "generalise one annotation, move two modules", not an archaeology exercise. Doing the
extraction *today* would cost two packages, two release streams, two changelogs and a
version-compatibility matrix, in exchange for a benefit no current consumer can name. The seam is
documented; the split waits for someone who needs it.

---

## What backchannel needs from this

This list is a contract, and it is short because backchannel's transport port is one method,
`notify()`, and backchannel's *envelope* traffic is push-only: there is no envelope in the
client → server direction at all. What the client originates travels some other way - a dialog reply
over REST or over a stream the *client* opens, a cancel request over REST, a `watch` over a stream the
client opens, none of them an envelope and none of them a call the transport port has to grow. Three
capabilities, and the parallel `backchannel` document lists the same three - each from its own side,
so the wording differs; if the *substance* ever drifts apart, one of the two documents is describing a
library that does not exist. Only two of the three are calls backchannel makes: `peer.notify()` and
`PeerRegistry.peers_for()`. The first is a property of the protocol that those two calls rely on, not
a third method.

1. **Symmetric `open`** - the acceptor can start a stream to a connected client without that client
   having asked for anything. This is how a progress update or a dialog prompt reaches the browser,
   and it needs no push-specific machinery at all.
2. **A one-shot push** - `peer.notify(payload)`, a stream that is opened and ended in a single call
   and returns no `Stream` object. Every backchannel push is one envelope and nothing else, so this
   is the only send shape it uses; and because it cannot leave a stream open - or, in the browser, an
   unobserved rejected promise from a handle nobody kept - a push can never accumulate open streams
   against the receiving peer's concurrency limit, or noise in the console. `peer.open()` returning a handle
   synchronously would make "just ignore the result" *look* fine at every call site, which is exactly
   why backchannel is specified against `notify()` instead.
3. **Peer lookup by session tag** - `peer.tags["session"]` plus `PeerRegistry.peers_for(session=...)
   -> list[Peer]`, with removal on close handled by muxws. A session may have several open tabs, so
   this returns a list and backchannel fans out over it. The same dict is also where backchannel keeps
   each connection's per-operation delivery overrides - a map value, read off a peer that list has
   already handed back rather than looked up by, replaced wholesale whenever that client re-declares
   it, and relied upon to die with the socket and be restored by nobody - see "`tags` is
   connection-scoped state".

Nothing else. In particular backchannel does not need stream resumption, ordering guarantees across
streams, delivery acknowledgement, or persistence from muxws - it provides its own, precisely because
muxws does not. **backchannel must not require any muxws API beyond these three.** It also does not
need a socket of its own: in the deployment both consumers are built for, backchannel's envelopes and
the viewsets adapter's commands share one connection and do not know about each other, which is what
"Two vocabularies over one socket" describes and what the whole stream model exists to make possible.

**Why the registry lives here and not in `MuxwsTransport`.** It is tempting to say that mapping
sessions to connections is application business and push it up a layer. The deciding argument is
plumbing: an index over connections has to be pruned when a connection dies, which means hooking
`peer.on_close` and getting the ordering right against `goaway`, drain, and socket death. muxws
already owns all three events; a `MuxwsTransport` that kept its own `session -> peers` map would be
re-deriving that close-hook logic from outside, and would get it subtly wrong in exactly the cases
(half-open socket, drain timeout) that muxws exists to define. So `PeerRegistry` ships in muxws,
`MuxwsTransport` calls `peers_for(session=...)` and holds no registry of its own. The registry is
per-process either way - which is why backchannel has a Redis backplane, and is unaffected by this
choice.

**`request`, `stream.send` and `stream.cancel` are not in that list**, and it is worth being explicit
that their consumer is somewhere else: the fastapi-viewsets WS adapter described above. That adapter
genuinely does unary request/response (one stream is one command, `request()` on the client side,
`reply()` on the server side), genuinely streams multiple payloads on one stream (a paginated list, a
long export via `stream.send`), and genuinely cancels mid-flight when a user navigates away
(`stream.cancel()`). Each capability in this library has a named real consumer, and it is not always
the same consumer.

Note the exact word in the requirement above: **must not *require***. Two things travel from
backchannel's dialing side, and neither widens the contract. The first is an optional accelerator -
sending a dialog reply over a client-opened stream instead of over REST - which is a `request()` from
the dialer; it is opt-in, every backchannel deployment works with it switched off, and nothing on the
acceptor side changes either way. The second is `watch`, which is not optional in the same sense but
needs nothing new either: it is a one-shot client-opened stream, and on the acceptor side it is an
`on_stream` handler assigning one map into `peer.tags` - the same two mechanisms items 2 and 3 already
name, used from the other end and by an ordinary application handler rather than by any new muxws API.
That `watch` carries a whole value and is idempotent and last-writer-wins is what keeps it that cheap:
the handler overwrites one key and returns, so a client re-declaring its overrides ten times in a
minute costs ten assignments and no registry work at all. Both are named here only so that "exactly
two calls" and "the client also talks" are not read as a disagreement between the two documents.

---

## Repository and package layout

One repository shipping two packages on a single version stream, mirroring how this repo ships
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
│   └── *_test.py              # pytest, colocated - same convention as this repo
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

`SPEC.md` is normative and versioned separately from the implementations by the generation integer in
the subprotocol name (v1 today; see "Versioning lives in the subprotocol"); the packages' semver
tracks the implementations. **One versioning convention covers both libraries in this pair**: a
single monotonically increasing integer, bumped only for a breaking wire change, carried in the
lowest-level place that can carry it - the `muxws.v1.<codec>` subprotocol for muxws, since it has no
frame that both peers are guaranteed to exchange, and `v` in the envelope for backchannel, which has
no handshake of its own to hang it on. Neither uses semver on the wire; semver is
for the packages. Python has **zero required runtime dependencies** (stdlib `json` and `asyncio` are
enough for the default codec and the peer); `starlette` and `websockets` are optional extras selected
by which transport you import, and `msgpack` is an optional extra selected by which codec you
register - `pip install muxws[msgpack]`. The TS package has zero runtime dependencies
in the browser entry point, `ws` as an optional peer dependency for `node.ts`, and
`@msgpack/msgpack` as an optional peer dependency reachable only through the `/msgpack` subpath.
`"sideEffects": false` in `package.json` is load-bearing rather than hygiene: it is what permits a
bundler to drop the msgpack subpath from a JSON build, as "Registration is explicit" sets out.

---

## Decisions that are fixed

These are settled. Implement them; do not relitigate them without the user.

1. One symmetric `Peer` per language. No separate client and server implementations.
2. Server push is the acceptor calling `open()`, and its fire-and-forget spelling is
   `peer.notify(payload)` - `open(payload, end=True)` returning no `Stream` and no awaitable at all.
   No push-specific protocol machinery beyond that.
3. Stream ids are allocated by the library, never by the caller. Dialer odd, acceptor even, id 0 for
   connection-level, **open ids monotonic per peer** (`data`/`reset` may name any live id in any
   order), never reused, `goaway` on exhaustion. The id is taken and the `open` frame enqueued in one
   synchronous step, which is what makes monotonicity structural rather than careful.
4. One frame per WebSocket message, spelled-out field names, and the frame-to-message mapping is a
   pluggable `Codec` - JSON the default, the shipped-by-default registration, and the byte-for-byte
   interoperability baseline both languages must reproduce exactly. Every size limit counts bytes of
   the whole **codec-encoded** message, in both languages.
5. Fragmentation above `MAX_FRAME_BYTES` is mandatory, not optional, and slices are
   cut at codepoint boundaries with the envelope and escaping budgeted for by the sender. The cap is a
   **protocol constant of 65536 (64 KiB)**, not a negotiated value: it bounds how long one stream may
   hold a socket with a single global message order, so a larger frame is worse and not better, and a
   negotiation would have optimised the wrong direction. A receiver may accept larger frames; a sender
   always fragments at the constant.
6. Interleaving is enforced, not hoped for: **at most one unsent fragment queued per stream, and the
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
    `payload`, nothing. Two independent consumers already define their own closed sets over one
    socket, and muxws would have had to pick a winner. One of those sets has since grown by a kind
    (backchannel's `watch`) and then twice replaced that kind's meaning outright - a traffic mode
    became a mute set, and the mute set became a whole-value map overriding a server-side delivery
    decision in either direction - all three times without a line of muxws changing and without the
    other consumer hearing about it, which is the property working, three times.
14. Auth at the handshake. Per-stream headers exist but are never interpreted.
15. No stream resumption across reconnects in v1. The reconnect helper restores exactly two things -
    the socket and the connection identity - and nothing else. Concretely: exponential backoff
    `min(initial * factor ** attempts, cap)` with symmetric jitter (250 ms, ×2, 30 s, ±30%,
    unlimited attempts); the attempt counter reset only when a connection is **established**, meaning
    socket open with the subprotocol accepted *and* hello acknowledged; a `ping`/`pong` heartbeat every
    20 s with a 10 s deadline, so half-open sockets are discovered rather than waited on; the
    application's `hello` payload captured once at `connect()` and replayed verbatim on every
    connection, before `on_reconnect` fires; and every stream that was live when the socket died
    failed locally with `CONNECTION_CLOSED`, surfaced as `ConnectionLost`, never left hanging.
    `open()`, `notify()` and `request()` fail fast while the peer is between sockets; nothing is
    queued for the next one. **Reconnection covers lost connections only** - `connect()` raises if the
    *first* attempt fails, whatever the retry configuration says, so a wrong URL surfaces instead of
    being retried forever.
16. `request()` has no default timeout.
17. `PeerRegistry` lives in muxws, indexes every key `peer.tags` held as of the last `register()`
    call - passing over values that cannot serve as a lookup key rather than raising on them, since a
    mutate-only key is under no obligation to be indexable - and returns `list[Peer]`. `tags` is
    per-connection application state that muxws reads never and persists never: any key may be
    overwritten as often as the application likes for the life of the socket, whatever its value is,
    a direct read sees the newest value immediately, `peers_for` keeps answering from the
    last `register()` until another one replaces the peer's entries wholesale, and the dict dies with
    the connection. So look up on keys you do not mutate and mutate keys you do not look up; a
    consumer that needs both on one key calls `register(peer)` after each write. A reconnect starts a
    fresh dict, filled only by the acceptor's framework, the replayed hello, and whatever later
    messages that client sends.
18. Both packages ship from one repo on one version stream, with a shared JSON conformance corpus.
19. The codec is chosen from the environment - `MUXWS_CODEC` via `muxws.conf.settings`,
    `VITE_MUXWS_CODEC` in the browser - and never from a call argument, `codec=` surviving only as a
    test override. Codecs are registered explicitly, never register themselves at import time, and the
    npm package declares `"sideEffects": false`. An unregistered name is a loud startup failure and
    never a silent fallback to JSON.
20. The configured codec rides the WebSocket subprotocol as `muxws.v1.<codec>` and the acceptor
    **asserts** it - on a mismatch it selects no subprotocol and refuses the upgrade with 400. No
    negotiation, no fallback, no per-connection multi-codec support. The client composes its own
    `CodecMismatch` diagnostic from what it offered, naming both `VITE_MUXWS_CODEC` and `MUXWS_CODEC`,
    because a browser cannot read the rejection body; the server logs the same thing where the body
    *is* readable. The subprotocol's version component is the breaking-change generation, and it is
    the only version on the wire; additive revisions are announced nowhere, because unknown frame
    types and fields are ignored and so need no announcement.
21. `peer.open()` is synchronous and returns a `Stream`. `Stream` is both awaitable - yielding the
    result payload (the remote's first) from a memoized, re-awaitable future, a `PromiseLike` rather
    than a `Promise` subclass in TypeScript - and async-iterable, and whichever of the two shapes is used
    first claims the stream while the other raises `StreamAlreadyConsumed`. Awaiting twice is not
    that error: the future is memoized and hands back the same value. `open()` takes no `timeout`,
    because it returns immediately and there is nothing for one to bound; deadlines live on
    `stream.result(timeout=)` and `peer.request(timeout=)`. `notify()` returns nothing;
    `connect()`, `accept()` and `request()` stay async, because each of them waits for something real.
    Sending on a stream that closed normally raises `StreamClosed`, distinct from `StreamReset` (the
    remote objected) and `ConnectionLost` (the socket died).
22. muxws sits at the bottom of the stack and **never depends on any package above it** - not
    backchannel, not fastapi-viewsets, not the frontend kit; not as an import, an optional extra, or
    a `TYPE_CHECKING` annotation. fastapi-viewsets depends on muxws directly for its own WS transport
    adapter, in parallel with backchannel's dependency, not through it.

## Decisions left to the implementer

- The exact internal decomposition of `Peer` (one read loop task plus a writer task, versus a single
  task with a send queue). A send queue is strongly suggested - it is the natural place to enforce
  the round-robin fairness that fixed decision 6 requires. The *data structure* is yours (a deque of
  per-stream queues, a ready-set with a rotating cursor); the policy is not.
- Whether `Stream` iteration buffers unbounded payloads when the consumer is slow, or applies a local
  high-water mark and stops reading the socket. Prefer the latter; the threshold is yours.
- The Python WebSocket client dependency for `muxws.connect` (`websockets` versus `aiohttp`), and
  whether both are supported as extras.
- Logging library integration details, metric names, and whether `on_frame` is sync or async.
- Whether `PeerRegistry` is a module-level singleton, an injectable instance, or both. (Both, with
  the singleton as a convenience over an instance, is the safe answer.)
- Which msgpack library each language binds to, and whether `cbor` ships at all. Both are optional
  extras behind the same `Codec` port, so the choice is reversible and costs nobody anything until
  they register one.
- Whether the codec registry is a module-level dict or an injectable object. The environment-driven
  path only ever needs the former; the `codec=` test override only ever needs an instance. Do not
  build a third thing.
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
(payload, cap, codec). Ships as a usable release for anyone wanting to speak the protocol by hand. The
`conformance/frames/` and `conformance/invalid/` corpora are written *here* and both languages read
them from day one - this is the milestone that makes cross-language agreement structural rather than
aspirational.

**The codec seam belongs in M1 and not later**, even though nothing but JSON will exist until well
after M3. It is the boundary that every send path and every receive path crosses, so retrofitting it
means touching all of them in both languages, in a codebase that by then has a state machine and two
transports layered on top - and it means rewriting the corpus M1 produced, because a corpus written
against "the JSON bytes" rather than against "a logical frame plus what a named codec makes of it" has
the assumption baked into its shape. Building the seam now costs an interface and an indirection;
building it at M5 costs a rewrite of the only artefact that keeps the two ports honest.

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
them exists without a handshake. At the end of M3 a Python peer and a TS peer can hold a real
conversation - the first live cross-language test.

**M4 - Connection liveness and shutdown.** `ping`/`pong` with liveness timers, `goaway` with drain
semantics, graceful `close()`. This is the milestone where "what happens when things go wrong at the
connection level" becomes defined rather than emergent, and the milestone at which open question 1
(does `headers` earn its place?) is answered by looking at whether anything actually uses it.

**This milestone lost most of its content and is deliberately kept anyway**, which is worth stating
rather than quietly shrinking a heading. It used to be "connection lifecycle", and the majority of it
was the `settings` exchange - the frame, the defaults-until-ack window, the ack-as-ordering-point
rule, `protocol_version` mismatch handling - all of which is gone with the frame. The concurrency
limit went with it in a different direction: it is now a receiver-side cap enforced locally, so it
belongs with the other receive-side caps in M5 and is built there. What is left is the liveness timer
and the shutdown handshake, and those two stay together and stay separate from M3 for one reason:
`goaway` drain ordering against in-flight streams is the part of this protocol most likely to be
subtly wrong, and it needs a milestone where it is the thing being tested rather than a detail
alongside two transports. It is now the smallest of the middle milestones, and that is the honest
shape of it rather than a gap to be filled.

**M5 - Robustness and convenience.** Fragmentation wired into the send path with the one-unsent-
fragment rule and round-robin writer selection, the receive-side caps together - the frame cap, the
`max_payload_bytes` bound enforced as fragments accumulate, and the concurrency limit answering
over-limit opens with `REFUSED` - `PeerRegistry`
(`register` / `registered` / `peers_for`, re-indexing on an explicit re-`register` after a tag change,
costing nothing at all for a tag that is only ever read off a peer and never looked up, and pruned by
the peer's own close hook), `on_frame` observability - and the reconnect helper on both
clients, which is the largest single item here and is a whole specified mechanism rather than a retry
loop: jittered exponential backoff, the attempt counter reset only on an *established* connection,
the heartbeat wired to the `ping`/`pong` frames M4 built, the `hello` payload replayed verbatim
before `on_reconnect` fires and the attempt failed if it does not complete,
`on_close(reason.will_retry)`, and `ConnectionLost` raised out of every stream-shaped call - both the
ones that were in flight when the socket died and the ones attempted while the peer is between
sockets. First release usable in production, and the first release `backchannel` can be built
against - its three contract items are symmetric `open` (M2), `notify` (M2), and the registry (here).

**M6 - Conformance, documentation, wire freeze.** The full `conformance/sequences/` corpus, the
live Python↔TS matrix in CI in both role assignments, `SPEC.md` promoted to normative under the v1
generation, and an explicit test that the forward-compatibility rule holds (an unknown frame type is
ignored and logged once, and the connection survives it). A second codec is the honest proof that the seam
built in M1 is a seam: ship `msgpack` as an optional extra in both languages, run the whole sequence
corpus through it by round-trip, and add the cross-language pair (Python peer and TS peer, both on
msgpack) to the CI matrix. If the seam is real this costs a codec module and a matrix row; if it is
not, M6 is when that is discovered rather than after 1.0. Tag 1.0; the wire format is frozen from
here - the JSON wire form is what is frozen, since it is the baseline every peer must reproduce.

**M7 - Developer documentation.** The VitePress site in a `docs/` workspace: a rationale and
architecture narrative, a quick start that works when copy-pasted, and an API reference covering every
public symbol in both languages with signature, parameters, return, raises and one runnable example -
all of it checked by tests that execute the examples and fail when a symbol is documented nowhere.

---

## Testing strategy

**Per language, unit level.** Codec round-trips, for every registered codec. Every state-table
transition, legal and illegal. Fragment splitting and assembly, including the pathological cases that
the fragmentation rules exist for: a payload where the byte-count slice point falls *inside* a
multi-byte codepoint (the splitter must move the boundary back, and in TypeScript must not split a
surrogate pair either), a payload of control characters whose escaping triples its encoded size under
JSON (the splitter must re-split rather than emit an over-cap frame), a slice-point sweep asserting
that no produced message exceeds the cap in bytes *of that codec's output*, a fragment
stream interrupted by a reset, and an over-cap payload with fragmentation disabled. The same payloads
live in the shared corpus, so both ports must produce the *same* fragment boundaries under the same
codec. Cancellation races - reset arriving after the stream already ended, reset arriving while a
fragment is half-assembled, cancel called twice. Frames for a closed id and for an id above the peer's
high-water mark, asserting silence in the first case and `goaway(PROTOCOL_ERROR)` in the second. Two
concurrent `open()` calls from different tasks, asserting that the ids on the wire are increasing -
which under a synchronous `open()` is a test that nothing has crept in between allocation and enqueue
that could suspend. Connection death with N streams open. The receiver's concurrency limit at the
boundary, asserting that the limit-plus-one `open` is answered with `reset(REFUSED)`, that the
opener's pending await raises `StreamRefused`, that the streams already open are undisturbed, and -
the assertion that catches a sender-side quota creeping back in - that the *opener* raised nothing
synchronously and put the `open` on the wire like any other. `max_payload_bytes` at the boundary too,
asserting the reset goes out on the fragment that crosses the limit rather than after the last one,
which is the only version of that test an implementation buffering to completion can fail.

**The handle's own contract needs tests, because every one of its guarantees is a promise about a
mistake.** `stream.id` readable on the line after `open()` with nothing awaited in between. `await
stream` twice, returning the same value the second time rather than raising or hanging - the memoized
future. `await` then `async for` on one stream, and `async for` twice, both asserting
`StreamAlreadyConsumed` and asserting that the *first* consumer still received everything. A stream
that is reset with nobody having awaited or iterated it, asserting in TypeScript that no unhandled
rejection is reported and that the failure did reach the peer's error hook - the test for the default
no-op rejection handler, which is invisible in every other test because everything else consumes its
handle. And in Python, that `peer.open(...)` on a line by itself emits no `RuntimeWarning`. One pair
pins the division of labour between the handle and `request()`: against a remote that sends two
payloads and then ends, `await peer.open(p)` resolves with the first and does not raise, while
`await peer.request(p)` raises - the assertion that would fail if either the count check migrated onto
the handle or `request()` quietly stopped waiting for the end.

**The reconnect helper needs tests of its own, and they are unit tests rather than conformance
fixtures**, because none of this is on the wire: it is entirely a property of the dialing peer, so the
shared corpus has nothing to say about it and both ports must test it separately against the in-memory
transport, with an injected clock and an injected random source. The schedule is a pure function of
(attempts, options, random draw) and is tested as one: assert `min(initial * factor ** attempts, cap)`
before jitter, assert every jittered delay lands inside ±`jitter` of it, and assert the cap actually
caps. Then the two failure modes that motivate the design. **The counter must reset only on an
established connection**: a server that accepts the socket and then drops it before the hello
completes must produce a growing delay sequence, not a fixed-interval hammer at the initial delay -
which is the test that fails under the obvious "reset in `onopen`" implementation. **And the first
attempt is not retried at all**: a `connect()` against a URL nothing is listening on must raise out of
the call, with unlimited retries configured, rather than returning a peer and dialling forever. **Jitter must
actually disperse**: run N peers whose sockets all die at the same simulated instant and assert their
first retry instants are distinct and spread across the expected window, which is the one assertion
that catches a jitter parameter that was accepted, stored, and never applied.

The identity replay has its own set, and it is the set that would have caught the "reconnected but
anonymous" failure. Drop and restore the socket three times and assert the acceptor received three
byte-identical hellos, that `on_reconnect` fired after each acknowledgement and never before it, and
that an application which registered no `on_reconnect` handler at all still ends up with a peer the
server can find in its registry. A hello that the acceptor resets, and a hello whose acknowledgement
never arrives, must both leave `on_reconnect` unfired, increment the attempt counter and produce a
backoff - not a peer sitting on an open socket the server cannot associate with anything.

**`tags` and the registry get the small set that pins the guarantees a consumer depends on**, and all
of them are about a dict that is written more than once. A tag written after `register()` must be
visible on `peer.tags` on the next line, must *not* be found by `peers_for` until `register(peer)` is
called again, and must be found by it immediately afterwards - under the new value and no longer
under the old one. Those three assertions fail in different places for different mistakes: a registry
that watched the dict and re-indexed on every write fails the second, and one that never re-indexes
at all fails the third. Then the pattern backchannel actually runs, which is the two halves of that
rule exercised together: register a peer under `session`, overwrite a *different*, never-looked-up
key - whose value is a whole map replaced outright each time, as backchannel's overrides are - a
hundred times without re-registering, and assert that every read sees the newest value, that
`peers_for(session=...)` returns the peer unchanged throughout, and that the registry's index did not
grow by a single entry. The last assertion is the one an implementation which quietly re-indexed on
assignment would fail, and it is what lets a consumer treat a repeated overwrite as free. One more
belongs with it, because it is the one that breaks on a map-valued tag rather than on a scalar one:
`register(peer)` called while that key is present must neither raise on the unhashable value nor
index it. And across a drop and restore, the acceptor's new peer must start with a `tags` containing
only what the framework and the replayed hello put there: a tag the previous connection's handler
added is gone, and the peer is not findable by it. That last one is the test backchannel's delivery
overrides rest on, and it is the one an implementation that "helpfully" carried tags forward would
fail.

Liveness and the streams that were live are the rest. A transport that silently swallows `pong` must
be discovered within `ping_interval + ping_timeout` and drop into the same backoff path as a clean
close, without the test waiting on anything resembling a TCP timeout. With N streams open in every
shape at once - one awaited handle, one `async for`, one in-flight `request()`, one bidirectional
stream mid-send - killing the socket must make every one of them raise `ConnectionLost`, and the
assertion that matters is that the test does not need its own timeout to finish: a hung `await` is the
failure being tested for, so the test must fail by *hanging detection*, not by an error nobody raised.
Assert also that `stream.closed` is set, that `on_close` fired exactly once with `will_retry` true,
that a second `await` on an already-failed handle raises the same error rather than blocking, that
`open()` during the disconnected window raises synchronously, and that nothing attempted in that
window appears on the wire after the new socket comes up. Finally, `max_attempts` exhausted fires
`on_close` once with `will_retry` false and never dials again.

**Codec configuration and the mismatch guard.** A configured name that was never registered, asserting
`CodecNotRegistered` at startup, that the message names the environment variable, the value it found
and the registered set, and that no socket was opened. A dialer and acceptor on different codecs,
asserting the acceptor selected no subprotocol and answered 400, that the dialer surfaced
`CodecMismatch` rather than a decode failure after connect or a bare connection error, that its
message names both `VITE_MUXWS_CODEC` and `MUXWS_CODEC`, that the acceptor logged both values, and
that *no* frame was exchanged. A peer offering `muxws.v2.json` against a v1 acceptor,
asserting the same rejection. And the negative test that keeps the guard honest: a dialer and acceptor
on the same non-default codec, asserting they interoperate normally.

**Cross-language conformance - mandatory, not optional.** Two implementations of one protocol diverge
silently unless something forces them not to. The corpus lives in `conformance/`, is plain JSON, and
is read verbatim by both `pytest` and `vitest`. Three kinds:

`conformance/frames/*.json` - a list of `{"name", "frame", "json_wire"}` triples: a *logical* frame,
plus the JSON wire form that frame must take. The corpus is deliberately not "logical frame → expected
bytes", because there is no longer one answer to what the bytes are. It is a corpus of logical frames,
with the JSON rendering pinned alongside them, and the two are checked differently:

- **The JSON codec is checked against `json_wire`.** Both suites assert `jsonCodec.decode(json_wire) ==
  frame` and `jsonCodec.decode(jsonCodec.encode(frame)) == frame`. JSON is the interoperability
  baseline - the one encoding a Python peer and a TypeScript peer must agree on byte-for-byte, since it
  is what every deployment falls back to and what every debugging session reads - so it is the codec
  that gets a pinned wire form at all. Note the asymmetry deliberately: the round-trip test compares
  *parsed objects*, never byte-identical output, so neither implementation is forced into a particular
  JSON key order or whitespace. One separate test does pin a canonical key order (`type`, `stream`,
  then remaining keys alphabetically) for the benefit of anyone diffing logs.
- **Every other codec is checked by round-trip over the same logical frames.** `codec.decode(
  codec.encode(frame)) == frame`, for every frame in the corpus, for every registered codec. No wire
  form is pinned, because pinning msgpack bytes in a JSON fixture file would be unreadable and would
  freeze an encoder version rather than a protocol. What must hold is that the logical frame survives,
  which is the actual contract the `Codec` port makes.
- **Cross-language, per codec.** A Python peer and a TypeScript peer configured with the same non-JSON
  codec must understand each other over the sequence corpus. That is the test the round-trip cannot
  replace: a codec can round-trip perfectly within one language while two language bindings of the
  "same" format disagree about how they represent, say, an integer key or a binary payload. Every
  codec that ships gets this pair in CI, or it does not ship.

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
it - which keeps the corpus honest about the rule that callers never invent ids. The top-level
`max_frame_bytes` is not a wire value and never was one on this connection: the cap is a protocol
constant, and the fixture is telling the *runner* to construct both peers with a lowered one, which is
a test-only construction argument and the only reason any value other than 65536 exists.

The interleaving rule needs its own fixture, because it is the one MUST in this document that a
plausible-looking implementation can violate while passing every other test:

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
sixteen fragments at once, fails this fixture and passes everything else - which is why it exists.

`conformance/invalid/*.json` - malformed and illegal input with the required reaction, asserted as
"which frame goes out and does the connection survive": wrong parity, an `open` with an id not
greater than that peer's highest previous open, `data` after `end`, an over-cap encoded message,
garbage JSON, a fragment sequence interrupted by a non-fragment frame, a stream-level frame naming an
id above the peer's high-water mark (connection dies), and a `data` frame for an already-closed id
(connection survives, nothing goes out). "Garbage JSON" generalises to "a message the configured codec
refuses to decode", and the fixture says which - so the same case can be replayed under a binary codec
by substituting a byte sequence that codec rejects, rather than being silently skipped there.

**Live integration, both role assignments.** In CI: a Python acceptor with a TypeScript dialer, *and*
a TypeScript acceptor with a Python dialer, running the same scenario script - concurrent unary
requests interleaved with a streaming export and a server push, one of them cancelled mid-flight, and
a `goaway` shutdown at the end. If the symmetry claim at the top of this document is real, both
directions pass with the same script. If only one direction passes, the claim is false and something
is direction-specific that should not be.

The same matrix gets one reconnect scenario, because the reconnect helper is the one component whose
correctness depends on a real socket dying in a way the in-memory transport cannot fully imitate: kill
the acceptor process with streams open, restart it, and assert that the dialer re-dials on a jittered
delay, replays a byte-identical hello that the *other language's* acceptor accepts, fires
`on_reconnect` exactly once after it, and that every stream that was open raised `ConnectionLost` in
the meantime. That last clause is what stops a passing reconnect from hiding a hung caller.

**Property-based, if cheap.** Generate random legal frame sequences, assert both decoders agree and
that the state machine never reaches an undefined transition. Hypothesis (Python) and fast-check (TS)
can share generated corpora via the same fixture directory.

---

## Open questions

These remain open, but none of them is left hanging: each carries a recommendation and the trigger
that would overturn it, because an implementer who reaches one of these needs an answer today. None
of them blocks M1.

1. **Should `headers` exist in v1 at all?** The application can put everything in the payload, and
   `headers` has exactly one justification that survives scrutiny: a dispatcher can read routing
   metadata before a possibly-fragmented body has fully arrived. **Recommendation: keep it, justified
   solely by that, and delete it at M4 if no consumer uses it** - the wire freezes at M6, so M4 is the
   last honest moment to remove a field. Note that the one plausible consumer is the fastapi-viewsets
   dispatcher reading the viewset name off `headers` to pick a handler while the command payload is
   still being reassembled, which is why the samples above use `headers={"viewset": "item"}` rather
   than an invented `topic`. backchannel does not use headers at all, and neither does the reconnect
   helper's hello - it accepts `hello_headers` for symmetry with `open()`, but a hello payload is
   small, arrives in one frame, and has nothing to dispatch on. So the count of real consumers is
   still one, and M4 is still the moment to check whether it stayed that way.

2. **Subpath packaging, for the Node acceptor and for the codecs.** The brief takes the position that
   `ws` belongs behind `muxws/node` and the heavy codecs behind
   `muxws/msgpack`, so that neither reaches a browser bundle that did not ask for it.
   The codec half of that claim carries a specific mechanism - constant substitution, then dead-code
   elimination, then tree-shaking, given no self-registration and `"sideEffects": false` - and a
   mechanism stated in a design document is a hypothesis until a bundler agrees with it.
   **Recommendation: verify it at M3 with an actual production build** (a JSON build and a msgpack
   build of the same app, comparing bundle contents, not just sizes) before either subpath becomes
   public API surface. The failure mode if it does not hold is mild - a few unused kilobytes, since
   the honest counterpoint in "Registration is explicit" already concedes bundle size is the weaker
   half of the argument - but the packaging shape should be settled before the API is.

3. **Should `request()` grow a configurable default timeout at the peer level?** Fixed decision today
   is "no default at all". **Recommendation: ship without it.** `Peer(default_timeout=...)` is a
   purely additive, non-breaking change, so there is no cost to waiting - add it when a consumer asks
   with a concrete case, not in anticipation of one. Shipping it early is the expensive direction: a
   default timeout that turns out to be wrong silently kills long actions in production. The reconnect
   rules strengthen the recommendation rather than weakening it: the failure a default timeout is
   really guarding against is a request that will never be answered, and "What happens to the streams
   that were live" makes connection death end exactly those, promptly, with a distinguishable error.
   What is left for a timeout to catch is a remote that is alive and simply slow - which is a decision
   only the caller can make.

4. **What exactly forces `window_update` to be built?** Written above as "a real producer/consumer
   speed mismatch with observed unbounded memory growth". **Recommendation: turn that into a metric
   with a threshold during M5** - the `Stream` iteration high-water mark being hit on a real
   deployment, counted and exported - so the decision is later made by data rather than by whoever is
   annoyed that day. Until the counter moves in production, the answer is no. Note that the answer is
   now also more expensive: with no capability advertisement on the wire, building it means a
   `muxws.v2` generation rather than an extension inside v1 (see "Versioning lives in the
   subprotocol"), which raises the bar for the evidence rather than changing what the evidence is.

5. **Name.** `muxws` is decided. Noted only for completeness: the name collides with a handful of
   small unrelated projects in other ecosystems, none on PyPI or npm under these scoped names. Not a
   reason to change it.
