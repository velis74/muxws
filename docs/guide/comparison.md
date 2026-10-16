# Comparison to HTTP/2 and HTTP/3

muxws uses the stream model of HTTP/2 and HTTP/3. This page lists what muxws has in common with those
protocols, what it lacks, and what it has that they do not. It is intended for readers who know HTTP/2
or HTTP/3.

## The model is HTTP/2's

The stream model muxws copies is HTTP/2's. HTTP/3 contributes almost none of it.

HTTP/3 is a thin layer over QUIC. It has no stream state machine, because QUIC manages stream states.
It has no flow control, because QUIC provides flow control. It has no `PING`, because QUIC has one.
The functionality HTTP/3 adds over HTTP/2 is implemented in QUIC, and a WebSocket does not provide that
layer.

muxws mirrors **HTTP/2**: the five-state stream machine, `PING` with an opaque nonce, one frame per
message, `GOAWAY` carrying the last stream the sender will finish, and headers, then body, then
trailers. The state table in [Architecture](/guide/architecture) is HTTP/2's without the two
`reserved` states, which exist only for `PUSH_PROMISE`.

HTTP/3 uses the same stream model as HTTP/2, which is why muxws names both. Per-stream loss recovery is
a QUIC feature that muxws does not provide; see
[Two kinds of head-of-line blocking](#two-kinds-of-head-of-line-blocking).

## Frame for frame

| muxws | HTTP/2 | HTTP/3 / QUIC | |
|---|---|---|---|
| `open` | `HEADERS` | `HEADERS` on a QUIC stream | headers and the first payload in one frame |
| `data` | `DATA` | `DATA` | `end` is a flag, not a frame |
| `reset` | `RST_STREAM` | QUIC `RESET_STREAM` | closes both directions; see below |
| `ping` / `pong` | `PING`, `PING+ACK` | QUIC `PING` | two types rather than a flag |
| `goaway` | `GOAWAY` | `GOAWAY` | plus a drain window |
| — | `WINDOW_UPDATE` | QUIC `MAX_STREAM_DATA` | **absent**; the name is reserved |
| — | `SETTINGS` | `SETTINGS` | absent |
| — | `PUSH_PROMISE` | `PUSH_PROMISE`, `MAX_PUSH_ID`, `CANCEL_PUSH` | not needed; see [Symmetric peers instead of server push](#symmetric-peers-instead-of-server-push) |
| — | `PRIORITY` | `PRIORITY_UPDATE` (RFC 9218) | absent; the writer is strictly fair |
| — | HPACK | QPACK | headers are sent uncompressed |

muxws has six frame types; HTTP/2 has ten, and HTTP/3 has seven plus the QUIC frames. The last column
of each absent row states the consequence of its absence.

## Two kinds of head-of-line blocking

**Application-level head-of-line blocking** occurs when several conversations share one channel: a
1 MB export occupies the send queue from its first byte to its last, and a 200-byte progress update on
another stream waits behind it. **muxws solves this.** A payload above `MAX_FRAME_BYTES` is split, the
frame cap is a constant so no stream occupies the queue for longer than one frame, and the writer
rotates between streams with at most one prepared frame per stream. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

**Transport-level head-of-line blocking** is caused by packet loss: one lost TCP segment stalls every
muxws stream until it is retransmitted. **muxws does not solve this, and no protocol above TCP can.**

Only the layer that handles packets can see packet loss. QUIC is that layer: every UDP datagram carries
frames tagged with a stream id and an offset, so when a datagram is lost QUIC knows which streams it
affected and delivers data for the other streams immediately.

A WebSocket gives muxws a single ordered byte stream that the kernel has already repaired. When a
segment is lost, all data after it is held back until the retransmission arrives, and the data that
reaches `receive()` has no gaps. Loss is not observable per stream at this layer. Per-stream loss
recovery requires replacing TCP, as QUIC does.

muxws therefore removes application-level head-of-line blocking and does not affect transport-level
head-of-line blocking. On a browser-to-server connection carrying many small conversations and some
large ones, application-level blocking is the more frequent of the two.

Three other QUIC features are unavailable for the same reason: **0-RTT resumption**, **connection
migration** across a network change, and **per-stream congestion control**.

## What muxws has that HTTP does not

### Symmetric peers instead of server push

muxws has no `PUSH_PROMISE` because it does not need one.

In HTTP/2 and HTTP/3, server push is a separate mechanism. The server sends a promise for a request it
created on the client's behalf, then delivers the response on a separate stream that the client can
cancel but not send on. A push carries no request body and is limited by a `MAX_PUSH_ID` quota. Chrome
removed HTTP/2 push in 2022, and no major browser implements HTTP/3 push.

In muxws the acceptor calls `open()`. The stream has the same type, handler, cancellation and
correlation as a stream the dialer opens. A push is a request with the roles swapped, and there is no
separate mechanism to implement or limit. See
[Rationale](/guide/rationale#symmetry-one-peer-type-per-language).

### Fairness is a MUST

RFC 9218 is advisory and HTTP scheduling is implementation-defined: a conforming HTTP/2 server may send
one response completely before sending another. In muxws the round-robin writer is a normative rule
with a conformance fixture; a writer that sends frames in FIFO order fails the suite.

### Reset codes define the receiver's reaction

HTTP error codes are diagnostic: they describe what happened. Each muxws reset code also defines what
the receiver must do: `REFUSED` means definitively not processed, so retry elsewhere; `CANCELLED` means
stop and do not retry; `PAYLOAD_TOO_LARGE` means do not retry unchanged. A caller can act on the code
without a library-specific convention. See [Errors](/guide/errors).

### The codec is part of the subprotocol

An HTTP body is opaque octets, and `Content-Type` is a header that arrives with the body. In muxws the
codec name is part of the subprotocol, so two peers with different codecs are refused at the handshake
with HTTP 400 and never exchange payloads. See [Codecs](/guide/codecs).

### Fragmentation of structured values

An HTTP body is a byte stream, so it can be chunked at any byte. muxws carries logical values, so it
defines how a value is split and reassembled, with exact fragment boundaries pinned in the shared
conformance corpus.

### Headers are typed values

`{"attempt": 2}` stays a number. In HTTP it is the string `"2"`, and both ends convert it.

### Reconnect and the registry

These are helpers above the protocol, with no HTTP equivalent: in HTTP, handling a lost connection is
left to the application. See [Reconnect](/guide/reconnect) and [Registry](/guide/registry).

## What is missing

### Flow control

muxws has no flow control: a fast producer can outrun a slow consumer. Two limits apply:
`MAX_FRAME_BYTES` bounds how long one stream occupies the send queue, and the receiver's
`max_concurrent_streams` bounds how many streams can be open at once. There is no per-stream or
per-connection window. A server that pushes telemetry faster than a browser processes it fills the
send buffer and then memory.

`window_update` is a reserved frame name, and a v1 peer must never send one. Flow control is reserved
for a later protocol generation. In muxws v1, backpressure consists of those two limits only.

### Priority

The writer is strictly fair: an interactive query and a 1 MB export get an equal share of the queue.
HTTP/2 defined a dependency tree, which implementations did not adopt; HTTP/3 replaced it with the
urgency-and-incremental scheme of RFC 9218. muxws has neither. A priority weight on `open()` would be a
local sender-side scheduling decision and would require no wire change.

### No `STOP_SENDING` distinct from `reset`

In QUIC, a receiver can ask the sender to stop sending on a stream without closing the other
direction. In muxws `reset` closes the stream in both directions. A peer cannot express "stop sending,
but finish what you were doing".

### Limits are not announced

muxws has no `SETTINGS`. `max_payload_bytes` and `max_concurrent_streams` are local limits of each
receiver and are not announced, so a sender learns of them only when the receiver answers with
`PAYLOAD_TOO_LARGE` or `REFUSED`. Without negotiation, two peers cannot disagree about a negotiated
value, and no exchange has to complete before the first stream is opened; the sender has to handle the
rejection. See [Connection lifecycle](/guide/connection-lifecycle).

### No header compression

`headers` are sent uncompressed on every `open`. With a few keys the overhead is negligible; with a full
trace context on every stream it is noticeable. HPACK and QPACK exist because HTTP sends the same set
of headers on every request. muxws traffic does not usually have that shape, but where it does,
compression would reduce the size.

## Measuring the cost yourself

This page describes protocol features. Throughput depends on the machine, so no figures are given here.

`python demo.py --bench` in a repository checkout measures it on the local machine. It sends the same
payload three ways over the same transport in the same run: a raw socket with a length prefix, one
WebSocket message per payload without muxws, and muxws. The ratio is computed against a measured
baseline, and the difference between consecutive modes separates the cost of the WebSocket from the
cost of muxws. See [Demos](/guide/demos#the-throughput-report) for the modes, the axes and the method.

The benchmark does not cover:

- **The TypeScript implementation.** Both ends of the benchmark are Python. Both implementations use
  the same wire format, as the conformance corpus verifies, but throughput depends on the
  implementation and the runtime, so a Python figure does not apply to TypeScript.
- **A link slow enough to be the bottleneck.** Over loopback and over a socket file the limit is CPU,
  and the ratio measures the cost of the envelope. When the transport saturates first, all three
  modes give the same throughput and the ratio does not measure muxws.

## See also

- [Rationale](/guide/rationale) — why the layer exists, and what an application would otherwise build
- [Demos](/guide/demos#the-throughput-report) — the throughput benchmark and how to read its ratio
- [Architecture](/guide/architecture) — the stream state machine compared here to HTTP/2's
- [Sizes & fragmentation](/guide/sizes-and-fragmentation) — the frame cap and the round-robin writer
- [Connection lifecycle](/guide/connection-lifecycle) — `goaway`, the drain window, and the absence of `settings`
- [Errors](/guide/errors) — the reset codes and the reaction each one requires
