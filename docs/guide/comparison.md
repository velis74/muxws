# Comparison to HTTP/2 and HTTP/3

muxws copies a stream model that HTTP settled on years ago, and copies it deliberately. This page is
the accounting: what is the same, what is missing, and what muxws has that HTTP never managed. It is
written for a reader who already knows one of those protocols and wants to know which of their
instincts carry across.

## The model is HTTP/2's

"HTTP/3 semantics" is the shorter phrase, and on its own it is misleading. Almost nothing muxws
copies is *from* HTTP/3.

HTTP/3 is a thin layer over QUIC. It has no stream state machine, because QUIC owns stream states.
It has no flow control, because QUIC owns flow control. It has no `PING`, because QUIC has one.
Everything HTTP/3 adds over HTTP/2 lives one layer below it — and that layer is exactly what a
WebSocket does not give you.

What muxws actually mirrors is **HTTP/2**: the five-state stream machine, `PING` with an opaque
nonce, one frame per message, `GOAWAY` carrying the last stream it will finish, headers then body
then trailers. The state table in [Architecture](/guide/architecture) is HTTP/2's, minus the two
`reserved` states that exist only to serve `PUSH_PROMISE`.

HTTP/3 still belongs in the sentence, because the *model* survived the move to QUIC unchanged and
its second outing is what proves it was right. But a reader who hears "HTTP/3" and goes looking for
per-stream loss recovery is looking for QUIC, and should read the next section before anything else.

## Frame for frame

| muxws | HTTP/2 | HTTP/3 / QUIC | |
|---|---|---|---|
| `open` | `HEADERS` | `HEADERS` on a QUIC stream | headers and the first payload in one frame |
| `data` | `DATA` | `DATA` | `end` is a flag, not a frame |
| `reset` | `RST_STREAM` | QUIC `RESET_STREAM` | one direction only; see below |
| `ping` / `pong` | `PING`, `PING+ACK` | QUIC `PING` | two types rather than a flag |
| `goaway` | `GOAWAY` | `GOAWAY` | plus a drain window |
| — | `WINDOW_UPDATE` | QUIC `MAX_STREAM_DATA` | **absent**; the name is reserved |
| — | `SETTINGS` | `SETTINGS` | absent by decision |
| — | `PUSH_PROMISE` | `PUSH_PROMISE`, `MAX_PUSH_ID`, `CANCEL_PUSH` | not needed; see [Symmetry](#symmetry-is-not-a-missing-feature) |
| — | `PRIORITY` | `PRIORITY_UPDATE` (RFC 9218) | absent; the writer is strictly fair |
| — | HPACK | QPACK | headers travel verbatim |

Six frame types against HTTP/2's ten and HTTP/3's seven-plus-QUIC. The subtraction is the point of
the design, and each row above says which subtraction you are paying for.

## Two kinds of head-of-line blocking

This is the distinction that matters most, and the one a reader arriving from HTTP/3 is most likely
to get wrong.

**Application-level head-of-line blocking** is what you cause yourself by putting more than one
conversation on one channel: a 1 MB export occupies the send queue from its first byte to its last,
and a 200-byte progress update on another stream waits behind it. **muxws solves this completely.**
A payload above `MAX_FRAME_BYTES` is split, the frame cap is a constant so no stream can hold the
queue longer than one frame's worth, and the writer rotates between streams with at most one prepared
frame each. See [Sizes & fragmentation](/guide/sizes-and-fragmentation).

**Transport-level head-of-line blocking** is what the network causes: one lost TCP segment stalls
every muxws stream until it is retransmitted. **muxws cannot solve this, and no framing choice above
TCP can.**

The reason is worth stating plainly rather than leaving as a limitation. Loss is visible only to
whoever owns the packets. QUIC owns them: every UDP datagram carries frames tagged with a stream id
and an offset, so when a datagram is lost QUIC knows precisely which streams it touched and delivers
the rest immediately. That is not a clever framing decision — it is what being the transport buys.

Under a WebSocket, muxws is handed a single ordered byte stream that the kernel has already repaired.
When a segment goes missing, everything behind it is held back until the retransmission arrives, and
what finally reaches `receive()` is gapless. There is nothing to recover per stream, because per
stream loss was never observable. Recovering it would mean moving below TCP, which means being QUIC.

So the honest summary is: **muxws removes the head-of-line blocking you inflict on yourself, and
leaves the one the network inflicts on you.** For a browser-to-server connection carrying dozens of
small conversations and the occasional large one, the first is the one you meet daily.

Three more QUIC properties are out of reach for the same reason and are listed once here: **0-RTT
resumption**, **connection migration** across a network change, and **per-stream congestion
control**.

## What muxws has that HTTP does not

### Symmetry is not a missing feature

The `PUSH_PROMISE` row in the table above reads like a gap and is the opposite of one.

In HTTP/2 and HTTP/3, server push is a *second mechanism*. The server promises a request it invented
on the client's behalf, then delivers the response on a separate stream that the client can only
cancel, never speak on. It carries no request body, it is bounded by a `MAX_PUSH_ID` quota, and both
browsers and servers have retired it — Chrome removed HTTP/2 push in 2022, and no major browser
implements the HTTP/3 version.

In muxws the acceptor calls `open()`. Same type, same handler, same cancellation, same correlation,
same everything. A push is a request with the roles swapped, and there is no second mechanism to
learn, to bound, or to deprecate. HTTP tried twice and neither attempt stuck; muxws removes the
problem instead of solving it. See [Rationale](/guide/rationale#symmetry-one-peer-type-per-language).

### Fairness is a MUST, not a recommendation

RFC 9218 is advisory and HTTP scheduling is implementation-defined: a conforming HTTP/2 server may
serialise one response completely before looking at another. In muxws the round-robin writer is a
normative rule with a conformance fixture behind it, and a mutation to a FIFO fails the suite. What
a reader gets from that is a guarantee rather than a hope.

### Reset codes carry a required reaction

HTTP error codes are diagnostic: they say what happened. The muxws table says what the receiver must
*do* — `REFUSED` means definitively not processed, so retry elsewhere; `CANCELLED` means stop and do
not retry; `PAYLOAD_TOO_LARGE` means do not retry unchanged. A caller can act on the difference
without a library-specific convention on top. See [Errors](/guide/errors).

### The codec is part of the connection's identity

An HTTP body is opaque octets and `Content-Type` is a hint that arrives with the body. In muxws the
codec name is in the subprotocol, so two peers that disagree are refused at the handshake with HTTP
400 rather than misreading each other's payloads later. See [Codecs](/guide/codecs).

### Fragmentation of structured values

HTTP never had this problem: a body is already a byte stream, so chunking it is free. muxws carries
logical values, so splitting and reassembly had to be designed, pinned to exact boundaries and frozen
in a shared corpus. It is work HTTP did not have to do.

### Headers are values, not strings

`{"attempt": 2}` stays a number. In HTTP it is `"2"`, and both ends re-derive the type.

### Reconnect and the registry

Above the protocol rather than in it, and HTTP has no equivalent: a connection is a connection, and
what happens when it dies is the application's problem. See [Reconnect](/guide/reconnect) and
[Registry](/guide/registry).

## What is missing

### Flow control — the real gap

Nothing stops a fast producer from outrunning a slow consumer. The two defences are blunt:
`MAX_FRAME_BYTES` bounds how long one stream holds the send queue, and the receiver's
`max_concurrent_streams` bounds how many producers may exist at once. Between them there is nothing
per stream and nothing per connection. A server pushing telemetry faster than a browser renders it
fills the send buffer and then memory.

`window_update` is reserved as a frame name and a v1 peer must never send one, so the shape of the
answer is agreed and its arrival is a later generation's business. Until then, treat "backpressure"
in muxws as meaning those two constants and nothing more.

### Priority

The writer is strictly fair, and strict fairness is not always right: an interactive query and a
1 MB export get an equal share of the queue. HTTP/2 tried a dependency tree and everyone abandoned
it; HTTP/3 replaced it with the much simpler urgency-and-incremental scheme of RFC 9218. muxws has
neither. A weight on `open()` would be a purely local sender-side decision and would need nothing on
the wire, which is what makes it a plausible addition rather than a wire change.

### No `STOP_SENDING` distinct from `reset`

QUIC lets a receiver say "stop sending me this" without destroying what is already in flight in the
other direction. In muxws `reset` closes the stream both ways. "I have read enough, but finish what
you were doing" cannot be expressed.

### Limits are discovered by failing

The consequence of having no `SETTINGS`. `max_payload_bytes` and `max_concurrent_streams` are each
one receiver's private defence, announced nowhere, so a sender learns them only by being answered
`PAYLOAD_TOO_LARGE` or `REFUSED`. That is a deliberate trade — a negotiated limit is a limit two
peers can disagree about, and the whole exchange has to happen before anything useful can be sent —
but the cost is real and lands on the sender. See
[Connection lifecycle](/guide/connection-lifecycle).

### No header compression

`headers` travel verbatim on every `open`. With a few keys that costs nothing; with a full trace
context on every stream it is not nothing. HPACK and QPACK exist because HTTP repeats the same
twenty headers on every request, which is not the shape of muxws traffic — but the saving is real
where the traffic does look like that.

## See also

- [Rationale](/guide/rationale) — why the layer exists at all, and what building it yourself costs
- [Architecture](/guide/architecture) — the state machine this page compares against HTTP/2's
- [Sizes & fragmentation](/guide/sizes-and-fragmentation) — the frame cap and the round-robin writer
- [Connection lifecycle](/guide/connection-lifecycle) — `goaway`, the drain window, and the absent `settings`
- [Errors](/guide/errors) — the reset codes and the reaction each one requires
