# Rationale

A WebSocket provides one ordered sequence of messages. An application that runs more than one
conversation over that sequence needs a multiplexing layer: correlation, stream termination, error
reporting, cancellation and fragmentation. muxws is that layer, implemented once in Python and once in
TypeScript against a shared conformance corpus.

## One socket, many streams

A **stream** in muxws is an independently addressed, independently cancellable, bidirectional
exchange. Any number of streams share one socket. Each stream has its own payloads, its own optional
headers, its own end and its own failure.

The two alternatives are one socket per conversation and a custom message envelope on one socket.

### Why not N sockets

With one socket per conversation, concurrency is handled by the transport:

- Each socket is a separate TCP connection with its own TLS handshake. Browsers limit the number of
  concurrent connections to one host, and the application cannot change that limit.
- Message order between sockets is undefined. Two messages that must be applied in sequence cannot be
  ordered when they travel on different connections.
- Cancelling a conversation requires closing its socket. If the socket carries more than one
  conversation, closing it ends all of them.
- Reconnect logic (backoff, jitter, re-authentication) runs per socket and is implemented N times.
- The server accepts, tracks and tears down a separate authenticated session per socket.

In muxws, cancelling a stream sends one frame and closes one stream. The socket and the other streams
on it are not affected.

### Why not a custom envelope

The other approach is one socket and an application-defined envelope, such as a `{ id, type, body }`
wrapper and a map of pending callbacks. Supporting the usual requirements adds these features to it:

- a correlation id, because replies arrive out of order;
- a last-message flag, because some replies are a sequence of values;
- a separate error shape, because an error is not a reply;
- cancellation, for work whose result is no longer needed;
- fragmentation, because a WebSocket has one global message order and a 4 MB message blocks every
  other message on the socket until its last byte is sent;
- round-robin scheduling between fragmenting messages, because a sender that emits all fragments of
  one payload before any other message still blocks the other messages.

muxws implements all of these. Each is tested against a shared cross-language corpus:

- **Stream ids are allocated by the library.** The dialer uses odd ids and the acceptor even ids, so
  both ends can open streams at any time without collisions or coordination. No API takes an id.
- **`end` is a flag on a payload frame**, not a separate message. A one-payload reply is one frame.
- **Reset codes define the receiver's reaction.** "Not accepted, definitively not processed - retry
  elsewhere" is a different code from "the handler raised", and the caller can handle them
  differently. See [Errors](/guide/errors).
- **Per-stream cancellation** closes the stream locally immediately and tells the remote to stop
  processing it.
- **Automatic fragmentation with a round-robin writer**: a 1 MB export does not delay a 200-byte
  progress update on another stream. See [Sizes & fragmentation](/guide/sizes-and-fragmentation).
- **A reconnect helper** with jittered backoff, a heartbeat that runs only while the connection is
  idle, and an opening payload replayed verbatim on every connection. See [Reconnect](/guide/reconnect).

## Relation to HTTP/2 and HTTP/3

muxws uses the stream model of **HTTP/2 and HTTP/3** on top of a WebSocket. The problems listed in the
previous section are solved in those protocols, and the solutions are widely deployed.

muxws copies these properties:

- many independent streams multiplexed over one connection;
- either end can open a stream, so server push needs no separate mechanism;
- headers, then a body, then optional trailers;
- unary request/response and streaming responses built on the same primitive;
- per-stream cancellation that does not affect other streams;
- connection-level graceful shutdown with a `goaway` that names the last stream it processed.

`open()`, `end`, trailers and `goaway` behave like their counterparts in gRPC and HTTP/2 clients.

### What is not copied

muxws copies the semantics of HTTP/2 and HTTP/3, not their transport. A WebSocket runs over one TCP
connection, which has these consequences:

- **No independent per-stream loss recovery.** In QUIC, a lost packet stalls only the stream it
  belonged to. Over TCP, one lost segment stalls *every* muxws stream until it is retransmitted. A
  framing layer above TCP cannot change this.
- **No 0-RTT, no connection migration, no per-stream congestion control.** These are QUIC transport
  features, below the layer muxws operates at.
- **One global message order.** A WebSocket delivers messages in send order across all streams,
  because the connection has one send queue. The frame cap is therefore a protocol constant, not a
  setting: it bounds how long one stream can occupy that queue.

muxws also has no equivalent of the HTTP/2 and HTTP/3 `SETTINGS` exchange. No limit is negotiated -
see [Connection lifecycle](./connection-lifecycle.md).

### Head-of-line blocking

Two different problems are called head-of-line blocking. muxws solves the first and cannot solve the
second.

**Application-level head-of-line blocking** occurs when several conversations share one channel and a
large message occupies the send queue: a 1 MB export is sent from its first byte to its last while a
200-byte progress update waits. **muxws solves this.** Large payloads are fragmented, the frame cap is
a constant so no stream occupies the queue for longer than one frame, and the writer rotates between
streams with at most one prepared frame per stream - see
[Sizes & fragmentation](./sizes-and-fragmentation.md).

**Transport-level head-of-line blocking** is caused by packet loss: one lost TCP segment stalls every
stream until it is retransmitted. **muxws does not solve this, and no protocol above TCP can.** Only
the layer that handles packets can see packet loss. When a message reaches muxws, the kernel has
either already filled the gap or is still holding back all data after it. QUIC solves this problem
because QUIC is the transport.

[Comparison to HTTP/2 and HTTP/3](./comparison.md) covers the differences in detail.

## What muxws does not do

**Routing.** A peer has one incoming-stream handler, registered with `on_stream`, and it is called as
`(payload, stream)` for every stream the remote opens. muxws does not inspect the payload: there is no
path matching, method dispatch, handler table or per-action registration. Dispatch is implemented in
the application's handler.

```python
# fragment
@peer.on_stream
async def handle(payload, stream):
    # The application dispatches; muxws never reads this dict.
    if payload["action"] == "list":
        await stream.reply({"items": []})
```

**Serialization of domain objects.** The codec converts a frame to a WebSocket message and back; it
does not interpret the payload. muxws defines no vocabulary inside `payload`: no `kind`, no reserved
key, no discriminator. A payload is any value the codec can encode. See [Codecs](/guide/codecs).

**RPC.** muxws has no service definitions, schemas, generated stubs or status codes, and never maps an
exception to a numeric status. A handler that raises produces a reset carrying a reason and an
optional structured error object, and the caller receives a `RemoteError`.

**Authentication.** Authentication happens at the WebSocket upgrade, before the peer exists. muxws
does not interpret credentials anywhere, including in per-stream `headers`. See
[Transports](/guide/transports).

## Symmetry: one `Peer` type per language

There is one `Peer` class in Python and one in TypeScript, and both ends of a connection use it. The
dialer's peer and the acceptor's peer differ in two things only: the parity of the stream ids they
allocate, and that only a dialer can have a reconnect helper.

Everything else is the same. The acceptor can call `open()`, `notify()` and `request()` at any time,
and the dialer receives those streams through its own `on_stream` handler. Server push is a client
request with the roles swapped: the same correlation (a stream id) and the same cancellation
(`stream.cancel()` from either end).

```python
# fragment
# On the acceptor, pushing to a client - the same three calls a client makes.
await peer.notify({"event": "deploy-finished"})
answer = await peer.request({"question": "are you still there?"}, timeout=5.0)  # 5.0 seconds
async for chunk in peer.open({"stream": "logs"}):
    print(chunk)
```

```typescript
// fragment
// On the dialer, receiving those pushes - the same handler an acceptor registers.
peer.onStream(async (payload, stream) => {
  await stream.reply({ ok: true });
});
```

Neither side is required to send a frame first, so a peer may open a stream with its first frame, in
either direction. See [Connection lifecycle](/guide/connection-lifecycle).

## See also

[`Peer`](/api/peer) &middot; [`Stream`](/api/stream) &middot; [`connect`](/api/connect) &middot;
[`accept`](/api/accept) &middot; [Errors](/api/errors)
