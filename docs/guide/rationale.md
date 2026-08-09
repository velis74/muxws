# Rationale

muxws exists because a WebSocket gives you exactly one thing: an ordered sequence of messages. Every
application that needs more than one conversation on that sequence has to build the same layer, and
that layer is larger than it looks the first time you start it.

## One socket, many streams

A **stream** in muxws is an independently addressed, independently cancellable, bidirectional
exchange. Any number of them share one socket. Each carries its own payloads, its own optional
headers, its own end, and its own failure.

There are two other ways to get concurrency, and both cost more than they appear to.

### Why not N sockets

Opening a socket per conversation moves the problem into the transport, where you have less control
over it, not more.

- Each socket is a TCP connection and a TLS handshake. A browser also caps how many it will hold
  open to one host, and that cap is not yours to raise.
- Order between sockets is undefined. Two messages that must be applied in sequence cannot be, once
  they are on different connections.
- Cancelling one conversation means closing its socket. If you multiplexed even two things onto it
  to save connections, you have just thrown away the other one.
- Reconnect is per socket. Backoff, jitter, and re-authentication get written N times and drift.
- On the server, each socket is a separate authenticated session to accept, track and tear down.

With muxws, cancelling a stream sends one frame and closes one exchange. The socket, and every other
stream on it, is untouched.

### Why not your own envelope

The other route is one socket and a message envelope of your own: a `{ id, type, body }` wrapper and
a map of pending callbacks. This works, and then it grows.

You add a correlation id, because replies arrive out of order. Then a "this is the last one" flag,
because some replies are a sequence rather than a value. Then a distinct error shape, because an
error is not a reply. Then cancellation, because the user navigated away and the export is still
running. Then you notice that a 4 MB message blocks every other message on the socket - a WebSocket
has a single global message order, and a large message occupies it from first byte to last - so you
add fragmentation. Then you discover fragmentation alone does not help, because a naive sender emits
all the fragments of one payload before looking at anything else, so you add a round-robin between
the fragmenting messages.

That list is muxws. Every item on it is reachable from "I just need to tell replies apart", and none
of them is optional once you are far enough in.

What you get instead, already written and already tested against a shared cross-language corpus:

- **Stream ids you never allocate.** The dialer takes odd ids, the acceptor even ones, so both ends
  can open at any moment without colliding and without asking. No API anywhere takes an id.
- **`end` as a flag on a payload**, not a separate message. A one-payload reply is one frame.
- **Reset codes with a defined reaction.** "Not accepted, definitively not processed - retry
  elsewhere" is a different answer from "the handler raised", and the caller can act on the
  difference. See [Errors](/guide/errors).
- **Per-stream cancellation** that closes locally at once and tells the remote to stop working.
- **Automatic fragmentation with a round-robin writer**, so a 1 MB export does not stall a 200-byte
  progress update on another stream. See [Sizes & fragmentation](/guide/sizes-and-fragmentation).
- **A reconnect helper** with jittered backoff, an idle-only heartbeat, and an opening payload
  replayed verbatim on every connection. See [Reconnect](/guide/reconnect).

## The three things muxws is not

**Not a router.** There is one incoming-stream handler per peer, registered with `on_stream`, and it
is invoked as `(payload, stream)` for every stream the remote opens. muxws does not look inside the
payload to decide anything: no path matching, no method dispatch, no handler table, no per-action
registration. If you want dispatch, write it in your handler, where you can see your own types.

```python
# fragment
@peer.on_stream
async def handle(payload, stream):
    # Dispatch is yours. muxws never reads this dict.
    if payload["action"] == "list":
        await stream.reply({"items": []})
```

**Not a serializer of domain objects.** The codec turns a frame into a WebSocket message and back;
what your payload *means* is not muxws's business. muxws defines no vocabulary inside `payload` - no
`kind`, no reserved key, no discriminator of any sort. A payload is whatever your codec can encode.
See [Codecs](/guide/codecs).

**Not an RPC framework.** There are no service definitions, no schemas, no generated stubs, and no
status codes: muxws never maps an exception to a numeric status of any kind. A handler that raises
produces a reset carrying a reason and an optional structured error object, and the caller sees a
`RemoteError` - not a 500.

It is also not an authentication mechanism. Authentication belongs at the WebSocket upgrade, before
the peer exists; muxws interprets no credential anywhere, including in per-stream `headers`. See
[Transports](/guide/transports).

## Symmetry: one `Peer` type per language

There is one `Peer` class in Python and one in TypeScript, and both ends of a connection use it. The
dialer's peer and the acceptor's peer differ in exactly two things: which parity of stream id they
allocate, and that only a dialer can have a reconnect helper, because only a dialer can dial.

Everything else is the same object. The acceptor can call `open()`, `notify()` and `request()`
whenever it likes, and the dialer receives those streams through *its* `on_stream` handler. Server
push is not a second mechanism bolted on: it is a client request with the roles swapped, with the
same correlation (a stream id) and the same cancellation story (`stream.cancel()` from either end).

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

A consequence worth stating plainly: a peer may open a stream on its very first frame, in either
direction, because there is nothing either side must send first. See
[Connection lifecycle](/guide/connection-lifecycle).

## See also

[`Peer`](/api/peer) &middot; [`Stream`](/api/stream) &middot; [`connect`](/api/connect) &middot;
[`accept`](/api/accept) &middot; [Errors](/api/errors)
