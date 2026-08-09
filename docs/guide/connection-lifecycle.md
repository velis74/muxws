# Connection lifecycle

A muxws connection has a beginning that costs nothing, a middle with one liveness mechanism, and a
defined end. There is nothing in between them that you have to wait for.

## A connection is established when the socket is

A connection is **established** the moment the WebSocket is open and the `muxws.v1.<codec>`
subprotocol has been accepted. That is the whole of it.

There is **no** post-socket handshake phase. There is **no** capability exchange. There is **no**
`settings` frame - the frame type does not exist. There is no frame that either side must send
before any other, in either direction. A peer may open a stream on its very first frame, and an
acceptor may push one to a dialer before the dialer has sent anything at all.

```python
# fragment
peer = await connect("ws://localhost:8000/ws")
# Nothing to await first. This is legal on the next line:
answer = await peer.request({"action": "list"})
```

The subprotocol is an **assertion, not a negotiation**. The dialer offers `muxws.v1.<codec>` as the
first entry of its subprotocol list; the acceptor either selects exactly that value or refuses the
upgrade with a 400, which surfaces as `CodecMismatch`. There is no fallback encoding, no list of
acceptable alternatives, and no runtime codec branching anywhere in the peer. The `v1` in that name
is also the only version on the wire. See [Codecs](/guide/codecs) and [Interop](/guide/interop).

::: info The hello is not a handshake
A dialer using the reconnect helper may configure a `hello=` payload. It is an ordinary stream, one
the acceptor's normal `on_stream` handler receives, and nothing marks it on the wire. It is
dialer-only and opt-in: a peer given no hello sends none and is established the instant its socket
is. When one **is** configured, `peer.is_open` / `peer.isOpen` stays false until that hello has been
acknowledged, so that no application frame can precede it. See [Reconnect](/guide/reconnect).
:::

## No limit is ever negotiated

No limit appears on the wire in any form. There is no capability list, no advertised maximum, and
nothing to parse from the remote about what it will accept. There are exactly three numbers, and
they are three different kinds of thing.

| Number | Default | Whose is it | How the other end learns it |
|---|---|---|---|
| `MAX_FRAME_BYTES` | 65536 bytes | **The protocol's.** A constant, not a setting. | It does not have to - both ends already know it. |
| `max_payload_bytes` / `maxPayloadBytes` | 67108864 bytes (64 MiB) | **The receiver's own defence.** | Only from the `PAYLOAD_TOO_LARGE` reset it provokes. |
| `max_concurrent_streams` / `maxConcurrentStreams` | 100 streams | **The receiver's own defence.** | Only from the `REFUSED` reset it provokes. |

`MAX_FRAME_BYTES` is never read from configuration and never negotiated: a sender always fragments
at it, and a receiver accepts anything up to it. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation) for why a bigger frame would be worse rather
than better.

The other two are set per peer, on `connect()` and on `accept()`, and they constrain what the
**remote** may do to this peer:

```python
# fragment
peer = await connect(
    "ws://localhost:8000/ws",
    max_payload_bytes=8 * 1024 * 1024,
    max_concurrent_streams=20,
)
```

```typescript
// fragment
const peer = await connect('ws://localhost:8000/ws', {
  maxPayloadBytes: 8 * 1024 * 1024,
  maxConcurrentStreams: 20,
});
```

Three consequences that follow directly from "not announced":

- **The sender never checks them.** `open()` does not raise when the receiver is already at its
  concurrency limit - it cannot know, and it puts the `open` frame on the wire like any other. The
  refusal comes back asynchronously as `StreamRefused` on the pending await, and `REFUSED` means the
  handler never ran, so the work is safe to retry elsewhere.
- **Raising a limit is done on the receiving peer.** The sender has no say. If your clients are
  uploading 100 MiB payloads, `max_payload_bytes` has to go up on the server.
- **The two ends may disagree, permanently and legitimately.** A browser-facing peer holding a
  smaller limit than an internal one is a normal deployment, not a misconfiguration.

`max_concurrent_streams` counts only the streams the **remote** opened on this peer. Streams this
peer opened itself never count against it - the limit exists to bound work the other side can
impose.

## `ping` / `pong` and `peer.ping()`

Liveness uses muxws `ping` and `pong` frames, not WebSocket control frames. That is not stylistic:
browsers do not expose WebSocket ping/pong to JavaScript at all, so a liveness mechanism built on
them cannot work on half the peers that exist.

A `ping` carries a `nonce`. The receiver echoes it verbatim in a `pong`, promptly, with **no**
application involvement - your `on_stream` handler never sees a ping and there is nothing to
implement.

`peer.ping()` measures the round trip:

```python
# fragment
rtt = await peer.ping()                 # seconds, as a float; default deadline 5.0 seconds
rtt = await peer.ping(timeout=2.0)      # 2.0 seconds
```

```typescript
// fragment
const rtt = await peer.ping();          // milliseconds; default deadline 5000 milliseconds
const fast = await peer.ping(2000);     // 2000 milliseconds
```

Two failure modes, and they are different:

- Calling it while the peer is between sockets raises `ConnectionLost` immediately.
- No `pong` within the deadline raises `ConnectionClosed`. A single lost `pong` does **not** kill the
  connection: the call fails and the socket is left alone.

Pings are matched by nonce rather than by order, so a `pong` that arrives after its ping's deadline
has already expired is dropped rather than credited to the next ping and reported as a round-trip
time that never happened.

A dialer using the reconnect helper also gets an automatic heartbeat, which pings only an
**otherwise idle** socket and bounds detection of a dead one by
`ping_interval + ping_timeout` (default 20.0 seconds + 10.0 seconds) /
`pingIntervalMs + pingTimeoutMs` (default 20000 milliseconds + 10000 milliseconds). See
[Reconnect](/guide/reconnect).

## Going away: `goaway`, `last_stream` and the drain window

`goaway` is how a peer says "I am stopping" without cutting off work already in progress. It carries
three fields:

| Field | Meaning |
|---|---|
| `code` | a reset code - `NO_ERROR` for an orderly shutdown, `PROTOCOL_ERROR` when the connection is being ended because the remote broke the rules |
| `reason` | free text for a human reading a log |
| `last_stream` | the highest id **the other peer opened** that this peer has processed and will still complete |

`last_stream` is the important one, and it is the other peer's parity, not this peer's. It draws a
line through the other end's work:

- Streams **at or below** `last_stream` were accepted and will be finished if they can be.
- Streams **above** it were never processed. On receiving the `goaway`, the peer resets those of its
  own streams locally with `REFUSED` - so their awaits raise `StreamRefused` - and sends nothing for
  them, because the remote has already stopped reading. `REFUSED` promises nothing ran, so those
  requests are safe to retry on a new connection.

After **sending** `goaway`, a peer opens no new streams of its own - `open()` raises
`ConnectionGoingAway` - and answers any new incoming `open` with `reset(REFUSED)` without invoking
the handler.

After **receiving** `goaway`, `open()` raises `ConnectionGoingAway` synchronously at the call site.
The connection is not dead; it just cannot start anything new. Dial again to get one that can.

```python
# fragment
try:
    stream = peer.open({"action": "list"})
except ConnectionGoingAway:
    peer = await connect("ws://localhost:8000/ws")
    stream = peer.open({"action": "list"})
```

### The drain window

`goaway` is followed by a **drain window**: surviving streams are given a bounded amount of time to
finish, and then the socket is closed regardless of whether they did. It is a deadline, not a poll
loop - a peer that waited for quiet would never close against a remote that keeps one stream open.

Whatever is still live when the deadline expires takes the socket-death path: it fails locally with
`ConnectionLost` and nothing is sent for it, because the socket is about to be gone.

### `peer.close()`

`close()` is the whole sequence in one call: send `goaway(NO_ERROR)`, drain, then close the socket.

```python
# fragment
await peer.close()                                    # drain defaults to 10.0 seconds
await peer.close(reason="deploying", drain=2.0)       # 2.0 seconds
```

```typescript
// fragment
await peer.close();                                              // drainMs defaults to 10000 milliseconds
await peer.close({ reason: 'deploying', drainMs: 2000 });        // 2000 milliseconds
```

`close()` is also the one call that means "and do not come back". On a dialer with a reconnect
helper it stops the helper **before** anything else, including when the peer is between sockets and
the helper is asleep in its backoff - so a deliberate close never dials again.

### Running out of stream ids

Ids are allocated monotonically, two at a time - the dialer takes the odd ones, the acceptor the
even - and are never reused. A peer whose next id would exceed 2³¹−1 cannot open another stream on
that connection. That is not an error: the peer starts exactly the shutdown above - `goaway`, drain,
close - and the stream that exhausted the space gets its drain window like any other. Applications
that hold one connection open for months should expect an ordinary reconnect, not an exception.

## The end: `on_close`

Every socket death on a peer you are holding fires `on_close` once with a `CloseReason` carrying
four fields: `code`,
`reason`, `was_clean` / `wasClean`, and `will_retry` / `willRetry`. Every live stream has already
been failed with `ConnectionLost` by the time it fires, so a handler can assume there is nothing
left to tidy up on the stream side.

```python
# fragment
@peer.on_close
def closed(reason):
    print(reason.code, reason.reason, reason.was_clean, reason.will_retry)
```

`will_retry` is false only when the reconnect helper has given up or `close()` was deliberate, and a
peer reports `will_retry=False` at most once, ever - so teardown in an `on_close` handler runs once
rather than twice.

## See also

[`connect`](/api/connect) &middot; [`accept`, `serve`, `select_subprotocol`](/api/accept) &middot;
[`Peer.ping`, `Peer.close`, `Peer.on_close`, `Peer.is_open`](/api/peer) &middot;
[`CloseReason`, `MAX_FRAME_BYTES`](/api/types) &middot;
[`ConnectionGoingAway`, `ConnectionClosed`, `ConnectionLost`, `StreamRefused`, `CodecMismatch`](/api/errors)
