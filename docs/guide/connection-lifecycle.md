# Connection lifecycle

A muxws connection is established when the WebSocket opens, uses `ping` / `pong` frames for liveness,
and ends with `goaway`, a drain window and a socket close. Neither end waits for a setup exchange
before sending frames.

## A connection is established when the socket is

A connection is **established** when the WebSocket is open and the `muxws.v1.<codec>` subprotocol has
been accepted.

muxws has no handshake phase after the WebSocket upgrade, no capability exchange, and no `settings`
frame type. Neither side is required to send a particular frame first. A peer may open a stream with
its first frame, and an acceptor may open a stream to a dialer before the dialer has sent any frame.

```python
# fragment
peer = await connect("ws://localhost:8000/ws")
# No setup to await. A request is valid immediately:
answer = await peer.request({"action": "list"})
```

The subprotocol is an **assertion, not a negotiation**. The dialer offers `muxws.v1.<codec>` as the
first entry of its subprotocol list; the acceptor either selects exactly that value or refuses the
upgrade with HTTP 400, which the dialer raises as `CodecMismatch`. The peer has no fallback encoding,
no list of acceptable alternatives, and no runtime codec branching. The `v1` in that name is the only
version identifier on the wire. See [Codecs](/guide/codecs) and [Interop](/guide/interop).

::: info The hello is not a handshake
A dialer using the reconnect helper may configure a `hello=` payload. The hello is an ordinary
stream, received by the acceptor's `on_stream` handler, and no field marks it on the wire. It is
optional and dialer-only: a peer without a hello sends none and is established when its socket opens.
When a hello **is** configured, `peer.is_open` / `peer.isOpen` stays false until the hello has been
acknowledged, so no application frame is sent before it. See [Reconnect](/guide/reconnect).
:::

## No limit is ever negotiated

No limit is sent on the wire. A peer does not advertise a capability list or a maximum, and does not
receive the remote's limits. muxws has three size and concurrency limits, each of a different kind:

| Number | Default | Whose is it | How the other end learns it |
|---|---|---|---|
| `MAX_FRAME_BYTES` | 65536 bytes | **The protocol.** A constant, not a setting. | Both ends have the same constant. |
| `max_payload_bytes` / `maxPayloadBytes` | 67108864 bytes (64 MiB) | **The receiver.** A local limit. | Only from the `PAYLOAD_TOO_LARGE` reset sent when it is exceeded. |
| `max_concurrent_streams` / `maxConcurrentStreams` | 100 streams | **The receiver.** A local limit. | Only from the `REFUSED` reset sent when it is exceeded. |

`MAX_FRAME_BYTES` is not configurable and not negotiated: a sender always fragments at it, and a
receiver accepts any frame up to it. See [Sizes & fragmentation](/guide/sizes-and-fragmentation) for
the effect of the frame size on interleaving.

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

Because the limits are not sent to the remote:

- **The sender does not check them.** `open()` does not raise when the receiver is at its concurrency
  limit; the sender does not know the limit and sends the `open` frame. The refusal arrives
  asynchronously as `StreamRefused` on the pending await. `REFUSED` means the handler did not run, so
  the work can be retried elsewhere.
- **A limit is raised on the receiving peer.** The sender cannot change it. For clients that upload
  100 MiB payloads, `max_payload_bytes` must be raised on the server.
- **The two ends may have different limits.** For example, a browser-facing peer may have a smaller
  limit than an internal one.

`max_concurrent_streams` counts only the streams the **remote** opened on this peer. Streams this
peer opened do not count against it; the limit bounds the work the remote can start on this peer.

## `ping` / `pong` and `peer.ping()`

Liveness uses muxws `ping` and `pong` frames, not WebSocket control frames. Browsers do not expose
WebSocket ping/pong to JavaScript, so a mechanism based on them would not work in a browser peer.

A `ping` carries a `nonce`. The receiver echoes it unchanged in a `pong` immediately, without
application involvement: the `on_stream` handler does not receive pings, and the application
implements nothing.

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

`ping()` fails in two ways:

- Called while the peer is between sockets, it raises `ConnectionLost` immediately.
- If no `pong` arrives within the deadline, it raises `ConnectionClosed`. The connection stays open:
  only the call fails.

Pings are matched by nonce, not by order. A `pong` that arrives after its ping's deadline is dropped;
it is not matched to a later ping.

A dialer using the reconnect helper also runs an automatic heartbeat. The heartbeat pings only an
**otherwise idle** socket, and detects a dead socket within
`ping_interval + ping_timeout` (default 20.0 seconds + 10.0 seconds) /
`pingIntervalMs + pingTimeoutMs` (default 20000 milliseconds + 10000 milliseconds). See
[Reconnect](/guide/reconnect).

## Going away: `goaway`, `last_stream` and the drain window

A peer sends `goaway` to stop accepting new streams while streams already in progress finish. It
carries three fields:

| Field | Meaning |
|---|---|
| `code` | a reset code - `NO_ERROR` for an orderly shutdown, `PROTOCOL_ERROR` when the connection is being ended because the remote broke the rules |
| `reason` | free text for a human reading a log |
| `last_stream` | the highest id **the other peer opened** that this peer has processed and will still complete |

`last_stream` is an id of the other peer's parity, not this peer's. It divides the other peer's
streams into two sets:

- Streams **at or below** `last_stream` were accepted and are completed if possible.
- Streams **above** it were not processed. On receiving the `goaway`, the peer resets those of its
  own streams locally with `REFUSED`, so their awaits raise `StreamRefused`, and sends no frame for
  them, because the remote has stopped reading them. `REFUSED` means the handler did not run, so those
  requests can be retried on a new connection.

After **sending** `goaway`, a peer opens no new streams (`open()` raises `ConnectionGoingAway`) and
answers each new incoming `open` with `reset(REFUSED)` without invoking the handler.

After **receiving** `goaway`, `open()` raises `ConnectionGoingAway` synchronously at the call site.
Existing streams continue, but no new stream can be opened on the connection. Dial a new connection
to open new streams.

```python
# fragment
try:
    stream = peer.open({"action": "list"})
except ConnectionGoingAway:
    peer = await connect("ws://localhost:8000/ws")
    stream = peer.open({"action": "list"})
```

### The drain window

`goaway` is followed by a **drain window**: the remaining streams have a fixed time to finish, after
which the socket is closed whether or not they have finished. The window is a deadline, so a remote
that keeps a stream open does not prevent the close.

Within the same window the peer sends every frame already queued, including every remaining fragment
of a large payload, before it closes the socket. A frame still queued at the deadline is not sent.

Streams still live when the deadline expires are handled as on socket death: each fails locally with
`ConnectionLost`, and no frame is sent for it.

### `peer.close()`

`close()` performs the whole sequence: send `goaway(NO_ERROR)`, drain, then close the socket.

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

`close()` also ends reconnection. On a dialer with a reconnect helper, it stops the helper **first**,
including when the peer is between sockets and the helper is waiting in its backoff, so the peer does
not dial again after `close()`.

### Running out of stream ids

Ids are allocated in increasing order, in steps of two (the dialer uses odd ids, the acceptor even
ids), and are never reused. When a peer allocates the last id at or below 2³¹−1, it starts the
shutdown described above (`goaway`, drain, close), and the stream that received that id gets the drain
window like any other stream. Further `open()` calls on that connection raise `ConnectionGoingAway`.
An application that keeps one connection open for a long time sees this as an ordinary reconnect.

## The end: `on_close`

Each socket death fires `on_close` once with a `CloseReason` that has four fields: `code`, `reason`,
`was_clean` / `wasClean`, and `will_retry` / `willRetry`. Before it fires, every live stream has been
failed with `ConnectionLost`, so the handler has no stream cleanup to do.

```python
# fragment
@peer.on_close
def closed(reason):
    print(reason.code, reason.reason, reason.was_clean, reason.will_retry)
```

`will_retry` is true only when a reconnect helper will dial again. It is false on a peer without a
reconnect helper, when the helper has stopped retrying, and after `close()`. A peer reports
`will_retry=False` at most once, so teardown code guarded by it in an `on_close` handler runs once.

## See also

[`connect`](/api/connect) &middot; [`accept`, `serve`, `select_subprotocol`](/api/accept) &middot;
[`Peer.ping`, `Peer.close`, `Peer.on_close`, `Peer.is_open`](/api/peer) &middot;
[`CloseReason`, `MAX_FRAME_BYTES`](/api/types) &middot;
[`ConnectionGoingAway`, `ConnectionClosed`, `ConnectionLost`, `StreamRefused`, `CodecMismatch`](/api/errors)
