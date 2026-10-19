# Reconnect

The reconnect helper is **dialer-only**. Every peer returned by `connect()` has one; the `reconnect=`
argument configures it, and without that argument the defaults below apply. `accept()` has no
`reconnect` parameter: an acceptor has no URL to dial and no address of its remote.

## The first dial is not retried

`connect()` **raises when the first attempt fails**, with the underlying error, regardless of
`reconnect=`. No option changes this.

```python
# fragment
from muxws import Reconnect, connect

# Unlimited retries configured, and this still raises.
peer = await connect("ws://localhost:9/typo", reconnect=Reconnect(max_attempts=None))
```

```ts
// fragment
// The same in TypeScript: `maxAttempts` defaults to Infinity, and this still throws.
const peer = await connect('ws://localhost:9/typo', { reconnect: new Reconnect() });
```

Reconnection applies only to connections that were **established and then lost**, not to the first
dial. If the first dial were retried indefinitely, a typo in the URL, an unreachable host or a codec
mismatch would produce no error, and the application would hold a peer that never connects.

`connect()` never returns a peer that is retrying in the background: it returns an established
connection or raises. To retry the first dial, write the loop in the application, which decides which
failures are permanent:

```python
# fragment
while True:
    try:
        peer = await connect(url, reconnect=Reconnect())
        break
    except OSError:
        await asyncio.sleep(1.0)  # seconds
```

`connect()` raises the underlying failure without wrapping it: `CodecMismatch` for a refused
handshake, `StreamTimeout` for a hello not acknowledged within `hello_timeout` (seconds, float), the
`StreamReset` for a hello the acceptor reset, and the transport's exception for any other failure.

## The backoff schedule

```text
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

`attempts` is the number of failures so far, so the *first* retry after a loss waits `initial_delay`
(0.25 seconds by default) / `initialDelayMs` (250 milliseconds by default), before jitter.

| Field | Python (`Reconnect`) | TypeScript (`Reconnect`) | Default |
|---|---|---|---|
| First delay | `initial_delay` — **seconds**, float | `initialDelayMs` — **milliseconds**, integer | 0.25 s / 250 ms |
| Growth | `factor` — dimensionless | `factor` — dimensionless | 2.0 |
| Ceiling | `max_delay` — **seconds**, float | `maxDelayMs` — **milliseconds**, integer | 30.0 s / 30000 ms |
| Jitter | `jitter` — fraction of the delay, 0..1 | `jitter` — fraction of the delay, 0..1 | 0.3 |
| Give up after | `max_attempts` — count, `None` for unlimited | `maxAttempts` — count, `Infinity` for unlimited | unlimited |

With the defaults, the delays before jitter are 0.25 s, 0.5 s, 1 s, 2 s, 4 s, 8 s, 16 s, and then
30 s for every further attempt.

**Jitter is applied to every computed delay, including capped ones.** Without jitter, N peers whose
sockets closed at the same time retry at the same time, and a restarting server receives all
reconnections at once. If jitter were applied only below the cap, retries during a long outage, which
run at the cap, would stay synchronised.

The constructor raises `ValueError` (Python) or `Error` (TypeScript) for four configurations:
`initial_delay` not greater than zero, `factor` not at least 1 (a decreasing schedule), a NaN
`max_delay`, and `jitter` outside 0..1. A NaN `initial_delay`, `factor` or `jitter` is refused by
the same checks.

The schedule is a pure function of the attempt count, so it can be computed without waiting:

```python
from muxws import Reconnect, should_retry, unjittered_delay

options = Reconnect(max_attempts=5)
[unjittered_delay(n, options) for n in range(5)]  # seconds: [0.25, 0.5, 1.0, 2.0, 4.0]
should_retry(5, options)  # False
```

## The counter resets only on an *established* connection

The helper's only persistent state is one attempt counter. It is reset only when the connection is
established, which requires **both**:

1. the socket is open with the `muxws.v1.<codec>` subprotocol accepted, **and**
2. the hello has been acknowledged.

A dial that opens a socket and then fails its hello is a **failed attempt**: the socket is closed, the
loss is reported through `on_close`, the counter is incremented, `on_reconnect` does *not* fire, and
the next delay is longer than the previous one.

**The counter is not reset when the socket opens.** A server whose socket listener works while its
backend is down, for example a load balancer in front of a failing service, accepts every socket
immediately and then fails or does not answer the hello. If the counter were reset when `dial()`
returned, each attempt would count as a success followed by a loss, `attempts` would be 0 at the start
of every cycle, and every delay would be `initial_delay`: retries at a fixed 250 ms interval against an
overloaded server.

## The heartbeat

muxws sends its own `ping` frame rather than a WebSocket control frame, because browsers do not expose
WebSocket control frames to JavaScript.

| | Python | TypeScript | Default |
|---|---|---|---|
| Ping an idle socket every | `ping_interval` — **seconds**, float | `pingIntervalMs` — **milliseconds**, integer | 20.0 s / 20000 ms |
| Declare dead if no pong within | `ping_timeout` — **seconds**, float | `pingTimeoutMs` — **milliseconds**, integer | 10.0 s / 10000 ms |

Both are arguments to `connect()`. Setting `ping_interval` to `0.0` seconds or below, or
`pingIntervalMs` to `0` milliseconds or below, disables the heartbeat; this is appropriate for a
deployment with its own liveness signal.

**Only an idle socket is pinged.** The interval is measured from the last frame sent or received on
the socket, not on a fixed schedule. A connection with traffic in either direction within the interval
is not pinged.

**A dead socket is detected within `ping_interval + ping_timeout`**, 30 seconds with the defaults. The
ping is sent at most one `ping_interval` (seconds) / `pingIntervalMs` (milliseconds) after the last
frame, and the wait for the pong ends after `ping_timeout` (seconds) / `pingTimeoutMs` (milliseconds).
Detection does not depend on TCP keepalive.

When the pong does not arrive, the socket is marked dead **and closed locally**. Without the local
close, the read loop would remain blocked in a `receive()` that never returns, the connection end
would not be detected, and the helper would not reconnect. After the local close, a dead socket follows
the same code path as a clean close.

The local close is sent with WebSocket code **1000**, not 1006. The peer *reports* 1006 for the loss,
but does not *send* it: 1006 means "closed without a close frame", and a transport that validates the
code rejects such a close and leaves the socket open.

`peer.ping()` sends the same frame on demand, with its own deadline:
`timeout=5.0` seconds in Python, `timeoutMs = 5000` milliseconds in TypeScript. It returns the
round-trip time: **seconds** as a float in Python, **milliseconds** as a number in TypeScript.

## The hello

The hello is a stream the dialer opens first on every connection, so that the acceptor can identify
the dialer on each new socket.

```python
# fragment
peer = await connect(
    url,
    hello={"tab": tab_id, "topics": ["orders"]},
    hello_headers={"client": "dashboard"},
    hello_timeout=10.0,  # seconds
    reconnect=Reconnect(),
)
```

**Captured once, at `connect()`**, by value. It is not re-read from the passed object, not
recomputed, and cannot be supplied as a callback. Modifying the dict after the call has no effect.

**Sent unchanged on every connection the peer makes**, with identical bytes each time.

**Sent as an ordinary `open(payload, headers=..., end=True)`.** No field marks it on the wire.

**Delivered to the acceptor's `on_stream` handler.** The acceptor registers no separate handler for it
and cannot disable it; it arrives as an ordinary stream, and the application recognises it by its
payload.

**Acknowledged when that handler returns.** A handler that returns without ending its stream ends it
implicitly, and that end is the acknowledgement; no application code sends it. To reject the identity,
the acceptor resets the stream; the dialer counts that as a failed attempt and backs off.

::: warning The hello cannot carry an answer back
The reconnect helper owns the hello stream and waits only for it to close, so **a payload the
acceptor replies with is discarded** without any error or log entry.

If the acceptor uses the hello as a subscription (tag the peer, register it, and reply with the list
of subscribed items), the reply is lost. Send that list on a stream the acceptor opens instead. That
is the same mechanism the acceptor uses for later updates, so the client receives the initial state
and the updates through one code path.
:::

Until the hello is acknowledged, `peer.is_open` / `peer.isOpen` is **false** and `peer.open()` raises
`ConnectionLost`. This ensures that no application frame precedes the hello on a new socket.

::: danger A credential MUST NOT go in the hello
Authentication belongs to the WebSocket upgrade, before `accept()`. The hello is checked only after
the socket is open, is sent unchanged on every reconnection, and reaches every frame observer as an
ordinary payload. See [Where authentication belongs](/guide/transports#where-authentication-belongs).
:::

## What a reconnect restores

`on_reconnect(attempt, peer)` fires once per re-established connection, after the socket is open with
the subprotocol accepted **and** after the hello is acknowledged. Nothing else is restored:

- **No stream is restored.** Every stream live at the time of the loss was failed with
  `ConnectionLost`. No stream is resumed and no in-flight frame is re-sent.
- **`Stream` objects are closed**, although the `Peer` object remains. Any stream held across a
  reconnect is closed; using it raises.
- **The id space starts empty.** The new socket allocates from 1 (dialer) or 2 (acceptor) again, and
  both high-water marks reset.
- **Nothing is buffered between sockets.** The writer's queues are discarded on socket death. A `send`
  attempted while the peer is between sockets raises; it does not wait for the next connection.
- **`peer.id` changes** to a new value, so logs show the reconnect as a new `conn=`.

The application reopens the streams it needs from `on_reconnect`.

```python
# fragment
@peer.on_reconnect
def resubscribe(attempt: int, peer) -> None:
    print(f"reconnection {attempt}; reopening subscriptions")
```

::: tip `tags` and reconnects
On the acceptor, a reconnect produces a new `Peer` with empty `tags`. See
[Tags end with their connection](/guide/registry#peer-tags).
:::

## Client identity

muxws does not generate or store a client identity. It has no concept of a tab id, a session id or a
client id, and does not inspect the `hello` payload. An application that needs a stable identity
across reconnections supplies one, and the storage location determines its lifetime:

| Storage | Suitability |
|---|---|
| `sessionStorage` | **Recommended.** One value per tab, kept across reloads of that tab and discarded when the tab closes. |
| `localStorage` | Not suitable. Shared across all tabs of the origin, so several tabs present the same identity and the acceptor cannot distinguish their subscriptions. |
| Module scope | Not suitable. Lost on reload, so a page refresh, which also closes the connection, produces a new identity. |

```ts
// fragment
// The application's job, not the library's.
let tab = sessionStorage.getItem('tab');
if (tab === null) {
  tab = crypto.randomUUID();
  sessionStorage.setItem('tab', tab);
}
const peer = await connect(url, { hello: { tab }, reconnect: new Reconnect() });
```

## Giving up

When `max_attempts` is set and the attempts are exhausted, the helper fires `on_close` **once** with
`will_retry` false and does not dial again. The previous socket loss reported `will_retry` true; this
close informs the application that no reconnection will follow.

At most one `will_retry=False` close is fired per peer, so a teardown handler guarded by it runs once.

`peer.close()` also stops the helper. It marks the peer as closing before any other step, so a close
called while the helper waits in its backoff prevents the next dial. A close called while a dial or a
hello is in progress ends that attempt: a socket the dial returns afterwards is closed without being
used, a hello acknowledged afterwards does not establish the connection, `is_open` / `isOpen` stays
false, and `on_reconnect` / `onReconnect` does not fire.

## See also

- [`api/reconnect`](/api/reconnect) — `Reconnect` / `ReconnectOptions` and all five fields
- [`api/connect`](/api/connect) — `hello`, `hello_headers`, `reconnect`, and the three durations
  `hello_timeout`, `ping_interval` and `ping_timeout` (seconds as floats; `helloTimeoutMs`,
  `pingIntervalMs` and `pingTimeoutMs` as milliseconds in TypeScript)
- [`api/peer`](/api/peer) — `on_reconnect`, `on_close`, `ping`, `close`, `is_open`, `id`
- [`api/errors`](/api/errors) — `ConnectionLost`, `StreamTimeout`, `CodecMismatch`
- [`api/types`](/api/types) — `CloseReason` and its four fields
