# Reconnect

The reconnect helper is **dialer-only**. An acceptor cannot dial — it has no URL and no idea who its
remote was — and asking for one raises.

## The first thing it does not do

`connect()` **raises when the first attempt fails**, with the underlying error, whatever `reconnect=`
says. There is no option that changes this.

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

Reconnection applies to connections that were **established and then lost**. It deliberately does not
apply to establishing the first one, because a peer that retried its first dial forever would turn a
typo in the URL, an unreachable host or a codec mismatch into silence: the application would hold
something that looks alive, is not, and never will be. Nobody would ever see the error.

`connect()` also never hands back a peer that is retrying in the background. Either it returns an
established connection or it raises. A caller who genuinely wants the first dial retried writes that
loop itself, where it can decide what a permanent failure looks like:

```python
# fragment
while True:
    try:
        peer = await connect(url, reconnect=Reconnect())
        break
    except OSError:
        await asyncio.sleep(1.0)  # seconds
```

What `connect()` raises is the underlying failure, unwrapped — `CodecMismatch` for a refused
handshake, `StreamTimeout` for a hello that nobody acknowledged within `hello_timeout` (seconds,
float), the `StreamReset` itself for a hello the acceptor refused, and whatever the transport raised
for anything else.

## The backoff schedule

```
delay = min(initial_delay * factor ** attempts, max_delay)
delay = delay * (1 + uniform(-jitter, +jitter))
```

`attempts` is the count of failures so far, so the *first* retry after a loss waits `initial_delay`
(0.25 seconds by default) / `initialDelayMs` (250 milliseconds by default) and no more.

| Field | Python (`Reconnect`) | TypeScript (`Reconnect`) | Default |
|---|---|---|---|
| First delay | `initial_delay` — **seconds**, float | `initialDelayMs` — **milliseconds**, integer | 0.25 s / 250 ms |
| Growth | `factor` — dimensionless | `factor` — dimensionless | 2.0 |
| Ceiling | `max_delay` — **seconds**, float | `maxDelayMs` — **milliseconds**, integer | 30.0 s / 30000 ms |
| Jitter | `jitter` — fraction of the delay, 0..1 | `jitter` — fraction of the delay, 0..1 | 0.3 |
| Give up after | `max_attempts` — count, `None` for unlimited | `maxAttempts` — count, `Infinity` for unlimited | unlimited |

Which produces, before jitter: 0.25 s, 0.5 s, 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s forever.

**Jitter is applied to every computed delay, including the capped ones.** Without it, N peers whose
sockets died in the same instant retry in the same instant, and a server coming back up is knocked
over by its own reconnection storm rather than by load. Applying it only below the cap would leave the
steady state — which is where a long outage spends all of its time — perfectly synchronised.

Three configurations are rejected where they are written rather than where they first misbehave:
`initial_delay` at or below zero, `factor` below 1 (a schedule that *shrinks*, which reads as a flaky
server for a week), and `jitter` outside 0..1.

The schedule is a pure function of the attempt count, so it can be read without waiting for it:

```python
from muxws import Reconnect, should_retry, unjittered_delay

options = Reconnect(max_attempts=5)
[unjittered_delay(n, options) for n in range(5)]  # seconds: [0.25, 0.5, 1.0, 2.0, 4.0]
should_retry(5, options)  # False
```

## The counter resets only on an *established* connection

The helper's entire persistent state is one attempt counter. It is reset at exactly one point in the
code, and established there means **both** of:

1. the socket is open with the `muxws.v1.<codec>` subprotocol accepted, **and**
2. the hello has been acknowledged.

A dial that produced a socket and then failed its hello is a **failed attempt**: the socket is closed,
the loss is reported through `on_close`, the counter climbs, `on_reconnect` does *not* fire, and the
next delay is longer than the last.

**Why resetting on socket-open is the bug.** The failure this guards against is a server whose socket
listener is healthy while its backend is down — a load balancer in front of a service that cannot
serve, which is the ordinary shape of an outage rather than an exotic one. Such a server accepts every
socket instantly and then fails, or never answers, the hello. If the counter reset when `dial()`
returned, every attempt would look like a success followed by an immediate loss, `attempts` would be 0
at the top of every cycle, and the delay would be `initial_delay` forever. Exponential backoff would
have silently flattened into a fixed-interval hammer at 250 ms — against precisely the server least
able to survive one. Every test would still be green, because the connection *is* being re-established
each time; the only visible symptom is the load on the far end.

Because the counter is reset in one place and one place only, a second reset call site is a real
hazard rather than harmless duplication: whichever of the two a later edit made load-bearing, the
other would keep the suite green while the schedule quietly stopped growing.

## The heartbeat

muxws sends its own `ping` frame rather than a WebSocket control frame, because browsers do not expose
control frames to JavaScript at all — a liveness mechanism built on them cannot work on half the peers
that exist.

| | Python | TypeScript | Default |
|---|---|---|---|
| Ping an idle socket every | `ping_interval` — **seconds**, float | `pingIntervalMs` — **milliseconds**, integer | 20.0 s / 20000 ms |
| Declare dead if no pong within | `ping_timeout` — **seconds**, float | `pingTimeoutMs` — **milliseconds**, integer | 10.0 s / 10000 ms |

Both are arguments to `connect()`. Setting `ping_interval` to `0.0` seconds — or `pingIntervalMs` to
`0` milliseconds — or below disables the heartbeat entirely, which is a legitimate choice for a
deployment with its own liveness signal.

**Idle means idle.** The timer is read from the time the last frame crossed the socket in *either*
direction, not from a fixed schedule. A busy connection never pays for a ping, because its own traffic
has already proved what the ping would ask.

**Detection is bounded by `ping_interval + ping_timeout`** — 30 seconds with the defaults. The ping
goes out at most one `ping_interval` (seconds) / `pingIntervalMs` (milliseconds) after the last frame,
and the wait for the pong gives up after `ping_timeout` (seconds) / `pingTimeoutMs` (milliseconds).
Nothing here waits on anything resembling a TCP-level keepalive, which is the entire reason the
heartbeat exists.

When the pong does not come back, the socket is declared dead **and closed locally**. The close is
what matters: without it the read loop stays parked on a `receive()` that will never return, nothing
ever learns the connection ended, and there is no backoff at all. Closing it locally makes a dead
socket take *the same* path as a clean close — one code path, not two.

The local close goes out with WebSocket code **1000**, never 1006. What the peer *reports* for the
death is 1006; what it *sends* to end the socket is not, because 1006 means "dropped without a close
frame" and a transport that validates it rejects the close outright and leaves the socket open.

`peer.ping()` is the same mechanism on demand, and it has its own separate deadline —
`timeout=5.0` seconds in Python, `timeoutMs = 5000` milliseconds in TypeScript. It returns the
round-trip time: **seconds** as a float in Python, **milliseconds** as a number in TypeScript.

## The hello

The hello is how a reconnected socket gets an identity the acceptor has already accepted.

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

Five facts, and each one is load-bearing:

**Captured once, at `connect()`.** By value, at the moment of the call. It is never re-read from the
object you passed, never recomputed, and cannot be supplied as a callback. Mutating your dict
afterwards changes nothing.

**Replayed verbatim on every connection this peer ever makes**, byte-identical each time. The
hundredth reconnection sends what the first one sent.

**Sent as an ordinary `open(payload, headers=..., end=True)`.** Nothing marks it on the wire. There is
no flag, no reserved field and no distinguishing shape — an observer of the wire cannot tell a hello
from any other unary open.

**Delivered to the acceptor's ordinary `on_stream` handler.** The acceptor does not register anything
special for it and cannot opt out of it; it arrives as a stream like any other, and it is the
application's job to recognise its own payload.

**Acknowledged simply by that handler returning.** A handler that returns without ending its stream
ends it implicitly, and that implicit end *is* the acknowledgement. No application code is required to
send one. An acceptor that wants to refuse the identity resets the stream instead, and the dialer
treats that as a failed attempt and backs off.

::: warning The hello cannot carry an answer back
The reconnect helper owns the hello stream and waits only for it to close, so **a payload the
acceptor replies with is discarded**. Nobody is reading it and nothing reports that.

This surprises people, because using the hello as a subscription — tag the peer, register it, and
answer with the list of things it is now subscribed to — is the obvious design, and the reply just
vanishes. Send that list on a stream the acceptor opens itself instead. That is better anyway: it is
the same mechanism the acceptor uses for every later update, so the client has one code path rather
than two, and the initial state arrives by the route the updates will arrive by.
:::

Until the hello is acknowledged, `peer.is_open` / `peer.isOpen` is **false** and `peer.open()` refuses
with `ConnectionLost`. That window is not an oversight: it is what guarantees no application frame can
precede the hello on a new socket and reach an acceptor that has not yet been told who is speaking.

::: danger A credential MUST NOT go in the hello
Authentication is a handshake concern. It belongs at the WebSocket upgrade — a cookie, an
`Authorization` header, a signed URL, or an extra subprotocol entry — where it is checked *before*
`accept()` and before any muxws frame exists. See [Transports](/guide/transports#where-authentication-belongs).

A credential in the hello is checked after the socket is already open and after the acceptor has
already spent resources on it, is replayed verbatim on every reconnection for the life of the process
(so it can never be rotated), and travels as an ordinary payload through whatever frame logging and
observers the connection carries. The hello answers "who is this, again"; it does not answer "should
this connection exist".
:::

## What a reconnect restores

`on_reconnect(attempt, peer)` fires once per re-established connection, after the socket is up with
the subprotocol accepted **and** after the hello is acknowledged. It guarantees exactly two things:

- a live socket, and
- an identity the acceptor has already accepted on it.

**And nothing else.** Specifically:

- **No stream survives.** Every stream live at the moment of the loss was already failed with
  `ConnectionLost`. Nothing is resumed, nothing is replayed, no in-flight frame is re-sent.
- **`Stream` objects do not survive** even though `Peer` does. Any stream you were holding across a
  reconnect is closed; using it raises.
- **The id space starts empty.** The new socket allocates from 1 (dialer) or 2 (acceptor) again, and
  both high-water marks reset.
- **Nothing is buffered between sockets.** The writer's queues are discarded on socket death. A `send`
  attempted while the peer is between sockets raises rather than waiting for the next connection.
- **`peer.id` changes.** It advances to a new value, so a log shows the reconnect as a new `conn=`
  rather than as one continuous connection.

What the application has to do is rebuild: reopen the streams it cares about, from `on_reconnect`.

```python
# fragment
@peer.on_reconnect
def resubscribe(attempt: int, peer) -> None:
    print(f"reconnection {attempt}; reopening subscriptions")
```

::: tip `tags` and reconnects
On the **acceptor** side a reconnect produces an entirely new `Peer` object with a fresh, empty
`tags` — the acceptor never learns that the socket it just accepted belongs to the client that was
here a moment ago, so there is nowhere for the old tags to come from. Anything the acceptor indexed
must be written again, which is what the hello handler is for. See [Registry](/guide/registry).

On the **dialer** side the `Peer` object itself survives, and the library neither reads nor clears its
`tags`; whatever a dialer wrote there is still there.
:::

## Client identity: a recommendation, not a feature

muxws mints nothing and stores nothing. It has no notion of a tab id, a session id or a client id, and
`hello` is an opaque payload it never inspects. If your application wants a stable identity across
reconnections, it has to supply one, and where it keeps that value matters:

| Storage | Verdict |
|---|---|
| `sessionStorage` | **Recommended.** One value per tab, surviving a reload of that tab and nothing more — which is exactly the lifetime of the thing being identified. |
| `localStorage` | Wrong. Shared across every tab of the origin, so three tabs of your app claim to be the same client and the acceptor cannot tell their subscriptions apart. |
| Module scope | Wrong. It dies on reload, so a page refresh — the single most common way a connection is lost — produces a client the server has never seen. |

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

This is deliberately not implemented. A library that minted an identity would have to choose its
lifetime for you, and the right lifetime is a property of the application.

## Giving up

With `max_attempts` set, exhausting it fires `on_close` **once** with `will_retry` false and never
dials again. That single close is the withdrawal of a promise: the last socket loss reported
`will_retry` true because a retry genuinely was coming, and an application told a reconnection was on
its way and never told otherwise would wait forever for one nobody is attempting.

At most one `will_retry=False` close ever reaches an application for one peer. A teardown handler that
ran twice is what saying it twice would cost.

`peer.close()` is the other way the helper stops: it sets the intent before anything else, so a close
called while the helper is asleep in its backoff stops the next dial rather than racing it.

## See also

- [`api/reconnect`](/api/reconnect) — `Reconnect` / `ReconnectOptions` and all five fields
- [`api/connect`](/api/connect) — `hello`, `hello_headers`, `reconnect`, and the three durations
  `hello_timeout`, `ping_interval` and `ping_timeout` (seconds as floats; `helloTimeoutMs`,
  `pingIntervalMs` and `pingTimeoutMs` as milliseconds in TypeScript)
- [`api/peer`](/api/peer) — `on_reconnect`, `on_close`, `ping`, `close`, `is_open`, `id`
- [`api/errors`](/api/errors) — `ConnectionLost`, `StreamTimeout`, `CodecMismatch`
- [`api/types`](/api/types) — `CloseReason` and its four fields
