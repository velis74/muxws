# Observability

Two seams, and a warning.

- The **`muxws.frames` logger** — one line per frame, off by default, for when you want to watch a
  connection without changing any code.
- **`peer.on_frame(handler)`** — the same event delivered to you as data, for metrics.

And the warning, which is at the bottom and is the most important thing on this page: muxws never logs
payload contents, but `error_serializer` does put exception text on the wire.

## The `muxws.frames` logger

One line per frame, in both directions, at `DEBUG`. Nothing is emitted at any other level, and the
call returns immediately when `DEBUG` is not enabled — so an unconfigured application pays a level
check per frame and nothing more.

```python
import logging

logging.basicConfig(level=logging.DEBUG, format="%(message)s")
logging.getLogger("muxws.frames").setLevel(logging.DEBUG)
```

TypeScript has no logging module to depend on — the browser entry point has zero runtime dependencies
— so it ships a level-filtered shim over `console`. It starts at `'warn'`, which is what an
unconfigured Python logger effectively does.

```ts
import { logger } from 'muxws';

logger.level = 'debug'; // 'debug' | 'info' | 'warn' | 'error' | 'silent'
```

### The format

```
muxws conn=b19-0 dir=tx type=open   stream=1 end=1 bytes=88 headers=1
muxws conn=b19-1 dir=rx type=open   stream=1 end=1 bytes=88 headers=1
muxws conn=b19-1 dir=tx type=data   stream=1 end=0 bytes=44
muxws conn=b19-1 dir=tx type=data   stream=1 end=1 bytes=90
muxws conn=b19-0 dir=rx type=data   stream=1 end=0 bytes=44
muxws conn=b19-0 dir=rx type=data   stream=1 end=1 bytes=90
muxws conn=b19-0 dir=tx type=ping   bytes=42
muxws conn=b19-1 dir=rx type=ping   bytes=42
muxws conn=b19-1 dir=tx type=pong   bytes=42
muxws conn=b19-0 dir=rx type=pong   bytes=42
muxws conn=b19-0 dir=tx type=open   stream=3 end=1 bytes=54
muxws conn=b19-1 dir=rx type=open   stream=3 end=1 bytes=54
muxws conn=b19-1 dir=tx type=reset  stream=3 bytes=97 code=2 reason='no'
muxws conn=b19-0 dir=rx type=reset  stream=3 bytes=97 code=2 reason='no'
muxws conn=b19-0 dir=tx type=open   stream=5 end=0 bytes=65077 frag=more
muxws conn=b19-0 dir=tx type=open   stream=5 end=0 bytes=65076 frag=more
muxws conn=b19-0 dir=tx type=open   stream=5 end=1 bytes=4982 frag=last
muxws conn=b19-0 dir=tx type=goaway bytes=42 code=0 last=0
```

Every field, in the order it appears:

| Field | When it appears | What it is |
|---|---|---|
| `conn=` | always | `peer.id` |
| `dir=` | always | `tx` on the way out, `rx` on the way in |
| `type=` | always | the frame type, padded to six characters so the columns line up |
| `stream=` | when the frame has a stream id | connection-level frames (`ping`, `pong`, `goaway`) have none |
| `end=` | on `open` and `data` only | `1` if this frame ends the sender's side |
| `bytes=` | always | the **encoded** length of the whole message, envelope included |
| `frag=` | on a fragment | `more` for every fragment but the last, `last` for the closing one |
| `headers=` | when there are headers | how **many** headers — never their names or values |
| `code=` | on `reset` and `goaway` | the numeric reset code |
| `last=` | on `goaway` | the `last_stream` cut-off |
| `reason=` | when there is one | the diagnostic text carried by a `reset` or `goaway` |

Reading the sample: two peers, `b19-0` the dialer and `b19-1` the acceptor, both of them logging, so
each frame appears twice — once as `tx` and once as `rx`. Stream 1 is a unary open answered with two
`data` frames, the second ending it. The `ping`/`pong` is a liveness round trip. Stream 3 was reset
with code 2 — `APPLICATION_ERROR`, a handler that raised. Stream 5 carries a payload too large for one
frame, so it is two `frag=more` fragments and one `frag=last`; note the byte counts, each at or under
the 65536-byte cap, and note that `end=1` appears only on the closing fragment. The `goaway` at the
end is a deliberate `peer.close()`.

`conn=` is the correlation id, and it identifies one **connection**, not one peer: a peer that
reconnects gets a new one, so two lines carrying the same `conn=` are always the same socket. See
[`peer.id`](/api/peer).

## `peer.on_frame(handler)`

The same event as data, for anything that is not a log line — a counter, a histogram, a live
inspector.

```python
# fragment
def count(direction: str, frame, byte_length: int) -> None:
    metrics.increment(f"muxws.frames.{direction}.{frame.type}")
    metrics.observe("muxws.frame_bytes", byte_length)


peer.on_frame(count)
```

```ts
// fragment
peer.onFrame((direction: 'tx' | 'rx', frame, byteLength: number) => {
  metrics.increment(`muxws.frames.${direction}.${frame.type}`);
  metrics.observe('muxws.frame_bytes', byteLength);
});
```

The signature is `(direction, frame, byte_length)` in Python and `(direction, frame, byteLength)` in
TypeScript.

| Parameter | Type | Meaning |
|---|---|---|
| `direction` | `"tx"` / `"rx"` | Outbound or inbound. |
| `frame` | `Frame` | The whole decoded frame, envelope and payload. |
| `byte_length` | `int` | The encoded length of the whole message, in bytes — the same number the log line prints. |

Four things about it:

**It fires for every frame in both directions**, including `ping`, `pong` and `goaway`, and including
each fragment of a fragmented payload separately.

**It fires whether or not the logger is enabled.** The two seams are independent; `on_frame` is not a
log handler.

**More than one handler is allowed.** `on_frame` appends rather than replaces. Python's returns the
handler, so it doubles as a decorator; TypeScript's `onFrame` returns nothing. (`on_stream` is the
opposite: one handler, and a second replaces the first and logs a warning.)

**A handler that raises is isolated.** It is logged, the remaining handlers still run, and the
connection is untouched. This matters more than it sounds: the handler runs *inside* the read loop and
the write loop, so an observer allowed to propagate would kill the connection it was only meant to be
watching. For the same reason, keep it fast and never block in it — a slow `on_frame` handler is a
slow connection.

`on_frame` hands you `frame.payload`. muxws does not log it; what you do with it is your decision, and
the section below is why that decision deserves a moment.

## What is never logged

**Payload contents never appear, at any level.** Not at `DEBUG`, not truncated, not hashed. Every
field the frame line prints is either a field of the envelope or a *count* of one — `headers=1` says
there was one header, and says nothing about which. Application data routinely holds secrets, and a
frame line is emitted for every frame, so a payload in the log is a payload in every log aggregator
the deployment has.

The one field that carries free text is `reason=`, and it is worth knowing exactly what that is:

- On a `goaway` or a protocol-level `reset`, it is text muxws wrote — the reason the connection or
  stream is being ended.
- On a `reset` with code `2` (`APPLICATION_ERROR`), it is **`str()` of the exception your handler
  raised**. In the sample above, `reason='no'` is a `ValueError("no")`.

So the frame log does not contain payloads, but it does contain your exception messages. That is the
same text the next section is about.

## What *does* leave the process

::: danger `error_serializer` puts exception text on the wire
When an `on_stream` handler raises, muxws resets the stream with `APPLICATION_ERROR` and calls this
peer's `error_serializer` to build the structured payload that rides the reset. The default:

```python
# fragment
def default_error_serializer(exc: BaseException) -> Any:
    return {"type": type(exc).__name__, "message": str(exc)}
```

`str(exc)` is not a curated message. Depending on what raised, it can contain a file path, a database
column name, a fragment of SQL, a primary key, an internal hostname, or the value that failed
validation. The reset also carries that text in its `reason` field.

**A public-facing deployment should replace it with a redacting one.**
:::

```python
# fragment
import logging
import uuid

logger = logging.getLogger("myapp")


def redacting_error_serializer(exc: BaseException) -> dict[str, str]:
    """Keep the detail on this side of the socket; send the remote a correlation id."""
    incident = uuid.uuid4().hex
    logger.exception("handler failed", extra={"incident": incident})
    return {"type": "InternalError", "incident": incident}


peer = await accept(websocket, error_serializer=redacting_error_serializer)
```

```ts
// fragment
const peer = await accept(socket, {
  errorSerializer: (error: unknown) => {
    const incident = crypto.randomUUID();
    console.error('handler failed', incident, error);
    return { type: 'InternalError', incident };
  },
});
```

It is chosen **per peer**, on `connect()` and on `accept()`, which is what lets one process redact on
its browser-facing connections and keep the detail on its internal ones. See
[Errors](/guide/errors#error-serializer-is-per-peer).

Two more notes on it:

- A serializer that raises is logged, and the reset still goes out — with no payload. The reset is
  unconditional, because a handler failure reaching the opener as *silence* leaves the caller waiting
  forever, which is worse than a reset with less detail in it.
- The reset's `reason` field is `str(exc)` regardless of what your serializer returns. If the message
  itself is sensitive, the exceptions your handlers raise need to be, too.

## Close reasons

`on_close(reason)` fires on **every** socket loss, once per loss, with the same four fields in both
languages.

| Field | Type | Meaning |
|---|---|---|
| `code` / `code` | `int` | The WebSocket close code. `1000` clean, `1006` abnormal, `1002` protocol error, `1011` internal. |
| `reason` / `reason` | `str` | Free text. `"no pong within 10.0s"` in Python, `"no pong within 10000ms"` in TypeScript, for a heartbeat death; whatever the remote sent for a clean close. |
| `was_clean` / `wasClean` | `bool` | Whether the socket closed with a proper close frame. |
| `will_retry` / `willRetry` | `bool` | Whether the reconnect helper intends to dial again. |

`will_retry` is the field to branch on: `true` means a reconnection is coming and there is nothing to
do, `false` means this peer is finished — either the attempt cap ran out or `close()` was deliberate.
At most one `will_retry: false` close ever fires for one peer, so a teardown hooked to it runs exactly
once.

Every live stream has already been failed with `ConnectionLost` by the time the handler runs, so
`peer.streams` is empty inside it.

## See also

- [`api/peer`](/api/peer) — `on_frame`, `on_close`, `on_reconnect`, `id`, `streams`
- [`api/types`](/api/types) — `CloseReason`, `ErrorSerializer`, `Frame` and every envelope field
- [`api/errors`](/api/errors) — `default_error_serializer`, `ResetCode`, `ConnectionLost`
- [`api/accept`](/api/accept) and [`api/connect`](/api/connect) — the `error_serializer` parameter
