# Observability

muxws provides three observation mechanisms:

- The **`muxws.frames` logger**: one line per frame, disabled by default, enabled through logging
  configuration without code changes.
- **`peer.on_frame(handler)`**: the same per-frame event delivered as data, for metrics.
- **`peer.on_exchange(handler)`**: one record per finished stream, with the payloads of both sides, the
  outcome and the timing, for request logging and metrics (see [Exchange log](#exchange-log)).

muxws never logs payload contents, but the default `error_serializer` sends exception text on the
wire; see [What does leave the process](#what-does-leave-the-process).

## The `muxws.frames` logger

The logger emits one line per frame, in both directions, at `DEBUG`. It emits nothing at any other
level. When `DEBUG` is not enabled, the call returns after the level check, so the per-frame overhead
of an unconfigured application is one level check.

```python
import logging

logging.basicConfig(level=logging.DEBUG, format="%(message)s")
logging.getLogger("muxws.frames").setLevel(logging.DEBUG)
```

The browser entry point has no runtime dependencies, so the TypeScript implementation includes a
level-filtered logger over `console`. Its default level is `'warn'`, which matches the effective level
of an unconfigured Python logger.

```ts
import { logger } from 'muxws';

logger.level = 'debug'; // 'debug' | 'info' | 'warn' | 'error' | 'silent'
```

### The format

```text
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
| `type=` | always | the frame type, padded to six characters so the columns align |
| `stream=` | when the frame has a stream id | connection-level frames (`ping`, `pong`, `goaway`) have none |
| `end=` | on `open` and `data` only | `1` if this frame ends the sender's side |
| `bytes=` | always | the **encoded** length of the whole message, envelope included |
| `frag=` | on a fragment | `more` for every fragment but the last, `last` for the closing one |
| `headers=` | when there are headers | how **many** headers — never their names or values |
| `code=` | on `reset` and `goaway` | the numeric reset code |
| `last=` | on `goaway` | the `last_stream` cut-off |
| `reason=` | when there is one | the diagnostic text carried by a `reset` or `goaway` |

The sample shows two peers, `b19-0` (the dialer) and `b19-1` (the acceptor), both logging, so each
frame appears twice: once as `tx` and once as `rx`. Stream 1 is a unary open answered with two `data`
frames; the second ends it. The `ping`/`pong` pair is a liveness round trip. Stream 3 is reset with
code 2, `APPLICATION_ERROR`, because the handler raised. Stream 5 carries a payload too large for one
frame, sent as two `frag=more` fragments and one `frag=last`; each byte count is at or under the
65536-byte limit, and `end=1` appears only on the last fragment. The final `goaway` is sent by
`peer.close()`.

`conn=` is the correlation id. It identifies one **connection**, not one peer: a peer that reconnects
gets a new id, so lines with the same `conn=` always belong to the same socket. See
[`peer.id`](/api/peer).

## `peer.on_frame(handler)`

`on_frame` delivers the same per-frame event as data, for uses other than logging, such as a counter,
a histogram or a live inspector.

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

**It fires for every frame in both directions**, including `ping`, `pong` and `goaway`, and once for
each fragment of a fragmented payload.

**It fires whether or not the logger is enabled.** The two mechanisms are independent; `on_frame` is
not a log handler.

**Multiple handlers can be registered.** `on_frame` adds a handler; it does not replace the previous
one. In Python it returns the handler, so it can be used as a decorator; TypeScript's `onFrame` returns
nothing. (`on_stream` differs: it holds one handler, and a second registration replaces the first and
logs a warning.)

**An exception in a handler is contained.** It is logged, the remaining handlers run, and the
connection is not affected. The handler runs *inside* the read loop and the write loop, so a
propagated exception would close the connection. For the same reason, the handler must return quickly
and must not block: a slow `on_frame` handler slows the connection.

The `frame` argument includes `frame.payload`. muxws does not log the payload; if the handler records
it, see the next section for what payloads may contain.

## What is never logged

**Payload contents are never logged, at any level**, in full, truncated or hashed. Every field of
the frame line is either an envelope field or a *count*: `headers=1` states that the frame had one
header and does not name it. Application data can contain secrets, and the frame line is emitted for
every frame, so logged payloads would reach every log aggregator of the deployment.

The only free-text field is `reason=`:

- On a `goaway` or a protocol-level `reset`, it is text written by muxws: the reason the connection or
  stream is ended.
- On a `reset` with code `2` (`APPLICATION_ERROR`), it is **`str()` of the exception the handler
  raised**. In the sample above, `reason='no'` comes from `ValueError("no")`.

The frame log therefore contains no payloads, but it does contain handler exception messages, the same
text described in the next section.

## What *does* leave the process

::: danger `error_serializer` puts exception text on the wire
When an `on_stream` handler raises, muxws resets the stream with `APPLICATION_ERROR` and calls the
peer's `error_serializer` to build the structured payload sent with the reset. The default:

```python
# fragment
def default_error_serializer(exc: BaseException) -> Any:
    return {"type": type(exc).__name__, "message": str(exc)}
```

`str(exc)` is not filtered. Depending on the exception, it can contain a file path, a database
column name, part of an SQL statement, a primary key, an internal hostname, or the value that failed
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

It is set **per peer**, on `connect()` and on `accept()`, so one process can redact errors on its
browser-facing connections and send full errors on its internal ones. See
[Errors](/guide/errors#error-serializer-is-per-peer).

- If the serializer raises, the exception is logged and the reset is sent without a payload. The
  reset is always sent; without it, the caller would wait indefinitely.
- The reset's `reason` field is `str(exc)` regardless of what the serializer returns. If an exception
  message would be sensitive, the handler must raise an exception with a different message.

## Close reasons

`on_close(reason)` fires once for **every** socket loss. `reason` has the same four fields in both
languages.

| Field | Type | Meaning |
|---|---|---|
| `code` / `code` | `int` | The WebSocket close code. `1000` clean, `1006` abnormal, `1002` protocol error, `1011` internal. |
| `reason` / `reason` | `str` | Free text. `"no pong within 10.0s"` in Python, `"no pong within 10000ms"` in TypeScript, when the heartbeat times out; the remote's close reason for a clean close. |
| `was_clean` / `wasClean` | `bool` | Whether the socket closed with a proper close frame. |
| `will_retry` / `willRetry` | `bool` | Whether the reconnect helper intends to dial again. |

Branch on `will_retry`: `true` means the reconnect helper will dial again and no action is required;
`false` means the peer will not reconnect, because it has no reconnect helper, the attempt limit was
reached, or `close()` was called. At most one `will_retry: false` close fires per peer, so teardown
code guarded by it runs once.

Before the handler runs, every live stream has been failed with `ConnectionLost`, so `peer.streams` is
empty inside it.

## Writing the muxws logs to a file

The muxws loggers are standard Python loggers, so the application's logging configuration decides
where their lines go.

| Logger | What it emits |
|---|---|
| `muxws.frames` | One line per frame at `DEBUG` (see [the format](#the-format)); connection and stream problems at `INFO`, `WARNING` and `ERROR`: unknown frame types, dropped oneway streams, protocol errors, a replaced `on_stream` handler, handlers that raised, and frames that could not be encoded or sent. |
| `muxws.reconnect` | At `DEBUG`, each failed dial and each socket that ended under the reconnect helper. |
| `muxws.registry` | No lines. |
| `muxws.origin` | At `INFO`, each handshake refused by `allowed_origins`. |
| `muxws.codec` | At `ERROR`, each subprotocol mismatch: a refused upgrade or a wrongly negotiated subprotocol. |

All five are children of `muxws`, so one entry for `muxws` covers them; an entry for a child overrides
its level.

### Django

A logging configuration takes effect in the process that applies it. For muxws that is the process
running the connections: under Django, the ASGI server process (uvicorn, daphne) that loads the
settings through `asgi.py`. Django applies `settings.LOGGING` with `logging.config.dictConfig` when it
sets up; another process, such as a Celery worker, applies its own configuration.

<<< @/examples/django_logging.py

- `muxws.frames` at `DEBUG` writes one line per frame and is meant for diagnosis; `INFO` turns the
  per-frame lines off, `WARNING` also the unknown frame types. `"muxws.reconnect": {"level": "DEBUG"}`
  adds the reconnect helper's failed dials.
- `"propagate": False` keeps the lines out of the root logger's handlers, such as the console.
- The `app.muxws.exchanges` entry belongs to the [exchange log](#exchange-log) below. Its `%(message)s`
  formatter writes the message alone, so each line of `exchanges.jsonl` is one JSON object.

Without Django, the same dictionary is applied with `logging.config.dictConfig(LOGGING)` at startup.

### TypeScript

The TypeScript logger writes to `console`: `debug` and `info` to standard output, `warn` and `error`
to standard error. `logger.level` selects what is written; a file is the redirection of the process's
output, for example `node server.js >> muxws.log 2>&1`, or the log driver of the container or service
manager.

## Exchange log

[`peer.on_exchange`](/api/peer#peer-on-exchange-python) calls its handler once per finished stream
with an [`Exchange`](/api/types#exchange): what the opening side sent (`request`), what the other side
sent (`response`), how the stream ended and how long it took. `as_dict()` turns it into a JSON-ready
record, so an exchange log is a handler that writes that record as one JSON line (JSONL).

::: danger The exchange log writes payloads
muxws never logs payloads (see [What is never logged](#what-is-never-logged)); `on_exchange` hands them
to the application, and a handler that writes them puts them in the log. Payloads and headers can
contain passwords, tokens and personal data. Redact them before writing, restrict access to the file,
and keep the exchange log off in production unless it is needed.
:::

### Python

<<< @/examples/exchange_logging.py

`log_exchanges(peer)` registers the handler on one peer; call it for every peer, dialers and acceptors
alike. A dialer keeps its handlers across reconnects, and each connection's streams carry its own
`conn`.

```python
# fragment
from muxws import accept

from exchange_logging import log_exchanges


@app.websocket("/ws")
async def ws(websocket):
    peer = await accept(websocket)
    log_exchanges(peer)
    peer.on_stream(handle)
    await peer.serve()
```

- `as_dict(trim_payload_size=4096)` writes a payload longer than 4096 UTF-8 bytes of JSON as a string:
  its first 4096 bytes, then `…(trimmed N bytes)…`. Headers and trailers are never trimmed.
- `as_dict()` returns a new structure, so the redaction function changes it in place without changing
  the `Exchange`. A trimmed payload reaches it as a string; `without_secrets` replaces such a string
  whole when it contains a `"password"` key.
- `on_exchange(handler, max_payloads=100)` hands the handler at most 100 payloads per side;
  `payload_count` in the record counts all of them.
- The handler runs on the event loop when the stream ends. The `app.muxws.exchanges` logger writes the
  line with whatever handlers the logging configuration gives it; the Django configuration
  [above](#django) gives it a `logging.FileHandler` with a `%(message)s` formatter. Without Django:

```python
# fragment
import logging

exchanges_logger = logging.getLogger("app.muxws.exchanges")
handler = logging.FileHandler("/var/log/myapp/exchanges.jsonl")
handler.setFormatter(logging.Formatter("%(message)s"))
exchanges_logger.addHandler(handler)
exchanges_logger.setLevel(logging.INFO)
exchanges_logger.propagate = False
```

### Metrics

The same hook feeds metrics; `max_payloads=0` records no payloads at all:

```python
# fragment
def measure(exchange):
    metrics.observe("muxws.stream_duration_seconds", exchange.duration)
    metrics.increment(f"muxws.streams.{exchange.outcome}")


peer.on_exchange(measure, max_payloads=0)
```

```ts
// fragment
peer.onExchange(
  (exchange) => {
    metrics.observe('muxws.stream_duration_ms', exchange.durationMs);
    metrics.increment(`muxws.streams.${exchange.outcome}`);
  },
  { maxPayloads: 0 },
);
```

### TypeScript (Node)

<<< @/examples/exchange-logging.ts

```ts
// fragment
import { logExchanges, openExchangeLog } from './exchange-logging';

const file = openExchangeLog('/var/log/myapp/exchanges.jsonl');

logExchanges(peer, file);
```

A `fs.WriteStream` in append mode buffers the writes, so the handler returns without waiting for the
disk. Durations in TypeScript are milliseconds (`durationMs`); the record is the same in both
languages.

### The record

```json
{"ts":"2026-11-19T09:32:22.359Z","conn":"c42-0","stream":1,"opened_by":"local","oneway":false,"request":{"headers":{"trace":"4f1a"},"payloads":[{"action":"get_user","id":7}],"payload_count":1,"trailers":null,"ended":true},"response":{"headers":{"status":200},"payloads":[{"id":7,"name":"Ana"}],"payload_count":1,"trailers":null,"ended":true},"outcome":"ok","reset_by":null,"code":null,"reason":null,"first_response_ms":0.212,"duration_ms":0.255}
```

| Key | Value |
|---|---|
| `ts` | When the stream was created, UTC, ISO 8601 with milliseconds and `Z`. |
| `conn`, `stream` | `peer.id` of the connection and the stream id. |
| `opened_by` | `"local"` when the logging peer opened the stream, `"remote"` when the other end did. |
| `oneway` | `true` for a `notify()` stream. |
| `request`, `response` | What the opening side and the other side sent: `headers`, `payloads`, `payload_count`, `trailers`, `ended`. |
| `outcome` | `"ok"`, `"reset"` or `"lost"`. |
| `reset_by`, `code`, `reason` | Who reset the stream, and the code and reason of the `reset`; `null` unless `outcome` is `"reset"`. |
| `first_response_ms`, `duration_ms` | Milliseconds from the creation of the stream to the answering side's first `data` frame (a `reset` does not count; `null` when there is none), and to its end. |

Every field is described under [`Exchange`](/api/types#exchange).

## See also

- [`api/peer`](/api/peer) — `on_frame`, `on_exchange`, `on_close`, `on_reconnect`, `id`, `streams`
- [`api/types`](/api/types) — `CloseReason`, `Exchange`, `ErrorSerializer`, `Frame` and every envelope field
- [`api/errors`](/api/errors) — `default_error_serializer`, `ResetCode`, `ConnectionLost`
- [`api/accept`](/api/accept) and [`api/connect`](/api/connect) — the `error_serializer` parameter
