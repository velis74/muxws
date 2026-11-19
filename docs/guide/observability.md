# Observability

muxws provides two observation mechanisms:

- The **`muxws.frames` logger**: one line per frame, disabled by default, enabled through logging
  configuration without code changes.
- **`peer.on_frame(handler)`**: the same per-frame event delivered as data, for metrics.

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
- The `app.muxws.exchanges` entry belongs to the [exchange log](#exchange-log) below.
  `"()": "queued_file.QueuedFileHandler"` is the import path of [the queued handler](#the-write-path);
  in a project it is the path of the module that holds it.

Without Django, the same dictionary is applied with `logging.config.dictConfig(LOGGING)` at startup.

### TypeScript

The TypeScript logger writes to `console`: `debug` and `info` to standard output, `warn` and `error`
to standard error. `logger.level` selects what is written; a file is the redirection of the process's
output, for example `node server.js >> muxws.log 2>&1`, or the log driver of the container or service
manager.

## Exchange log

The exchange log writes one JSON line (JSONL) per stream: what the opening side sent, what the other
side sent, how the stream ended and how long it took. It is a recipe, not part of muxws: copy
`exchange_log.py` or `exchange-log.ts` into the application. It is built on
[`on_frame`](#peer-on-frame-handler) and [`on_close`](#close-reasons) and reassembles fragmented
payloads with `Assembler`.

::: danger The exchange log writes payloads
muxws never logs payloads (see [What is never logged](#what-is-never-logged)); this recipe does.
Payloads and headers can contain passwords, tokens and personal data, and the log file holds them.
Remove them with `redact` (payloads) and `include` (whole streams), restrict access to the file, and
keep the exchange log off in production unless it is needed.
:::

### Python

::: details `exchange_log.py`

<<< @/examples/exchange_log.py

:::

```python
# fragment
from muxws import accept, resolve_codec

from exchange_log import ExchangeLog


def without_password(payload, side):
    if isinstance(payload, dict) and "password" in payload:
        return {**payload, "password": "***"}
    return payload


exchanges = ExchangeLog(resolve_codec(), redact=without_password)


@app.websocket("/ws")
async def ws(websocket):
    peer = await accept(websocket)
    exchanges.attach(peer)
    peer.on_stream(handle)
    await peer.serve()
```

`ExchangeLog(codec, *, write=None, logger="app.muxws.exchanges", trim_payload_size=4096,
max_messages=100, redact=None, include=None, clock=None)`

| Parameter | Default | Meaning |
|---|---|---|
| `codec` | required | The codec of the connections: the `codec=` given to `accept()` / `connect()`, or `resolve_codec()` for the configured one. It decodes payloads reassembled from fragments. |
| `write` | `None` | `callable(str)` receiving each finished line, without a newline. `None` passes the line to `logging.getLogger(logger).info`. |
| `logger` | `"app.muxws.exchanges"` | The logger name used when `write` is `None`. |
| `trim_payload_size` | `4096` | Longest logged payload, in UTF-8 bytes of its JSON text. A longer payload is logged as a string: its first `trim_payload_size` bytes, cut on a character boundary, then `…(trimmed N bytes)…`, where `N` is the number of bytes cut off. A shorter one is logged as its JSON value. `None` never trims. Headers and trailers are never trimmed. |
| `max_messages` | `100` | Payloads kept per side of a stream. Further payloads are not decoded and are counted in `dropped`. `None` keeps all. |
| `redact` | `None` | `callable(payload, side) -> payload`, applied to every kept payload before it is serialised; `side` is `"request"` or `"response"`. |
| `include` | `None` | `callable(headers, payload) -> bool`, called once per stream with the opening frame's headers (`None` when it has none) and payload (reassembled when fragmented, `None` when absent or when the stream ends before it is complete). `False` leaves the stream out of the log. `None` logs every stream. |
| `clock` | `None` | Monotonic time in seconds, for the durations; `None` is `time.monotonic`. |

`attach(peer)` registers an `on_frame` and an `on_close` handler on `peer`. One `ExchangeLog` can be
attached to any number of peers, dialers and acceptors alike. A dialer's reconnected sockets are logged
too, each under its own `conn`.

Serialising a payload converts `bytes` to base64 text, `NaN` and infinities to `null`, lone surrogates
to `\uXXXX` escapes and any other value JSON cannot hold to its `str()`, so every line is strict JSON in
UTF-8.

### TypeScript (Node)

::: details `exchange-log.ts`

<<< @/examples/exchange-log.ts

:::

```ts
// fragment
import { createWriteStream } from 'node:fs';

import { JsonCodec } from 'muxws';

import { ExchangeLog } from './exchange-log';

const file = createWriteStream('/var/log/myapp/exchanges.jsonl', { flags: 'a' });
const exchanges = new ExchangeLog({ codec: new JsonCodec(), write: (line) => file.write(`${line}\n`) });

exchanges.attach(peer);
```

`new ExchangeLog({ codec, write, trimPayloadSize, maxMessages, redact, include, clock })` takes the
same options as the Python class, in camelCase, with two differences: `write` is required, and `clock`
returns milliseconds (default `performance.now`). A `fs.WriteStream` in append mode buffers the writes,
so `write` returns without waiting for the disk.

### The record

```json
{"ts":"2026-11-19T09:32:22.359Z","conn":"c42-0","stream":1,"opened_by":"local","oneway":false,"request":{"headers":{"trace":"4f1a"},"payloads":[{"action":"get_user","id":7}],"trailers":null,"dropped":0,"bytes":103},"response":{"headers":{"status":200},"payloads":[{"id":7,"name":"Ana"}],"trailers":null,"dropped":0,"bytes":120},"outcome":"ok","code":null,"reason":null,"first_response_ms":0.212,"duration_ms":0.255}
```

The keys always appear in this order:

| Key | Value |
|---|---|
| `ts` | When the stream was opened, UTC, ISO 8601 with milliseconds and `Z`. |
| `conn` | `peer.id` of the connection. |
| `stream` | The stream id. |
| `opened_by` | `"local"` when the logging peer opened the stream, `"remote"` when the other end did. |
| `oneway` | `true` for a `notify()` stream. |
| `request` | What the opening side sent, whichever side writes the log. |
| `response` | What the other side sent. |
| `outcome` | `"ok"` when both sides ended, `"reset"` when either side reset the stream, `"lost"` when the connection closed first. |
| `code`, `reason` | The code and reason of the `reset` frame; `null` for `"ok"` and `"lost"`. |
| `first_response_ms` | Milliseconds from the open to the first frame of the other side; `null` when none arrived. |
| `duration_ms` | Milliseconds from the open to the end of the stream. |

`request` and `response` have the same shape:

| Key | Value |
|---|---|
| `headers` | The side's leading headers, or `null`. |
| `payloads` | The side's payloads in order, at most `max_messages`. A fragmented payload appears once, decoded. |
| `trailers` | The side's trailers, or `null`. |
| `dropped` | Payloads beyond `max_messages`. |
| `bytes` | The sum of `byte_length` of the side's frames on the stream, fragments and `reset` included. |

A stream's line is written when both sides have ended, when either side resets it, or when the
connection closes; an `on_close` writes every unfinished stream of that connection with `"lost"`. A
`notify()` stream is written as soon as its open is complete, with an empty `response` and `"ok"`.
Frames without a stream (`ping`, `pong`, `goaway`) are not logged.

### The write path

The `on_frame` handler runs inside the peer's read loop and write loop, so `write`, `redact` and
`include` must return quickly (see [`peer.on_frame`](#peer-on-frame-handler)). An exception raised
by `write`, `redact`, `include` or the serialisation drops that stream's record and never reaches the
connection; the other streams are logged. Python logs the exception at `ERROR` under the logger named
after the module (`exchange_log`), TypeScript at `warn` on the muxws `logger`.

In Python the default `write` is a logging call, and a `logging.FileHandler` writes to disk on the
calling thread, which is the event loop. `QueuedFileHandler` puts the record on a queue instead, and a
`QueueListener` thread appends it to the file:

<<< @/examples/queued_file.py

The Django configuration [above](#django) attaches it to `app.muxws.exchanges` with a `%(message)s`
formatter, so each line in the file is exactly one record. Without Django:

```python
# fragment
import logging

from queued_file import QueuedFileHandler

exchanges_logger = logging.getLogger("app.muxws.exchanges")
exchanges_logger.addHandler(QueuedFileHandler("/var/log/myapp/exchanges.jsonl"))
exchanges_logger.setLevel(logging.INFO)
exchanges_logger.propagate = False
```

## See also

- [`api/peer`](/api/peer) — `on_frame`, `on_close`, `on_reconnect`, `id`, `streams`
- [`api/types`](/api/types) — `CloseReason`, `ErrorSerializer`, `Frame` and every envelope field
- [`api/errors`](/api/errors) — `default_error_serializer`, `ResetCode`, `ConnectionLost`
- [`api/accept`](/api/accept) and [`api/connect`](/api/connect) — the `error_serializer` parameter
