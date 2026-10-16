---
outline: deep
---

# Errors

Every error muxws raises is an instance of one class hierarchy per language. The shared classes have
the same names and the same inheritance in Python and TypeScript. The class of an exception
identifies the failure; a caller does not need to inspect the message.

The **shared** classes — everything down to and including the two `Transport…` bases — are importable
from the package root in both languages. The classes indented under those bases are **transport
errors**: each one belongs to a single transport, is imported from that transport's own module, and
is never re-exported from the root. The right-hand column says where each of those comes from.

```text
MuxwsError                          from muxws / 'muxws' — the whole tree is catchable as this
├── ProtocolError
├── ConnectionClosed
├── ConnectionGoingAway
├── StreamAlreadyConsumed
├── StreamClosed
├── CodecError
│   ├── CodecNotRegistered
│   └── CodecMismatch
├── TransportUrlError               this transport cannot open that address; py: also a ValueError
│   ├── UnixUrlError                py: muxws.transports.unix   ts: 'muxws/node'
│   ├── WebsocketUrlError           py: muxws.transports.websockets_
│   └── WsUrlError                  ts: 'muxws/node'
├── TransportUnsupportedError       this runtime has no such transport; py: also a RuntimeError
│   ├── UnixSocketsUnsupportedError py: muxws.transports.unix   ts: 'muxws'
│   ├── WebsocketsNotInstalledError py: muxws.transports.websockets_
│   └── WsNotInstalledError         ts: 'muxws/node'
└── StreamReset
    ├── RemoteError
    ├── StreamTimeout
    ├── StreamRefused
    └── ConnectionLost
```

**Why transport errors are not exported from `muxws`.** `SocketAdapter` is public, and third-party
adapters can implement transports this repository does not ship. A third-party adapter cannot add a
class to `muxws/errors.py` or `ts/errors.ts`. A transport error therefore subclasses one of the
shared bases and is defined in the module of the transport that raises it. An application that does
not know which transport handles an address catches `TransportUrlError`. Both bases have no
dependencies, so `except TransportUrlError` works in a process that cannot import the transport that
raised the error.

TypeScript has no module paths below its entry points: `package.json` publishes `muxws`, `muxws/node`
and `muxws/msgpack`. A TypeScript transport error is exported from the entry point that ships its
transport and from no other. `UnixSocketsUnsupportedError` is exported from `muxws`, because that
entry point dials with the platform `WebSocket` and refuses the `ws+unix:` scheme. `WsUrlError` and
`WsNotInstalledError` are exported from `muxws/node`. The browser bundle contains neither.

The `connect()` exported from `muxws` does not wrap a URL that the platform `WebSocket` constructor
rejects. From that entry point a malformed URL raises the runtime's `DOMException` (jsdom:
`The URL 'nonsense' is invalid.`; undici: `TypeError: Invalid URL`), which is not a `MuxwsError`.
`muxws/node` raises a `TransportUrlError` for each of these URLs. Python raises a `TransportUrlError`
for every URL its parsers reach. The exception is an authority with an unbalanced `[` (`ws://[::1`):
`urllib.parse.urlsplit` rejects it before either check, so `connect()` raises a bare `ValueError`.

`except MuxwsError` catches every transport error without importing a transport module.
`except TransportUrlError` and `except TransportUnsupportedError` each catch one of the two groups.

`StreamClosed` is not a `StreamReset`: a stream that ended normally while a last `send()` was in
flight is an expected race, not a failure. `ConnectionLost` is a `StreamReset`: for a stream, socket
loss is one more way of ending early, so one `except StreamReset` covers every early end.

Reset codes 5 and 9:

- **Reset code 5 (`STREAM_LIMIT`) is retired.** No name maps to it and it is never sent. A peer that
  receives it treats it as any other unknown code: it resets that one stream and keeps the
  connection. The code for refusing a stream over a concurrency limit is `REFUSED` (4).
- **Reset code 9, `CONNECTION_CLOSED`, must never appear on the wire.** It is synthesised locally on
  every stream that is live when the socket dies. On the wire it would tell the remote that the
  remote's own connection had died, which is false. `stream.reset()` refuses it with a
  `ProtocolError` and does not send it.

## `MuxwsError`

The root class. `except MuxwsError` catches every error class this library defines and no other
exception.

### Signature

```python
class MuxwsError(Exception):
    """Root of every error this library raises."""
```

```ts
export class MuxwsError extends Error {
  constructor(message?: string);
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` (TypeScript) | `string \| undefined` | `undefined` | The human-readable text, stored on `Error.message`. |
| `*args` (Python) | `object` | none | Inherited from `Exception` unchanged; the first argument becomes `str(exc)`. |

`MuxwsError` defines no `__init__` in Python and takes the arguments of `Exception`. In TypeScript
it sets `name` to `'MuxwsError'`, so the class can be identified by name after a structured clone or
across a bundle boundary.

### Return

A new exception instance. Constructing one does not raise it.

### Raises

Raises: nothing.

### Example

```python
from muxws import MuxwsError, StreamClosed

try:
    raise StreamClosed("stream 3 closed normally; nothing more can be sent on it")
except MuxwsError as exc:
    print(type(exc).__name__, "|", exc)
```

```ts
import { MuxwsError, StreamClosed } from 'muxws';

try {
  throw new StreamClosed('stream 3 closed normally; nothing more can be sent on it');
} catch (error) {
  if (error instanceof MuxwsError) console.log(error.name, '|', error.message);
}
```

## `ProtocolError`

This peer or the remote broke the protocol: a frame with no `type`, a frame carrying both `payload`
and `fragment`, a codec handed a message it cannot represent, or a local call that would put an
illegal value on the wire. It indicates a bug and is not retryable.

### Signature

```python
class ProtocolError(MuxwsError):
    """This peer or the remote violated the specification."""
```

```ts
export class ProtocolError extends MuxwsError {
  constructor(message?: string);
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` (TypeScript) | `string \| undefined` | `undefined` | The human-readable text. |
| `*args` (Python) | `object` | none | Inherited from `Exception`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws import from_mapping, ProtocolError

try:
    from_mapping({"stream": 1, "payload": "hi"})
except ProtocolError as exc:
    print("rejected:", exc)
```

```ts
import { ProtocolError, fromMapping } from 'muxws';

try {
  fromMapping({ stream: 1, payload: 'hi' });
} catch (error) {
  if (error instanceof ProtocolError) console.log('rejected:', error.message);
}
```

## `ConnectionClosed`

The socket closed. Raised by `peer.ping()` when the pong deadline expires, and by a socket adapter's
`receive` / `send_text` / `send_bytes` when the socket is already closed. `peer.serve()` catches it,
kills the peer and **returns** without raising. A stream **never** raises it; a stream raises
`ConnectionLost` instead, so a per-stream `except StreamReset` covers socket loss and a handler for
`ConnectionClosed` deals only with the connection.

### Signature

```python
class ConnectionClosed(MuxwsError):
    def __init__(self, message: str = "", *, code: int = 1006, reason: str = "", was_clean: bool = False) -> None: ...
```

```ts
export class ConnectionClosed extends MuxwsError {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;

  constructor(message?: string, options: { code?: number; reason?: string; wasClean?: boolean } = {});
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | `""` / `undefined` | The exception text. Python falls back to `reason`, then to `"connection closed"`, when it is empty. TypeScript's fallback chain is `message ?? reason ?? 'connection closed'` and `reason` already defaults to `''`, so an omitted message yields an **empty** `error.message` there — pass the text you want. |
| `code` | `int` / `number` | `1006` | The WebSocket close code. `1006` is "abnormal closure", the code the platform reports when no close frame was exchanged. Keyword-only in Python; a field of the options object in TypeScript. |
| `reason` | `str` / `string` | `""` | The close frame's reason text, verbatim. |
| `was_clean` / `wasClean` | `bool` / `boolean` | `False` / `false` | Whether a close handshake completed. |

### Return

A new exception instance carrying `code`, `reason` and `was_clean` / `wasClean` as attributes.

### Raises

Raises: nothing.

### Example

```python
from muxws import ConnectionClosed

closed = ConnectionClosed(code=1001, reason="server going down", was_clean=True)
print(closed.code, closed.was_clean, "|", closed)
print(ConnectionClosed().code, "|", ConnectionClosed())
```

```ts
import { ConnectionClosed } from 'muxws';

const closed = new ConnectionClosed('server going down', { code: 1001, reason: 'server going down', wasClean: true });
console.log(closed.code, closed.wasClean, '|', closed.message);
// Omitting the message leaves it empty in TypeScript; Python's twin says "connection closed".
console.log(new ConnectionClosed().code, '|', JSON.stringify(new ConnectionClosed().message));
```

## `ConnectionGoingAway`

`peer.open()` was called after a `goaway` arrived from the remote, or after this peer sent one. It is
raised **synchronously at the call site**, because `open()` is synchronous and returns no future that
could carry the failure.

### Signature

```python
class ConnectionGoingAway(MuxwsError):
    """`open()` was called after a `goaway` arrived. Raised synchronously at the call site."""
```

```ts
export class ConnectionGoingAway extends MuxwsError {
  constructor(message?: string);
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` (TypeScript) | `string \| undefined` | `undefined` | The human-readable text. |
| `*args` (Python) | `object` | none | Inherited from `Exception`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import ConnectionGoingAway, JsonCodec, Peer, ResetCode
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        await asyncio.sleep(30.0)  # keeps one stream live, so the drain window stays open

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    dialer.open({"in": "flight"})
    await asyncio.sleep(0.05)
    # `close()` sends goaway before it drains, so the socket is still open here.
    # `drain=` is a number of seconds, as a float.
    closing = asyncio.create_task(acceptor.close(ResetCode.NO_ERROR, "shutting down", drain=1.0))
    await asyncio.sleep(0.05)

    try:
        dialer.open({"too": "late"})
    except ConnectionGoingAway as exc:
        print("refused at the call site:", type(exc).__name__, "|", exc)

    closing.cancel()
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { ConnectionGoingAway, JsonCodec, Peer, ResetCode, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(async () => {
    // Never settles, so one stream stays live and the drain window stays open.
    await new Promise(() => undefined);
  });
  void dialer.serve();
  void acceptor.serve();

  dialer.open({ in: 'flight' });
  await new Promise((resolve) => setTimeout(resolve, 50));
  // `close()` sends goaway before it drains, so the socket is still open here.
  // `drainMs` is a number of milliseconds, as an integer.
  void acceptor.close({ code: ResetCode.NO_ERROR, reason: 'shutting down', drainMs: 200 });
  await new Promise((resolve) => setTimeout(resolve, 50));

  try {
    dialer.open({ too: 'late' });
  } catch (error) {
    if (error instanceof ConnectionGoingAway) console.log('refused at the call site:', error.name, '|', error.message);
  }
}

void main();
```

## `StreamAlreadyConsumed`

A `Stream` is both awaitable and async-iterable, and the first use claims it. Awaiting and iterating
the same stream, in either order, or iterating it twice raises this. It always indicates a caller
bug: one payload sequence cannot be consumed twice.

### Signature

```python
class StreamAlreadyConsumed(MuxwsError):
    """A stream was awaited and iterated, or iterated twice (WSM-API-014)."""
```

```ts
export class StreamAlreadyConsumed extends MuxwsError {
  constructor(message?: string);
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` (TypeScript) | `string \| undefined` | `undefined` | The human-readable text. |
| `*args` (Python) | `object` | none | Inherited from `Exception`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, StreamAlreadyConsumed
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        await stream.reply({"seen": payload})

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open({"one": "shot"})
    print("awaited:", await stream)
    try:
        async for _ in stream:
            pass
    except StreamAlreadyConsumed as exc:
        print("second use refused:", type(exc).__name__)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, StreamAlreadyConsumed, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(async (payload, stream) => {
    await stream.reply({ seen: payload });
  });
  void dialer.serve();
  void acceptor.serve();

  const stream = dialer.open({ one: 'shot' });
  console.log('awaited:', await stream);
  try {
    for await (const _ of stream) void _;
  } catch (error) {
    if (error instanceof StreamAlreadyConsumed) console.log('second use refused:', error.name);
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `StreamClosed`

`send()`, `end()` or `reply()` on a stream that closed **normally**. It is neither a `StreamReset`
nor a `ProtocolError`: a normal close that races a last `send()` is an expected outcome, not a
failure and not a caller bug. A caller can catch it and continue.

### Signature

```python
class StreamClosed(MuxwsError):
    """`send()`/`end()`/`reply()` on a stream that closed **normally**."""
```

```ts
export class StreamClosed extends MuxwsError {
  constructor(message?: string);
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` (TypeScript) | `string \| undefined` | `undefined` | The human-readable text. |
| `*args` (Python) | `object` | none | Inherited from `Exception`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, StreamClosed
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open({"start": True})
    await stream.end({"last": True})
    try:
        await stream.send({"one": "more"})
    except StreamClosed as exc:
        print("expected race:", type(exc).__name__, "|", exc)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, StreamClosed, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  void dialer.serve();
  void acceptor.serve();

  const stream = dialer.open({ start: true });
  // `end` takes an options object in TypeScript; the last payload is a field of it.
  await stream.end({ payload: { last: true } });
  try {
    await stream.send({ one: 'more' });
  } catch (error) {
    if (error instanceof StreamClosed) console.log('expected race:', error.name, '|', error.message);
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `CodecError`

A configuration failure. It is not a `StreamReset` and is not retryable: a retry with the same
codec configuration fails the same way.

### Signature

```python
class CodecError(MuxwsError):
    def __init__(self, message: str, *, configured: str | None = None, available: list[str] | None = None) -> None: ...
```

```ts
export class CodecError extends MuxwsError {
  readonly configured: string | null;
  readonly available: string[];

  constructor(message: string, options: { configured?: string; available?: string[] } = {});
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string` | required | The exception text. |
| `configured` | `str \| None` / `string \| undefined` | `None` / `undefined` | The codec name that was asked for. Keyword-only in Python; stored as `null` in TypeScript when omitted. |
| `available` | `list[str] \| None` / `string[] \| undefined` | `None` / `undefined` | The names that *were* registered, so the message can name both sides. Stored as `[]` when omitted. |

### Return

A new exception instance carrying `configured` and `available` as attributes.

### Raises

Raises: nothing.

### Example

```python
from muxws import CodecError

failure = CodecError("codec 'protobuf' is not registered", configured="protobuf", available=["json"])
print(failure.configured, failure.available, "|", failure)
print(CodecError("bare").configured, CodecError("bare").available)
```

```ts
import { CodecError } from 'muxws';

const failure = new CodecError("codec 'protobuf' is not registered", {
  configured: 'protobuf',
  available: ['json'],
});
console.log(failure.configured, failure.available, '|', failure.message);
console.log(new CodecError('bare').configured, new CodecError('bare').available);
```

## `CodecNotRegistered`

The configured codec name is not registered. `get_codec()` / `getCodec()` raises it **before any
socket is opened**, so a misconfigured codec fails at startup. muxws never falls back to JSON.

### Signature

```python
class CodecNotRegistered(CodecError):
    def __init__(self, message: str, *, configured: str | None = None, available: list[str] | None = None) -> None: ...
```

```ts
export class CodecNotRegistered extends CodecError {
  constructor(message: string, options: { configured?: string; available?: string[] } = {});
}
```

The constructor is `CodecError`'s, inherited unchanged in Python and forwarded in TypeScript.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string` | required | The exception text. The library's own message names the environment variable, the value found and the registered set. |
| `configured` | `str \| None` / `string \| undefined` | `None` / `undefined` | The codec name that was asked for. |
| `available` | `list[str] \| None` / `string[] \| undefined` | `None` / `undefined` | The names that were registered. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws import CodecNotRegistered, get_codec

try:
    get_codec("protobuf")
except CodecNotRegistered as exc:
    print(exc.configured, "not in", exc.available)
```

```ts
import { CodecNotRegistered, getCodec } from 'muxws';

try {
  getCodec('protobuf');
} catch (error) {
  if (error instanceof CodecNotRegistered) console.log(error.configured, 'not in', error.available);
}
```

## `CodecMismatch`

The two ends are configured with different codecs, and the WebSocket handshake was rejected. The
acceptor raises it after answering the upgrade with HTTP 400. The dialer composes it from the codec
name it offered, because a browser receives a refused handshake as a generic error with no body.

### Signature

```python
class CodecMismatch(CodecError):
    def __init__(self, message: str, *, configured: str | None = None, available: list[str] | None = None) -> None: ...
```

```ts
export class CodecMismatch extends CodecError {
  constructor(message: string, options: { configured?: string; available?: string[] } = {});
}
```

The constructor is `CodecError`'s, inherited unchanged in Python and forwarded in TypeScript.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string` | required | The exception text. |
| `configured` | `str \| None` / `string \| undefined` | `None` / `undefined` | The codec this end speaks. |
| `available` | `list[str] \| None` / `string[] \| undefined` | `None` / `undefined` | The names registered on this end. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws.subprotocol import mismatch_error

failure = mismatch_error("msgpack")
print(type(failure).__name__, "|", failure.configured)
```

```ts
// `mismatchError` is internal and not exported from the package root; the dialer composes this
// error itself (WSM-CDC-024). The example constructs one directly.
import { CodecMismatch } from 'muxws';

const failure = new CodecMismatch('the acceptor refused the muxws handshake for codec msgpack', {
  configured: 'msgpack',
});
console.log(failure instanceof CodecMismatch, '|', failure.configured);
```

## `StreamReset`

A stream ended early. The base class of every early end; it carries the reset code, the reason and
the stream id. `except StreamReset` covers every way a stream can fail to finish.

Python types the `code` attribute `ResetCode | int`: a code this generation defines is the enum
member, and an undefined code (a later generation's, or the retired 5) is a bare `int`. A reset with
an unknown code still ended the stream, so it is represented rather than rejected. TypeScript types
it `ResetCode`, a numeric enum, which can hold any number.

### Signature

```python
class StreamReset(MuxwsError):
    code: ResetCode | int = ResetCode.NO_ERROR

    def __init__(
        self,
        reason: str | None = None,
        *,
        code: ResetCode | int | None = None,
        stream_id: int | None = None,
    ) -> None: ...
```

```ts
export class StreamReset extends MuxwsError {
  readonly code: ResetCode;
  readonly reason: string | null;
  readonly streamId: number | null;

  constructor(reason?: string | null, options: { code?: ResetCode; streamId?: number | null } = {});
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | The reason text from the `reset` frame. When absent, the exception's message becomes the code's name — Python spells an undefined code as `"reset code 42"`, while TypeScript's `ResetCode[code]` is `undefined` for such a number and the message ends up empty. |
| `code` | `ResetCode \| int \| None` / `ResetCode \| undefined` | `None` / `ResetCode.NO_ERROR` | The reset code. In Python, `None` leaves the class attribute in place; the subclasses set their code through that class attribute. Keyword-only in Python. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream this ended. Keyword-only in Python; stored as `null` in TypeScript when omitted. |

### Return

A new exception instance carrying `code`, `reason` and `stream_id` / `streamId`.

### Raises

Raises: nothing. An undefined code is stored as a plain integer rather than rejected.

### Example

```python
from muxws import ResetCode, StreamReset

reset = StreamReset("the export was abandoned", code=ResetCode.CANCELLED, stream_id=7)
print(reset.code.name, reset.stream_id, "|", reset)

unknown = StreamReset(code=42, stream_id=9)
print(type(unknown.code).__name__, unknown.code, "|", unknown)
```

```ts
import { ResetCode, StreamReset } from 'muxws';

const reset = new StreamReset('the export was abandoned', { code: ResetCode.CANCELLED, streamId: 7 });
console.log(ResetCode[reset.code], reset.streamId, '|', reset.message);

const unknown = new StreamReset(null, { code: 42 as ResetCode, streamId: 9 });
console.log(unknown.code, '|', unknown.message);
```

## `StreamReset.clone()`

Python only. `send()` on a reset stream raises the stream's *stored* failure. Raising the same
instance repeatedly appends a traceback frame each time, so a caller that retries in a loop grows an
object that is never released. `clone()` returns a fresh instance of the same failure with no
traceback.

TypeScript has no equivalent: a JavaScript `Error` captures its stack once, at construction, and
rethrowing it does not extend the stack.

### Signature

```python
def clone(self) -> StreamReset: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `self` | `StreamReset` | required | The stored failure to copy. Subclass identity, attributes and message are all preserved. |

### Return

A new instance of `type(self)` with the same `code`, `reason`, `stream_id` and args, and no
traceback.

### Raises

Raises: nothing.

### Example

```python
from muxws import ResetCode, StreamTimeout

original = StreamTimeout("the deadline expired", stream_id=3)
copy = original.clone()
print(type(copy).__name__, copy.code is ResetCode.TIMEOUT, copy.stream_id, copy is original)
```

## `RemoteError`

The remote handler raised. Its code is always `APPLICATION_ERROR` (2). `reason` carries the message
and `payload` carries the structured error object the remote's `error_serializer` produced, if the
remote sent one.

### Signature

```python
class RemoteError(StreamReset):
    code = ResetCode.APPLICATION_ERROR

    def __init__(self, reason: str | None = None, *, stream_id: int | None = None, payload: Any = None) -> None: ...
```

```ts
export class RemoteError extends StreamReset {
  readonly payload: unknown;

  constructor(reason?: string | null, options: { streamId?: number | null; payload?: unknown } = {});
}
```

`RemoteError` **overrides** the constructor: it has no `code` parameter, because the code is fixed.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | The remote's message. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream failed. Keyword-only in Python. |
| `payload` | `Any` / `unknown` | `None` / `undefined` | The serialized error object from the remote. `None` in Python and `null` in TypeScript when the remote sent none. |

### Return

A new exception instance whose `code` is `ResetCode.APPLICATION_ERROR`.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, RemoteError, ResetCode
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        raise ValueError("no such account")

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    try:
        await dialer.request({"account": 1}, timeout=2.0)
    except RemoteError as exc:
        print(exc.code is ResetCode.APPLICATION_ERROR, exc.payload, "|", exc)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { Peer, JsonCodec, RemoteError, ResetCode, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(() => {
    throw new Error('no such account');
  });
  void dialer.serve();
  void acceptor.serve();

  try {
    await dialer.request({ account: 1 }, { timeoutMs: 2000 });
  } catch (error) {
    if (error instanceof RemoteError) {
      console.log(error.code === ResetCode.APPLICATION_ERROR, error.payload, '|', error.message);
    }
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `StreamTimeout`

A local deadline expired and the remote was told to stop working. Its code is always `TIMEOUT` (6).
Raised by the calls that wait: `stream.result(timeout=)` in seconds as a float,
`stream.result({ timeoutMs })` in milliseconds as an integer, and the same on `peer.request`.
`peer.open()` has no deadline of its own; it does not wait for anything.

### Signature

```python
class StreamTimeout(StreamReset):
    code = ResetCode.TIMEOUT
```

```ts
export class StreamTimeout extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {});
}
```

Python defines no `__init__` here: the constructor is `StreamReset`'s, so it also accepts a
keyword-only `code=`. Passing one overrides the class attribute and produces a `StreamTimeout` whose
code is not `TIMEOUT`; callers should not pass it. TypeScript's constructor fixes the code and has no
parameter for it.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | The reason text. When absent, the message becomes `"TIMEOUT"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; not to be passed. `None` keeps `ResetCode.TIMEOUT`. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream timed out. Keyword-only in Python. |

### Return

A new exception instance whose `code` is `ResetCode.TIMEOUT`.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, ResetCode, StreamTimeout
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        # Never answers. `closed` is set when this stream ends (WSM-API-023), so the handler returns
        # then instead of running on.
        await stream.closed.wait()

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    try:
        # 0.2 seconds, as a float - Python spells every duration in seconds.
        await dialer.request({"slow": True}, timeout=0.2)
    except StreamTimeout as exc:
        print(exc.code is ResetCode.TIMEOUT, exc.stream_id, "|", exc)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, ResetCode, type Stream, StreamTimeout, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(async (_payload: unknown, stream: Stream) => {
    // Never answers. `closed` resolves when this stream ends (WSM-API-023), so the handler returns
    // then. A timer would keep the process running after the example finishes.
    await stream.closed;
  });
  void dialer.serve();
  void acceptor.serve();

  try {
    // 200 milliseconds, as an integer - TypeScript spells every duration in milliseconds.
    await dialer.request({ slow: true }, { timeoutMs: 200 });
  } catch (error) {
    if (error instanceof StreamTimeout) console.log(error.code === ResetCode.TIMEOUT, error.streamId);
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `StreamRefused`

Not accepted and **definitively not processed**. Its code is always `REFUSED` (4). It is the only
reset after which a retry is safe without knowing whether the operation is idempotent. It is sent when
no handler is registered, when the connection is already going away, or when the open exceeds the
receiver's concurrency limit. Retry on another connection if one is available, otherwise after a
delay.

### Signature

```python
class StreamRefused(StreamReset):
    code = ResetCode.REFUSED
```

```ts
export class StreamRefused extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {});
}
```

Python defines no `__init__` here; the constructor is `StreamReset`'s and the note under
`StreamTimeout` applies.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | Why it was refused. When absent, the message becomes `"REFUSED"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; not to be passed. `None` keeps `ResetCode.REFUSED`. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream was refused. Keyword-only in Python. |

### Return

A new exception instance whose `code` is `ResetCode.REFUSED`.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, ResetCode, StreamRefused
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    # No on_stream handler is registered on the acceptor at all.
    acceptor = Peer(right, codec=codec, is_dialer=False)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    try:
        await dialer.request({"anyone": "home?"}, timeout=2.0)
    except StreamRefused as exc:
        print(exc.code is ResetCode.REFUSED, "|", exc)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, ResetCode, StreamRefused, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  // No onStream handler is registered on the acceptor at all.
  const acceptor = new Peer(right, { codec, isDialer: false });
  void dialer.serve();
  void acceptor.serve();

  try {
    await dialer.request({ anyone: 'home?' }, { timeoutMs: 2000 });
  } catch (error) {
    if (error instanceof StreamRefused) console.log(error.code === ResetCode.REFUSED, '|', error.message);
  }

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `ConnectionLost`

Synthesised locally on every stream that is live when the socket dies. Its code is
`CONNECTION_CLOSED` (9), which **must never appear on the wire**: nothing sends it, and
`stream.reset(ResetCode.CONNECTION_CLOSED)` raises `ProtocolError` instead of sending it.

It is a `StreamReset`, so one `except StreamReset` around a stream also covers socket loss.
`ConnectionClosed`, which the *connection* raises, is not a `StreamReset`, because it does not
concern a single stream.

Nothing is buffered while a peer is between sockets. A reconnect yields a new socket and an accepted
identity; no stream survives it.

### Signature

```python
class ConnectionLost(StreamReset):
    code = ResetCode.CONNECTION_CLOSED
```

```ts
export class ConnectionLost extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {});
}
```

Python defines no `__init__` here; the constructor is `StreamReset`'s and the note under
`StreamTimeout` applies.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | Why the socket died. When absent, the message becomes `"CONNECTION_CLOSED"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; not to be passed. `None` keeps `ResetCode.CONNECTION_CLOSED`. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream was lost. Keyword-only in Python. |

### Return

A new exception instance whose `code` is `ResetCode.CONNECTION_CLOSED`.

### Raises

Raises: nothing. Constructing one is always legal; only *sending* the code is not.

### Example

```python
import asyncio

from muxws import ConnectionLost, JsonCodec, Peer, ProtocolError, ResetCode
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        # Never answers. `closed` is set when this stream ends (WSM-API-023), so the handler returns
        # then instead of running on.
        await stream.closed.wait()

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open({"work": "forever"})
    await asyncio.sleep(0.05)
    try:
        await stream.reset(ResetCode.CONNECTION_CLOSED)
    except ProtocolError as exc:
        print("never on the wire:", exc)

    await left.drop()
    try:
        await stream
    except ConnectionLost as exc:
        print(exc.code is ResetCode.CONNECTION_CLOSED, exc.stream_id, "|", exc)

    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { ConnectionLost, JsonCodec, Peer, ProtocolError, ResetCode, type Stream, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(async (_payload: unknown, stream: Stream) => {
    // Never answers. `closed` resolves when this stream ends (WSM-API-023), so the handler returns
    // then. A timer would keep the process running after the example finishes.
    await stream.closed;
  });
  void dialer.serve();
  void acceptor.serve();

  const stream = dialer.open({ work: 'forever' });
  await new Promise((resolve) => setTimeout(resolve, 50));
  try {
    await stream.reset(ResetCode.CONNECTION_CLOSED);
  } catch (error) {
    if (error instanceof ProtocolError) console.log('never on the wire:', error.message);
  }

  left.drop();
  try {
    await stream;
  } catch (error) {
    if (error instanceof ConnectionLost) console.log(error.code === ResetCode.CONNECTION_CLOSED, error.streamId);
  }
}

void main();
```

## `ResetCode`

Numeric on the wire, named in both APIs. The same enumeration serves `reset` and `goaway`. There are
**nine** members: `5` is missing because it is retired, and no name maps to it.

| Value | Name | What it means |
|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, an orderly shutdown; on `reset`, "done and no longer interested". Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. What `stream.cancel()` sends. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. Surfaces locally as `RemoteError`. |
| 3 | `PROTOCOL_ERROR` | The peer violated the protocol. |
| 4 | `REFUSED` | Not accepted and definitively not processed. Surfaces locally as `StreamRefused`. |
| — | *(5 is retired)* | Retired code `STREAM_LIMIT`. The number is never reused and never sent; a peer receiving it treats it as any unknown code. |
| 6 | `TIMEOUT` | A local deadline expired. Surfaces locally as `StreamTimeout`. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message or a reassembled payload exceeded what the receiver accepts. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies. **Never appears on the wire.** Surfaces locally as `ConnectionLost`. |

An unrecognised code (a later generation's, or the retired 5) resets the named stream and leaves the
connection open. Converting the wire value directly into a closed enumeration would raise inside the
read loop and leave the peer apparently open, with every pending await hanging.

### Signature

```python
class ResetCode(IntEnum):
    NO_ERROR = 0
    CANCELLED = 1
    APPLICATION_ERROR = 2
    PROTOCOL_ERROR = 3
    REFUSED = 4
    # 5 is retired - see the class docstring.
    TIMEOUT = 6
    PAYLOAD_TOO_LARGE = 7
    INTERNAL_ERROR = 8
    CONNECTION_CLOSED = 9
```

```ts
export enum ResetCode {
  NO_ERROR = 0,
  CANCELLED = 1,
  APPLICATION_ERROR = 2,
  PROTOCOL_ERROR = 3,
  REFUSED = 4,
  // 5 is retired - see the doc comment above.
  TIMEOUT = 6,
  PAYLOAD_TOO_LARGE = 7,
  INTERNAL_ERROR = 8,
  CONNECTION_CLOSED = 9,
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `value` | `int` / `number` | required | `ResetCode(value)` in Python looks a member up by its number and raises `ValueError` for one that does not exist. TypeScript's numeric enum is not callable; index it as `ResetCode[value]` for the name, which is `undefined` for an unknown number. |

### Return

Python: the `ResetCode` member for `value`. TypeScript: `ResetCode` is a type and a value namespace,
not a function.

### Raises

Python's `ResetCode(value)` raises `ValueError` for a number no member carries, including `5`.
TypeScript raises nothing.

### Example

```python
from muxws import ResetCode

print(len(ResetCode), [member.name for member in ResetCode])
print(ResetCode.CANCELLED == 1, int(ResetCode.PAYLOAD_TOO_LARGE))
try:
    ResetCode(5)
except ValueError:
    print("5 is retired: no member maps to it")
```

```ts
import { ResetCode } from 'muxws';

const names = Object.values(ResetCode).filter((entry) => typeof entry === 'string');
console.log(names.length, names);
console.log(ResetCode.CANCELLED === 1, ResetCode.PAYLOAD_TOO_LARGE);
console.log('5 is retired:', ResetCode[5] === undefined);
```

## `exception_for_reset` / `exceptionForReset`

Builds the `StreamReset` subclass that represents a code arriving on the wire, falling back to
`StreamReset` itself for a code with no dedicated class. Four codes have one:
`APPLICATION_ERROR` → `RemoteError`, `TIMEOUT` → `StreamTimeout`, `REFUSED` → `StreamRefused`,
`CONNECTION_CLOSED` → `ConnectionLost`.

In TypeScript this is exported from the package root. In Python it is not re-exported from `muxws`;
import it from `muxws.errors`.

### Signature

```python
def exception_for_reset(
    code: ResetCode | int,
    reason: str | None = None,
    *,
    stream_id: int | None = None,
    payload: Any = None,
) -> StreamReset: ...
```

```ts
export function exceptionForReset(
  code: ResetCode,
  reason?: string | null,
  options: { streamId?: number | null } = {},
): StreamReset;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode \| int` / `ResetCode` | required | The reset code. A defined code selects its class; anything else produces a plain `StreamReset` carrying the raw number. |
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | The reason text from the frame. |
| `stream_id` / `streamId` | `int \| None` / `number \| null \| undefined` | `None` / `undefined` | Which stream ended. Keyword-only in Python; a field of the options object in TypeScript. |
| `payload` (Python only) | `Any` | `None` | The structured error object a `reset(APPLICATION_ERROR)` may carry. Meaningful only for `RemoteError` and ignored for every other code. TypeScript's options object has no `payload` field, so a `RemoteError` built here always carries `null`. |

### Return

A `StreamReset` instance — the dedicated subclass when one exists, otherwise `StreamReset` itself.

### Raises

Raises: nothing.

### Example

```python
from muxws.errors import exception_for_reset, ResetCode

for code in (ResetCode.APPLICATION_ERROR, ResetCode.REFUSED, ResetCode.PROTOCOL_ERROR, 5):
    failure = exception_for_reset(code, "because", stream_id=3)
    print(code, "->", type(failure).__name__, failure.code)
```

```ts
import { ResetCode, exceptionForReset } from 'muxws';

[ResetCode.APPLICATION_ERROR, ResetCode.REFUSED, ResetCode.PROTOCOL_ERROR, 5 as ResetCode].forEach((code) => {
  const failure = exceptionForReset(code, 'because', { streamId: 3 });
  console.log(code, '->', failure.name, failure.code);
});
```

## `default_error_serializer` / `defaultErrorSerializer`

Turns a handler's exception into the `payload` of the `reset(APPLICATION_ERROR)` frame that reports
it. This is the default. Each peer can be given its own serializer with `error_serializer=` /
`errorSerializer:` on `connect()` and `accept()`, so one process can redact on a browser-facing
connection and not on an internal one.

::: warning
The default puts the exception's class name **and its message text** on the wire. A public-facing
deployment should replace it with one that redacts: exception messages often contain file paths, SQL
and identifiers that must not reach the remote.
:::

### Signature

```python
def default_error_serializer(exc: BaseException) -> Any: ...
```

```ts
export function defaultErrorSerializer(error: unknown): unknown;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `exc` / `error` | `BaseException` / `unknown` | required | The exception the handler raised. TypeScript takes `unknown` because JavaScript permits throwing anything. |

### Return

Python returns `{"type": type(exc).__name__, "message": str(exc)}`. TypeScript returns
`{ type: error.name, message: error.message }` for an `Error`, and
`{ type: typeof error, message: String(error) }` for anything else thrown. The value must be
representable by the configured codec, because it goes on the wire as the reset frame's `payload`.

### Raises

Raises: nothing.

### Example

```python
from muxws import default_error_serializer

print(default_error_serializer(ValueError("no such account")))


def redacting_error_serializer(exc: BaseException) -> dict[str, str]:
    """What a public-facing deployment passes as error_serializer= instead."""
    return {"type": type(exc).__name__, "message": "the request could not be completed"}


print(redacting_error_serializer(ValueError("SELECT * FROM accounts WHERE id = 7")))
```

```ts
import { defaultErrorSerializer } from 'muxws';

console.log(defaultErrorSerializer(new Error('no such account')));
console.log(defaultErrorSerializer('a bare string can be thrown too'));

/** What a public-facing deployment passes as errorSerializer: instead. */
function redactingErrorSerializer(error: unknown): unknown {
  return { type: error instanceof Error ? error.name : typeof error, message: 'the request could not be completed' };
}

console.log(redactingErrorSerializer(new Error('SELECT * FROM accounts WHERE id = 7')));
```

## `TransportUrlError`

The base class for *this transport cannot open the address it was given*. It is never raised
directly: a caller receives one of its concrete subclasses, named after the transport whose URL
grammar the address violates. Catch the base when the transport a configured URL names is unknown or
irrelevant.

It does not cover every entry point. A malformed `ws+unix://` URL raises `UnixUrlError` (a
`TransportUrlError`) in Python and from `muxws/node`, but the `muxws` entry point refuses the scheme
before parsing it and raises `UnixSocketsUnsupportedError`, because that build has no transport that
can open a socket file. The handler that covers every port is
`except (TransportUrlError, TransportUnsupportedError)` in Python and `instanceof MuxwsError` in
TypeScript.

In Python it is a `ValueError` as well as a `MuxwsError`: code that does not know muxws and catches
`ValueError` around a URL catches it, and so does an application whose single handler is
`except MuxwsError`. TypeScript has one prototype chain, so the class extends `MuxwsError` and is
identified by `name`; the handler that always catches it is `instanceof MuxwsError`.

It is always raised **before the dial**, never from a failed dial. As a result, a URL whose text
contains `HTTP 400` is not reported as a refused handshake, and a bad address is never raised later
from a background reconnection.

### Signature

```python
class TransportUrlError(MuxwsError, ValueError): ...
```

```ts
export class TransportUrlError extends MuxwsError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the package root in both languages: `from muxws import TransportUrlError`,
`import { TransportUrlError } from 'muxws'`. Its subclasses are not — see the note under the tree at
the top of this page.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | required in Python, `undefined` in TypeScript | The exception text. A library-raised one quotes the address and states the rule it broke. |
| `options.cause` (TypeScript) | `unknown` | `undefined` | The underlying library's original error, so its message is preserved. Python chains with `raise … from exc` instead, which puts the original on `__cause__`. |

### Return

A new exception instance. Constructing one does not raise it.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import connect, TransportUrlError

BAD_URLS = ["ws:/nohost", "ws+unix:///run/muxws/api.sock:ws"]


async def main() -> None:
    """Two transports raise two concrete classes; `except TransportUrlError` catches both.

    Neither transport module is imported.
    """
    for url in BAD_URLS:
        try:
            await connect(url)
        except TransportUrlError as exc:
            print(f"{url} -> {type(exc).__name__}")


asyncio.run(main())
```

```ts
import 'muxws';
import { TransportUrlError } from 'muxws';
import { connect } from 'muxws/node';

// The concrete class is `WsUrlError`; this catch does not name it. `TransportUrlError` is exported
// from the package root and catches the URL errors of every transport, including third-party ones.
try {
  await connect('nonsense');
} catch (error) {
  if (!(error instanceof TransportUrlError)) throw error;
  console.log(error.name, '| framing:', String((error.cause as Error).message));
}
```

## `TransportUnsupportedError`

The base class for *this runtime cannot provide that transport*: the interpreter has no `AF_UNIX`, an
optional dependency is not installed, or the entry point's bundle does not include the transport. It
is never raised directly.

It is a separate base from `TransportUrlError` because the two require different actions.
`TransportUrlError` means *correct the address*. `TransportUnsupportedError` means *the address is
valid; change the environment the code runs in*. No retry, backoff or different URL resolves it. In
Python it is a `RuntimeError` as well as a `MuxwsError`.

A subclass raised because a package is missing **names the install command** in its message.
`ModuleNotFoundError: No module named 'websockets'` does not state that the fix is
`pip install muxws[websockets]`.

### Signature

```python
class TransportUnsupportedError(MuxwsError, RuntimeError): ...
```

```ts
export class TransportUnsupportedError extends MuxwsError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the package root in both languages: `from muxws import TransportUnsupportedError`,
`import { TransportUnsupportedError } from 'muxws'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | required in Python, `undefined` in TypeScript | The exception text. A library-raised one names the missing dependency, kernel feature or entry point, and the remedy. |
| `options.cause` (TypeScript) | `unknown` | `undefined` | The original `ERR_MODULE_NOT_FOUND` or platform error. Python uses `raise … from exc`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws import MuxwsError, TransportUnsupportedError
from muxws.transports.unix import UnixSocketsUnsupportedError

try:
    raise UnixSocketsUnsupportedError("this interpreter has no socket.AF_UNIX")
except TransportUnsupportedError as exc:
    # Caught through the shared base from the package root.
    print(type(exc).__name__, "|", exc)

print(issubclass(TransportUnsupportedError, MuxwsError), issubclass(TransportUnsupportedError, RuntimeError))
```

```ts
import { MuxwsError, TransportUnsupportedError } from 'muxws';

const failure = new TransportUnsupportedError('this build ships no filesystem transport');
console.log(failure.name, failure instanceof MuxwsError, failure instanceof TransportUnsupportedError);
```

## `UnixUrlError`

A `ws+unix://` URL that cannot be dialled: one naming no socket file, one whose request target does
not begin with `/`, or a `wss+unix://` URL, which is not a scheme muxws has. Raised from `connect()`
before any socket is opened, so it is never raised later from a background reconnection.

The `ws+unix:` grammar belongs to one transport in each port, so the class is defined in that
transport's module, not in the shared error module. Python raises it from
[`parse_unix_url`](./transports.md#parse-unix-url-python) in `muxws.transports.unix`; TypeScript
raises it from the `ws+unix:` parser behind `muxws/node`, the only entry point that can dial a socket
file.

Both ports refuse the same three shapes, so one URL gives the same result in either port's
configuration. On Windows the Python port answers two of them differently:
`socket.AF_UNIX` is checked after the `wss+unix:` refusal but before the two grammar checks, so a bad
request target or a URL naming no socket file is a `UnixSocketsUnsupportedError` there. The
TypeScript port answers `UnixUrlError` for all three on every platform.

### Signature

```python
class UnixUrlError(TransportUrlError): ...
```

```ts
export class UnixUrlError extends TransportUrlError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the transport, not the package root: `from muxws.transports.unix import UnixUrlError`,
`import { UnixUrlError } from 'muxws/node'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | required in Python, `undefined` in TypeScript | The exception text, inherited from the base. The library's own message quotes the URL and states the rule it broke. |
| `options.cause` (TypeScript) | `unknown` | `undefined` | The parser error being framed, where there was one. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import connect, MuxwsError
from muxws.transports.unix import UnixUrlError


async def main() -> None:
    try:
        await connect("ws+unix:///run/muxws/api.sock:ws")
    except UnixUrlError as exc:
        print("bad url:", exc)
    except MuxwsError:
        print("something else muxws reports")


asyncio.run(main())
```

```ts
import 'muxws';
import { connect, UnixUrlError } from 'muxws/node';

try {
  await connect('ws+unix:///run/muxws/api.sock:ws');
} catch (error) {
  if (error instanceof UnixUrlError) console.log('bad url:', error.message);
  else throw error;
}
```

## `UnixSocketsUnsupportedError`

A `ws+unix://` URL that this runtime cannot dial, however the URL is written. The two ports raise it
under different conditions; in both it means that a `ws+unix:` URL cannot be opened here.

**Python:** the interpreter has no `socket.AF_UNIX` (Windows). Raised from the URL parse, so the
error is raised from the `connect()` call, not from a background reconnect attempt. The message names
the platform, the scheme and the reason.

**TypeScript:** `connect()` from the package root received a `ws+unix:` URL. Neither a browser nor
Node's global `WebSocket` can open a filesystem socket, so the root entry point refuses the scheme
before dialling, and the message names `muxws/node` as the import that can dial it. `muxws/node`
never raises it: on Windows `net.connect({ path })` opens a named pipe instead of failing, and no
`ws+unix:` URL can address a named pipe, because `new URL()` rejects the backslashes in
`\\.\pipe\name` on every platform. A `muxws/node` dial of a POSIX path on Windows fails with an
ordinary connect error naming the path.

In both cases the call itself is valid, so the Python class is a `RuntimeError`.

### Signature

```python
class UnixSocketsUnsupportedError(TransportUnsupportedError): ...
```

```ts
export class UnixSocketsUnsupportedError extends TransportUnsupportedError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

In Python it is imported from the transport module, not the package root:
`from muxws.transports.unix import UnixSocketsUnsupportedError`. In TypeScript the root entry point
refuses the scheme, so the class is exported from there: `import { UnixSocketsUnsupportedError } from 'muxws'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | required in Python, `undefined` in TypeScript | The exception text. Python's names `socket.AF_UNIX` and points at `ws://` and `wss://`; TypeScript's names `muxws/node`. |
| `options.cause` (TypeScript) | `unknown` | `undefined` | Not set by the library: the refusal is a scheme check and wraps no underlying error. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws.transports.unix import parse_unix_url, UnixSocketsUnsupportedError

try:
    parse_unix_url("ws+unix:///run/muxws/api.sock:/ws")
except UnixSocketsUnsupportedError as exc:
    print("not on this platform:", exc)  # Only ever printed on Windows.
```

```ts
import { connect, UnixSocketsUnsupportedError } from 'muxws';

try {
  await connect('ws+unix:///run/muxws/api.sock:/ws');
} catch (error) {
  // Always reached: the root entry point cannot dial a socket file on any platform.
  if (error instanceof UnixSocketsUnsupportedError) console.log(error.message);
  else throw error;
}
```

## `WebsocketUrlError` (Python)

A `ws://` or `wss://` URL that the `websockets` library cannot turn into something dialable: one with
no hostname (`ws:/nohost`, `ws://user@/x`), one whose scheme is not `ws` or `wss`
(`http://example.com/x`), one whose port is not an integer (`ws://host:notaport/x`), or a string that
is not a URL at all. Raised from `connect()` before any socket is opened, with the `InvalidURI` or
`ValueError` that `websockets.uri.parse_uri` raised chained as `__cause__`; the message quotes that
library's wording.

It also covers the logical `ws://` URI that a `ws+unix://` URL is converted to, because
`unix_connect(uri=…)` parses that string: `ws+unix://user@/a.sock:/y` raises a `WebsocketUrlError`
about `ws://user@/y`. A malformed `ws+unix:` URL raises `UnixUrlError`: that grammar is checked
first, with the standard library only, so the result does not depend on whether `websockets` is
installed.

This class refuses `http://` and `https://` URLs. Neither TypeScript entry point refuses them: the
platform `WebSocket` and `ws` both convert those schemes to `ws:`/`wss:` and dial.

The URL is parsed before the dial, outside the handler for failed dials. That handler identifies a
refused muxws handshake by looking for a 400; if the parse ran inside it, a URL whose text contains
`HTTP 400` would be reported as a `CodecMismatch` for a connection that was never made.
[`verify_dialable_url`](./transports.md#verify-dialable-url-python) runs the parse.

### Signature

```python
class WebsocketUrlError(TransportUrlError): ...
```

Imported from the transport module, not the package root:
`from muxws.transports.websockets_ import WebsocketUrlError`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` | required | The exception text, inherited from the base. The library's own message quotes the URL and repeats what `websockets` said about it. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import connect, TransportUrlError
from muxws.transports.websockets_ import WebsocketUrlError


async def main() -> None:
    try:
        await connect("ws:/nohost")
    except WebsocketUrlError as exc:
        print("bad url:", exc)
        print("because:", type(exc.__cause__).__name__)
    print(issubclass(WebsocketUrlError, TransportUrlError))


asyncio.run(main())
```

## `WebsocketsNotInstalledError` (Python)

`connect()` was called and `import websockets` failed. One class covers a `ws://` dial and a
`ws+unix://` dial, because both import the same dependency.

**The message names the extra:** `pip install muxws[websockets]`, which the module also exports as
[`INSTALL_HINT`](./transports.md#install-hint-python). The condition is permanent and local and has
one fix; `ModuleNotFoundError: No module named 'websockets'` does not name the fix.

Only an **absent** package is converted: a `ModuleNotFoundError` whose `name` is exactly
`websockets`. Any other import failure (a broken install, a syntax error inside the package) is
re-raised unchanged, because reporting it as an uninstalled package would direct the reader to
reinstall it and would hide the message that names the actual fault. A missing *sub*module
(`websockets.asyncio`) counts as broken, not absent. `WsNotInstalledError` applies the same rule to
`ERR_MODULE_NOT_FOUND`.

`muxws.transports.websockets_` is importable when `websockets` is absent, because the module imports
nothing from it at module scope; `except WebsocketsNotInstalledError` therefore works without the
dependency. The import is in [`require_websockets`](./transports.md#require-websockets-python), which
raises this class and checks whether this process can dial without dialling.

### Signature

```python
class WebsocketsNotInstalledError(TransportUnsupportedError): ...
```

Imported from the transport module, not the package root:
`from muxws.transports.websockets_ import WebsocketsNotInstalledError`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` | required | The exception text, inherited from the base. The library's own message names `pip install muxws[websockets]`. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```python
from muxws import TransportUnsupportedError
from muxws.transports.websockets_ import WebsocketsNotInstalledError

# The class is importable whether or not `websockets` is installed.
try:
    raise WebsocketsNotInstalledError("connect() needs the websockets library: pip install muxws[websockets]")
except TransportUnsupportedError as exc:
    print(type(exc).__name__, "|", exc)
```

## `WsUrlError` (TypeScript)

The `ws` package's `WebSocket` constructor refused the URL. Thrown by the `connect()` exported from
`muxws/node`, and named after the third-party library that performs the dial, as Python's
`WebsocketUrlError` is named after `websockets`.

`ws` reports a URL it cannot parse as a `SyntaxError` (`Invalid URL: nonsense`), which is not a
`MuxwsError` and is not caught by an `instanceof MuxwsError` handler. muxws wraps it in `WsUrlError`
and keeps the original on `cause`.

A **dial** failure is not a URL failure and is never wrapped: an unreachable host rejects with
Node's `ENOTFOUND`, a refused connection with `ECONNREFUSED`, and a TLS failure with the TLS error.
`TransportUrlError` means the address could not be parsed, not that the connection failed.

### Signature

```ts
export class WsUrlError extends TransportUrlError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the entry point that ships the transport: `import { WsUrlError } from 'muxws/node'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `string \| undefined` | `undefined` | The exception text; the library's own quotes the URL and repeats what `ws` said. |
| `options.cause` | `unknown` | `undefined` | The `SyntaxError` `ws` threw. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```ts
import 'muxws';
import { connect, WsUrlError } from 'muxws/node';

try {
  await connect('nonsense');
} catch (error) {
  if (!(error instanceof WsUrlError)) throw error;
  console.log(error.name, '| ws said:', String((error.cause as Error).message));
}
```

## `WsNotInstalledError` (TypeScript)

`await import('ws')` inside the `muxws/node` dial failed because the optional peer dependency is not
installed. **The message names the install that fixes it:** `npm install ws`.

Without the class the error is `Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'ws' imported
from …/node_modules/muxws/dist/node.js`, which names a path inside `node_modules` and no remedy.

Only `ERR_MODULE_NOT_FOUND` is converted. Any other import failure (a broken install, a syntax error
inside `ws`) is rethrown unchanged, because reporting it as an absent package would direct the reader
to reinstall a package that is installed.

The import is dynamic and runs only on the dial path, so `accept()`, `serve()`, `handleProtocols()`
and `refuseMismatchedUpgrade()` do not import `ws`.

### Signature

```ts
export class WsNotInstalledError extends TransportUnsupportedError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the entry point that ships the transport:
`import { WsNotInstalledError } from 'muxws/node'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `string \| undefined` | `undefined` | The exception text; the library's own names `npm install ws`. |
| `options.cause` | `unknown` | `undefined` | The original `ERR_MODULE_NOT_FOUND` error. |

### Return

A new exception instance.

### Raises

Raises: nothing.

### Example

```ts
import { TransportUnsupportedError } from 'muxws';
import { WsNotInstalledError } from 'muxws/node';

// Raised only where `ws` is not installed; the class is importable either way, so
// `catch (e) { if (e instanceof WsNotInstalledError) }` works in both cases.
const failure = new WsNotInstalledError("the 'ws' package is not installed: npm install ws");
console.log(failure.name, failure instanceof TransportUnsupportedError, failure.message.includes('npm install ws'));
```

## See also

- [Codecs](./codec.md) — `CodecError`, `CodecNotRegistered` and `CodecMismatch` are raised by the
  codec layer.
- [Unix domain sockets](../guide/transports.md#unix-domain-sockets) — the URL grammar `UnixUrlError`
  enforces, and what each port does on Windows.
- [Errors](../guide/errors.md#writing-an-adapter-of-your-own) — what to subclass, and where to put
  it, if you are writing a transport of your own.
- [Types](./types.md) — `ErrorSerializer`, the type `error_serializer=` must satisfy.
- [Peer](./peer.md) — where `error_serializer` / `errorSerializer` is configured.
- [Stream](./stream.md) — `cancel()`, `reset()` and the calls that raise these classes.
