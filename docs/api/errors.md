---
outline: deep
---

# Errors

Every failure muxws reports is an instance of one class hierarchy per language, and the two
hierarchies are the same shape class for class. What a caller catches tells it what happened without
inspecting a message.

```
MuxwsError
├── ProtocolError
├── ConnectionClosed
├── ConnectionGoingAway
├── StreamAlreadyConsumed
├── StreamClosed
├── CodecError
│   ├── CodecNotRegistered
│   └── CodecMismatch
└── StreamReset
    ├── RemoteError
    ├── StreamTimeout
    ├── StreamRefused
    └── ConnectionLost
```

`StreamClosed` sits outside `StreamReset` on purpose: a stream that ended normally while a last
`send()` was in flight is an expected race, not a failure. `ConnectionLost` sits *inside*
`StreamReset` for the mirror reason: from a stream's point of view the socket dying is just another
way the stream ended early, so one `except StreamReset` covers every early end.

Two facts about reset codes that the numbers alone will not tell you:

- **Reset code 5 is retired.** It was `STREAM_LIMIT`. No name maps to it, it is never sent, and a
  peer that receives it treats it as any other unknown code: reset that one stream, keep the
  connection. If you are reaching for a "too many streams" code, the one you want is `REFUSED` (4).
- **Reset code 9, `CONNECTION_CLOSED`, must never appear on the wire.** It is synthesised locally, on
  every stream that was live at the instant the socket died. Sending it would tell a remote that
  *its* connection had died, which is both false and unfalsifiable — so `stream.reset()` refuses it
  with a `ProtocolError` rather than putting it on the socket.

## `MuxwsError`

The root. `except MuxwsError` catches everything this library raises and nothing it does not, which
is what an application's top-level handler wants.

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

`MuxwsError` defines no `__init__` of its own in Python, so it takes exactly what `Exception` takes.
In TypeScript it sets `name` to `'MuxwsError'`, which is what makes the class identifiable after a
structured clone or across a bundle boundary.

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
illegal value on the wire. It is a bug report, not a retryable condition.

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

The socket died. Raised by `peer.serve()` and by peer-level calls; **never** by a stream — a stream
sees `ConnectionLost` instead, so a per-stream `except StreamReset` keeps working and a
connection-level handler stays connection-level.

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
| `was_clean` / `wasClean` | `bool` / `boolean` | `False` / `false` | Whether a close handshake actually completed. |

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
raised **synchronously at the call site**, because `open()` is synchronous — there is no future to
attach the failure to.

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
    # `close()` sends goaway first and only then drains, so the socket is still up here.
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
  // `close()` sends goaway first and only then drains, so the socket is still up here.
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

A `Stream` is both awaitable and async-iterable, and the first use claims it. Awaiting a stream you
have already iterated — or iterating one twice — raises this. It is a caller bug, always: the two
shapes cannot both consume the same payload sequence.

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

`send()`, `end()` or `reply()` on a stream that closed **normally**. Deliberately neither a
`StreamReset` nor a `ProtocolError`: a normal close racing a last `send()` is an expected outcome,
not a failure and not a caller bug. Catch it and move on.

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

A configuration failure, not a stream failure and not retryable. It lives outside `StreamReset`
because retrying a misconfigured codec produces the same result forever.

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

The configured codec name was never registered. Raised **before any socket is opened**, by
`get_codec()` / `getCodec()`, which is the whole point: a deployment that believes it is running
msgpack and silently is not would otherwise never find out. There is no fallback to JSON, ever.

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

The two ends do not speak the same codec, so the WebSocket handshake was rejected. The acceptor
raises it after answering the upgrade with HTTP 400; a dialer composes it from the codec name it
offered, because a refused handshake reaches a browser as a generic error with no body.

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
// `mismatchError` is the library's own factory and is not exported from the package root - the
// dialer composes this failure for you (WSM-CDC-024). Construct one directly to see its shape.
import { CodecMismatch } from 'muxws';

const failure = new CodecMismatch('the acceptor refused the muxws handshake for codec msgpack', {
  configured: 'msgpack',
});
console.log(failure instanceof CodecMismatch, '|', failure.configured);
```

## `StreamReset`

A stream ended early. The base class of every early end, carrying the reset code, its reason and the
stream id. `except StreamReset` is the one clause that covers "this stream did not finish", however
it failed.

Python's `code` attribute is typed `ResetCode | int`: a code this generation defines arrives as the
enum member, and one it does not — a future generation's, or the retired 5 — arrives as a bare
integer. Refusing to represent an unknown code would be refusing to hear a reset that really
happened. TypeScript types it `ResetCode`, which is a numeric enum and therefore holds an unknown
number just as happily.

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
| `code` | `ResetCode \| int \| None` / `ResetCode \| undefined` | `None` / `ResetCode.NO_ERROR` | The reset code. In Python, `None` leaves the class attribute in place — which is how the subclasses pin their own code. Keyword-only in Python. |
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

A Python-only method. `send()` on a reset stream raises the stream's *stored* failure; raising the
same instance repeatedly appends a traceback frame every time, so a caller that keeps retrying grows
an object nothing ever releases. `clone()` hands back a fresh instance of the same failure with no
traceback of its own.

TypeScript has no equivalent because a JavaScript `Error` captures its stack once, at construction,
and rethrowing it does not extend anything.

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
and `payload` carries the structured error object the remote's `error_serializer` produced — if it
sent one.

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

Note that `RemoteError` **overrides** the constructor rather than inheriting it: there is no `code`
parameter, because the code is fixed.

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
Raised by the calls that wait — `stream.result(timeout=)` in seconds as a float,
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
code is not `TIMEOUT` — do not. TypeScript's constructor pins the code and offers no way to set it.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | The reason text. When absent, the message becomes `"TIMEOUT"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; leave it alone. `None` keeps `ResetCode.TIMEOUT`. |
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
        # Never answers, and waits on the stream rather than on a clock: `closed` is set the moment
        # the caller's deadline resets this stream (WSM-API-023), which is what a handler should
        # cooperate with rather than sleep through.
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
    // A handler that never answers, spelled as a wait on the stream rather than on a clock. `closed`
    // resolves the moment the caller's deadline resets this stream (WSM-API-023), so the example
    // ends when the exchange does - a timer would hold the process open long after the last line was
    // printed, and would also model a handler that ignores the cancellation it is being sent.
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

Not accepted and **definitively not processed**. Its code is always `REFUSED` (4). That guarantee is
what makes it the one reset a caller may retry blindly: no handler was registered, the connection was
already going away, or the open was beyond the receiver's own concurrency limit. Retry elsewhere if
another connection is available, otherwise after a delay.

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
`StreamTimeout` applies unchanged.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | Why it was refused. When absent, the message becomes `"REFUSED"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; leave it alone. `None` keeps `ResetCode.REFUSED`. |
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

Synthesised locally when the socket dies, on every stream that was live at that instant. Its code is
`CONNECTION_CLOSED` (9), and that code **must never appear on the wire** — nothing sends it, and
`stream.reset(ResetCode.CONNECTION_CLOSED)` raises `ProtocolError` rather than emitting it.

It is a `StreamReset` so that one `except StreamReset` around a stream covers socket death too.
`ConnectionClosed`, which the *connection* raises, is not a `StreamReset` for the same reason
inverted: it is not about any one stream.

Nothing is buffered while a peer is between sockets. A reconnect gives you a live socket and an
accepted identity, and no stream survives it.

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
`StreamTimeout` applies unchanged.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` / `string \| null \| undefined` | `None` / `undefined` | Why the socket died. When absent, the message becomes `"CONNECTION_CLOSED"`. |
| `code` (Python only) | `ResetCode \| int \| None` | `None` | Inherited from `StreamReset`; leave it alone. `None` keeps `ResetCode.CONNECTION_CLOSED`. |
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
        # Never answers, and waits on the stream rather than on a clock: `closed` is set the moment
        # the caller's deadline resets this stream (WSM-API-023), which is what a handler should
        # cooperate with rather than sleep through.
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
    // A handler that never answers, spelled as a wait on the stream rather than on a clock. `closed`
    // resolves the moment the caller's deadline resets this stream (WSM-API-023), so the example
    // ends when the exchange does - a timer would hold the process open long after the last line was
    // printed, and would also model a handler that ignores the cancellation it is being sent.
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
| — | *(5 is retired)* | Was `STREAM_LIMIT`. The number is never reused and never sent; a peer receiving it treats it as any unknown code. |
| 6 | `TIMEOUT` | A local deadline expired. Surfaces locally as `StreamTimeout`. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message or a reassembled payload exceeded what the receiver accepts. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies. **Never appears on the wire.** Surfaces locally as `ConnectionLost`. |

An unrecognised code — a future generation's, or the retired 5 — resets the named stream and leaves
the connection alive. Turning the wire value straight into a closed enumeration would raise out of
the read loop and leave the peer looking open with every await hanging.

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
it. This is the default; every peer may be given its own with `error_serializer=` /
`errorSerializer:` on `connect()` and `accept()`, which is per peer — so one process can redact on
its browser-facing connection and not on its internal one.

::: warning
The default puts the exception's class name **and its message text** on the wire. A public-facing
deployment should replace it with one that redacts: exception messages routinely contain file paths,
SQL, and identifiers the remote has no business seeing.
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

## See also

- [Codecs](./codec.md) — `CodecError`, `CodecNotRegistered` and `CodecMismatch` all come out of the
  codec seam.
- [Types](./types.md) — `ErrorSerializer`, the type `error_serializer=` must satisfy.
- [Peer](./peer.md) — where `error_serializer` / `errorSerializer` is configured.
- [Stream](./stream.md) — `cancel()`, `reset()` and the calls that raise these classes.
