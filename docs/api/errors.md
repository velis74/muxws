---
outline: deep
---

# Errors

Every failure muxws reports is an instance of one class hierarchy per language, and the shared part
of the two hierarchies is the same shape class for class. What a caller catches tells it what
happened without inspecting a message.

The **shared** classes — everything down to and including the two `Transport…` bases — are importable
from the package root in both languages. The classes indented under those bases are **transport
errors**: each one belongs to a single transport, is imported from that transport's own module, and
is never re-exported from the root. The right-hand column says where each of those comes from.

```
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

**Why a transport error is not importable from `muxws`.** The rule follows from the adapter seam.
`SocketAdapter` is public and third parties are expected to write adapters for transports this
repository does not ship — and a third party cannot add a class to `muxws/errors.py` or to
`ts/errors.ts`. A convention that required a root export would be one only this repository could
follow, so the convention is the other one: subclass a shared base, keep the class beside the code
that raises it, and let `except TransportUrlError` be what an application writes when it does not know
or care which transport was asked for the address. Both bases are plain classes with no dependency of
their own, which is what makes that `except` writable in a process that cannot even import the
transport that raised.

TypeScript has no module path below its entry points — `package.json` publishes `muxws`, `muxws/node`
and `muxws/msgpack`, and nothing finer — so the same rule reads there as "exported from the entry
point that ships that transport, and from no other". That is why `UnixSocketsUnsupportedError` comes
from `muxws` — the entry point that refuses the scheme is the one dialling with the platform
`WebSocket` — while `WsUrlError` and `WsNotInstalledError` come from `muxws/node`, and why the browser
bundle contains neither of the `ws`-flavoured names.

One asymmetry is worth knowing before you write the handler: the `connect()` exported from `muxws`
does not frame a URL the platform's own `WebSocket` constructor rejects. A malformed URL arrives from
that entry point as the runtime's `DOMException` — jsdom says `The URL 'nonsense' is invalid.`, undici
says `TypeError: Invalid URL` — and neither is a `MuxwsError`. `muxws/node` answers a
`TransportUrlError` for every one of those, and Python for every one its parsers reach: the exception
is an authority with an unbalanced `[` (`ws://[::1`), which `urllib.parse.urlsplit` refuses before
either check and which therefore leaves `connect()` as a bare `ValueError`.

`except MuxwsError` still catches every one of them without importing anything, and so does
`except TransportUrlError` / `except TransportUnsupportedError` for the half a caller usually wants.

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

The socket died. Raised by `peer.ping()` when the pong deadline expires, and by a socket adapter's
`receive` / `send_text` / `send_bytes` when it is asked to work a socket that is gone. `peer.serve()`
catches it, kills the peer and **returns**, so a read loop that ends does not raise into whatever
awaited it. It is **never** raised by a stream — a stream sees `ConnectionLost` instead, so a
per-stream `except StreamReset` keeps working and a connection-level handler stays connection-level.

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

## `TransportUrlError`

The base for *this transport cannot open the address it was given*. Never raised directly: what
reaches a caller is always one of the concrete classes below it, named after the transport whose
grammar the address broke. Catch this one when you do not know, or do not care, which transport a
configured URL names.

It is not, on its own, enough for every entry point. A bad `ws+unix://` URL is a `UnixUrlError` — a
`TransportUrlError` — in Python and behind `muxws/node`, but the `muxws` entry point refuses the
scheme before it parses the grammar and answers `UnixSocketsUnsupportedError`, because that build
ships no transport that could open a socket file however the URL is spelled. The handler that covers
every port is `except (TransportUrlError, TransportUnsupportedError)` in Python and
`instanceof MuxwsError` in TypeScript.

In Python it is a `ValueError` as well as a `MuxwsError`. Both halves are load-bearing: a caller who
never heard of this library is already catching `ValueError` around a URL it typed, and an
application whose one handler is `except MuxwsError` must not have a bad address leak through it. In
TypeScript there is one prototype chain, so the class extends `MuxwsError` and carries its identity
in `name`; the handler that must not be escapable there is `instanceof MuxwsError`.

It is always raised **before the dial**, never out of a failed one. That ordering is what keeps a URL
whose text happens to contain `HTTP 400` from being reported as a refused handshake, and it is what
stops a bad address from resurfacing hours later out of a background reconnection.

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
| `options.cause` (TypeScript) | `unknown` | `undefined` | The underlying library's own error, kept so its wording survives the translation. Python chains with `raise … from exc` instead, which puts the original on `__cause__`. |

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
    """Two transports, two grammars, two concrete classes - and one `except` that covers both.

    Written without importing either transport module, which is the whole point of the shared base.
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

// The concrete class is `WsUrlError`, and this catch never names it: `TransportUrlError` comes from
// the package root and is the same `except` over every transport, including ones muxws does not ship.
try {
  await connect('nonsense');
} catch (error) {
  if (!(error instanceof TransportUrlError)) throw error;
  console.log(error.name, '| framing:', String((error.cause as Error).message));
}
```

## `TransportUnsupportedError`

The base for *this runtime cannot provide that transport at all*: no `AF_UNIX` in the interpreter, an
optional dependency that was never installed, an entry point whose bundle deliberately does not carry
the transport. Never raised directly.

It is a separate base from `TransportUrlError` rather than a flag on it because the two ask the
reader for different actions. `TransportUrlError` means *retype the address*.
`TransportUnsupportedError` means *the address is fine, change where or how you are running* — and no
retry, no backoff and no different URL can turn one into the other. In Python it is a `RuntimeError`
as well as a `MuxwsError`, which says exactly that to a caller who never heard of muxws.

A subclass raised because a package is missing **names the install that fixes it**. That is the whole
value of the class: `ModuleNotFoundError: No module named 'websockets'` is what the interpreter
already said, and it does not tell the reader that the answer is `pip install muxws[websockets]`.

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
    # One `except` for "this deployment cannot do that", written without importing the transport
    # that could not be provided.
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
not begin with `/`, or a `wss+unix://` URL, which is not a scheme muxws has. Raised out of
`connect()` before any socket is touched, and therefore before the first dial attempt, so it cannot
reappear later out of a background reconnection.

It is the `ws+unix:` **grammar's** error, and that grammar belongs to one transport in each port, so
the class lives with the transport rather than in the shared error module. Python raises it from
[`parse_unix_url`](./transports.md#parse-unix-url-python) in `muxws.transports.unix`; TypeScript
raises it from the `ws+unix:` parser behind `muxws/node`, which is the only entry point that can dial
a socket file at all.

Both ports refuse the same three shapes, so a deployment can paste one URL into either port's
configuration and get the same answer. On Windows the Python port answers two of them differently:
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

A `ws+unix://` URL that this runtime cannot dial at all — as opposed to one it will not dial because
of how it is written. The two ports reach it from different directions, and the sentence to the
reader is the same in both: *a `ws+unix:` URL cannot be opened here.*

**Python:** the interpreter has no `socket.AF_UNIX`, which means Windows. Raised from the URL parse,
so the message names the platform, the scheme and the reason, and the failure arrives out of the
`connect()` call rather than out of a background reconnect attempt.

**TypeScript:** `connect()` from the package root was given a `ws+unix:` URL. Neither a browser nor
Node's global `WebSocket` can open a filesystem socket, so the root entry point refuses the scheme up
front and the message names `muxws/node` as the import that can dial it. `muxws/node` has no
counterpart on any platform: `net.connect({ path })` opens a named pipe on Windows rather than
failing, and no `ws+unix:` URL can address a named pipe anyway, because `new URL()` rejects the
backslashes in `\\.\pipe\name` everywhere. A `muxws/node` dial of a POSIX-looking path on Windows
fails with an ordinary connect error naming the path it tried.

Nothing about the call was wrong in either case, which is why the Python class is a `RuntimeError`.

### Signature

```python
class UnixSocketsUnsupportedError(TransportUnsupportedError): ...
```

```ts
export class UnixSocketsUnsupportedError extends TransportUnsupportedError {
  constructor(message?: string, options?: { cause?: unknown });
}
```

Imported from the transport that refuses, not the package root in Python:
`from muxws.transports.unix import UnixSocketsUnsupportedError`. In TypeScript the refusing transport
*is* the root entry point's: `import { UnixSocketsUnsupportedError } from 'muxws'`.

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `message` | `str` / `string \| undefined` | required in Python, `undefined` in TypeScript | The exception text. Python's names `socket.AF_UNIX` and points at `ws://` and `wss://`; TypeScript's names `muxws/node`. |
| `options.cause` (TypeScript) | `unknown` | `undefined` | Unused by the library's own raise — the refusal is a scheme check, not a framed failure. |

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
  // Always taken: the root entry point cannot dial a socket file on any platform.
  if (error instanceof UnixSocketsUnsupportedError) console.log(error.message);
  else throw error;
}
```

## `WebsocketUrlError` (Python)

A `ws://` or `wss://` URL that the `websockets` library cannot turn into something dialable: one with
no hostname (`ws:/nohost`, `ws://user@/x`), one whose scheme is not `ws` or `wss`
(`http://example.com/x`), one whose port is not an integer (`ws://host:notaport/x`), or a string that
is not a URL at all. Raised from `connect()` before any socket is opened, with the `InvalidURI` or
`ValueError` that `websockets.uri.parse_uri` produced chained as `__cause__`, so the underlying
library's own wording is framed rather than replaced.

It also covers the logical `ws://` URI that a `ws+unix://` URL is turned into, because that string is
what `unix_connect(uri=…)` parses: `ws+unix://user@/a.sock:/y` is a `WebsocketUrlError` about
`ws://user@/y`. A malformed `ws+unix:` URL is still a `UnixUrlError` — that grammar is checked first,
and by stdlib alone, so it answers the same way whether or not `websockets` is installed.

An `http://` or `https://` URL is one of the shapes this class refuses. Neither TypeScript port
refuses it: the platform `WebSocket` and `ws` both upgrade those schemes to `ws:`/`wss:` and dial.

The parse happens before the dial and outside the failed-dial handler, which is what keeps the two
apart: that handler decides whether a failure was a refused muxws handshake by looking for a 400, and
a URL whose own text contains `HTTP 400` would otherwise be reported as a `CodecMismatch` on a
connection that was never made. [`verify_dialable_url`](./transports.md#verify-dialable-url-python)
is the function that runs it.

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

`connect()` was called and `import websockets` failed. One class covers both arms — a `ws://` dial
and a `ws+unix://` dial fail on the identical import — because it is the identical dependency.

**The message names the extra:** `pip install muxws[websockets]`, which the module also exports as
[`INSTALL_HINT`](./transports.md#install-hint-python) so the remedy is spelled in one place. That is
the whole reason the class exists: the condition is permanent, local, and has exactly one fix, and
`ModuleNotFoundError: No module named 'websockets'` names none of it.

Only the package being **absent** is claimed — a `ModuleNotFoundError` whose `name` is exactly
`websockets`. Any other import failure, a broken install or a syntax error inside the package, is
re-raised untouched, because a corrupt package reported as an uninstalled one sends the reader to
reinstall something they already have and throws away the only message naming the real fault. A
missing *sub*module (`websockets.asyncio`) counts as broken, not absent. `WsNotInstalledError` narrows
identically, on `ERR_MODULE_NOT_FOUND`.

`muxws.transports.websockets_` stays importable when `websockets` is absent — nothing is imported at
module scope — which is what makes `except WebsocketsNotInstalledError` writable at all. A module that
had to import its own dependency to define the class would raise the very `ImportError` the class
exists to replace. The import itself lives in
[`require_websockets`](./transports.md#require-websockets-python), which is where this class is raised
and the only way to ask whether this process can dial without dialling.

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

# The class is importable whether or not `websockets` is - which is the point of it.
try:
    raise WebsocketsNotInstalledError("connect() needs the websockets library: pip install muxws[websockets]")
except TransportUnsupportedError as exc:
    print(type(exc).__name__, "|", exc)
```

## `WsUrlError` (TypeScript)

The `ws` package's `WebSocket` constructor refused the URL. Thrown by the `connect()` exported from
`muxws/node`, and named after the third-party library that owns that dial, exactly as Python's
`WebsocketUrlError` is named after `websockets`.

`ws` reports a URL it cannot read as a real `SyntaxError` — `Invalid URL: nonsense` — which is not a
`MuxwsError` and is therefore invisible to an application's one handler. It is framed here, with the
original on `cause` so the wording survives.

A **dial** failure is not a URL failure and is never wrapped: an unreachable host still rejects with
Node's own `ENOTFOUND`, a refused connection with `ECONNREFUSED`, and a TLS failure with whatever TLS
said. `TransportUrlError` means the address could not be read, not that nothing answered at it.

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

Without the class the reader gets `Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'ws' imported
from …/node_modules/muxws/dist/node.js` — a path they did not write, in a package they did not
install, naming no remedy.

Only `ERR_MODULE_NOT_FOUND` is claimed. Any other import failure — a broken install, a syntax error
inside `ws` itself — is rethrown untouched, because a corrupt package reported as an absent one sends
the reader to reinstall something that is already there.

The import is dynamic and lives on the dial path alone, which is why `accept()`, `serve()`,
`handleProtocols()` and `refuseMismatchedUpgrade()` all keep working in a process that never dials.

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

// Reachable only in a checkout without `ws`; the class is importable either way, which is what
// makes `catch (e) { if (e instanceof WsNotInstalledError) }` writable at all.
const failure = new WsNotInstalledError("the 'ws' package is not installed: npm install ws");
console.log(failure.name, failure instanceof TransportUnsupportedError, failure.message.includes('npm install ws'));
```

## See also

- [Codecs](./codec.md) — `CodecError`, `CodecNotRegistered` and `CodecMismatch` all come out of the
  codec seam.
- [Unix domain sockets](../guide/transports.md#unix-domain-sockets) — the URL grammar `UnixUrlError`
  enforces, and what each port does on Windows.
- [Errors](../guide/errors.md#writing-an-adapter-of-your-own) — what to subclass, and where to put
  it, if you are writing a transport of your own.
- [Types](./types.md) — `ErrorSerializer`, the type `error_serializer=` must satisfy.
- [Peer](./peer.md) — where `error_serializer` / `errorSerializer` is configured.
- [Stream](./stream.md) — `cancel()`, `reset()` and the calls that raise these classes.
