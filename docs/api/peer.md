---
outline: deep
---

# Peer

A `Peer` is one end of one WebSocket. Each language has one peer type, and both ends use it: a server
pushes a stream to a browser with the same `open()` the browser uses, and the browser receives it
with the same `on_stream` handler the server uses. Server push is a client request with the roles
swapped, with the same correlation and the same cancellation.

A peer is obtained from [`connect()`](./connect.md) or [`accept()`](./accept.md). The constructor is
public and the in-memory examples below use it; over a real socket, use the factories, which resolve
the codec and perform the handshake.

Properties that apply throughout this page:

- **`peer.id` is a per-process prefix plus a per-connection counter.** It is not unique across
  processes, and it **changes** when a reconnected peer takes a new socket.
- **`open()` takes no deadline.** Deadlines are parameters of the calls that wait: `stream.result(timeout=)`
  in seconds, `stream.result({ timeoutMs })` in milliseconds, and the same on `request()`.

Durations are seconds as floats in Python and milliseconds as integers in TypeScript, at every
occurrence.

## `Peer()` (Python)

Construct a peer over an already-open socket. `connect()` and `accept()` call this for you.

### Signature

```python
def __init__(
    self,
    socket: SocketAdapter,
    *,
    codec: Codec,
    is_dialer: bool,
    error_serializer: ErrorSerializer | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `SocketAdapter` | required | The transport. The peer accesses the connection only through this adapter. |
| `codec` | `Codec` | required | The codec both ends agreed on at the handshake. The peer does not negotiate it and never changes it. |
| `is_dialer` | `bool` | required | Which parity this peer allocates: the dialer takes odd stream ids, the acceptor even ones. If both ends pass the same value, both allocate the same ids, starting with the same first id. |
| `error_serializer` | `ErrorSerializer \| None` | `None` | Turns a handler's exception into the `reset(APPLICATION_ERROR)` payload. `None` means `default_error_serializer`, which puts the class name and message on the wire. |
| `max_frame_bytes` | `int` | `MAX_FRAME_BYTES` (65536) | For tests only. A cap too small to hold an envelope plus one indivisible unit raises `ProtocolError`. |
| `max_payload_bytes` | `int` | `67_108_864` | This peer's own reassembly limit in bytes of encoded output. Never announced. |
| `max_concurrent_streams` | `int` | `100` | How many streams the remote may hold open here at once. Never announced. |

### Return

`None` — it is a constructor. The instance it initialises is open (`is_open` is true) but not
serving: nothing is read until something awaits `peer.serve()`.

### Raises

- `ProtocolError` — `max_frame_bytes` cannot hold an envelope plus one indivisible unit of payload.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True, max_concurrent_streams=8)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)
    print(dialer.is_dialer, acceptor.is_dialer, dialer.is_open, len(dialer.streams))


asyncio.run(main())
```

## `new Peer()` (TypeScript)

### Signature

```ts
constructor(socket: SocketAdapter, options: PeerOptions);
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `SocketAdapter` | required | The transport. |
| `options` | `PeerOptions` | required | `codec` and `isDialer` are mandatory; the rest are the local limits. See [`PeerOptions`](#peeroptions-typescript). |

### Return

A `Peer`. It is open and not serving; nothing is read until `peer.serve()` is called.

### Raises

- `ProtocolError` — `maxFrameBytes` cannot hold an envelope plus one indivisible unit of payload.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true, maxConcurrentStreams: 8 });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });
console.log(dialer.isDialer, acceptor.isDialer, dialer.isOpen, dialer.streams.size);
```

## `PeerOptions` (TypeScript)

The constructor's options object. It mirrors `Peer.__init__`'s keyword arguments.

### Signature

```ts
export interface PeerOptions {
  codec: Codec;
  isDialer: boolean;
  errorSerializer?: ErrorSerializer;
  maxFrameBytes?: number;
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `codec` | `Codec` | required | The codec both ends agreed on at the handshake. |
| `isDialer` | `boolean` | required | Odd stream ids when true, even when false. |
| `errorSerializer` | `ErrorSerializer` | `defaultErrorSerializer` | Turns a handler's failure into the `reset(APPLICATION_ERROR)` payload. |
| `maxFrameBytes` | `number` | `MAX_FRAME_BYTES` (65536) | Lowers the protocol frame cap; for tests only. |
| `maxPayloadBytes` | `number` | `DEFAULT_MAX_PAYLOAD_BYTES` (67108864) | This peer's own reassembly limit in bytes. |
| `maxConcurrentStreams` | `number` | `DEFAULT_MAX_CONCURRENT_STREAMS` (100) | How many streams the remote may hold open here. |

### Return

None — `PeerOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type PeerOptions } from 'muxws';

const options: PeerOptions = {
  codec: new JsonCodec(),
  isDialer: true,
  maxPayloadBytes: 1_048_576,
  maxConcurrentStreams: 16,
};

const [socket] = memoryPair();
const peer = new Peer(socket, options);
console.log(options.maxPayloadBytes, options.maxConcurrentStreams, peer.isDialer);
```

## `peer.open()` (Python)

Open a stream. **Synchronous**: it returns a `Stream` without suspending, and it never queues waiting
for capacity.

### Signature

```python
def open(self, payload: Any = None, *, headers: dict[str, Any] | None = None, end: bool = False) -> Stream:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `None` | The opening payload, encoded by the codec and fragmented automatically if it exceeds the frame cap. `None` puts a `null` payload on the wire. |
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on the `open` frame, as the peer's open interceptors leave it ([`intercept_open()`](#peer-intercept-open-python)). muxws does not interpret it; the remote handler sees it as `stream.headers`. Credentials go in the upgrade request, not here. |
| `end` | `bool` | `False` | Send `end` on the `open` frame, which half-closes this side immediately: the request is complete and only the response is still to come. `end` is a flag on a frame, never a frame of its own. |

### Return

`Stream` — live and already enqueued. It is both awaitable (the first payload) and async-iterable
(every payload); whichever you use first claims it, and the other then raises
`StreamAlreadyConsumed`.

### Raises

All raised synchronously. None is raised for the number of open streams.

- `ConnectionLost` — `is_open` is false: the peer is between sockets, closed, or a dialer still
  waiting for its hello acknowledgement. Nothing is buffered for a later socket.
- `ConnectionGoingAway` — a `goaway` has arrived, or this peer has sent one, or the connection's
  stream ids are exhausted.
- whatever an open interceptor raises, unchanged (see [`intercept_open()`](#peer-intercept-open-python)).
- `TypeError` — an open interceptor returned an awaitable, or the interceptors left a key that is
  not a `str`.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        for index in range(3):
            await stream.send({"chunk": index, "of": payload})
        await stream.end()

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("report", headers={"trace": "abc"}, end=True)
    print(stream.id)
    async for chunk in stream:
        print(chunk)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.open()` (TypeScript)

### Signature

```ts
open<T = unknown>(payload?: unknown, options?: OpenOptions): Stream<T>;
open<T = unknown>(options: OpenOptions): Stream<T>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | `null` | The opening payload. A lone argument is read as the options object **only** when it has at least one own key and every own key is `payload`, `headers` or `end`; `open({})` is therefore a payload of `{}`, and a caller who means "no arguments" writes `open()`. |
| `options` | `OpenOptions` | `{}` | `payload`, `headers`, `end`. See [`OpenOptions`](#openoptions-typescript). |

### Return

`Stream<T>` — live and already enqueued, `PromiseLike` and `AsyncIterable` at once. The first use
claims it.

### Raises

Throws, synchronously:

- `ConnectionLost` — `isOpen` is false: the peer is between sockets, closed, or a dialer still waiting
  for its hello acknowledgement.
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the stream ids are exhausted.
- whatever an open interceptor throws, unchanged (see [`interceptOpen()`](#peer-interceptopen-typescript)).
- `TypeError` — an open interceptor returned a thenable, or the interceptors left a symbol key.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  for (const index of [0, 1, 2]) await stream.send({ chunk: index, of: payload });
  await stream.end();
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('report', { headers: { trace: 'abc' }, end: true });
console.log(stream.id);
for await (const chunk of stream) console.log(chunk);
await dialer.close({ drainMs: 100 });
```

## `OpenOptions` (TypeScript)

`open()`'s options object. It has **no** `timeoutMs`, because `open()` does not wait. The calls that
wait are `stream.result({ timeoutMs })` and
`peer.request(payload, { timeoutMs })`.

### Signature

```ts
export interface OpenOptions {
  payload?: unknown;
  headers?: Record<string, unknown>;
  end?: boolean;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | `null` | The opening payload, when it is given in the options object rather than positionally. |
| `headers` | `Record<string, unknown>` | `null` | Per-stream metadata on the `open` frame, as the peer's open interceptors leave it ([`interceptOpen()`](#peer-interceptopen-typescript)); the remote handler reads it as `stream.headers`. |
| `end` | `boolean` | `false` | Half-close this side on the `open` frame itself. |

### Return

None — `OpenOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, type OpenOptions, Peer } from 'muxws';

const options: OpenOptions = { payload: { report: 'daily' }, headers: { trace: 'abc' }, end: true };

const [socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
const stream = peer.open(options);
console.log(stream.id, options.end, Object.keys(stream.headers ?? {}).length);
```

## `peer.notify()` (Python)

One-way push: open a stream carrying the payload, end it, and return `None`.

### Signature

```python
async def notify(self, payload: Any = None, *, headers: dict[str, Any] | None = None) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `None` | The payload. Sent on the `open` frame, with `end` set. |
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on that frame, as the peer's open interceptors leave it. |

### Return

`None`. No stream handle is returned, because a notification has no reply to wait for.

### Raises

The exceptions of `open()`, under the same conditions:

- `ConnectionLost` — `is_open` is false (see [`open()`](#peer-open-python)).
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the ids are exhausted.
- whatever an open interceptor raises, and `TypeError` for an awaitable it returned or a non-`str`
  key it left (see [`intercept_open()`](#peer-intercept-open-python)).

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    seen: asyncio.Future[object] = asyncio.get_running_loop().create_future()

    async def handler(payload: object, stream: Stream) -> None:
        print("headers:", stream.headers)
        seen.set_result(payload)

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    print(await dialer.notify({"event": "cache-invalidated"}, headers={"topic": "cache"}))
    print(await asyncio.wait_for(seen, 5.0))

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.notify()` (TypeScript)

### Signature

```ts
async notify(payload?: unknown, options?: { headers?: Record<string, unknown> }): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | `null` | The payload, sent on the `open` frame with `end` set. Unlike `open()`, a lone object here is always the payload; the options object is the second argument. |
| `options.headers` | `Record<string, unknown>` | none | Per-stream metadata on that frame, as the peer's open interceptors leave it. |

### Return

`Promise<void>`. No stream handle is returned; the stream is consumed internally, and a failure on it
is reported to `peer.onError`.

### Raises

Rejects with the errors `open()` throws: `ConnectionLost`, `ConnectionGoingAway`, whatever an open
interceptor throws, and `TypeError` for a thenable it returned or a symbol key it left (see
[`interceptOpen()`](#peer-interceptopen-typescript)). `notify` is an `async` method, so these arrive
as a **rejection**, not as a synchronous throw: `peer.notify(x).catch(...)` catches them, and a
`try`/`catch` around an un-awaited `peer.notify(x)` does not.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

const seen = new Promise<unknown>((resolve) => {
  acceptor.onStream((payload: unknown, stream: Stream) => {
    console.log('headers:', stream.headers);
    resolve(payload);
  });
});

void dialer.serve();
void acceptor.serve();

console.log(await dialer.notify({ event: 'cache-invalidated' }, { headers: { topic: 'cache' } }));
console.log(await seen);
await dialer.close({ drainMs: 100 });
```

## `peer.request()` (Python)

The unary call: `open(payload, end=True)`, awaited until the stream ends.

Unlike `await stream`, it raises `ProtocolError` when the remote sends a second payload, instead of
discarding the extra values.

### Signature

```python
async def request(
    self,
    payload: Any = None,
    *,
    headers: dict[str, Any] | None = None,
    timeout: float | None = None,
) -> Any:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `None` | The request payload. |
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on the `open` frame, as the peer's open interceptors leave it. |
| `timeout` | `float \| None` — seconds | `None` | The deadline, in seconds. `None` waits forever. When it expires, `reset(TIMEOUT)` is sent to the remote **before** `StreamTimeout` is raised, so the remote stops processing the stream. |

### Return

`Any` — the single payload the remote sent.

### Raises

- `ConnectionLost` — `is_open` is false when the call is made (see [`open()`](#peer-open-python)),
  or the socket closed while waiting.
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the ids are exhausted.
- whatever an open interceptor raises, and `TypeError` for an awaitable it returned or a non-`str`
  key it left (see [`intercept_open()`](#peer-intercept-open-python)).
- `StreamTimeout` — `timeout` seconds expired. `reset(TIMEOUT)` has already been sent to the remote.
- `ProtocolError` — the remote sent more than one payload (its stream is reset with
  `PROTOCOL_ERROR`), or ended the stream without sending one.
- `RemoteError` — the remote handler raised; `.payload` carries whatever its `error_serializer`
  produced.
- `StreamRefused` — the remote refused the stream without running anything: no handler, going away,
  or its concurrency limit reached. Safe to retry elsewhere.
- `StreamReset` — any other reset code, including one this generation does not define.
- `asyncio.CancelledError` — the calling task was cancelled. The remote has already been sent
  `reset(CANCELLED)` and the stream is closed, with or without a `timeout`.

### Example

```python
import asyncio

from muxws import Peer, ProtocolError, RemoteError, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        if payload == "boom":
            raise RuntimeError("the handler failed")
        if payload == "two":
            await stream.send({"first": True})
            await stream.end({"second": True})
            return
        await stream.reply({"echo": payload})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    print(await dialer.request({"n": 1}, headers={"trace": "abc"}, timeout=5.0))

    try:
        await dialer.request("boom", timeout=5.0)
    except RemoteError as exc:
        print("remote error:", exc.payload)

    try:
        await dialer.request("two", timeout=5.0)
    except ProtocolError as exc:
        print("protocol error:", str(exc)[:38])

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.request()` (TypeScript)

### Signature

```ts
request<T = unknown>(payload?: unknown, options?: RequestOptions): Promise<T>;
request<T = unknown>(options: RequestOptions): Promise<T>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | `null` | The request payload. A lone argument is read as options only when every own key is `payload`, `headers`, `end` or `timeoutMs` and there is at least one. |
| `options` | `RequestOptions` | `{}` | `OpenOptions` plus `timeoutMs`. See [`RequestOptions`](#requestoptions-typescript). |

### Return

`Promise<T>` — the single payload the remote sent.

### Raises

Rejects with the same set as Python: `ConnectionLost`, `ConnectionGoingAway`, `StreamTimeout`,
`ProtocolError` (more than one payload, or none at all), `RemoteError`, `StreamRefused`,
`StreamReset`, plus whatever an open interceptor throws and `TypeError` for a thenable it returned
or a symbol key it left (see [`interceptOpen()`](#peer-interceptopen-typescript)). `request` is an
`async` method, so all of these arrive as a **rejection**, including the errors thrown by the
synchronous `open()` inside it; `request()` never throws synchronously.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ProtocolError, RemoteError, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  if (payload === 'boom') throw new Error('the handler failed');
  if (payload === 'two') {
    await stream.send({ first: true });
    await stream.end({ payload: { second: true } });
    return;
  }
  await stream.reply({ echo: payload });
});

void dialer.serve();
void acceptor.serve();

console.log(await dialer.request({ n: 1 }, { headers: { trace: 'abc' }, timeoutMs: 5000 }));

try {
  await dialer.request('boom', { timeoutMs: 5000 });
} catch (error) {
  if (error instanceof RemoteError) console.log('remote error:', error.payload);
}

try {
  await dialer.request('two', { timeoutMs: 5000 });
} catch (error) {
  if (error instanceof ProtocolError) console.log('protocol error:', error.message.slice(0, 38));
}

await dialer.close({ drainMs: 100 });
```

## `RequestOptions` (TypeScript)

`OpenOptions` plus `timeoutMs`, the deadline of `request()`.

### Signature

```ts
export interface RequestOptions extends OpenOptions {
  timeoutMs?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `timeoutMs` | `number` — milliseconds | none (waits forever) | The deadline in milliseconds. On expiry the remote gets `reset(TIMEOUT)` before the caller gets `StreamTimeout`. |
| `payload` | `unknown` | `null` | Inherited from `OpenOptions`. |
| `headers` | `Record<string, unknown>` | `null` | Inherited from `OpenOptions`. |
| `end` | `boolean` | `false` | Inherited from `OpenOptions`. `request()` sets `end` itself, so this field is not read by it. |

### Return

None — `RequestOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type RequestOptions, type Stream } from 'muxws';

const options: RequestOptions = { headers: { trace: 'abc' }, timeoutMs: 5000 };

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });
acceptor.onStream(async (payload: unknown, stream: Stream) => stream.reply({ echo: payload }));

void dialer.serve();
void acceptor.serve();

console.log(options.timeoutMs, await dialer.request('hi', options));
await dialer.close({ drainMs: 100 });
```

## `peer.on_stream()` (Python)

Register the incoming-stream handler. A peer has at most one; a second call replaces the first and
logs a warning.

A peer with no handler answers every incoming `open` with `reset(REFUSED, "no on_stream handler")`.
`connect(on_stream=...)` registers the handler before the hello is sent, so a stream the acceptor
opens as soon as it receives the hello is not refused.

### Signature

```python
def on_stream(self, handler: StreamHandler) -> StreamHandler:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `StreamHandler` | required | `(payload, stream)`, sync or async. The payload is the opening payload, already reassembled. Returning without ending the stream ends it implicitly; raising resets it with `APPLICATION_ERROR`, never with `REFUSED`, which states that no work was done. |

### Return

`StreamHandler` — the handler itself, so `@peer.on_stream` works as a decorator.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    @acceptor.on_stream
    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply({"seen": payload, "headers": stream.headers})

    print(handler.__name__)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    print(await dialer.request("hello", headers={"trace": "abc"}, timeout=5.0))

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.onStream()` (TypeScript)

### Signature

```ts
onStream(handler: StreamHandler): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `StreamHandler` | required | `(payload, stream) => void \| Promise<void>`. One per peer; a second call replaces the first and logs a warning. Returning without ending the stream ends it implicitly; throwing resets it with `APPLICATION_ERROR`. |

### Return

`void`. Unlike the Python method, it does not return the handler, so it cannot be used as a decorator.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  await stream.reply({ seen: payload, headers: stream.headers });
});

void dialer.serve();
void acceptor.serve();

console.log(await dialer.request('hello', { headers: { trace: 'abc' }, timeoutMs: 5000 }));
await dialer.close({ drainMs: 100 });
```

## `peer.on_close()` (Python)

Register a handler for socket loss. Every registered handler is called. Handlers are isolated: an
exception from one is logged and the others still run.

### Signature

```python
def on_close(self, handler: Callable[[Any], None]) -> Callable[[Any], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[Any], None]` | required | Called with a `CloseReason` - `code`, `reason`, `was_clean`, `will_retry` - after every live stream has already been failed with `ConnectionLost`. `will_retry` is true when the reconnect helper will dial again and false when the peer is finished. |

### Return

`Callable[[Any], None]` — the handler itself, so `@peer.on_close` works as a decorator.

### Raises

Raises: nothing.

A peer reports at most one close with `will_retry=False`. A peer that exhausted `max_attempts` does
not report its last socket loss a second time.

### Example

```python
import asyncio

from muxws import CloseReason, Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    @dialer.on_close
    def closed(reason: CloseReason) -> None:
        print("code", reason.code, "clean", reason.was_clean, "will_retry", reason.will_retry)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    await asyncio.sleep(0)

    await acceptor_socket.drop()
    await asyncio.sleep(0.1)
    print("is_open:", dialer.is_open)

    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.onClose()` (TypeScript)

### Signature

```ts
onClose(handler: (reason: CloseReason) => void): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `(reason: CloseReason) => void` | required | Called with `{ code, reason, wasClean, willRetry }` after every live stream has been failed with `ConnectionLost`. Handlers are isolated: one that throws is logged and the rest still run. |

### Return

`void`.

### Raises

Raises: nothing.

### Example

```ts
import { type CloseReason, JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

dialer.onClose((reason: CloseReason) => {
  console.log('code', reason.code, 'clean', reason.wasClean, 'willRetry', reason.willRetry);
});

void dialer.serve();
void acceptor.serve();

await acceptorSocket.drop();
await new Promise<void>((resolve) => setTimeout(resolve, 50));
console.log('isOpen:', dialer.isOpen);
```

## `peer.on_reconnect()` (Python)

Register a handler that is called once per **re**-established connection. The first connection does
not call it.

When the handler runs, the socket is open and the acceptor has acknowledged the hello on it. No
stream survives a reconnect, nothing is replayed, and the new socket starts with no allocated stream
ids. On the acceptor a reconnect creates a new `Peer`, so its `tags` start empty.

### Signature

```python
def on_reconnect(self, handler: Callable[[int, Any], None]) -> Callable[[int, Any], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[int, Any], None]` | required | Called `(attempt, peer)` where `attempt` counts re-establishments from 1. Called after the hello has been acknowledged, so the acceptor has processed the hello when the handler runs. Isolated: an exception from the handler is logged and does not stop the reconnect loop. |

### Return

`Callable[[int, Any], None]` — the handler itself, so `@peer.on_reconnect` works as a decorator.

### Raises

Raises: nothing.

### Example

```python
import asyncio

import websockets

from muxws import CloseReason, connect, Peer, Reconnect, select_subprotocol, serve, Stream


async def handler(payload: object, stream: Stream) -> None:
    await stream.reply(payload)


async def route(socket: object) -> None:
    await serve(socket, handler=handler)


async def main() -> None:
    async with websockets.serve(route, "127.0.0.1", 0, select_subprotocol=select_subprotocol) as server:
        port = server.sockets[0].getsockname()[1]
        peer = await connect(
            f"ws://127.0.0.1:{port}",
            hello={"client": "docs"},
            reconnect=Reconnect(initial_delay=0.05, max_delay=0.2),
        )

        @peer.on_reconnect
        def reconnected(attempt: int, reconnected_peer: Peer) -> None:
            print("reconnected", attempt, "open", reconnected_peer.is_open)

        @peer.on_close
        def closed(reason: CloseReason) -> None:
            print("lost the socket, will_retry", reason.will_retry)

        for connection in list(server.connections):
            await connection.close()
        await asyncio.sleep(1.0)

        print(await peer.request("hi", timeout=5.0))
        await peer.close(drain=0.1)


asyncio.run(main())
```

## `peer.onReconnect()` (TypeScript)

### Signature

```ts
onReconnect(handler: (attempt: number, peer: Peer) => void): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `(attempt: number, peer: Peer) => void` | required | Called once per re-established connection, after the hello acknowledgement, with the re-establishment count starting at 1. Isolated: an exception from the handler is logged and does not stop the reconnect loop. |

### Return

`void`.

### Raises

Raises: nothing.

### Example

```ts
import { type CloseReason, JsonCodec, type Peer, Reconnect, registerCodec, type Stream } from 'muxws';
import { connect, handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';
import { WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
server.on('connection', (socket) => {
  void serve(socket, { handler: async (payload: unknown, stream: Stream) => stream.reply(payload) }).catch(
    () => undefined,
  );
});
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const peer = await connect(`ws://127.0.0.1:${port}`, {
  hello: { client: 'docs' },
  reconnect: new Reconnect({ initialDelayMs: 50, maxDelayMs: 200 }),
});
peer.onReconnect((attempt: number, reconnected: Peer) => console.log('reconnected', attempt, 'open', reconnected.isOpen));
peer.onClose((reason: CloseReason) => console.log('lost the socket, willRetry', reason.willRetry));

server.clients.forEach((client) => client.close());
await new Promise<void>((resolve) => setTimeout(resolve, 1000));

console.log(await peer.request('hi', { timeoutMs: 5000 }));
await peer.close({ drainMs: 100 });
server.close();
```

## `peer.on_frame()` (Python)

Register a frame observer. It is called for every frame in both directions: before encoding for an
outgoing frame, after decoding for an incoming one.

The observer receives the logical frame, not the encoded bytes. muxws itself never logs payload
**contents**; whether the observer logs `frame.payload` is up to the application.

### Signature

```python
def on_frame(self, handler: Callable[[str, Frame, int], None]) -> Callable[[str, Frame, int], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[str, Frame, int], None]` | required | Called `(direction, frame, byte_length)`: `direction` is `"tx"` or `"rx"`, `frame` is the logical `Frame`, `byte_length` is the encoded size in bytes. It runs inside the read and write loops and should return quickly. Isolated: an exception from it is logged and the connection stays open. |

### Return

`Callable[[str, Frame, int], None]` — the handler itself, so `@peer.on_frame` works as a decorator.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Frame, Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    @dialer.on_frame
    def observe(direction: str, frame: Frame, byte_length: int) -> None:
        print(direction, frame.type, "stream", frame.stream, "bytes>0", byte_length > 0)

    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply({"echo": payload})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    await dialer.request("hi", timeout=5.0)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.onFrame()` (TypeScript)

### Signature

```ts
onFrame(handler: (direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `(direction: 'tx' \| 'rx', frame: Frame, byteLength: number) => void` | required | Called for every frame in both directions, before encode and after decode, with the encoded size in bytes. Isolated: an exception from it is logged and the connection stays open. |

### Return

`void`.

### Raises

Raises: nothing.

### Example

```ts
import { type Frame, JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

dialer.onFrame((direction: 'tx' | 'rx', frame: Frame, byteLength: number) => {
  console.log(direction, frame.type, 'stream', frame.stream, 'bytes>0', byteLength > 0);
});

acceptor.onStream(async (payload: unknown, stream: Stream) => stream.reply({ echo: payload }));

void dialer.serve();
void acceptor.serve();

await dialer.request('hi', { timeoutMs: 5000 });
await dialer.close({ drainMs: 100 });
```

## `peer.onError()` (TypeScript)

Register a handler for a stream failure that has **no consumer**: a stream that was never awaited or
iterated, or the internal stream behind `notify()`, which returns no handle.

There is no Python equivalent. In TypeScript an unhandled rejection can terminate the process, so
`Stream` attaches a rejection handler to its internal promise in its constructor and reports the
failure here. With no handler registered, the failure is logged at debug level through the muxws
`logger`.

### Signature

```ts
onError(handler: (error: unknown, stream: Stream | null) => void): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `(error: unknown, stream: Stream \| null) => void` | required | Called with the failure and the stream it belongs to. Handlers run in registration order and are isolated, as for `onClose`, `onReconnect` and `onFrame`: one that throws is logged and the rest still run. |

### Return

`void`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

dialer.onError((error: unknown, stream: Stream | null) => {
  console.log('unconsumed failure on stream', stream?.id, (error as Error).message.slice(0, 24));
});

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reset(ResetCode.APPLICATION_ERROR, 'nothing is listening');
});

void dialer.serve();
void acceptor.serve();

dialer.open('nobody awaits this');
await new Promise<void>((resolve) => setTimeout(resolve, 50));
await dialer.close({ drainMs: 100 });
```

## `peer.intercept_open()` (Python)

Register an interceptor for the headers of every stream this peer opens. `open()`, `notify()` and
`request()` call it synchronously, after the `ConnectionLost` / `ConnectionGoingAway` checks and before
a stream id is allocated.

The interceptor is called as `(headers, payload)`. `headers` is a fresh `dict` holding a shallow copy
of the caller's `headers=` (empty when none were passed); the interceptor adds, replaces or deletes
keys by mutating it, and the dict as the last interceptor left it becomes the `open` frame's headers
and `stream.headers`. The caller's own dict is never modified, and changes made to the interceptor's
dict after `open()` returned do not reach the frame. When the caller passed no headers and the
interceptors left the dict empty, the frame carries no `headers` field.

Interceptors run in registration order; registering one function twice is two registrations. They
belong to the `Peer`, so a dialer keeps them across reconnects. The reconnect helper's hello does not
pass through them. The leading headers this peer answers with pass through
[`intercept_reply_headers()`](#peer-intercept-reply-headers-python) instead. `connect()`, `accept()`
and `serve()` take the same interceptors as `open_interceptors=`.

### Signature

```python
def intercept_open(self, interceptor: OpenInterceptor) -> Callable[[], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`OpenInterceptor`](./types.md#openinterceptor) | required | Called `(headers, payload)` inside every `open()`. Must be synchronous; its return value is ignored. |

### Return

`Callable[[], None]` — removes this registration. A second call does nothing.

### Raises

Registration raises nothing. During `open()`:

- whatever the interceptor raises, unchanged. No later interceptor runs, no stream id is allocated and
  nothing is sent; `notify()` and `request()` raise it from their await.
- `TypeError` — the interceptor returned an awaitable (a coroutine is closed first), or the
  interceptors left a key that is not a `str`.

### Example

```python
import asyncio
import contextvars

from muxws import JsonCodec, Peer, Stream
from muxws.transports.memory import memory_pair

current_trace: contextvars.ContextVar[str | None] = contextvars.ContextVar("current_trace", default=None)


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    def add_trace(headers: dict, _payload: object) -> None:
        trace = current_trace.get()
        if trace is not None:
            headers.setdefault("trace", trace)

    unregister = dialer.intercept_open(add_trace)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply(dict(stream.headers))

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    current_trace.set("t-42")
    print(await dialer.request("first", timeout=5.0))  # {'trace': 't-42'}
    unregister()
    print(await dialer.request("second", timeout=5.0))  # {}

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.interceptOpen()` (TypeScript)

Register an interceptor for the headers of every stream this peer opens. `open()`, `notify()` and
`request()` call it synchronously, after the `ConnectionLost` / `ConnectionGoingAway` checks and before
a stream id is allocated. Semantics are those of the Python entry above: a fresh shallow copy of the
caller's `headers` (an empty object when none were passed) mutated in place, registration order,
kept by a dialer across reconnects, not applied to the hello, and no `headers` field on the frame
when the caller passed none and the interceptors added none. The leading headers this peer answers
with pass through [`interceptReplyHeaders()`](#peer-interceptreplyheaders-typescript) instead.
`connect()`, `accept()` and `serve()` take the same interceptors as `openInterceptors`.

### Signature

```ts
interceptOpen(interceptor: OpenInterceptor): () => void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`OpenInterceptor`](./types.md#openinterceptor) | required | Called `(headers, payload)` inside every `open()`. Must be synchronous; its return value is ignored. |

### Return

`() => void` — removes this registration. A second call does nothing.

### Raises

Registration throws nothing. During `open()`:

- whatever the interceptor throws, unchanged. No later interceptor runs, no stream id is allocated and
  nothing is sent; `notify()` and `request()` reject with it.
- `TypeError` — the interceptor returned a thenable (an `async` function, for one), or the
  interceptors left a symbol key.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

let currentTrace: string | null = 't-42';
const unregister = dialer.interceptOpen((headers) => {
  if (currentTrace !== null && headers.trace === undefined) headers.trace = currentTrace;
});

acceptor.onStream(async (_payload: unknown, stream: Stream) => stream.reply({ ...stream.headers }));

void dialer.serve();
void acceptor.serve();

console.log(await dialer.request('first', { timeoutMs: 5000 })); // { trace: 't-42' }
unregister();
currentTrace = null;
console.log(await dialer.request('second', { timeoutMs: 5000 })); // {}
await dialer.close({ drainMs: 100 });
```

## `peer.intercept_reply_headers()` (Python)

Register an interceptor for the leading headers this peer answers with. It is called once per stream
the **remote** opened, synchronously, by the call that sends this peer's first frame on that stream:
`stream.send_headers()`, `send()`, `end()`, `reply()`, or the implicit end of a handler that returns
without ending its stream.

The interceptor is called as `(headers, stream)`. `headers` is a fresh `dict` holding a shallow copy
of the `headers=` that call was given (empty when it was given none); the interceptor adds, replaces
or deletes keys by mutating it, and the dict as the last interceptor left it becomes the frame's
headers and `stream.reply_headers`. `stream` is the stream being answered, so `stream.headers` and
`stream.payload` are the opener's. The caller's own dict is never modified. When the call was given
no headers and the interceptors left the dict empty, the frame carries no `headers` field.

A frame on a stream this peer opened does not pass through it, and neither does a `reset`. Interceptors
run in registration order. `connect()`, `accept()` and `serve()` take the same interceptors as
`reply_headers_interceptors=`.

### Signature

```python
def intercept_reply_headers(self, interceptor: ReplyHeadersInterceptor) -> Callable[[], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`ReplyHeadersInterceptor`](./types.md#replyheadersinterceptor) | required | Called `(headers, stream)` by the call that sends this peer's first frame on a stream the remote opened. Must be synchronous; its return value is ignored. |

### Return

`Callable[[], None]` — removes this registration. A second call does nothing.

### Raises

Registration raises nothing. During the call that sends the first frame:

- whatever the interceptor raises, unchanged. No later interceptor runs, nothing is sent, and the
  stream's first frame is still unsent, so a later call on the stream can carry headers.
- `TypeError` — the interceptor returned an awaitable (a coroutine is closed first), or the
  interceptors left a key that is not a `str`.

In the implicit end of a handler that returns without ending its stream, the failure is treated as a
handler failure: the stream is reset with `APPLICATION_ERROR` and the error serializer's payload.

### Example

```python
import asyncio
import time

from muxws import JsonCodec, Peer, Stream
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    def add_server_time(headers: dict, stream: Stream) -> None:
        headers["server-time"] = round(time.time())
        if "trace" in stream.headers:
            headers["trace"] = stream.headers["trace"]

    acceptor.intercept_reply_headers(add_server_time)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply({"rows": 3})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("export", headers={"trace": "t-42"}, end=True)
    await stream
    print(sorted(stream.reply_headers))  # ['server-time', 'trace']

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.interceptReplyHeaders()` (TypeScript)

Register an interceptor for the leading headers this peer answers with. Semantics are those of the
Python entry above: once per stream the remote opened, called by the call that sends this peer's first
frame on it (`sendHeaders()`, `send()`, `end()`, `reply()`, or the implicit end of a handler that
returns), on a fresh shallow copy of that call's `headers` mutated in place, in registration order,
and no `headers` field on the frame when the call passed none and the interceptors added none.
`connect()`, `accept()` and `serve()` take the same interceptors as `replyHeadersInterceptors`.

### Signature

```ts
interceptReplyHeaders(interceptor: ReplyHeadersInterceptor): () => void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`ReplyHeadersInterceptor`](./types.md#replyheadersinterceptor) | required | Called `(headers, stream)` by the call that sends this peer's first frame on a stream the remote opened. Must be synchronous; its return value is ignored. |

### Return

`() => void` — removes this registration. A second call does nothing.

### Raises

Registration throws nothing. During the call that sends the first frame:

- whatever the interceptor throws, unchanged, as a rejection of that call. No later interceptor runs,
  nothing is sent, and the stream's first frame is still unsent.
- `TypeError` — the interceptor returned a thenable, or the interceptors left a symbol key.

In the implicit end of a handler that returns without ending its stream, the failure is treated as a
handler failure: the stream is reset with `APPLICATION_ERROR`.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.interceptReplyHeaders((headers, stream) => {
  headers['server-time'] = Math.round(Date.now() / 1000);
  if (stream.headers.trace !== undefined) headers.trace = stream.headers.trace;
});
acceptor.onStream(async (_payload: unknown, stream: Stream) => stream.reply({ rows: 3 }));

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('export', { headers: { trace: 't-42' }, end: true });
await stream;
console.log(Object.keys(stream.replyHeaders).sort()); // [ 'server-time', 'trace' ]
await dialer.close({ drainMs: 100 });
```

## `peer.intercept_incoming_headers()` (Python)

Register an interceptor for the leading headers the remote sends. It is called exactly once per
stream, synchronously inside the read loop, when the remote's first frame on that stream arrives:

- on a stream the **remote** opened, its `open` (the first fragment of a fragmented one), before the
  `on_stream` handler is invoked. `stream.local` is false and the headers are `stream.headers`.
- on a stream **this** peer opened, the remote's first `data`, before that frame's payload reaches
  the stream. `stream.local` is true and the headers are `stream.reply_headers`.

`headers` is a read-only `MappingProxyType`, empty when the frame carried none. A `reset` is not an
answer and calls no interceptor; neither does a stream that closes unanswered, nor an `open` refused
for the concurrency limit. Interceptors run in registration order. A dialer keeps them across
reconnects; on the acceptor a reconnect is a new `Peer`, which starts with none, so an acceptor passes
them to `accept()` or `serve()` as `incoming_headers_interceptors=`, and a dialer that must see a
stream pushed in answer to its hello passes them to `connect()` the same way.

### Signature

```python
def intercept_incoming_headers(self, interceptor: IncomingHeadersInterceptor) -> Callable[[], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`IncomingHeadersInterceptor`](./types.md#incomingheadersinterceptor) | required | Called `(headers, stream)` once per stream. Must be synchronous; its return value is ignored. It runs inside the read loop, so it should be fast. |

### Return

`Callable[[], None]` — removes this registration. A second call does nothing.

### Raises

Raises: nothing. An interceptor that raises, or returns an awaitable (a coroutine is closed without
running), is logged on `muxws.frames`; the remaining interceptors still run, and the frame, the stream
and the connection are processed as if no interceptor had failed.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, Stream
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    def show(headers, stream: Stream) -> None:
        side = "reply" if stream.local else "open"
        print(side, "headers on stream", stream.id, dict(headers))

    dialer.intercept_incoming_headers(show)
    acceptor.intercept_incoming_headers(show)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply({"rows": 3}, headers={"content-type": "application/json"})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    await dialer.request("export", headers={"trace": "t-42"}, timeout=5.0)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.interceptIncomingHeaders()` (TypeScript)

Register an interceptor for the leading headers the remote sends. Semantics are those of the Python
entry above: once per stream, synchronously inside the read loop, at the remote's `open` (before the
`onStream` handler) or at the remote's first `data` on a stream this peer opened (before its
payload), with `stream.local` telling the two apart. `headers` is a frozen copy, empty when the frame
carried none.

### Signature

```ts
interceptIncomingHeaders(interceptor: IncomingHeadersInterceptor): () => void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `interceptor` | [`IncomingHeadersInterceptor`](./types.md#incomingheadersinterceptor) | required | Called `(headers, stream)` once per stream. Must be synchronous; its return value is ignored. |

### Return

`() => void` — removes this registration. A second call does nothing.

### Raises

Raises: nothing. An interceptor that throws, or returns a thenable (its rejection is silenced), is
logged through `logger.error`; the remaining interceptors still run, and the frame, the stream and the
connection are processed as if no interceptor had failed.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

const show = (headers: Readonly<Record<string, unknown>>, stream: Stream): void => {
  console.log(stream.local ? 'reply' : 'open', 'headers on stream', stream.id, headers);
};
dialer.interceptIncomingHeaders(show);
acceptor.interceptIncomingHeaders(show);

acceptor.onStream(async (_payload: unknown, stream: Stream) =>
  stream.reply({ rows: 3 }, { headers: { 'content-type': 'application/json' } }),
);

void dialer.serve();
void acceptor.serve();

await dialer.request('export', { headers: { trace: 't-42' }, timeoutMs: 5000 });
await dialer.close({ drainMs: 100 });
```

## `peer.ping()` (Python)

Send a `ping` frame and wait for its `pong`, returning the round-trip time in **seconds**.

It is a muxws `ping` frame, not a WebSocket control frame, because browsers do not expose WebSocket
control frames to JavaScript. The remote peer answers with the same nonce without involving the
application.

### Signature

```python
async def ping(self, timeout: float = 5.0) -> float:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `timeout` | `float` — seconds | `5.0` | How long to wait for the matching `pong`, in seconds. This is the deadline for **an explicit ping**; the heartbeat started by `connect()` has its own, `ping_timeout`, which defaults to 10.0 seconds. |

### Return

`float` — the round-trip time in seconds, measured on the event loop's clock.

### Raises

- `ConnectionLost` — the peer is between sockets; there is nothing to ping.
- `ConnectionClosed` — no `pong` within `timeout` seconds (code 1006), or the socket closed while
  waiting, in which case the `ConnectionClosed` describing that closure is raised.

A missing `pong` fails only this call; the connection stays open. Closing a connection whose pings go
unanswered is done by the heartbeat, not by this method.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    seconds = await dialer.ping(timeout=5.0)
    print("round trip is a non-negative number of seconds:", seconds >= 0.0)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.ping()` (TypeScript)

### Signature

```ts
async ping(timeoutMs: number = DEFAULT_PING_TIMEOUT_MS): Promise<number>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `timeoutMs` | `number` — milliseconds | `DEFAULT_PING_TIMEOUT_MS` (5000) | How long to wait for the matching `pong`, in milliseconds. The heartbeat's own deadline is `pingTimeoutMs`, which defaults to 10000 milliseconds. |

### Return

`Promise<number>` — the round-trip time in **milliseconds**, measured on `performance.now()`.

### Raises

Rejects with:

- `ConnectionLost` — the peer is between sockets.
- `ConnectionClosed` — no `pong` within `timeoutMs` milliseconds (code 1006), or the socket closed
  while waiting.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

void dialer.serve();
void acceptor.serve();

const milliseconds = await dialer.ping(5000);
console.log('round trip is a non-negative number of milliseconds:', milliseconds >= 0);
await dialer.close({ drainMs: 100 });
```

## `peer.close()` (Python)

Close the connection in order: send `goaway`, wait up to the drain deadline for live streams to end
and for every queued frame to be sent, then close the socket. Queued frames include every remaining
fragment of a fragmented payload.

On a dialer it first stops the reconnect helper, **before** the `is_open` check, so a `close()` called
while the helper waits out a backoff delay prevents the next dial.

### Signature

```python
async def close(self, code: ResetCode = ResetCode.NO_ERROR, reason: str | None = None, drain: float = 10.0) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode` | `ResetCode.NO_ERROR` | The code carried by the `goaway` frame. Positional, unlike most muxws options. |
| `reason` | `str \| None` | `None` | Free text on the `goaway` and on the WebSocket close frame. The close frame carries at most 123 UTF-8 bytes of it, cut at a character boundary; the `goaway` and `on_close` carry the full text. |
| `drain` | `float` — seconds | `10.0` | How long, in seconds, to let live streams finish and queued frames be sent before the socket is closed regardless. The socket is closed at the deadline even if the remote keeps a stream open or frames are still queued. Streams still live at the deadline fail locally with `ConnectionLost`. |

### Return

`None`. On a peer that is already closed it only stops the reconnect helper.

### Raises

Raises: nothing. An exception from the socket adapter's `close()` is logged as a warning, and the peer
is still marked closed.

### Example

```python
import asyncio

from muxws import Peer, ResetCode
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    await dialer.close(ResetCode.NO_ERROR, "shutting down", drain=0.1)
    print("is_open:", dialer.is_open)
    await dialer.close(drain=0.1)
    print("closing twice is a no-op:", dialer.is_open)

    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.close()` (TypeScript)

### Signature

```ts
async close(options: CloseOptions = {}): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `options` | `CloseOptions` | `{}` | `code`, `reason`, `drainMs`. See [`CloseOptions`](#closeoptions-typescript). |

### Return

`Promise<void>`. A second close is a no-op, apart from stopping the reconnect helper.

### Raises

Raises: nothing. An exception from the socket adapter's `close()` is logged as a warning, and the peer
is still marked closed.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

void dialer.serve();
void acceptor.serve();

await dialer.close({ code: ResetCode.NO_ERROR, reason: 'shutting down', drainMs: 100 });
console.log('isOpen:', dialer.isOpen);
await dialer.close({ drainMs: 100 });
console.log('closing twice is a no-op:', dialer.isOpen);
```

## `CloseOptions` (TypeScript)

### Signature

```ts
export interface CloseOptions {
  code?: ResetCode;
  reason?: string;
  drainMs?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode` | `ResetCode.NO_ERROR` | The code on the `goaway` frame. |
| `reason` | `string` | `null` | Free text on the `goaway` and the WebSocket close frame. The close frame carries at most 123 UTF-8 bytes of it, cut at a character boundary; the `goaway` and `onClose` carry the full text. |
| `drainMs` | `number` — milliseconds | `DEFAULT_DRAIN_MS` (10000) | How long, in milliseconds, to let live streams finish and queued frames be sent before closing regardless. The Python equivalent is `drain=10.0` seconds. |

### Return

None — `CloseOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { type CloseOptions, JsonCodec, memoryPair, Peer, ResetCode } from 'muxws';

const options: CloseOptions = { code: ResetCode.NO_ERROR, reason: 'shutting down', drainMs: 100 };

const [socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
void peer.serve();
await peer.close(options);
console.log(options.drainMs, 'milliseconds of drain; isOpen', peer.isOpen);
```

## `peer.serve()` (Python)

Run the read loop until the socket closes. No frame is received before this is awaited; it also
starts the writer task.

`connect()` and the module-level `serve()` call it. An application calls it directly when it
constructed the `Peer` itself or obtained it from `accept()`, or when it needs a handle to the task.

### Signature

```python
async def serve(self) -> None:
```

### Parameters

None — `serve()` takes no arguments.

### Return

`None` — it returns when the socket closes. A socket closure, clean or not, raises nothing: the peer
is marked closed, every live stream fails with `ConnectionLost`, the `on_close` handlers are called,
and `serve()` returns.

An exception raised inside the read loop is logged and does not propagate: the peer sends
`goaway(INTERNAL_ERROR)` and the frames already queued, for at most 10 seconds, is marked closed with
code 1011, closes the socket with 1011, and `serve()` returns. From the moment `goaway` is queued,
`open()` raises `ConnectionGoingAway`.

### Raises

- `asyncio.CancelledError` — the task running it was cancelled. Cancelling that task is how the read
  loop of a directly constructed peer is stopped.
- Whatever the socket adapter's `close()` raises while failing the connection on a protocol error.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply(payload)

    acceptor.on_stream(handler)
    serving = asyncio.create_task(acceptor.serve())
    dialing = asyncio.create_task(dialer.serve())

    print(await dialer.request("hi", timeout=5.0))
    await dialer.close(drain=0.1)

    # The acceptor's socket closes with the dialer's, so its read loop returns.
    await asyncio.wait_for(serving, 5.0)
    print("acceptor serve() returned; is_open:", acceptor.is_open)

    dialing.cancel()
    await asyncio.gather(dialing, return_exceptions=True)


asyncio.run(main())
```

## `peer.serve()` (TypeScript)

### Signature

```ts
async serve(): Promise<void>;
```

### Parameters

None — it takes no arguments.

### Return

`Promise<void>` — resolves when the socket closes, cleanly or not.

### Raises

Rejects with the error that ended the read loop, after sending `goaway(INTERNAL_ERROR)` and the frames
already queued (for at most 10 seconds), marking the peer closed (code 1011) and closing the socket
with 1011, so `isOpen` is already false when the promise rejects. From the moment `goaway` is queued,
`open()` throws `ConnectionGoingAway`. The Python `serve()` returns in the same situation. A socket closure resolves the
promise and does not reject it.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => stream.reply(payload));

const serving = acceptor.serve();
void dialer.serve();

console.log(await dialer.request('hi', { timeoutMs: 5000 }));
await dialer.close({ drainMs: 100 });

await serving;
console.log('acceptor serve() resolved; isOpen:', acceptor.isOpen);
```

## `peer.id` (Python)

The connection's log correlation id.

It is **three lowercase hex characters chosen once per process**, a hyphen, and a counter incremented
once per connection in that process. It is not unique across processes: two processes can produce
the same `id`. It **changes when a reconnected peer adopts a new socket**, so a log shows each
reconnect as a new `conn=`.

### Signature

```python
self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
```

### Parameters

None — `id` is a plain instance attribute, assigned in `__init__` and reassigned when the peer adopts
a new socket. It is writable; muxws assigns a new value when the peer adopts its next socket.

### Return

`str` — for example `"a3f-0"`.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    first_socket, second_socket = memory_pair()
    first = Peer(first_socket, codec=JsonCodec(), is_dialer=True)
    second = Peer(second_socket, codec=JsonCodec(), is_dialer=False)

    prefix, counter = first.id.split("-")
    print("prefix is three hex characters:", len(prefix) == 3)
    print("the counter is per connection:", int(second.id.split("-")[1]) == int(counter) + 1)
    print("the prefix is per process:", second.id.split("-")[0] == prefix)


asyncio.run(main())
```

## `peer.id` (TypeScript)

The same format as the Python attribute: a prefix of three hex characters chosen once per module
load, a hyphen, and a counter incremented once per connection. Not unique across processes; it
changes on reconnect.

### Signature

```ts
id: string;
```

### Parameters

None — it is a mutable public field, assigned in the constructor and again in `adoptSocket`.

### Return

`string` — for example `'a3f-0'`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [firstSocket, secondSocket] = memoryPair();
const first = new Peer(firstSocket, { codec: new JsonCodec(), isDialer: true });
const second = new Peer(secondSocket, { codec: new JsonCodec(), isDialer: false });

const [prefix, counter] = first.id.split('-');
console.log('prefix is three hex characters:', prefix.length === 3);
console.log('the counter is per connection:', Number(second.id.split('-')[1]) === Number(counter) + 1);
console.log('the prefix is per module load:', second.id.split('-')[0] === prefix);
```

## `peer.tags` (Python)

A plain `dict` that muxws does not read. An application stores data about the remote end in it, such
as a user id, a tenant or a set of subscriptions, and a `PeerRegistry` finds the peer by those
values.

On the acceptor, a reconnect creates a new `Peer` with an empty `tags`, so **nothing stored there
survives a disconnect**. The acceptor repopulates it when it handles the hello of the new
connection.

### Signature

```python
self.tags: dict[str, Any] = {}
```

### Parameters

None — it is a plain instance attribute and is mutated like any dict.

### Return

`dict[str, Any]` — the live dict, not a copy. Writing to it writes to the peer.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    socket, _ = memory_pair()
    peer = Peer(socket, codec=JsonCodec(), is_dialer=False)

    print(peer.tags)
    peer.tags["user"] = "u-17"
    peer.tags["rooms"] = ["general", "random"]
    print(peer.tags["user"], peer.tags["rooms"])


asyncio.run(main())
```

## `peer.tags` (TypeScript)

### Signature

```ts
readonly tags: Record<string, unknown> = {};
```

### Parameters

None — a `readonly` property holding a mutable object: keys can be set and deleted, the object
cannot be replaced.

### Return

`Record<string, unknown>` — the live object, not a copy.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });

console.log(Object.keys(peer.tags).length);
peer.tags.user = 'u-17';
peer.tags.rooms = ['general', 'random'];
console.log(peer.tags.user, peer.tags.rooms);
```

## `peer.streams` (Python)

The streams that are live on this connection right now, keyed by stream id.

Nothing is retained per **closed** stream: a stream that ends is removed from this map immediately.
The per-stream state of a connection is two integers plus this map of live streams.

### Signature

```python
@property
def streams(self) -> Mapping[int, Stream]:
```

### Parameters

None — it is a read-only property.

### Return

`Mapping[int, Stream]` — a **copy** taken when the property is read. Mutating it does not affect the
peer, and it does not update as streams open and close.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    first = dialer.open("one")
    second = dialer.open("two")
    print(sorted(dialer.streams), first.id, second.id)

    await first.cancel("done with it")
    print("cancelled streams are dropped:", sorted(dialer.streams))

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.streams` (TypeScript)

### Signature

```ts
get streams(): ReadonlyMap<number, Stream>;
```

### Parameters

None — it is a getter.

### Return

`ReadonlyMap<number, Stream>` — a new `Map` built on each read, so it is a snapshot rather than a live
view.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

void dialer.serve();
void acceptor.serve();

const first = dialer.open('one');
const second = dialer.open('two');
console.log([...dialer.streams.keys()].sort(), first.id, second.id);

await first.cancel('done with it');
console.log('cancelled streams are dropped:', [...dialer.streams.keys()]);

await dialer.close({ drainMs: 100 });
```

## `peer.is_open` (Python)

Whether this peer can be used now: true when the socket is open **and** the connection is
established.

The two conditions differ only for a dialer with a hello. Between adopting a socket and the
acknowledgement of the hello, the socket is open but the connection is not established; an
application frame sent then would reach the acceptor before the hello. `is_open` is false in that
interval, and `open()` raises `ConnectionLost`.

### Signature

```python
@property
def is_open(self) -> bool:
```

### Parameters

None — read-only property.

### Return

`bool` — true when a frame may be sent and a stream may be opened.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    print("after construction:", dialer.is_open)

    await acceptor_socket.drop()
    await asyncio.sleep(0.1)
    print("after the socket dies:", dialer.is_open)

    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.isOpen` (TypeScript)

### Signature

```ts
get isOpen(): boolean;
```

### Parameters

None — getter.

### Return

`boolean` — socket open **and** connection established, as described for the Python property.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

void dialer.serve();
void acceptor.serve();
console.log('after construction:', dialer.isOpen);

await acceptorSocket.drop();
await new Promise<void>((resolve) => setTimeout(resolve, 50));
console.log('after the socket dies:', dialer.isOpen);
```

## `peer.is_dialer` (Python)

Which end this is. The dialer allocates odd stream ids, the acceptor even ones. Apart from that,
only a dialer can have a reconnect helper.

### Signature

```python
@property
def is_dialer(self) -> bool:
```

### Parameters

None — read-only property.

### Return

`bool` — true for a peer built by `connect()`, false for one built by `accept()`.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    print(dialer.is_dialer, acceptor.is_dialer)
    print("odd ids for the dialer:", dialer.open("x").id % 2 == 1)
    print("even ids for the acceptor:", acceptor.open("y").id % 2 == 0)


asyncio.run(main())
```

## `peer.isDialer` (TypeScript)

### Signature

```ts
get isDialer(): boolean;
```

### Parameters

None — getter.

### Return

`boolean` — true for a peer built by `connect()`, false for one built by `accept()`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

console.log(dialer.isDialer, acceptor.isDialer);
console.log('odd ids for the dialer:', dialer.open('x').id % 2 === 1);
console.log('even ids for the acceptor:', acceptor.open('y').id % 2 === 0);
```

## `peer.last_activity` (Python)

When a frame last crossed this socket, in **either** direction, on the event loop's clock in seconds.

The heartbeat reads this value and sends a `ping` only after `ping_interval` seconds without a frame
in either direction, so a connection that carries traffic sends no heartbeat pings.

### Signature

```python
@property
def last_activity(self) -> float:
```

### Parameters

None — read-only property.

### Return

`float` — seconds on `asyncio.get_running_loop().time()`, the same clock the heartbeat compares it
against. It is `0.0` on a peer across which no frame has yet passed.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    print("no frame has crossed yet:", dialer.last_activity == 0.0)

    await dialer.ping(timeout=5.0)
    idle_seconds = asyncio.get_running_loop().time() - dialer.last_activity
    print("idle for less than a second:", idle_seconds < 1.0)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `peer.lastActivity` (TypeScript)

### Signature

```ts
get lastActivity(): number;
```

### Parameters

None — getter.

### Return

`number` — **milliseconds** on `performance.now()`'s scale, the clock `peer.clock()` reads. `0` until
the first frame crosses.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

void dialer.serve();
void acceptor.serve();
console.log('no frame has crossed yet:', dialer.lastActivity === 0);

await dialer.ping(5000);
console.log('idle for less than 1000 milliseconds:', dialer.clock() - dialer.lastActivity < 1000);
await dialer.close({ drainMs: 100 });
```

## `DEFAULT_PING_TIMEOUT_MS` (TypeScript)

### Signature

```ts
export const DEFAULT_PING_TIMEOUT_MS = 5000;
```

### Parameters

None — it is a constant.

### Return

`number` — 5000 milliseconds, the deadline an explicit `peer.ping()` uses when none is given. The
Python equivalent is `timeout: float = 5.0` seconds. It is **not** the heartbeat's deadline, which
defaults to 10000 milliseconds.

### Raises

Raises: nothing.

### Example

```ts
import { DEFAULT_PING_TIMEOUT_MS, JsonCodec, memoryPair, Peer } from 'muxws';

console.log(DEFAULT_PING_TIMEOUT_MS, 'milliseconds');

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });
void dialer.serve();
void acceptor.serve();

console.log('an explicit deadline is the same number:', (await dialer.ping(DEFAULT_PING_TIMEOUT_MS)) >= 0);
await dialer.close({ drainMs: 100 });
```

## `DEFAULT_DRAIN_MS` (TypeScript)

### Signature

```ts
export const DEFAULT_DRAIN_MS = 10_000;
```

### Parameters

None — it is a constant.

### Return

`number` — 10000 milliseconds, `peer.close()`'s drain window when `drainMs` is not given. The
Python equivalent is `drain: float = 10.0` seconds.

### Raises

Raises: nothing.

### Example

```ts
import { DEFAULT_DRAIN_MS, JsonCodec, memoryPair, Peer } from 'muxws';

console.log(DEFAULT_DRAIN_MS, 'milliseconds of drain by default');

const [socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
void peer.serve();
// No stream is live, so close() returns without waiting for the default drain window.
await peer.close({ drainMs: DEFAULT_DRAIN_MS });
console.log('isOpen:', peer.isOpen);
```

## `DEFAULT_MAX_PAYLOAD_BYTES` (TypeScript)

### Signature

```ts
export const DEFAULT_MAX_PAYLOAD_BYTES = 67_108_864;
```

### Parameters

None — it is a constant.

### Return

`number` — 64 MiB in bytes: the largest reassembled payload a peer accepts unless told otherwise.
Local to the peer that holds it, never announced, and checked as fragments arrive rather than after
reassembly.

### Raises

Raises: nothing.

### Example

```ts
import { DEFAULT_MAX_PAYLOAD_BYTES, JsonCodec, memoryPair, Peer } from 'muxws';

console.log(DEFAULT_MAX_PAYLOAD_BYTES, 'bytes =', DEFAULT_MAX_PAYLOAD_BYTES / 1024 / 1024, 'MiB');

const [socket] = memoryPair();
// The limit is set on the receiving peer; the sending peer is not informed of it.
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false, maxPayloadBytes: DEFAULT_MAX_PAYLOAD_BYTES * 2 });
console.log(peer.isOpen);
```

## `DEFAULT_MAX_CONCURRENT_STREAMS` (TypeScript)

### Signature

```ts
export const DEFAULT_MAX_CONCURRENT_STREAMS = 100;
```

### Parameters

None — it is a constant.

### Return

`number` — 100: how many streams the **remote** may hold open on a peer at once. Counted over the
remote's opens alone; this peer's own streams do not count against it. Never sent to the remote; the
remote observes it only as the `reset(REFUSED)` it receives when it exceeds the limit.

### Raises

Raises: nothing.

### Example

```ts
import { DEFAULT_MAX_CONCURRENT_STREAMS, JsonCodec, memoryPair, Peer } from 'muxws';

console.log(DEFAULT_MAX_CONCURRENT_STREAMS, 'streams the remote may hold open here');

const [socket] = memoryPair();
const peer = new Peer(socket, {
  codec: new JsonCodec(),
  isDialer: false,
  maxConcurrentStreams: DEFAULT_MAX_CONCURRENT_STREAMS,
});
console.log('our own opens do not consume it:', peer.open('a').id, peer.open('b').id);
```

## See also

- [`connect()`](./connect.md) and [`accept()`](./accept.md) — where peers come from.
- [`Stream`](./stream.md) — what `open()` returns and what a handler is given.
- [`Reconnect`](./reconnect.md) — the schedule behind `on_reconnect`.
- [Errors](./errors.md) — `ConnectionLost`, `ConnectionGoingAway`, `RemoteError` and the rest.
- [Registry](./registry.md) — finding a peer by its `tags`.
- [Types](./types.md) — `CloseReason`, `StreamHandler`, `ErrorSerializer`, `OpenInterceptor`,
  `ReplyHeadersInterceptor`, `IncomingHeadersInterceptor`, `Frame`.
