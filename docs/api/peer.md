---
outline: deep
---

# Peer

One `Peer` is one end of one WebSocket. There is exactly one peer type per language and both ends use
it: a server pushing a stream to a browser calls the same `open()` the browser calls, and the browser
receives it through the same `on_stream` handler the server uses. Server push is a client request
with the roles swapped, with the same correlation and the same cancellation.

A peer is obtained from [`connect()`](./connect.md) or [`accept()`](./accept.md). Constructing one
directly is public and is what the in-memory examples below do, but an application over a real socket
should use the factories, which resolve the codec and perform the handshake.

Two properties of the object are worth stating before the tables:

- **`peer.id` is a per-process prefix plus a per-connection counter.** It is not unique across
  processes, and it **changes** when a reconnected peer takes a new socket.
- **`open()` takes no deadline.** Deadlines belong to the calls that wait: `stream.result(timeout=)`
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
| `socket` | `SocketAdapter` | required | The transport. muxws never touches anything else about the connection. |
| `codec` | `Codec` | required | The codec both ends agreed on at the handshake. It is not negotiated here and never switched later. |
| `is_dialer` | `bool` | required | Which parity this peer allocates: the dialer takes odd stream ids, the acceptor even ones. Getting it wrong on both ends is how two peers collide on id 1. |
| `error_serializer` | `ErrorSerializer \| None` | `None` | Turns a handler's exception into the `reset(APPLICATION_ERROR)` payload. `None` means `default_error_serializer`, which puts the class name and message on the wire. |
| `max_frame_bytes` | `int` | `MAX_FRAME_BYTES` (65536) | Test-only. A cap too small to hold an envelope plus one indivisible unit is rejected here rather than looping in the splitter. |
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
| `maxFrameBytes` | `number` | `MAX_FRAME_BYTES` (65536) | Test-only lowering of the protocol frame cap. |
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
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on the `open` frame. muxws never reads it; the remote handler sees it as `stream.headers`. Not a place for credentials - authentication belongs at the upgrade. |
| `end` | `bool` | `False` | Send `end` on the `open` frame, which half-closes this side immediately: the request is complete and only the response is still to come. `end` is a flag on a frame, never a frame of its own. |

### Return

`Stream` — live and already enqueued. It is both awaitable (the first payload) and async-iterable
(every payload); whichever you use first claims it, and the other then raises
`StreamAlreadyConsumed`.

### Raises

Exactly two, both synchronous, and never one for concurrency:

- `ConnectionLost` — the peer is between sockets. Nothing is buffered for the next one.
- `ConnectionGoingAway` — a `goaway` has arrived, or this peer has sent one, or the connection's
  stream ids are exhausted.

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

- `ConnectionLost` — the peer is between sockets.
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the stream ids are exhausted.

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

`open()`'s options object. It carries **no** `timeoutMs`, in this or any milestone: `open()` does not
wait, so it has no deadline to take. The calls that wait are `stream.result({ timeoutMs })` and
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
| `headers` | `Record<string, unknown>` | `null` | Per-stream metadata on the `open` frame; the remote handler reads it as `stream.headers`. |
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

One-shot push: open a stream, send the payload, end it, and hand back nothing at all.

### Signature

```python
async def notify(self, payload: Any = None, *, headers: dict[str, Any] | None = None) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `None` | The payload. It rides the `open` frame with `end` set. |
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on that frame. |

### Return

`None` — deliberately. There is no handle to await, because there is nothing to wait for: a
`notify()` that returned a stream would invite a caller to await a reply the shape does not promise.

### Raises

The two `open()` raises, for the same reasons:

- `ConnectionLost` — the peer is between sockets.
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the ids are exhausted.

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
| `payload` | `unknown` | `null` | The payload, riding the `open` frame with `end` set. Unlike `open()`, a lone object here is always the payload; the options object is the second argument. |
| `options.headers` | `Record<string, unknown>` | none | Per-stream metadata on that frame. |

### Return

`Promise<void>` — resolving to nothing. There is no stream handle: the stream behind a `notify()` is
claimed internally so a failure on it is reported to `peer.onError` rather than lost.

### Raises

Rejects with the two `open()` throws: `ConnectionLost` and `ConnectionGoingAway`. They originate in
the synchronous `open()` inside this call, but `notify` is an `async` method, so they reach you as a
**rejection** rather than as a throw at the call site — `peer.notify(x).catch(...)` catches them, and
a bare `try`/`catch` around an un-awaited `peer.notify(x)` does not.

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

`open(payload, end=True)` awaited to the stream's end: the unary call.

Unlike `await stream`, it polices a second payload. A unary call that quietly discarded the extra
values would hide a handler bug rather than report it.

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
| `headers` | `dict[str, Any] \| None` | `None` | Per-stream metadata on the `open` frame. |
| `timeout` | `float \| None` — seconds | `None` | The deadline, in seconds. `None` waits forever. When it expires the remote is sent `reset(TIMEOUT)` **before** the caller is told, so the remote stops working rather than finishing into a caller that has gone. |

### Return

`Any` — the single payload the remote sent.

### Raises

- `ConnectionLost` — the peer is between sockets when the call is made, or the socket died while
  waiting.
- `ConnectionGoingAway` — a `goaway` has been received or sent, or the ids are exhausted.
- `StreamTimeout` — `timeout` seconds expired. The remote has already been told.
- `ProtocolError` — the remote sent more than one payload (its stream is reset with
  `PROTOCOL_ERROR`), or ended the stream without sending one.
- `RemoteError` — the remote handler raised; `.payload` carries whatever its `error_serializer`
  produced.
- `StreamRefused` — the remote refused the stream without running anything: no handler, going away,
  or its concurrency limit reached. Safe to retry elsewhere.
- `StreamReset` — any other reset code, including one this generation does not define.

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
`StreamReset`. `ConnectionLost` and `ConnectionGoingAway` originate in the synchronous `open()`
inside this call, but `request` is an `async` method, so every one of them arrives as a **rejection**
— there is no failure `request()` throws at the call site.

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

`OpenOptions` plus the deadline, because this call waits.

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

Register the one incoming-stream handler. There is exactly one per peer: a second call replaces the
first and logs a warning.

A peer with no handler answers every incoming `open` with `reset(REFUSED, "no on_stream handler")`,
which is why `connect(on_stream=...)` exists - it registers the handler before the hello goes out, so
an acceptor that pushes a stream the instant it sees the hello is not refused.

### Signature

```python
def on_stream(self, handler: StreamHandler) -> StreamHandler:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `StreamHandler` | required | `(payload, stream)`, sync or async. The payload is the opening payload, already reassembled. Returning without ending the stream ends it implicitly; raising resets it with `APPLICATION_ERROR` and never with `REFUSED`, because `REFUSED` promises the work did not happen. |

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

`void` — unlike Python's, this one does not hand the handler back, so there is no decorator form.

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

Register a handler for socket loss. Every registered handler is called, each isolated from the
others: one that raises is logged and costs the rest nothing.

### Signature

```python
def on_close(self, handler: Callable[[Any], None]) -> Callable[[Any], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[Any], None]` | required | Called with a `CloseReason` - `code`, `reason`, `was_clean`, `will_retry` - after every live stream has already been failed with `ConnectionLost`. `will_retry` is the only reliable way to tell "the helper is backing off" from "this peer is finished". |

### Return

`Callable[[Any], None]` — the handler itself, so `@peer.on_close` works as a decorator.

### Raises

Raises: nothing.

There is at most one `will_retry=False` close per peer, ever: a peer that exhausted `max_attempts`
does not also report the last socket loss a second time.

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

Register a handler that fires once per **re**-established connection. The first connection does not
fire it: it was established, not re-established.

It guarantees exactly two things and nothing more: a live socket, and an identity the acceptor has
already accepted on it. No stream survives a reconnect, nothing is replayed, and the new socket's id
space starts empty. `tags` start empty on the acceptor's side, because over there a reconnect is a
whole new `Peer`.

### Signature

```python
def on_reconnect(self, handler: Callable[[int, Any], None]) -> Callable[[int, Any], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[int, Any], None]` | required | Called `(attempt, peer)` where `attempt` counts re-establishments from 1. Fired after the hello has been acknowledged, never before - which is what makes "the acceptor already knows who this is" true when it runs. Isolated: a handler that raises is logged and cannot stop the reconnect loop. |

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
| `handler` | `(attempt: number, peer: Peer) => void` | required | Called once per re-established connection, after the hello acknowledgement, with the re-establishment count starting at 1. Isolated from the supervisor: a handler that throws is logged and the loop survives. |

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

Register a frame observer: every frame, both directions, before encode on the way out and after
decode on the way in.

The payload's **contents** never reach it in encoded form and are never logged by muxws itself - the
observer is handed the logical frame, so what you do with `frame.payload` is your decision and your
responsibility.

### Signature

```python
def on_frame(self, handler: Callable[[str, Frame, int], None]) -> Callable[[str, Frame, int], None]:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `Callable[[str, Frame, int], None]` | required | Called `(direction, frame, byte_length)`: `direction` is `"tx"` or `"rx"`, `frame` is the logical `Frame`, `byte_length` is the encoded size in bytes. It runs inside the read and write loops, so it is isolated - one that raises is logged and the connection survives - and it should be fast. |

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
| `handler` | `(direction: 'tx' \| 'rx', frame: Frame, byteLength: number) => void` | required | Called for every frame in both directions, before encode and after decode, with the encoded size in bytes. Isolated: one that throws is logged and the connection survives. |

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

Register a handler for a stream failure that has **no consumer to hand it to**: a stream nobody
awaited or iterated, or the fire-and-forget stream behind `notify()`, which has no handle at all.

This one has no Python twin. In Python a future nobody retrieved is a warning the runtime prints; in
TypeScript an unhandled rejection can take a process down, so `Stream` silences its internal promise
from the constructor and reports the failure here instead. With no handler registered, the failure is
logged at debug level under `muxws`.

### Signature

```ts
onError(handler: (error: unknown, stream: Stream | null) => void): void;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `handler` | `(error: unknown, stream: Stream \| null) => void` | required | Called with the failure and the stream it belongs to. Handlers run in registration order with **no per-handler isolation**, unlike `onClose`, `onReconnect` and `onFrame`: one that throws stops the rest. The throw itself is swallowed by the stream close path that reported it, so it cannot take a connection down - it just loses the remaining handlers. |

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

## `peer.ping()` (Python)

Send a `ping` frame and wait for its `pong`, returning the round-trip time in **seconds**.

It is a muxws `ping` frame and not a WebSocket control frame, because browsers do not expose control
frames to JavaScript: a liveness mechanism built on them cannot work on half the peers that exist.
The remote echoes the nonce with no application involvement whatever.

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
- `ConnectionClosed` — no `pong` within `timeout` seconds (code 1006), or the socket died while
  waiting, in which case the death's own `ConnectionClosed` is raised.

A lost `pong` is not a lost connection: this call fails and the connection is untouched. Turning a
run of them into a verdict is the heartbeat's job, not this one's.

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
- `ConnectionClosed` — no `pong` within `timeoutMs` milliseconds (code 1006), or the socket died
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

Shut the connection down in order: send `goaway`, let the streams the remote is still allowed to
finish drain, then close the socket.

On a dialer it also stops the reconnect helper, **before** the `is_open` check, so a deliberate close
during a backoff window really does stop the next dial. A close that returned early there would leave
the helper to dial behind the caller's back and hand the application a live connection it had already
given up on.

### Signature

```python
async def close(self, code: ResetCode = ResetCode.NO_ERROR, reason: str | None = None, drain: float = 10.0) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode` | `ResetCode.NO_ERROR` | The code carried by the `goaway` frame. Positional, unlike most muxws options. |
| `reason` | `str \| None` | `None` | Free text on the `goaway` and on the WebSocket close frame. |
| `drain` | `float` — seconds | `10.0` | How long, in seconds, to let live streams finish before the socket is closed regardless. A deadline, not a poll loop: a peer that waited for quiet would never close against a remote that keeps one stream open. Whatever is still live at the deadline fails locally with `ConnectionLost`. |

### Return

`None`. Calling it on a peer that is already closed is a no-op - except that the reconnect helper is
stopped first, which is the point of the ordering.

### Raises

Raises: nothing of its own. An exception raised by the socket adapter's `close()` propagates: Python does not
guard that call, where the TypeScript port does.

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

Raises: nothing. A socket that cannot be closed is already gone: the failure is logged as a warning and the
peer still dies.

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
| `reason` | `string` | `null` | Free text on the `goaway` and the WebSocket close frame. |
| `drainMs` | `number` — milliseconds | `DEFAULT_DRAIN_MS` (10000) | How long, in milliseconds, to let live streams finish before closing regardless. The mirror of Python's `drain=10.0` seconds. |

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

Run the read loop until the socket closes. Nothing is received before this is awaited, and the writer
task is started by it.

`connect()` and the module-level `serve()` call it for you. An application calls it directly only
when it built the `Peer` itself, or when it wants to hold the task.

### Signature

```python
async def serve(self) -> None:
```

### Parameters

None — `serve()` takes no arguments. What it does is fixed by the peer it belongs to.

### Return

`None` — it returns when the socket closes. A socket that dies is not an error here: the peer is
declared dead, every live stream fails with `ConnectionLost`, `on_close` fires, and this returns.

### Raises

- `asyncio.CancelledError` — if the task running it is cancelled, which is the ordinary way to stop a
  peer you built yourself.
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

    # The acceptor's socket ends with the dialer's, so its read loop returns on its own.
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

`Promise<void>` — resolves when the socket closes cleanly.

### Raises

Rejects with whatever killed the read loop. Unlike Python's, which returns on any failure it can
attribute, this one re-throws after declaring the peer dead: a peer whose read loop died while still
reporting `isOpen` would be the worst possible state, so it dies first and rejects second. A socket
that closes normally resolves rather than rejecting.

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

It is **three lowercase hex characters drawn once per process**, a hyphen, and a counter incremented
once per connection in that process. So: it is not unique across processes, two processes can and
will produce the same `id`, and it **changes when a reconnected peer takes a new socket** - by
design, so a log shows a reconnect as a new `conn=` rather than as one continuous connection.

### Signature

```python
self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
```

### Parameters

None — `id` is a plain instance attribute, assigned in `__init__` and reassigned when the peer adopts
a new socket. Writing to it is not forbidden and not useful; muxws re-derives it on the next adopt.

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

The same shape and the same warnings: a per-module-load prefix of three hex characters, a hyphen, and
a counter that advances once per connection. Not unique across processes, and it changes on
reconnect.

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

An ordinary dict, with ordinary dict semantics, that muxws never reads. It is where an application
puts what it knows about the other end - a user id, a tenant, a set of subscriptions - so that a
`PeerRegistry` can find this peer by it.

It **dies with the socket**: on the acceptor a reconnect is a whole new `Peer` with an empty `tags`,
so nothing an application put there survives a disconnect. Re-populating it is part of what the hello
is for.

### Signature

```python
self.tags: dict[str, Any] = {}
```

### Parameters

None — it is a plain instance attribute. Mutate it like any dict.

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

None — a readonly reference to a mutable object: you may set and delete keys, you may not replace the
object.

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

Nothing is retained per **closed** stream: a stream that ends is dropped from this map immediately,
which is what keeps a long-lived connection's bookkeeping to two integers plus the live map.

### Signature

```python
@property
def streams(self) -> Mapping[int, Stream]:
```

### Parameters

None — it is a read-only property.

### Return

`Mapping[int, Stream]` — a **copy** taken at the moment you read it. Mutating it does not affect the
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

Whether this peer may be used right now. It is **two** things at once: the socket is open, and the
connection is established.

The difference matters only to a dialer with a hello. Between adopting a socket and having that hello
acknowledged, the socket is up but the connection is not: an application frame accepted in that
window would reach an acceptor that has not yet been told who is speaking. `is_open` is false there,
and `open()` refuses.

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

`boolean` — socket-open **and** established, for the reason the Python entry gives.

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

Which end this is. The dialer allocates odd stream ids, the acceptor even ones; that is the whole of
the asymmetry between them, plus the fact that only a dialer can have a reconnect helper.

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

It exists because idle means idle. The heartbeat reads this stamp rather than pinging on a fixed
schedule, so a busy connection never spends a ping to learn what its own traffic already proved.

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

`number` — 5000 milliseconds, the deadline an explicit `peer.ping()` uses when none is given. Python
spells the same number `timeout: float = 5.0` seconds. It is **not** the heartbeat's deadline, which
is 10000 milliseconds.

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

`number` — 10000 milliseconds, `peer.close()`'s drain window when `drainMs` is not given. Python
spells it `drain: float = 10.0` seconds.

### Raises

Raises: nothing.

### Example

```ts
import { DEFAULT_DRAIN_MS, JsonCodec, memoryPair, Peer } from 'muxws';

console.log(DEFAULT_DRAIN_MS, 'milliseconds of drain by default');

const [socket] = memoryPair();
const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
void peer.serve();
// Nothing is live, so even the full default window returns at once.
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
// Raising it is done on the receiver; a sender has no say and is told nothing.
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
remote's opens alone; this peer's own streams do not consume it. Never announced, so a sender learns
of it only from the `reset(REFUSED)` it provokes.

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
- [Types](./types.md) — `CloseReason`, `StreamHandler`, `ErrorSerializer`, `Frame`.
