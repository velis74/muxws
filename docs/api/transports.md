---
outline: deep
---

# Transports

`SocketAdapter` is the **only** transport-specific code in muxws. Everything above it — the peer, the
streams, fragmentation, the writer, reconnection — is written against four methods and knows nothing
about ASGI, `ws`, or the browser. Supporting a new WebSocket library means writing one adapter and
changing nothing else.

The library ships five adapters:

| Adapter | Language | Role | Import |
|---|---|---|---|
| `StarletteSocket` | Python | acceptor | `muxws.transports.starlette` |
| `WebsocketsSocket` | Python | either | `muxws.transports.websockets_` |
| `MemorySocket` | both | either | `muxws.transports.memory` / `muxws` |
| `BrowserSocket` | TypeScript | dialer | `muxws` |
| `WsSocket` | TypeScript | acceptor | `muxws/node` |

Authentication does not belong in any of them. It belongs at the HTTP upgrade, before `accept()` —
a cookie, an `Authorization` header, or a bearer entry in the subprotocol list — and never in the
reconnect hello or in a stream's `headers`, both of which arrive after the socket is already open.

## `SocketAdapter`

One WebSocket, seen the only way the peer is allowed to see it.

Text and binary sends are **separate methods**, never one polymorphic `send`. The peer picks between
them from `codec.binary`, which is declared rather than sniffed, so an adapter never has to guess
what kind of message it is holding.

In TypeScript every method may be synchronous: the memory pair has nothing to await and a browser
socket's `send` returns immediately. The peer awaits them regardless, so an implementation is free to
return either a value or a promise.

### Signature

```python
@runtime_checkable
class SocketAdapter(Protocol):
    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

```ts
export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
```

### Parameters

The methods an implementation must provide:

| Name | Type | Default | What it does |
|---|---|---|---|
| `send_text(text)` / `sendText(text)` | `str` / `string` | none — required | Puts one text WebSocket message on the wire. Called when `codec.binary` is false. |
| `send_bytes(data)` / `sendBytes(bytes)` | `bytes` / `ArrayBuffer` | none — required | Puts one binary WebSocket message on the wire. Called when `codec.binary` is true. |
| `receive()` | `() -> str \| bytes` / `() => Promise<string \| ArrayBuffer>` | none — required | Waits for the next inbound message and returns it. Raises `ConnectionClosed` when the socket dies — that is how the peer learns the connection is gone, so an adapter that returned a sentinel instead would hang the read loop. |
| `close(code, reason)` | `code: int = 1000`, `reason: str = ""` / `code?: number`, `reason?: string` | `1000`, `""` | Closes the socket. Should be idempotent: the peer may call it on a socket that is already gone. |

### Return

`SocketAdapter` is a type, not a callable. Python's is a `runtime_checkable` `Protocol`, so
`isinstance(obj, SocketAdapter)` is `True` for any object carrying those four method **names** — it
checks names, not signatures, which is why `accept()` tests for a framework socket *before* it tests
for an adapter.

### Raises

Raises: nothing itself. `receive()` implementations raise `ConnectionClosed` on socket death, and
send implementations raise it when asked to write to a socket that is already closed.

`ConnectionClosed` is the one error this protocol *mandates*, which is why it lives in the shared
error module and not in any transport's: every adapter raises it because the contract requires it of
every adapter. Anything else your adapter reports — an address it cannot open, a dependency it cannot
find — is yours, and belongs in your own module as a subclass of `TransportUrlError` or
`TransportUnsupportedError`. Both bases are exported from the package root, precisely so that an
adapter written outside this repository can subclass them; see
[Writing an adapter of your own](../guide/errors.md#writing-an-adapter-of-your-own).

### Example

```python
import asyncio

from muxws import JsonCodec, Peer, SocketAdapter
from muxws.transports.memory import memory_pair


class CountingSocket:
    """A SocketAdapter that wraps another one and counts what goes past."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.sent = 0
        self.received = 0

    async def send_text(self, text: str) -> None:
        self.sent += 1
        await self._inner.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        self.sent += 1
        await self._inner.send_bytes(data)

    async def receive(self) -> str | bytes:
        message = await self._inner.receive()
        self.received += 1
        return message

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._inner.close(code, reason)


async def main() -> None:
    left, right = memory_pair()
    counting = CountingSocket(left)
    print("is a SocketAdapter:", isinstance(counting, SocketAdapter))

    codec = JsonCodec()
    dialer = Peer(counting, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        await stream.reply({"echo": payload})

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    print(await dialer.request({"n": 1}, timeout=2.0))
    print("frames out:", counting.sent, "| frames in:", counting.received)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, type SocketAdapter, memoryPair } from 'muxws';

/** A SocketAdapter that wraps another one and counts what goes past. */
class CountingSocket implements SocketAdapter {
  sent = 0;
  received = 0;

  constructor(private readonly inner: SocketAdapter) {}

  sendText(text: string): Promise<void> | void {
    this.sent += 1;
    return this.inner.sendText(text);
  }

  sendBytes(bytes: ArrayBuffer): Promise<void> | void {
    this.sent += 1;
    return this.inner.sendBytes(bytes);
  }

  async receive(): Promise<string | ArrayBuffer> {
    const message = await this.inner.receive();
    this.received += 1;
    return message;
  }

  close(code?: number, reason?: string): Promise<void> | void {
    return this.inner.close(code, reason);
  }
}

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const counting = new CountingSocket(left);
  const codec = new JsonCodec();
  const dialer = new Peer(counting, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  acceptor.onStream(async (payload, stream) => {
    await stream.reply({ echo: payload });
  });
  void dialer.serve();
  void acceptor.serve();

  console.log(await dialer.request({ n: 1 }, { timeoutMs: 2000 }));
  console.log('frames out:', counting.sent, '| frames in:', counting.received);

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `MemorySocket`

Two adapters wired to each other, with no socket anywhere. It ships in the package rather than in the
test tree because the reconnect tests and the conformance runner both need it — and because it is by
far the easiest way to exercise a peer pair in your own tests, and in every example on this site.

`sent` holds every message this side put on the wire, in order, already encoded.

In Python the methods are coroutines; in TypeScript `sendText`, `sendBytes`, `close`, `drop` and
`inject` are synchronous and only `receive` returns a promise.

### Signature

```python
class MemorySocket:
    def __init__(self) -> None: ...

    sent: list[str | bytes]

    @property
    def is_closed(self) -> bool: ...

    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...

    async def drop(self) -> None: ...

    def inject(self, message: str | bytes) -> None: ...
```

```ts
export class MemorySocket implements SocketAdapter {
  readonly sent: (string | ArrayBuffer)[];

  get isClosed(): boolean;
  sendText(text: string): void;
  sendBytes(bytes: ArrayBuffer): void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): void;
  drop(): void;
  inject(message: string | ArrayBuffer): void;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(constructor)* | — | — | Takes no arguments. An unwired socket delivers nothing; use `memory_pair()` / `memoryPair()` to get two that are wired to each other. |
| `sent` | `list[str \| bytes]` / `(string \| ArrayBuffer)[]` | `[]` | Every message this side sent, in order. Read it to assert on the wire without decoding. |
| `is_closed` / `isClosed` | `bool` / `boolean` | `False` / `false` | Whether this end has been closed or dropped. |
| `close(code, reason)` | `code: int = 1000`, `reason: str = ""` / `code?: number`, `reason?: string` | `1000`, `""` | Closes **both** ends cleanly, waking each side's pending `receive()`. The pair has no wire, so neither argument travels anywhere; they exist because the port declares them. |
| `drop()` | `() -> None` / `() => void` | — | Simulates socket death: no close frame, no warning, both ends simply stop. Every stream that was live raises `ConnectionLost`. |
| `inject(message)` | `str \| bytes` / `string \| ArrayBuffer` | required | Pushes a raw, already-encoded message into **this** side's inbox, bypassing the other peer. It is how the conformance runner delivers frames no correct implementation would send. |

### Return

`receive()` returns the next message. `send_text` / `send_bytes` / `close` / `drop` return `None` /
`void`, `inject` returns `None` / `void`, and `is_closed` / `isClosed` is a boolean property.

### Raises

`receive()` raises `ConnectionClosed` (code 1006) when the socket is closed or dropped while it is
waiting or already drained. `send_text` / `send_bytes` raise `ConnectionClosed` (code 1006) on a
closed socket. `close`, `drop` and `inject` raise nothing.

### Example

```python
import asyncio

from muxws import ConnectionClosed, Frame, JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()

    await left.send_text(codec.encode(Frame("ping", nonce="abc")))
    print("wire:", left.sent[0])
    print("other end reads:", codec.decode(await right.receive()).type)

    right.inject('{"type":"pong","nonce":"abc"}')
    print("injected:", codec.decode(await right.receive()).type)

    print("closed before drop:", left.is_closed)
    await left.drop()
    print("closed after drop: ", left.is_closed, right.is_closed)
    try:
        await right.receive()
    except ConnectionClosed as exc:
        print("receive raises:", exc.code, "|", exc)


asyncio.run(main())
```

```ts
import { ConnectionClosed, JsonCodec, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();

  left.sendText(codec.encode({ type: 'ping', nonce: 'abc' }) as string);
  console.log('wire:', left.sent[0]);
  console.log('other end reads:', codec.decode(await right.receive()).type);

  right.inject('{"type":"pong","nonce":"abc"}');
  console.log('injected:', codec.decode(await right.receive()).type);

  console.log('closed before drop:', left.isClosed);
  left.drop();
  console.log('closed after drop: ', left.isClosed, right.isClosed);
  try {
    await right.receive();
  } catch (error) {
    if (error instanceof ConnectionClosed) console.log('receive raises:', error.code, '|', error.message);
  }
}

void main();
```

## `memory_pair` / `memoryPair`

Two `MemorySocket`s wired to each other. Whatever one sends, the other receives. `drop()` on either
simulates socket death for both.

In TypeScript this is exported from the package root. In Python it is not re-exported from `muxws`;
import it from `muxws.transports.memory`.

### Signature

```python
def memory_pair() -> tuple[MemorySocket, MemorySocket]: ...
```

```ts
export function memoryPair(): [MemorySocket, MemorySocket];
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

A two-element tuple / array of wired `MemorySocket`s. Neither end is a "dialer" or an "acceptor" —
that is decided by the `Peer` you put on each one.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import JsonCodec, Peer
from muxws.transports.memory import memory_pair


async def main() -> None:
    left, right = memory_pair()
    codec = JsonCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)

    @acceptor.on_stream
    async def handle(payload, stream):
        for index in range(3):
            await stream.send({"chunk": index})
        await stream.end({"chunk": "done"})

    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    async for chunk in dialer.open({"action": "stream"}):
        print(chunk)

    await dialer.close(drain=0.1)
    for task in tasks:
        task.cancel()


asyncio.run(main())
```

```ts
import { JsonCodec, Peer, memoryPair } from 'muxws';

async function main(): Promise<void> {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const dialer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });

  acceptor.onStream(async (payload, stream) => {
    for (const index of [0, 1, 2]) await stream.send({ chunk: index });
    // `end` takes an options object in TypeScript; the last payload is a field of it.
    await stream.end({ payload: { chunk: 'done' } });
  });
  void dialer.serve();
  void acceptor.serve();

  for await (const chunk of dialer.open({ action: 'stream' })) console.log(chunk);

  await dialer.close({ drainMs: 100 });
}

void main();
```

## `StarletteSocket` and `perform_upgrade`

The Starlette / FastAPI acceptor. `starlette` is imported inside the module, so installing muxws does
not install a web framework — importing this module is how an application says it wants one.

```bash
pip install "muxws[starlette]"
```

`perform_upgrade` is the important half. **The endpoint must not call `websocket.accept()`**: muxws
performs the upgrade itself because it is the only party that knows which subprotocol to select. On a
codec mismatch the ASGI denial response goes out *before* any accept, as HTTP **400** with no
subprotocol selected — `websocket.close()` before accept would render 403, which is not the same
answer.

You normally never name either symbol: `muxws.accept(websocket)` recognises a Starlette `WebSocket`
and calls `perform_upgrade` for you. They are documented because an application that wants the
adapter without the peer — or wants to see exactly where the upgrade happens — needs them.

### Signature

```python
class StarletteSocket:
    def __init__(self, websocket: Any) -> None: ...

    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


async def perform_upgrade(websocket: Any, configured: str) -> StarletteSocket: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `websocket` (constructor) | `Any` | required | A `starlette.websockets.WebSocket` that has **already** been accepted. Typed `Any` so the module can be imported without starlette installed. |
| `websocket` (`perform_upgrade`) | `Any` | required | A `starlette.websockets.WebSocket` in the `CONNECTING` state. Positional. |
| `configured` | `str` | required | The codec name this acceptor speaks, e.g. `"json"`. The offered subprotocol list is matched against `muxws.v1.<configured>`. Positional. |
| `code` (`close`) | `int` | `1000` | The WebSocket close code sent to the client. |
| `reason` (`close`) | `str` | `""` | The close frame's reason text. |

### Return

`perform_upgrade` returns a `StarletteSocket` wrapping the now-accepted websocket. `close()` returns
`None`, and does nothing when the client is already disconnected.

### Raises

- `ProtocolError` from `perform_upgrade` when the websocket is not in the `CONNECTING` state, which
  means the endpoint accepted it first.
- `CodecMismatch` from `perform_upgrade` when no offered subprotocol matches; the HTTP 400 has
  already been sent by then. `muxws.serve()` catches this and returns quietly, because raising would
  turn an ordinary misconfiguration into a traceback out of the endpoint.
- `ConnectionClosed` from `receive()` when the client disconnects.
- `ProtocolError` from `receive()` for an ASGI message that is neither text nor bytes.

### Example

```python
import asyncio
import contextlib
import socket

import uvicorn

from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket

import muxws

from muxws.transports.starlette import perform_upgrade


async def echo(payload, stream) -> None:
    await stream.reply({"echo": payload})


async def endpoint(websocket: WebSocket) -> None:
    # Never `await websocket.accept()` first: perform_upgrade selects the subprotocol.
    adapter = await perform_upgrade(websocket, "json")
    print("adapter:", type(adapter).__name__)
    await muxws.serve(adapter, handler=echo)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


async def main() -> None:
    port = free_port()
    app = Starlette(routes=[WebSocketRoute("/ws", endpoint)])
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)

    peer = await muxws.connect(f"ws://127.0.0.1:{port}/ws")
    # 5.0 seconds, as a float.
    print(await peer.request({"n": 1}, timeout=5.0))
    await peer.close(drain=0.5)

    server.should_exit = True
    with contextlib.suppress(asyncio.CancelledError):
        await serving


asyncio.run(main())
```

## `WebsocketsSocket`

The adapter for the `websockets` library, in **either** role: it wraps a connection the library
handed you, whether that connection was dialled or accepted.

```bash
pip install "muxws[websockets]"
```

The module name keeps its trailing underscore — `muxws.transports.websockets_` — because a
`websockets.py` inside the package would shadow the library it imports.

`websockets` completes the handshake before it calls your handler, so an acceptor must install
`muxws.select_subprotocol` as the library's `select_subprotocol=` hook to refuse a mismatched codec
with HTTP 400. Doing it afterwards, on an open socket, is the "complete the handshake and close
later" that the 400 exists to avoid. As a last resort — for a transport offering neither hook —
`muxws.transports.websockets_.verify_negotiated(negotiated, configured)` checks an already-open
socket and raises `CodecMismatch`; the caller then closes with `POLICY_VIOLATION` (1008), which the
same module exports.

Dialling through `muxws.connect()` builds this adapter for you and there is nothing to name.

It is also the adapter over a Unix domain socket, unchanged and unaware: `unix_serve` and a
`ws+unix://` dial produce the same `websockets` connection object as their TCP counterparts, and this
class never learns which one it got. That is the strongest evidence the library has for the claim that
the adapter is the only transport-specific code in it — a whole transport arrived without touching
these four methods.

### Signature

```python
class WebsocketsSocket:
    def __init__(self, connection: Any) -> None: ...

    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `connection` | `Any` | required | A `websockets` connection object, client or server side. Typed `Any` so the module imports without `websockets` installed. |
| `code` (`close`) | `int` | `1000` | The WebSocket close code. |
| `reason` (`close`) | `str` | `""` | The close frame's reason text. |

### Return

The constructor returns the adapter. `send_text`, `send_bytes` and `close` return `None`; `receive`
returns the next message.

### Raises

`ConnectionClosed` from `send_text`, `send_bytes` and `receive` when the underlying connection has
closed. The close code is read from the close frame that was exchanged, falling back to `1006` when
there was none, and `was_clean` is true only for code `1000`.

### Example

```python
import asyncio

import websockets

import muxws

from muxws.transports.websockets_ import WebsocketsSocket


async def echo(payload, stream) -> None:
    await stream.reply({"echo": payload})


async def handle(connection) -> None:
    # `websockets` completed the handshake already, so the adapter wraps an open socket.
    adapter = WebsocketsSocket(connection)
    print("adapter:", type(adapter).__name__)
    await muxws.serve(adapter, handler=echo)


async def main() -> None:
    # `select_subprotocol=` is what refuses a mismatched codec with HTTP 400 at the handshake.
    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        port = service.sockets[0].getsockname()[1]
        peer = await muxws.connect(f"ws://127.0.0.1:{port}")
        # 5.0 seconds, as a float.
        print(await peer.request({"n": 1}, timeout=5.0))
        await peer.close(drain=0.5)


asyncio.run(main())
```

## `BrowserSocket`

The browser half of the seam, over the platform's global `WebSocket`. It is the only file in the
browser entry point that knows what a WebSocket is, and it pulls in no dependency to do it.

A WebSocket is a push source and the peer's read loop is a pull loop, so inbound messages queue in
the adapter and `receive()` drains that queue. Listeners are attached in the constructor rather than
after the handshake, so a message that arrives before the read loop starts is queued rather than
dropped. A pending `receive()` rejects when the socket closes, which is how the peer learns the
connection died.

`connect()` from `muxws` builds one for you. Name the class yourself when you want the adapter
without the peer, or when you are handing a test double in.

### Signature

```ts
export class BrowserSocket implements SocketAdapter {
  readonly socket: WebSocket;

  constructor(socket: WebSocket);

  static connect(url: string, codecName: string, options?: BrowserSocketOptions): Promise<BrowserSocket>;

  get isClosed(): boolean;
  sendText(text: string): void;
  sendBytes(bytes: ArrayBuffer): void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` (constructor) | `WebSocket` | required | An already-constructed platform socket, open or still connecting. Its `binaryType` is set to `'arraybuffer'` here, which is what makes an inbound binary message an `ArrayBuffer` and nothing else. |
| `url` (`connect`) | `string` | required | The `ws://` or `wss://` URL to dial. |
| `codecName` (`connect`) | `string` | required | The codec name, offered as `muxws.v1.<codecName>` in first position. |
| `options` (`connect`) | `BrowserSocketOptions` | `{}` | Extra subprotocol entries; see below. |
| `code` (`close`) | `number` | `1000` | The WebSocket close code. |
| `reason` (`close`) | `string` | `''` | The close frame's reason text. |

### Return

`connect()` returns a `Promise<BrowserSocket>` that resolves once the handshake is complete **and**
the negotiated subprotocol is exactly `muxws.v1.<codecName>`. `close()` returns a promise that
resolves once the socket is actually closed, and is idempotent. `receive()` returns the next message.

### Raises

- `CodecMismatch` from `connect()` when the handshake fails — a refused upgrade reaches a browser as
  a generic error with no body, so the mismatch is composed from the codec name that was offered —
  and again when the server completed the handshake having negotiated something else, or nothing. In
  the second case the socket is closed with `1008` first.
- `ConnectionClosed` from `sendText` / `sendBytes` when the socket is not open, and from a pending
  `receive()` when it closes.
- `ProtocolError` from a queued `receive()` when the socket delivers a message that is neither a
  string nor an `ArrayBuffer`.

### Example

```ts
import { WebSocketServer } from 'ws';

import { BrowserSocket, JsonCodec, Peer } from 'muxws';
import { handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';

async function main(): Promise<void> {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address() as { port: number };
  server.on('connection', (socket) => {
    void serve(socket, {
      handler: async (payload, stream) => {
        await stream.reply({ echo: payload });
      },
    });
  });

  const adapter = await BrowserSocket.connect(`ws://127.0.0.1:${port}`, 'json');
  console.log('negotiated:', adapter.socket.protocol, '| closed:', adapter.isClosed);

  const peer = new Peer(adapter, { codec: new JsonCodec(), isDialer: true });
  void peer.serve();
  // 5000 milliseconds, as an integer.
  console.log(await peer.request({ n: 1 }, { timeoutMs: 5000 }));

  await peer.close({ drainMs: 500 });
  server.close();
}

void main();
```

## `BrowserSocketOptions`

The options object `BrowserSocket.connect` takes. It carries exactly one field.

### Signature

```ts
export interface BrowserSocketOptions {
  subprotocols?: readonly string[];
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `subprotocols` | `readonly string[] \| undefined` | `undefined` | Application subprotocol entries, appended **after** the muxws one. A bearer token is the common case — the browser cannot set a request header, and the subprotocol list is the one place a credential can ride the upgrade. The acceptor ignores every one of them. |

### Return

Nothing — it is an interface.

### Raises

Raises: nothing.

### Example

```ts
import { WebSocketServer } from 'ws';

import { BrowserSocket, type BrowserSocketOptions } from 'muxws';
import { handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';

async function main(): Promise<void> {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address() as { port: number };
  server.on('connection', (socket, request) => {
    console.log('offered:', request.headers['sec-websocket-protocol']);
    socket.close(1000, '');
  });

  const options: BrowserSocketOptions = { subprotocols: ['bearer.abc123'] };
  const adapter = await BrowserSocket.connect(`ws://127.0.0.1:${port}`, 'json', options);
  console.log('negotiated:', adapter.socket.protocol);

  await adapter.close();
  server.close();
}

void main();
```

## `WsSocket`

The Node acceptor's adapter, over the `ws` package. It is imported **only** by the `muxws/node`
subpath: nothing reachable from `muxws` may import it, or a browser bundle pulls in a dependency a
browser cannot run.

```bash
npm install ws
```

`accept()` and `serve()` from `muxws/node` build one for you and also verify the negotiated
subprotocol; construct it directly only when you have already done that verification yourself.

### Signature

```ts
export class WsSocket implements SocketAdapter {
  constructor(socket: NodeWebSocket);

  sendText(text: string): void;
  sendBytes(bytes: ArrayBuffer): void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): void;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `WebSocket` from `ws` | required | The connection `ws` handed you. Its `binaryType` is set to `'arraybuffer'` here, and the `message`, `close` and `error` listeners are attached in the constructor so nothing that arrives before the read loop starts is lost. |
| `code` (`close`) | `number` | `1000` | The WebSocket close code. |
| `reason` (`close`) | `string` | `''` | The close frame's reason text. |

### Return

The constructor returns the adapter. `sendText`, `sendBytes` and `close` return `void`; `receive`
returns a promise for the next message.

### Raises

`ConnectionClosed` from `sendText` / `sendBytes` once the socket has closed or errored, and from
`receive()` for the same reason. A `close` event produces code and reason from the close frame with
`wasClean` true only for `1000`; an `error` event produces code `1006`.

### Example

```ts
import { WebSocketServer } from 'ws';

import { JsonCodec, Peer } from 'muxws';
import { WsSocket, handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { connect } from 'muxws/node';

async function main(): Promise<void> {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address() as { port: number };

  server.on('connection', (socket) => {
    // `accept()` does this and verifies the subprotocol; here it is spelled out.
    const adapter = new WsSocket(socket);
    const peer = new Peer(adapter, { codec: new JsonCodec(), isDialer: false });
    peer.onStream(async (payload, stream) => {
      await stream.reply({ echo: payload });
    });
    void peer.serve();
  });

  const dialer = await connect(`ws://127.0.0.1:${port}`);
  console.log(await dialer.request({ n: 1 }, { timeoutMs: 5000 }));

  await dialer.close({ drainMs: 500 });
  server.close();
}

void main();
```

> The two hooks the snippets above install, `handleProtocols` and `refuseMismatchedUpgrade`, are
> documented on [Accepting a connection](/api/accept). They belong beside `accept()` and `serve()`
> rather than with the adapters, because they run *before* there is a socket for an adapter to wrap.
> The second one is not optional: a `ws` server that installs only `handleProtocols` completes
> handshakes it is required to refuse.

## See also

- [Codec](./codec.md) — `codec.binary`, which decides whether the peer calls `send_text` or
  `send_bytes`.
- [Connect](./connect.md) and [Accept](./accept.md) — the factories that build these adapters for
  you.
- [Errors](./errors.md) — `ConnectionClosed` and `CodecMismatch`, the two an adapter raises, plus
  `TransportUrlError` and `TransportUnsupportedError`, the two bases an adapter of your own subclasses.
- [Guide: transports](../guide/transports.md) — one runnable snippet per framework, and where
  authentication belongs.
