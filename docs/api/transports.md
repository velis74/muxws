---
outline: deep
---

# Transports

`SocketAdapter` implementations are the **only** transport-specific code in muxws. The peer, streams,
fragmentation, the writer and reconnection use only the four `SocketAdapter` methods and do not depend
on ASGI, `ws` or the browser. Supporting a new WebSocket library requires one new adapter and no other
change.

The library ships five adapters:

| Adapter | Language | Role | Import |
|---|---|---|---|
| `StarletteSocket` | Python | acceptor | `muxws.transports.starlette` |
| `WebsocketsSocket` | Python | either | `muxws.transports.websockets_` |
| `MemorySocket` | both | either | `muxws.transports.memory` / `muxws` |
| `BrowserSocket` | TypeScript | dialer | `muxws` |
| `WsSocket` | TypeScript | either | `muxws/node` |

No adapter performs authentication. Authenticate at the HTTP upgrade, before `accept()`, with a
cookie, an `Authorization` header or a bearer entry in the subprotocol list. `accept()`'s
`allowed_origins` / `allowedOrigins` checks the handshake's `Origin` header. Do not authenticate in
the reconnect hello or in a stream's `headers`: both arrive after the socket is open.

## `SocketAdapter`

The interface through which the peer uses one WebSocket.

Text and binary sends are **separate methods**, not one polymorphic `send`. The peer chooses between
them from the codec's declared `codec.binary`, so an adapter does not inspect the message type.

In TypeScript every method except `receive()` may return synchronously or return a promise: the
memory pair has nothing to await, and a browser socket's `send` returns immediately. The peer awaits
the result in both cases. `receive()` always returns a promise.

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
| `send_text(text)` / `sendText(text)` | `str` / `string` | none; required | Puts one text WebSocket message on the wire. Called when `codec.binary` is false. |
| `send_bytes(data)` / `sendBytes(bytes)` | `bytes` / `ArrayBuffer` | none; required | Puts one binary WebSocket message on the wire. Called when `codec.binary` is true. |
| `receive()` | `() -> str \| bytes` / `() => Promise<string \| ArrayBuffer>` | none; required | Waits for the next inbound message and returns it. Raises `ConnectionClosed` when the socket closes; this is the peer's only signal that the connection is gone. An adapter that returned a sentinel instead, or kept waiting after the socket closed, would leave the read loop and every pending await hanging. |
| `close(code, reason)` | `code: int = 1000`, `reason: str = ""` / `code?: number`, `reason?: string` | `1000`, `""` | Closes the socket. Must be idempotent: the peer may call it on a socket that is already closed. `code` must not be `1006`, which a peer may not send. |

### Return

`SocketAdapter` is a type, not a callable. Python's is a `runtime_checkable` `Protocol`, so
`isinstance(obj, SocketAdapter)` is `True` for any object that has the four method **names**. It
checks names, not signatures; for that reason `accept()` tests for a framework socket *before* it
tests for an adapter.

### Raises

Raises: nothing itself. `receive()` implementations raise `ConnectionClosed` when the socket closes,
and send implementations raise it when called on a socket that is already closed.

`ConnectionClosed` is the only error this protocol requires, so it is defined in the shared error
module and not in a transport module. Any other error an adapter reports, such as an address it
cannot open or a dependency it cannot find, is defined in the adapter's own module as a subclass of
`TransportUrlError` or `TransportUnsupportedError`. Both bases are exported from the package root so
that adapters outside this repository can subclass them; see
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

Two adapters connected to each other in memory, without a network socket. It is part of the package,
not the test tree, because the reconnect tests and the conformance runner use it. It is also the
simplest way to run a peer pair in application tests, and every example on this site uses it.

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
| *(constructor)* | — | — | Takes no arguments. An unconnected socket delivers nothing; use `memory_pair()` / `memoryPair()` to get two connected sockets. |
| `sent` | `list[str \| bytes]` / `(string \| ArrayBuffer)[]` | `[]` | Every message this side sent, in order. Read it to assert on the wire without decoding. |
| `is_closed` / `isClosed` | `bool` / `boolean` | `False` / `false` | Whether this end has been closed or dropped. |
| `close(code, reason)` | `code: int = 1000`, `reason: str = ""` / `code?: number`, `reason?: string` | `1000`, `""` | Closes **both** ends cleanly and wakes each side's pending `receive()`. The pair sends no close frame, so both arguments are ignored; they exist because `SocketAdapter` declares them. |
| `drop()` | `() -> None` / `() => void` | — | Simulates an abrupt socket loss: both ends close without a close frame. Every live stream raises `ConnectionLost`. |
| `inject(message)` | `str \| bytes` / `string \| ArrayBuffer` | required | Pushes a raw, already-encoded message into **this** side's inbox, bypassing the other peer. The conformance runner uses it to deliver frames a conforming implementation does not send. |

### Return

`receive()` returns the next message. `send_text` / `send_bytes` / `close` / `drop` return `None` /
`void`, `inject` returns `None` / `void`, and `is_closed` / `isClosed` is a boolean property.

### Raises

`receive()` raises `ConnectionClosed` (code 1006) when the socket is closed or dropped while it is
waiting or already drained, on every call after the close; messages queued before the close are
returned first. `send_text` / `send_bytes` raise `ConnectionClosed` (code 1006) on a
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

Two connected `MemorySocket`s. A message sent on one is received on the other. `drop()` on either
simulates an abrupt socket loss for both.

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

A two-element tuple / array of connected `MemorySocket`s. Neither end is a dialer or an acceptor;
the role is set by the `Peer` constructed on each one.

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

The Starlette / FastAPI acceptor. `starlette` is imported inside the functions that need it, so
installing muxws does not install a web framework; an application that imports this module must
install `starlette` itself.

```bash
pip install "muxws[starlette]"
```

**The endpoint must not call `websocket.accept()`**: `perform_upgrade` performs the upgrade, because
it selects the subprotocol. On a codec mismatch it sends the ASGI denial response *before* any
accept, as HTTP **400** with no subprotocol selected. `websocket.close()` before accept would send a
403 instead.

Applications do not usually use either symbol directly: `muxws.accept(websocket)` recognises a
Starlette `WebSocket` and calls `perform_upgrade`. Use them directly to get the adapter without a
peer.

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
| `websocket` (constructor) | `Any` | required | A `starlette.websockets.WebSocket` that has **already** been accepted. Typed `Any` so the module can be imported without `starlette` installed. |
| `websocket` (`perform_upgrade`) | `Any` | required | A `starlette.websockets.WebSocket` in the `CONNECTING` state. Positional. |
| `configured` | `str` | required | The codec name this acceptor speaks, e.g. `"json"`. The offered subprotocol list is matched against `muxws.v1.<configured>`. Positional. |
| `code` (`close`) | `int` | `1000` | The WebSocket close code sent to the client. |
| `reason` (`close`) | `str` | `""` | The close frame's reason text. |

### Return

`perform_upgrade` returns a `StarletteSocket` wrapping the now-accepted websocket. `close()` returns
`None`, and does nothing when the client is already disconnected.

### Raises

- `ProtocolError` from `perform_upgrade` when the websocket is not in the `CONNECTING` state, that is,
  when the endpoint already accepted it.
- `CodecMismatch` from `perform_upgrade` when no offered subprotocol matches; the HTTP 400 has
  already been sent. `muxws.serve()` catches it and returns without raising, so a codec
  misconfiguration does not produce a traceback from the endpoint.
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
    print(await peer.request({"n": 1}, timeout=5.0))
    await peer.close(drain=0.5)

    server.should_exit = True
    with contextlib.suppress(asyncio.CancelledError):
        await serving


asyncio.run(main())
```

## `WebsocketsSocket`

The adapter for the `websockets` library, in **either** role: it wraps a `websockets` connection,
whether dialled or accepted.

```bash
pip install "muxws[websockets]"
```

The module name has a trailing underscore (`muxws.transports.websockets_`), because a
`websockets.py` inside the package would shadow the library it imports.

`websockets` completes the handshake before it calls the connection handler, so an acceptor must
install `muxws.select_subprotocol` as the library's `select_subprotocol=` hook to refuse a mismatched
codec with HTTP 400. A check after the handshake can only close an already-open socket, which the
protocol forbids where the transport can refuse the upgrade. For a transport with neither hook,
[`verify_negotiated`](#verify-negotiated-python) checks an already-open socket and raises
`CodecMismatch`.

`muxws.connect()` builds this adapter when it dials.

It is also the adapter over a Unix domain socket: `unix_serve` and a `ws+unix://` dial produce the
same `websockets` connection object as their TCP counterparts, and the adapter does not distinguish
them.

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
| `connection` | `Any` | required | A `websockets` connection object, client or server side. Typed `Any` so the module can be imported without `websockets` installed. |
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
        print(await peer.request({"n": 1}, timeout=5.0))
        await peer.close(drain=0.5)


asyncio.run(main())
```

## `require_websockets()` (Python)

Imports `websockets` and returns the module, or raises `WebsocketsNotInstalledError` naming the
extra that installs it. Both dial paths (`ws:`/`wss:` and `ws+unix:`) call it. It is the supported
way to check whether this process can dial without dialling.

Only an **absent** package raises that class: a `ModuleNotFoundError` whose `name` is exactly
`websockets`. A missing submodule, or a package that fails while importing, re-raises its own
`ImportError`.

### Signature

```python
def require_websockets() -> Any: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

`Any`: the imported `websockets` module. Typed `Any` because `muxws.transports.websockets_` does not
import `websockets` at module scope, so the package has no required runtime dependencies.

### Raises

- `WebsocketsNotInstalledError` — `websockets` is not installed. A `TransportUnsupportedError`; its
  message contains [`INSTALL_HINT`](#install-hint-python).

### Example

```python
from muxws.transports.websockets_ import require_websockets

print("dialable:", require_websockets().__name__)
```

## `verify_dialable_url()` (Python)

Refuses a URL that `websockets` cannot dial, before any socket is opened. `connect()` calls it once,
before the first dial, so a bad address raises from the `connect()` call and never from a background
reconnection.

### Signature

```python
def verify_dialable_url(url: str, *, uri: str | None = None) -> None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | The URL passed to `connect()`. It is quoted in the message and, when `uri` is `None`, it is the URL that is parsed. |
| `uri` | `str \| None` | `None` | The logical `ws://` URI a `ws+unix:` dial builds and passes to `unix_connect(uri=...)`. When given, it is parsed instead of `url`, because the `ws+unix:` handshake parses it; the message then quotes both. Keyword-only. |

### Return

`None`. It returns when the URL is dialable and raises when it is not.

### Raises

- `WebsocketsNotInstalledError` — checked first, because the URL parser is supplied by the dependency.
- `WebsocketUrlError` — `websockets.uri.parse_uri` rejected the string. The `InvalidURI` or
  `ValueError` it raised is chained as `__cause__`.

### Example

```python
from muxws.transports.websockets_ import verify_dialable_url, WebsocketUrlError

verify_dialable_url("ws://127.0.0.1:8000/ws")
print("ws://127.0.0.1:8000/ws is dialable")

try:
    verify_dialable_url("ws+unix:///run/api.sock:/y", uri="ws://user@/y")
except WebsocketUrlError as exc:
    print("refused:", exc)
```

## `verify_negotiated()` (Python)

Checks the subprotocol on an already-open socket. Used only for a transport that offers neither a
selection hook nor a way to deny the upgrade; an acceptor that can answer 400 uses
[`select_subprotocol`](./accept.md#select-subprotocol-python) instead.

It does not close the socket. `connect()` and `accept()` close it with `POLICY_VIOLATION` and
re-raise; `serve()` catches the `CodecMismatch` from `accept()` and returns.

### Signature

```python
def verify_negotiated(negotiated: str | None, configured: str) -> None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `negotiated` | `str \| None` | required | The subprotocol the handshake negotiated. `None` means the peer selected none, which is refused like any other mismatch. Positional. |
| `configured` | `str` | required | The codec name this end speaks, e.g. `"json"`. The match is against `muxws.v1.<configured>` exactly. Positional. |

### Return

`None`. It returns when the two match.

### Raises

- `CodecMismatch` — they do not match. One error line naming both subprotocols and the `MUXWS_CODEC`
  and `VITE_MUXWS_CODEC` environment variables is logged under the `muxws.codec` logger before the
  raise.

### Example

```python
from muxws import CodecMismatch
from muxws.transports.websockets_ import verify_negotiated

verify_negotiated("muxws.v1.json", "json")
print("agreed")

try:
    verify_negotiated("muxws.v1.msgpack", "json")
except CodecMismatch as exc:
    print("refused:", exc)
```

## `POLICY_VIOLATION` (Python)

The WebSocket close code a dialer uses when a codec mismatch is detected on an already-open socket:
`1008`. Exported so that third-party transports can close with the same code.

### Signature

```python
POLICY_VIOLATION = 1008
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | `int` | `1008` | A module constant, not a call. |

### Return

Nothing — it is a value.

### Raises

Raises: nothing.

### Example

```python
from muxws.transports.websockets_ import POLICY_VIOLATION

print("close code:", POLICY_VIOLATION)
```

## `INSTALL_HINT` (Python)

The command that installs this transport's dependency: `pip install muxws[websockets]`.
`WebsocketsNotInstalledError`'s message uses this constant.

### Signature

```python
INSTALL_HINT = "pip install muxws[websockets]"
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | `str` | `"pip install muxws[websockets]"` | A module constant, not a call. |

### Return

Nothing — it is a value.

### Raises

Raises: nothing.

### Example

```python
from muxws.transports.websockets_ import INSTALL_HINT

print(INSTALL_HINT)
```

## `parse_unix_url()` (Python)

Parses the `ws+unix:` URL grammar. This string transformation is the entire Unix transport in
Python; it opens no socket. `connect()` calls it on **every** URL, before anything is opened, to
select the dial path: a `ws:` or `wss:` URL returns `None` and is passed to `websockets.connect()`
unchanged. Python's `connect()` resolves the codec before it calls this function, so an unregistered
codec name is reported before a refused URL; the browser `connect()` refuses the scheme first.

The grammar is that of the `ws` npm package, with one difference: the URL's path and query are split
on the **first** colon and the remainder is kept whole, whereas `ws` splits on every colon and drops
everything after the second. The part before the colon is the file to open; the part after is the
HTTP request target, which is `/` when there is no colon or nothing follows it. The scheme is
case-insensitive, `ws+unix:/run/api.sock:/ws` and `ws+unix:relative.sock:/ws` are accepted, and an
authority (which precedes the path and is not part of the split) becomes the `Host` header. The query
is re-attached before the split, so it belongs to the request target when there is a colon and to the
file name when there is not: `ws+unix:///run/api.sock?tenant=42` opens a file named
`/run/api.sock?tenant=42`. The path is used as `urlsplit` returns it, without percent-decoding:
`ws+unix:///tmp/my dir/a.sock` opens `/tmp/my dir/a.sock`, and `ws+unix:///tmp/a%20b.sock` opens a
file named `a%20b.sock`. `muxws/node` derives the same path from the same URL.

### Signature

```python
def parse_unix_url(url: str) -> UnixTarget | None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | Any URL `connect()` was given. Positional. |

### Return

`UnixTarget | None`: a `UnixTarget` for a `ws+unix:` URL, and `None` for every URL whose scheme is
neither `ws+unix:` nor `wss+unix:`, including malformed ones, which `websockets` validates.

### Raises

- `ValueError` — `urllib.parse.urlsplit` cannot parse the string; this happens for an authority with
  an unbalanced `[` (`ws://[::1`). `urlsplit` raises before the scheme is checked, so it is **not** a
  `MuxwsError` and `except TransportUrlError` around `connect()` does not catch it. Every other
  unparseable URL is validated by `websockets` and raises `WebsocketUrlError`.
- `UnixUrlError` — a `wss+unix:` URL, which is not a scheme muxws supports; a URL naming no socket
  file; or a request target that does not begin with `/`. Such a target would be folded into the
  authority (`…/a.sock:ws` becomes `ws://localhostws`, a valid URL), so the dial would open the right
  file, request `/`, and succeed against an acceptor that does not route on the target.
- `UnixSocketsUnsupportedError` — the interpreter has no `socket.AF_UNIX` (Windows). The platform
  check runs after the `wss+unix:` refusal and before the two grammar checks.

### Example

```python
from muxws.transports.unix import parse_unix_url

print(parse_unix_url("ws+unix:///run/muxws/api.sock:/ws?tenant=42"))
print(parse_unix_url("ws+unix:///run/muxws/api.sock"))
print(parse_unix_url("ws+unix://gateway/run/muxws/api.sock:/ws"))
print(parse_unix_url("ws://127.0.0.1:8000/ws"))
```

## `UnixTarget` (Python)

The return type of `parse_unix_url`: the two arguments of `unix_connect(path, uri=...)`. Over a
filesystem socket the connection target and the HTTP request are separate: the file is where to
connect, and the handshake is an HTTP request that needs a request target and a `Host`, which cannot
be derived from the path.

Frozen. `connect()` parses the URL once, outside the dial closure, so every reconnection dials the
same target.

### Signature

```python
@dataclass(frozen=True, slots=True)
class UnixTarget:
    path: str
    uri: str
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `path` | `str` | required | The filesystem path of the listening socket, as written in the URL. The kernel limits it to about 108 bytes (104 on macOS); a longer path raises `OSError: AF_UNIX path too long`, which names neither the length nor the component to shorten. |
| `uri` | `str` | required | `ws://<authority><target>`: the logical URL of the handshake. Its authority is the URL's own, or `localhost` when the URL has none. It is sent as the request line and the `Host` header. |

### Return

A frozen dataclass instance, built by `parse_unix_url`.

### Raises

Raises: nothing.

### Example

```python
from muxws.transports.unix import parse_unix_url, UnixTarget

target = parse_unix_url("ws+unix:///run/muxws/api.sock:/ws")
assert isinstance(target, UnixTarget)
print(target.path, "|", target.uri)
```

## `SCHEME` (Python)

The scheme this transport handles: `"ws+unix"`. Compare a configured URL's scheme against this
constant rather than a literal. `parse_unix_url` matches it case-insensitively, because `urlsplit`
lowercases the scheme.

### Signature

```python
SCHEME = "ws+unix"
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | `str` | `"ws+unix"` | A module constant, not a call. |

### Return

Nothing — it is a value.

### Raises

Raises: nothing.

### Example

```python
from urllib.parse import urlsplit

from muxws.transports.unix import SCHEME

print(SCHEME, "|", urlsplit("WS+UNIX:///run/muxws/api.sock:/ws").scheme == SCHEME)
```

## `BrowserSocket`

The browser adapter, over the platform's global `WebSocket`. It is the only module in the browser
entry point that uses `WebSocket`, and it has no dependencies.

A WebSocket delivers messages through events and the peer's read loop pulls them, so inbound messages
are queued in the adapter and `receive()` takes from that queue. Listeners are attached in the
constructor, before the handshake completes, so a message that arrives before the read loop starts is
queued. A pending `receive()` rejects when the socket closes; this tells the peer the connection is
gone.

`connect()` from `muxws` constructs one. Use the class directly to get the adapter without a peer, or
to wrap a test double.

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
| `socket` (constructor) | `WebSocket` | required | An already-constructed platform socket, open or still connecting. The constructor sets its `binaryType` to `'arraybuffer'`, so every inbound binary message is an `ArrayBuffer`. |
| `url` (`connect`) | `string` | required | The `ws://` or `wss://` URL to dial, passed unchanged to the platform `WebSocket` constructor. The `ws+unix:` refusal is in `connect()` from `muxws`, not in this adapter, so `BrowserSocket.connect('ws+unix://…')` reaches that constructor and throws the platform's `DOMException`, not `UnixSocketsUnsupportedError`. |
| `codecName` (`connect`) | `string` | required | The codec name, offered as `muxws.v1.<codecName>` in first position. |
| `options` (`connect`) | `BrowserSocketOptions` | `{}` | Extra subprotocol entries; see below. |
| `code` (`close`) | `number` | `1000` | The WebSocket close code. |
| `reason` (`close`) | `string` | `''` | The close frame's reason text. |

### Return

`connect()` returns a `Promise<BrowserSocket>` that resolves once the handshake is complete **and**
the negotiated subprotocol is exactly `muxws.v1.<codecName>`. `close()` is idempotent and returns a
promise that resolves once the socket is closed. `receive()` returns the next message.

### Raises

- `CodecMismatch` from `connect()` when the socket closes before opening. A browser receives a
  refused upgrade and an unreachable server as the same generic error with no status, so the message
  is composed from the offered codec name and names both causes. Also from `connect()` when the
  handshake completed with a different subprotocol or none; in that case one error line naming both
  subprotocols and both environment variables is logged through `logger`, and the socket is closed
  with `1008` before the rejection.
- `ConnectionClosed` from `sendText` / `sendBytes` when the socket is not open, and from a pending
  `receive()` when it closes.
- `ProtocolError` from `receive()` when the socket delivers a message that is neither a string nor
  an `ArrayBuffer`.

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
  console.log(await peer.request({ n: 1 }, { timeoutMs: 5000 }));

  await peer.close({ drainMs: 500 });
  server.close();
}

void main();
```

## `BrowserSocketOptions`

The options object of `BrowserSocket.connect`. It has one field.

### Signature

```ts
export interface BrowserSocketOptions {
  subprotocols?: readonly string[];
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `subprotocols` | `readonly string[] \| undefined` | `undefined` | Application subprotocol entries, appended **after** the muxws entry. Typically a bearer token: a browser cannot set request headers on a WebSocket upgrade, and the subprotocol list is the only place to send a credential with it. The muxws acceptor ignores these entries. |

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

The Node adapter over the `ws` package, in **either** role. It is imported **only** by the
`muxws/node` subpath. No module reachable from `muxws` imports it, so the browser bundle does not
contain `ws`, which does not run in a browser.

```bash
npm install ws
```

`accept()` and `serve()` from `muxws/node` construct one and verify the negotiated subprotocol;
construct it directly only after verifying the subprotocol. The `connect()` from `muxws/node` wraps
every socket it dials (`ws:`, `wss:` and `ws+unix:`) in one, so it is also the TypeScript adapter over
a Unix domain socket.

It has no `isClosed` property. `BrowserSocket` and `MemorySocket` have one; `SocketAdapter` does not
require it.

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
| `socket` | `WebSocket` from `ws` | required | A `ws` connection. The constructor sets its `binaryType` to `'arraybuffer'` and attaches the `message`, `close` and `error` listeners, so messages that arrive before the read loop starts are queued. |
| `code` (`close`) | `number` | `1000` | The WebSocket close code. |
| `reason` (`close`) | `string` | `''` | The close frame's reason text. |

### Return

The constructor returns the adapter. `sendText`, `sendBytes` and `close` return `void`; `receive`
returns a promise for the next message.

### Raises

`ConnectionClosed` from `sendText` / `sendBytes` once the socket has closed or errored, and from
`receive()` once the queued messages are consumed. A `close` event produces the code and reason of
the close frame, with `wasClean` true only for `1000`; an `error` event produces code `1006`.

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
    // `accept()` does this and also verifies the subprotocol.
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

> The two hooks installed in the examples above, `handleProtocols` and `refuseMismatchedUpgrade`, are
> documented on [Accepting a connection](/api/accept), with `accept()` and `serve()`, because they run
> *before* there is a socket for an adapter to wrap. Both are required: a `ws` server that installs
> only `handleProtocols` completes handshakes it must refuse.

## See also

- [Codec](./codec.md) — `codec.binary`, which decides whether the peer calls `send_text` or
  `send_bytes`.
- [Connect](./connect.md) and [Accept](./accept.md) — the functions that construct these adapters.
- [Errors](./errors.md) — `ConnectionClosed` and `CodecMismatch`, which adapters raise, and
  `TransportUrlError` and `TransportUnsupportedError`, the bases for a third-party adapter's errors.
- [Guide: transports](../guide/transports.md) — one runnable snippet per framework, and where
  authentication belongs.
- [Guide: Unix domain sockets](../guide/transports.md#unix-domain-sockets) — the `ws+unix:` URL
  grammar `parse_unix_url` implements, an acceptor and a dialer on each side of a socket file, and
  what each port does on Windows.
