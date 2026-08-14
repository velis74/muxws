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
| `WsSocket` | TypeScript | either | `muxws/node` |

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
[`verify_negotiated`](#verify-negotiated-python) checks an already-open socket and raises
`CodecMismatch`.

Dialling through `muxws.connect()` builds this adapter for you and there is nothing to name.

It is also the adapter over a Unix domain socket, unchanged and unaware: `unix_serve` and a
`ws+unix://` dial produce the same `websockets` connection object as their TCP counterparts, and this
class never learns which one it got.

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

## `require_websockets()` (Python)

Import `websockets` and hand the module back, or raise `WebsocketsNotInstalledError` naming the extra
that installs it. It is the gate both dial arms go through, and the only supported way to ask whether
this process can dial without dialling.

Only the package being **absent** becomes that class: a `ModuleNotFoundError` whose `name` is exactly
`websockets`. A missing submodule or a package that fails while importing keeps its own `ImportError`.

### Signature

```python
def require_websockets() -> Any: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| *(none)* | — | — | Takes no arguments. |

### Return

`Any` — the imported `websockets` module. Typed `Any` because the module cannot be imported at
`muxws.transports.websockets_`'s own module scope; that is what keeps the package's required runtime
dependencies at zero.

### Raises

- `WebsocketsNotInstalledError` — `websockets` is not installed. A `TransportUnsupportedError`, and
  its message carries [`INSTALL_HINT`](#install-hint-python).

### Example

```python
from muxws.transports.websockets_ import require_websockets

print("dialable:", require_websockets().__name__)
```

## `verify_dialable_url()` (Python)

Refuse a URL `websockets` cannot dial, before anything opens a socket. `connect()` calls it once, at
the call, so a bad address raises out of the call the caller made rather than out of a background
reconnection.

### Signature

```python
def verify_dialable_url(url: str, *, uri: str | None = None) -> None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | The URL the caller typed. It is the one quoted in the message, and — when `uri` is `None` — the one parsed. |
| `uri` | `str \| None` | `None` | The logical `ws://` URI a `ws+unix:` dial synthesises and hands to `unix_connect(uri=...)`. When it is given it is what gets parsed, because it is what that arm's handshake parses; the message then quotes both. Keyword-only. |

### Return

`None` — it returns when the URL is dialable and raises when it is not.

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

Check the subprotocol on an already-open socket. The last resort, for a transport that offers neither
a selection hook nor a way to deny the upgrade; an acceptor that can answer 400 uses
[`select_subprotocol`](./accept.md#select-subprotocol-python) instead.

It closes nothing. What the caller does after the raise differs by caller: `connect()` closes the
socket with `POLICY_VIOLATION` and re-raises, `accept()` lets the `CodecMismatch` out with the socket
untouched, and `serve()` swallows it and returns, also leaving the socket open.

### Signature

```python
def verify_negotiated(negotiated: str | None, configured: str) -> None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `negotiated` | `str \| None` | required | The subprotocol the handshake actually settled on. `None` means the peer selected nothing, which is refused like any other mismatch. Positional. |
| `configured` | `str` | required | The codec name this end speaks, e.g. `"json"`. The match is against `muxws.v1.<configured>` exactly. Positional. |

### Return

`None` — it returns when the two agree.

### Raises

- `CodecMismatch` — they do not agree. Both names are logged under the `muxws.codec` logger first.

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

The WebSocket close code a dialer uses when the mismatch could only be found on an already-open
socket: `1008`. Exported so a transport of your own closes with the same code muxws does.

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

The command that installs this transport's dependency, spelled the way a reader can paste it:
`pip install muxws[websockets]`. It is named once, here, so the remedy in
`WebsocketsNotInstalledError`'s message cannot drift out of date.

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

The `ws+unix:` URL grammar, and the whole of the Unix transport in Python: a string transformation
with no socket in it. `connect()` calls it on **every** URL, before anything is opened, so it is the
branch rather than a validator — a `ws:` or `wss:` URL comes back `None` and goes to
`websockets.connect()` untouched. `connect()` resolves the codec before it gets here, so a URL this
function refuses and a codec name that is not registered are ordered codec-first in Python; the
browser `connect()` refuses the scheme first.

The grammar is the one the `ws` npm package has dialled for years, with one correction: the URL's path
and query are split on the **first** colon and the remainder is kept whole, where `ws` splits on every
colon and drops what follows the second. The part in front of the colon is the file to open; the part
behind is the HTTP request target, and it is `/` when there is no colon and when nothing follows one.
The scheme is case-insensitive, `ws+unix:/run/api.sock:/ws` and `ws+unix:relative.sock:/ws` parse, and
an authority — which sits before the path and so outside the split — becomes the `Host` header. The
query is re-attached before the split, so it travels with the request target when there is a colon and
stays part of the file name when there is not: `ws+unix:///run/api.sock?tenant=42` opens a file called
`/run/api.sock?tenant=42`.

### Signature

```python
def parse_unix_url(url: str) -> UnixTarget | None: ...
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | Any URL `connect()` was given. Positional. |

### Return

`UnixTarget | None` — a `UnixTarget` for a `ws+unix:` URL, `None` for every URL whose scheme is
neither `ws+unix:` nor `wss+unix:`, malformed ones included: judging those belongs to `websockets`.

### Raises

- `ValueError` — the string is one `urllib.parse.urlsplit` itself cannot read, which is an authority
  with an unbalanced `[` (`ws://[::1`). It comes out of `urlsplit` before the scheme is looked at, so
  it is **not** a `MuxwsError` and `except TransportUrlError` around `connect()` does not catch it.
  Every other unreadable URL is `websockets`' to judge and comes back as `WebsocketUrlError`.
- `UnixUrlError` — a `wss+unix:` URL, which is not a scheme muxws has; a URL naming no socket file; or
  a request target that does not begin with `/`. The third is refused because such a target is folded
  into the authority instead — `…/a.sock:ws` becomes `ws://localhostws`, a valid URL — so the dial
  would open the right file, ask for `/`, and succeed against an acceptor that does not route.
- `UnixSocketsUnsupportedError` — the interpreter has no `socket.AF_UNIX`, which means Windows. The
  platform check runs after the `wss+unix:` refusal and before the two grammar checks.

### Example

```python
from muxws.transports.unix import parse_unix_url

print(parse_unix_url("ws+unix:///run/muxws/api.sock:/ws?tenant=42"))
print(parse_unix_url("ws+unix:///run/muxws/api.sock"))
print(parse_unix_url("ws+unix://gateway/run/muxws/api.sock:/ws"))
print(parse_unix_url("ws://127.0.0.1:8000/ws"))
```

## `UnixTarget` (Python)

What `parse_unix_url` returns: everything `unix_connect(path, uri=...)` needs, and nothing about
muxws. Two fields, because over a filesystem socket the transport and the request really are separate
— the file says where to connect, and the handshake is still an HTTP request that needs a target and a
`Host`, neither of which can be derived from a path.

Frozen. `connect()` parses the URL once, outside the dial closure, so every reconnection dials this
same target rather than re-deriving it.

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
| `path` | `str` | required | The filesystem path of the listening socket, as written in the URL. The kernel caps it at about 108 bytes (104 on macOS), and an over-length one raises `OSError: AF_UNIX path too long`, which names neither the length nor the component to shorten. |
| `uri` | `str` | required | `ws://<authority><target>` — the logical URL the handshake claims to be for. Its authority is the URL's own, or `localhost` when the URL has none, and it is what goes on the wire as the request line and the `Host` header. |

### Return

A frozen dataclass instance; `parse_unix_url` is what builds one.

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

The scheme this transport owns: `"ws+unix"`. Compare a configured URL's scheme against it rather than
against a literal, and note that `parse_unix_url` matches it case-insensitively, as `urlsplit` folds
the scheme before the comparison.

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
| `url` (`connect`) | `string` | required | The `ws://` or `wss://` URL to dial, handed to the platform `WebSocket` constructor as it stands. The `ws+unix:` refusal belongs to `connect()` in `muxws` and not to this adapter, so `BrowserSocket.connect('ws+unix://…')` reaches that constructor and throws the platform's `DOMException` rather than `UnixSocketsUnsupportedError`. |
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

The Node adapter over the `ws` package, in **either** role. It is imported **only** by the `muxws/node`
subpath: nothing reachable from `muxws` may import it, or a browser bundle pulls in a dependency a
browser cannot run.

```bash
npm install ws
```

`accept()` and `serve()` from `muxws/node` build one for you and also verify the negotiated
subprotocol; construct it directly only when you have already done that verification yourself. The
`muxws/node` `connect()` wraps every socket it dials in one too — `ws:`, `wss:` and `ws+unix:` alike —
so this is also the TypeScript adapter over a Unix domain socket.

In TypeScript it is the one adapter without an `isClosed`; `BrowserSocket` and `MemorySocket` both
have one, and the `SocketAdapter` port requires it of neither.

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
- [Guide: Unix domain sockets](../guide/transports.md#unix-domain-sockets) — the `ws+unix:` URL
  grammar `parse_unix_url` implements, an acceptor and a dialer on each side of a socket file, and
  what each port does on Windows.
