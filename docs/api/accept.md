---
outline: deep
---

# accept

`accept()` is the acceptor's entry point: it turns one inbound WebSocket into a `Peer`. `serve()` is
`accept()` plus "register this handler and run the read loop until the socket closes", which is the
whole of most acceptors.

The acceptor is the peer that did not dial. It is otherwise identical to the dialer: the same `Peer`
type, the same `open()`, the same cancellation. What it does not have is a reconnect helper - it has
nothing to dial - and what it does have is the handshake, because the acceptor is the only party that
can decide which subprotocol to select.

Two things are worth knowing before the tables:

- **`max_payload_bytes`, `max_concurrent_streams` and `error_serializer` belong to this peer alone.**
  Nothing about them is announced or negotiated. The remote learns of a limit only from the reset it
  provokes, so raising a limit is done on the *receiving* peer; the sender has no say.
- **The upgrade is refused, not accepted-then-closed.** A dialer offering a codec this acceptor does
  not speak gets HTTP 400 wherever the transport allows one, which is what
  `select_subprotocol` (Python, `websockets`), `accept()` (Python, Starlette) and
  `refuseMismatchedUpgrade` (TypeScript, `ws`) exist to do.

An acceptor on a Unix domain socket is an ordinary acceptor. In Python it is
`websockets.asyncio.server.unix_serve(handler, path, select_subprotocol=muxws.select_subprotocol)`;
in Node it is a `WebSocketServer` attached to an `http.Server` that listens on a socket file. Both
hand these functions the same connection object a TCP listener does, and nothing on this page changes
— see [Unix domain sockets](/guide/transports#unix-domain-sockets) for the two runnable acceptors.

## `accept()` (Python)

Accept an inbound connection and return a peer that is **not yet serving**: nothing is read from the
socket until something awaits `peer.serve()`.

### Signature

```python
async def accept(
    socket: SocketAdapter | Any,
    *,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
) -> Peer:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `SocketAdapter \| Any` | required | One of three things: a Starlette/FastAPI `WebSocket`, which is **upgraded here** because `accept()` is the only party that knows which subprotocol to select; a `websockets` connection, which has already handshaken and is only verified; or anything that already implements `SocketAdapter`, which is taken as it is. |
| `max_payload_bytes` | `int` | `67_108_864` | The largest reassembled payload this peer accepts, in bytes of encoded output. Checked as fragments arrive, not after reassembly. Local and never announced. |
| `max_concurrent_streams` | `int` | `100` | How many streams the **remote** may have open here at once. The excess `open` is answered `reset(REFUSED)` without the handler running, so the opener may safely take it elsewhere. Local and never announced. |
| `error_serializer` | `ErrorSerializer \| None` | `None` | Turns a handler's exception into the payload of the `reset(APPLICATION_ERROR)` frame. `None` means `default_error_serializer`, which puts the class name and `str(exc)` **on the wire**; a public-facing acceptor should replace it with a redacting one. |
| `codec` | `Codec \| None` | `None` | An explicit codec, which wins over `settings.codec`. `None` looks the configured name up in the registry and never falls back to JSON. |
| `max_frame_bytes` | `int` | `MAX_FRAME_BYTES` (65536) | Test-only. The frame cap is a protocol constant; the conformance runner lowers it to exercise fragmentation without megabyte fixtures. A value too small to hold an envelope plus one indivisible unit is rejected here. |

### Return

`Peer` — an acceptor peer. Its socket is open and the connection is established (an acceptor has no
hello to wait for), but its read loop is not running: call `peer.on_stream(...)` and then await
`peer.serve()`, or use `serve()` below, which does both.

### Raises

- `CodecNotRegistered` — the configured codec name was never registered. Raised before the socket is
  touched.
- `CodecMismatch` — the dialer's offer does not carry `muxws.v1.<this codec>`. For a Starlette socket
  the ASGI 400 has already gone out when this is raised. For a `websockets` connection nothing has
  been sent and nothing is closed: the check runs on an already-open socket, and closing it is the
  caller's job. An acceptor that installs `select_subprotocol` refuses the mismatch at the handshake
  instead and never reaches this.
- `ProtocolError` — the Starlette socket had already been accepted by the application (muxws performs
  the upgrade itself), or the object is neither a `SocketAdapter` nor a socket muxws knows how to
  upgrade, or `max_frame_bytes` is too small to hold an envelope plus one unit of payload.

### Example

```python
import asyncio

from muxws import accept, Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = await accept(acceptor_socket, max_concurrent_streams=8, max_payload_bytes=1_048_576)

    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply({"seen": payload})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    print(await dialer.request({"n": 1}, timeout=5.0))

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `serve()` (Python)

Accept, register `handler`, and run the read loop until the socket closes. This is the body of a
FastAPI WebSocket route.

### Signature

```python
async def serve(socket: SocketAdapter | Any, *, handler: StreamHandler, **peer_options: Any) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `SocketAdapter \| Any` | required | The same three kinds `accept()` takes. |
| `handler` | `StreamHandler` | required | `(payload, stream)`, sync or async, called once per incoming stream. Returning without ending the stream ends it implicitly; raising resets it with `APPLICATION_ERROR`. |
| `**peer_options` | `Any` | — | Passed straight to `accept()`: `max_payload_bytes`, `max_concurrent_streams`, `error_serializer`, `codec`, `max_frame_bytes`. An unknown name raises `TypeError` from `accept()`. |

### Return

`None` — it returns when the socket closes, cleanly or otherwise.

### Raises

- `CodecNotRegistered` — the configured codec name was never registered.
- `ProtocolError` — the socket is not one muxws can adapt, was already accepted, or `max_frame_bytes`
  is too small to hold an envelope plus one unit of payload.
- `TypeError` — an unknown keyword in `**peer_options`.

It **swallows** two and returns instead. `CodecMismatch`: there is no peer, and raising would put a
traceback in the application's route for an ordinary misconfiguration. Where the socket was a
Starlette one the 400 has already gone out; where it was a `websockets` connection nothing was sent
and nothing was closed, so that socket is still open and still the application's to close.
`ConnectionClosed`: a socket that ends is how this function is supposed to finish.

### Example

```python
import asyncio

import websockets

from muxws import connect, select_subprotocol, serve, Stream


async def handler(payload: object, stream: Stream) -> None:
    if payload == "stream me":
        for index in range(3):
            await stream.send({"chunk": index})
        await stream.end()
        return
    await stream.reply({"seen": payload})


async def route(socket: object) -> None:
    await serve(socket, handler=handler, max_concurrent_streams=64)


async def main() -> None:
    async with websockets.serve(route, "127.0.0.1", 0, select_subprotocol=select_subprotocol) as server:
        port = server.sockets[0].getsockname()[1]
        peer = await connect(f"ws://127.0.0.1:{port}")
        print(await peer.request("hello", timeout=5.0))
        async for chunk in peer.open("stream me", end=True):
            print(chunk)
        await peer.close(drain=0.1)


asyncio.run(main())
```

## `select_subprotocol()` (Python)

The handshake hook for the `websockets` library: which subprotocol to select, or a refusal.

Install it as `websockets.serve(..., select_subprotocol=muxws.select_subprotocol)`. `websockets`
completes the handshake before calling the connection handler, so the decision has to be handed to it
up front.

The refusal is **raised, not returned**. Returning `None` here would answer 101 with no
`Sec-WebSocket-Protocol` header and leave the mismatch to be discovered on an already-open socket;
raising `NegotiationError` is what `websockets` turns into the HTTP 400 the protocol asks for.

It is a thin wrapper over `muxws.subprotocol`, whose public names are the pieces every hook in either
port is built from: `PREFIX` (`"muxws.v1."`), `offer(codec_name, extra=None)` for the list a dialer
sends, `select(offered, configured)` for the entry an acceptor picks or `None` to refuse,
`find_offer(offered)` for the single `muxws.v1.*` entry in an offer, `generation_of(entry)` for the
generation number in one, and `mismatch_error(configured)` for the `CodecMismatch` a dialer composes
for itself. TypeScript exports `PREFIX` and `select` from `muxws`; the rest are internal there.

### Signature

```python
def select_subprotocol(connection: Any, subprotocols: list[str]) -> str:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `connection` | `Any` | required | The `websockets` server connection. muxws ignores it entirely - the decision depends only on what was offered and on `settings.codec`. Present because `websockets` passes it. |
| `subprotocols` | `list[str]` | required | The `Sec-WebSocket-Protocol` entries the dialer offered, in order. Only the one carrying the `muxws.v1.` prefix is read; every other entry belongs to the application and is left alone. |

### Return

`str` — the selected entry, always `f"muxws.v1.{settings.codec}"`.

### Raises

- `websockets.exceptions.NegotiationError` — the offer carries a different codec, a different muxws
  generation, or no muxws entry at all. `websockets` answers HTTP 400. The full diagnostic (what was
  offered against what this acceptor is configured for) is logged under the `muxws.codec` logger
  before the exception is raised.
- `ModuleNotFoundError` — `websockets` is not installed.

### Example

```python
import asyncio

import websockets

from muxws import select_subprotocol, serve, Stream
from muxws.subprotocol import PREFIX


async def handler(payload: object, stream: Stream) -> None:
    await stream.reply(payload)


async def route(socket: object) -> None:
    await serve(socket, handler=handler)


async def main() -> None:
    print(select_subprotocol(None, ["muxws.v1.json", "bearer.token.abc"]))

    async with websockets.serve(route, "127.0.0.1", 0, select_subprotocol=select_subprotocol) as server:
        port = server.sockets[0].getsockname()[1]
        try:
            await websockets.connect(f"ws://127.0.0.1:{port}", subprotocols=[f"{PREFIX}msgpack"])
        except Exception as exc:  # noqa: BLE001 - the type depends on the websockets release
            status = getattr(getattr(exc, "response", None), "status_code", None)
            print("a mismatched codec is refused at the handshake with", status)


asyncio.run(main())
```

## `accept()` (TypeScript, `muxws/node`)

Wrap an accepted `ws` connection in a peer. The connection has already handshaken by the time `ws`
hands it over, so the subprotocol is verified on the open socket and the socket is closed with 1008
if it disagrees.

It takes a `ws` `WebSocket` and nothing else — it reads `socket.protocol` and wraps the argument in a
`WsSocket`. Python's `accept()` also takes anything already implementing `SocketAdapter`; the
TypeScript way to use an adapter of your own is `new Peer(adapter, { codec, isDialer: false })`.

There is no browser `accept()`: a browser cannot accept a WebSocket.

### Signature

```ts
export async function accept(socket: NodeWebSocket, options: AcceptOptions = {}): Promise<Peer>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `WebSocket` (from `ws`) | required | The connection `WebSocketServer` emitted. Its `protocol` must be `muxws.v1.<codec>`. |
| `options` | `AcceptOptions` | `{}` | The four local settings plus the codec override. See [`AcceptOptions`](#acceptoptions-typescript). |

### Return

`Promise<Peer>` — an acceptor peer whose read loop is **not** running. Register `onStream` and then
call `peer.serve()`, or use `serve()` below.

### Raises

Rejects with:

- `CodecNotRegistered` — the configured codec name is not in the registry. `muxws/node` registers
  nothing itself: the JSON registration is a side effect of importing `muxws`, so a Node-only process
  must import `muxws` or call `registerCodec` during bootstrap.
- `CodecMismatch` — the negotiated subprotocol is not `muxws.v1.<codec>`. The socket has been closed
  with 1008 by the time this rejects.
- `ProtocolError` — `maxFrameBytes` is too small to hold an envelope plus one indivisible unit.

### Example

```ts
import { JsonCodec, registerCodec, type Stream } from 'muxws';
import { accept, connect, handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
server.on('connection', (socket) => {
  void accept(socket, { maxConcurrentStreams: 8, maxPayloadBytes: 1_048_576 }).then((peer) => {
    peer.onStream(async (payload: unknown, stream: Stream) => {
      await stream.reply({ seen: payload });
    });
    return peer.serve();
  });
});
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const dialer = await connect(`ws://127.0.0.1:${port}`);
console.log(await dialer.request({ n: 1 }, { timeoutMs: 5000 }));
await dialer.close({ drainMs: 100 });
server.close();
```

## `serve()` (TypeScript, `muxws/node`)

Accept, register `handler`, and run the read loop until the socket closes.

### Signature

```ts
export async function serve(socket: NodeWebSocket, options: AcceptOptions & { handler: StreamHandler }): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `socket` | `WebSocket` (from `ws`) | required | The connection `WebSocketServer` emitted. |
| `options.handler` | `StreamHandler` | required | `(payload, stream)`, called once per incoming stream. Returning without ending the stream ends it implicitly; throwing resets it with `APPLICATION_ERROR`. |
| `options` (rest) | `AcceptOptions` | `{}` | `maxPayloadBytes`, `maxConcurrentStreams`, `errorSerializer`, `codec`, `maxFrameBytes`, passed through to `accept()`. |

### Return

`Promise<void>` — resolves when the socket closes, and also resolves on a `CodecMismatch`, which it
swallows exactly as Python's `serve()` does. `accept()` has closed the socket with 1008 by then, so
there is nothing left to do with it, and rejecting inside a `ws` connection handler surfaces as an
unhandled rejection that takes the process down.

### Raises

Rejects with everything `accept()` rejects with **except** `CodecMismatch`: `CodecNotRegistered`,
`ProtocolError`, plus whatever `peer.serve()` fails with - a read loop that dies takes the peer down
first and then rejects with the same error.

### Example

```ts
import { JsonCodec, registerCodec, type Stream } from 'muxws';
import { connect, handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';
import { WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
server.on('connection', (socket) => {
  void serve(socket, {
    maxConcurrentStreams: 64,
    handler: async (payload: unknown, stream: Stream) => {
      if (payload === 'stream me') {
        for (const index of [0, 1, 2]) await stream.send({ chunk: index });
        await stream.end();
        return;
      }
      await stream.reply({ seen: payload });
    },
  }).catch(() => undefined);
});
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const peer = await connect(`ws://127.0.0.1:${port}`);
console.log(await peer.request('hello', { timeoutMs: 5000 }));
for await (const chunk of peer.open('stream me', { end: true })) console.log(chunk);
await peer.close({ drainMs: 100 });
server.close();
```

## `AcceptOptions` (TypeScript)

Everything `accept()` and `serve()` accept besides the socket. Every field is optional.

### Signature

```ts
export interface AcceptOptions {
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
  errorSerializer?: ErrorSerializer;
  codec?: Codec;
  maxFrameBytes?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `maxPayloadBytes` | `number` | `67_108_864` | The largest reassembled payload this peer accepts, in bytes. Enforced on the fragment that crosses it. Local, never announced. |
| `maxConcurrentStreams` | `number` | `100` | How many streams the remote may hold open here at once. The excess is refused without the handler running. Local, never announced. |
| `errorSerializer` | `ErrorSerializer` | `defaultErrorSerializer` | Turns a handler's failure into the `reset(APPLICATION_ERROR)` payload. The default puts the error's `name` and `message` **on the wire**; replace it with a redacting one on a public-facing acceptor. |
| `codec` | `Codec` | `getCodec(settings.codec)` | An explicit codec, which wins over the configured name. |
| `maxFrameBytes` | `number` | `MAX_FRAME_BYTES` (65536) | Test-only lowering of the protocol frame cap. |

### Return

None — `AcceptOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, registerCodec } from 'muxws';
import type { AcceptOptions } from 'muxws/node';

registerCodec('json', new JsonCodec());

const options: AcceptOptions = {
  maxPayloadBytes: 1_048_576,
  maxConcurrentStreams: 16,
  // On a public-facing acceptor, redact: the default serializer puts the error's text on the wire.
  errorSerializer: () => ({ type: 'Error', message: 'internal error' }),
};

console.log(options.maxPayloadBytes, options.maxConcurrentStreams, options.errorSerializer?.(new Error('boom')));
```

## `handleProtocols()` (TypeScript, `muxws/node`)

The **selection** hook for a `ws` server: which subprotocol to echo back.

It is the closest thing in TypeScript to Python's `select_subprotocol`, and it is deliberately only
half the job: `ws` decides the subprotocol here, but returning `false` selects nothing rather than
refusing - `ws` still answers 101, just without a `Sec-WebSocket-Protocol` header. Refusing with 400
is `refuseMismatchedUpgrade`'s job, and an acceptor that installs only this hook completes handshakes
it should have rejected.

### Signature

```ts
export function handleProtocols(protocols: Set<string>): string | false;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `protocols` | `Set<string>` | required | The offered subprotocol entries, as `ws` supplies them. Only the `muxws.v1.` entry is read; the rest belong to the application. |

### Return

`string | false` — the selected `muxws.v1.<codec>` entry, or `false` when the offer does not match, in
which case `ws` selects nothing.

### Raises

Raises: nothing. A mismatch is reported by the return value and logged by `select` through the muxws
logger, at error level, so `logger.level` can turn it down.

### Example

```ts
import { JsonCodec, registerCodec } from 'muxws';
import { handleProtocols } from 'muxws/node';
import { WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

console.log(handleProtocols(new Set(['muxws.v1.json', 'bearer.token.abc'])));
console.log(handleProtocols(new Set(['muxws.v1.msgpack'])));

// Installed on the server, always together with refuseMismatchedUpgrade.
const server = new WebSocketServer({ port: 0, handleProtocols });
await new Promise<void>((resolve) => server.once('listening', resolve));
console.log('listening on port', (server.address() as { port: number }).port > 0);
server.close();
```

## `refuseMismatchedUpgrade()` (TypeScript, `muxws/node`)

Install the **refusal** on a `ws` server, so a codec it does not speak is answered HTTP 400.

`handleProtocols` cannot do this: whatever it returns, `ws` completes the handshake with 101.
`shouldHandle` is the hook that aborts an upgrade with a status, so the acceptor needs both. The
inherited `shouldHandle` runs first, so a server constructed with `path` keeps that check.

### Signature

```ts
export function refuseMismatchedUpgrade(server: WebSocketServer): WebSocketServer;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `server` | `WebSocketServer` (from `ws`) | required | The server to wrap. Its `shouldHandle` is replaced with one that also requires a matching `muxws.v1.<codec>` entry in the request's `Sec-WebSocket-Protocol` header. |

### Return

`WebSocketServer` — the same instance, mutated and returned so it can be wrapped inline:
`refuseMismatchedUpgrade(new WebSocketServer({ port, handleProtocols }))`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, PREFIX, registerCodec } from 'muxws';
import { handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { WebSocket, WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const status = await new Promise<number | undefined>((resolve) => {
  const client = new WebSocket(`ws://127.0.0.1:${port}`, [`${PREFIX}msgpack`]);
  client.once('unexpected-response', (_request, response) => resolve(response.statusCode));
  client.once('error', () => resolve(undefined));
});
console.log('a mismatched codec is refused at the handshake with', status);
server.close();
```

## `select()` (TypeScript)

The decision both TypeScript hooks are built on: the value an acceptor selects, or `null` to refuse.
Its Python twin is `muxws.subprotocol.select`, which `select_subprotocol` wraps.

Use it directly only when you are writing an adapter for a server muxws does not ship a hook for.

### Signature

```ts
export function select(offered: readonly string[], configured: string): string | null;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `offered` | `readonly string[]` | required | The subprotocol entries the dialer offered, in order. |
| `configured` | `string` | required | The codec name this acceptor is configured for, normally `settings.codec`. |

### Return

`string | null` — `muxws.v1.<configured>` when the offer carries exactly that entry, `null` otherwise.
`null` means refuse the handshake: a mismatched codec, a different muxws generation and an offer with
no muxws entry at all are one answer, because muxws never negotiates a fallback.

### Raises

Raises: nothing. A refusal is a `null` return plus one error-level line through the muxws
[`logger`](./types.md#logger-and-loglevel-typescript), naming what was offered, what is configured,
and which environment variable to change. It reaches `console.error` at the default level and is
silenced by lowering `logger.level`.

### Example

```ts
import { select, settings } from 'muxws';

console.log(select(['muxws.v1.json', 'bearer.token.abc'], 'json'));
console.log(select(['muxws.v2.json'], 'json'));
console.log(select([], settings.codec));
```

## See also

- [`connect()`](./connect.md) — the dialer's entry point.
- [`Peer`](./peer.md) — what `accept()` returns.
- [Transports](./transports.md) — the `SocketAdapter` port and the adapters muxws ships.
- [Errors](./errors.md) — every class named above.
