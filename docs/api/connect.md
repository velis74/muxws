---
outline: deep
---

# connect

`connect()` dials a URL, asserts the codec at the WebSocket handshake, sends the hello if one is
configured, starts the read loop, and hands back a `Peer` that is already serving. It is the dialer's
only entry point; everything an application does afterwards is done on the `Peer`.

Three things about it are worth knowing before the tables below:

- **A failed first attempt raises.** Reconnection covers connections that were established and then
  lost, never the first dial - a peer that retried a typo in the URL forever would report nothing at
  all. There is no option that changes this.
- **`max_payload_bytes`, `max_concurrent_streams` and `error_serializer` belong to this peer alone.**
  None of them is announced, negotiated or visible to the remote; the remote learns of a limit only
  from the reset it provokes.
- **Durations are seconds as floats in Python and milliseconds as integers in TypeScript.** The two
  ports carry the same defaults expressed in their own unit.

The Python package dials with the `websockets` library, so `pip install muxws[websockets]` is what
makes `connect()` work. In TypeScript there are two `connect()` functions: the one exported from
`muxws` dials with the platform `WebSocket` (the browser, and Node 22+ where that global exists), and
the one exported from `muxws/node` dials with the `ws` package and can set handshake headers.

## `connect()` (Python)

Dial `url` and return a serving peer.

### Signature

```python
async def connect(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    subprotocols: list[str] | None = None,
    hello: Any = None,
    hello_headers: dict[str, Any] | None = None,
    reconnect: Reconnect | None = None,
    ping_interval: float = 20.0,
    ping_timeout: float = 10.0,
    hello_timeout: float = 10.0,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
    on_stream: StreamHandler | None = None,
    on_close: Callable[[Any], None] | None = None,
    on_reconnect: Callable[[int, Any], None] | None = None,
) -> Peer:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | The WebSocket URL to dial, `ws://` or `wss://`. Re-dialled unchanged on every reconnection. |
| `headers` | `dict[str, str] \| None` | `None` | Extra HTTP headers for the upgrade request, passed to `websockets.connect(additional_headers=...)`. This is where a credential belongs, not in the hello. |
| `subprotocols` | `list[str] \| None` | `None` | Extra subprotocol entries appended **after** the `muxws.v1.<codec>` entry. The acceptor ignores every one of them; they exist for application-level handshake tricks. |
| `hello` | `Any` | `None` | The opening payload replayed verbatim on every connection this peer ever makes. Captured by value at this call and never re-read, so mutating your own object afterwards changes nothing. |
| `hello_headers` | `dict[str, Any] \| None` | `None` | Headers for the hello's `open` frame, captured by value alongside `hello`. Supplying either one makes the peer "hello-configured": it is not established until the acceptor's handler ends that stream. |
| `reconnect` | `Reconnect \| None` | `None` | The backoff schedule. `None` means `Reconnect()` - the defaults, with unlimited attempts. Reconnection applies only after a connection has been established once. |
| `ping_interval` | `float` — seconds | `20.0` | How long the socket may be **idle** before the heartbeat sends a `ping` frame. Idle means no frame in either direction. `0` or less disables the heartbeat. |
| `ping_timeout` | `float` — seconds | `10.0` | How long the heartbeat waits for the matching `pong` before declaring the socket dead. Detection is bounded by `ping_interval + ping_timeout` seconds. Not the same number as `peer.ping()`'s own default of 5.0 seconds. |
| `hello_timeout` | `float` — seconds | `10.0` | How long a hello may go unacknowledged before the attempt is failed. On the first connection that failure is what `connect()` raises. |
| `max_payload_bytes` | `int` | `67_108_864` | The largest reassembled payload **this** peer accepts, in bytes of encoded output. Local, never announced; a sender that crosses it gets `reset(PAYLOAD_TOO_LARGE)` on the fragment that crossed it. |
| `max_concurrent_streams` | `int` | `100` | How many streams the **remote** may have open here at once. Local, never announced; the excess open is refused without the handler running. |
| `error_serializer` | `ErrorSerializer \| None` | `None` | Turns a handler's exception into the payload of the `reset(APPLICATION_ERROR)` frame. `None` means `default_error_serializer`, which puts the exception's class name and `str(exc)` **on the wire** - replace it with a redacting one on any public-facing connection. |
| `codec` | `Codec \| None` | `None` | An explicit codec, which wins over `settings.codec`. `None` looks the configured name up in the registry and never falls back to JSON. |
| `max_frame_bytes` | `int` | `MAX_FRAME_BYTES` (65536) | Test-only. The frame cap is a protocol constant; the conformance runner lowers it to exercise fragmentation without megabyte fixtures. |
| `on_stream` | `StreamHandler \| None` | `None` | The incoming-stream handler, registered **before** the hello goes out. Passing it here rather than calling `peer.on_stream(...)` afterwards is what stops an acceptor's immediate push from being answered `reset(REFUSED, "no on_stream handler")`. |
| `on_close` | `Callable[[Any], None] \| None` | `None` | Called with a `CloseReason` on every socket loss after this one. A first connection that never establishes reports itself by raising, not through this handler. |
| `on_reconnect` | `Callable[[int, Any], None] \| None` | `None` | Called `(attempt, peer)` once per **re**-established connection, after the hello is acknowledged. The first connection does not fire it. |

### Return

`Peer` — a dialer peer whose read loop is already running as a background task, whose hello (if any)
has been acknowledged, and whose reconnect supervisor is running. `peer.is_open` is true when this
returns.

### Raises

- `CodecNotRegistered` — the configured codec name was never registered. Raised before any socket is
  touched.
- `CodecMismatch` — the acceptor refused the upgrade with HTTP 400, or completed it having negotiated
  something other than `muxws.v1.<codec>`. The socket is closed with 1008 in the second case.
- `StreamTimeout` — the hello was not acknowledged within `hello_timeout` seconds.
- `StreamReset` (or a subclass: `StreamRefused`, `RemoteError`, `ConnectionLost`) — the acceptor reset
  the hello stream, or the socket died under it.
- `ModuleNotFoundError` — `websockets` is not installed; `connect()` needs `muxws[websockets]`.
- Whatever the dial itself raised — `OSError` for a refused TCP connection, `websockets`'
  `InvalidHandshake` family for a broken upgrade. It is propagated unaltered.

**A failed first attempt raises whatever `reconnect` says.** Passing `reconnect=Reconnect()` does not
make `connect()` retry the first dial, and no option does; a caller who wants the first dial retried
writes that loop itself, where it can decide what a permanent failure looks like.

### Example

```python
import asyncio

import websockets

from muxws import connect, Reconnect, select_subprotocol, serve, Stream


async def echo(payload: object, stream: Stream) -> None:
    await stream.reply({"echo": payload})


async def handle(socket: object) -> None:
    await serve(socket, handler=echo)


async def main() -> None:
    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=select_subprotocol) as server:
        port = server.sockets[0].getsockname()[1]
        peer = await connect(
            f"ws://127.0.0.1:{port}",
            hello={"client": "docs"},
            reconnect=Reconnect(initial_delay=0.25, max_delay=5.0),
            ping_interval=20.0,
            ping_timeout=10.0,
        )
        print(await peer.request({"say": "hi"}, timeout=5.0))
        await peer.close(drain=0.1)


asyncio.run(main())
```

## `connect()` (TypeScript, `muxws`)

Dial `url` with the platform `WebSocket` and return a serving peer. This is the browser entry point;
it also works on any Node that exposes a global `WebSocket`. It cannot set handshake headers, because
a browser cannot.

### Signature

```ts
export async function connect(url: string, options: ConnectOptions = {}): Promise<Peer>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `string` | required | The WebSocket URL to dial. Re-dialled unchanged, with the same subprotocol offer, on every reconnection. |
| `options` | `ConnectOptions` | `{}` | Everything else. Each field is documented under [`ConnectOptions`](#connectoptions-typescript) below. |

### Return

`Promise<Peer>` — resolves with a dialer peer that is already serving and, if a hello was configured,
already acknowledged. `peer.isOpen` is true when it resolves.

### Raises

Rejects with:

- `CodecNotRegistered` — the configured codec name is not in the registry. Thrown before any socket is
  touched. Note that importing only `muxws/node` registers nothing; the JSON registration is a side
  effect of importing `muxws`.
- `CodecMismatch` — the acceptor refused the handshake, or negotiated something other than
  `muxws.v1.<codec>`.
- `StreamTimeout` — the hello was not acknowledged within `helloTimeoutMs` milliseconds.
- `StreamReset` (or `StreamRefused`, `RemoteError`, `ConnectionLost`) — the acceptor reset the hello
  stream, or the socket died under it.
- Whatever the underlying `WebSocket` failed with — an unreachable host surfaces as the platform's own
  error.

**A failed first attempt rejects whatever `reconnect` says**, for the reason the Python entry gives.

### Example

```ts
import { connect, JsonCodec, registerCodec, type Stream } from 'muxws';
import { accept, handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { WebSocketServer } from 'ws';

// `muxws/node` registers no codec of its own; importing `muxws` is what ships JSON registered.
registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
server.on('connection', (socket) => {
  void accept(socket).then((peer) => {
    peer.onStream(async (payload: unknown, stream: Stream) => {
      await stream.reply({ echo: payload });
    });
    return peer.serve();
  });
});
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const peer = await connect(`ws://127.0.0.1:${port}`, {
  hello: { client: 'docs' },
  pingIntervalMs: 20_000,
  pingTimeoutMs: 10_000,
});
console.log(await peer.request({ say: 'hi' }, { timeoutMs: 5000 }));
await peer.close({ drainMs: 100 });
server.close();
```

## `connect()` (TypeScript, `muxws/node`)

The same contract over the `ws` package, and the only one of the two that can set handshake headers.

### Signature

```ts
export async function connect(url: string, options: NodeConnectOptions = {}): Promise<Peer>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `string` | required | The WebSocket URL to dial, over the `ws` package. |
| `options` | `NodeConnectOptions` | `{}` | `ConnectOptions` plus `headers`. See [`NodeConnectOptions`](#nodeconnectoptions-typescript). |

### Return

`Promise<Peer>` — as for the browser entry point.

### Raises

The same set as the browser `connect()`, plus:

- `Error` with the message `unexpected server response: <status>` — the upgrade was answered with a
  status other than 101 and other than the 400 that means a refused muxws handshake.
- A `ws` transport error — an unreachable host, a TLS failure, a socket reset during the upgrade.

`ws` is an optional peer dependency imported inside the dial closure, so `accept()` and
`handleProtocols()` keep working in a process that never dials; a process that does dial needs
`npm i ws`.

### Example

```ts
import { JsonCodec, registerCodec, type Stream } from 'muxws';
import { accept, connect, handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { WebSocketServer } from 'ws';

registerCodec('json', new JsonCodec());

const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
server.on('connection', (socket) => {
  void accept(socket).then((peer) => {
    peer.onStream(async (payload: unknown, stream: Stream) => {
      await stream.reply({ echo: payload });
    });
    return peer.serve();
  });
});
await new Promise<void>((resolve) => server.once('listening', resolve));
const { port } = server.address() as { port: number };

const peer = await connect(`ws://127.0.0.1:${port}`, { headers: { 'x-request-id': 'docs-1' } });
console.log(await peer.request({ say: 'hi' }, { timeoutMs: 5000 }));
await peer.close({ drainMs: 100 });
server.close();
```

## `ConnectOptions` (TypeScript)

Everything `connect()` accepts besides the URL. Every field is optional.

### Signature

```ts
export interface ConnectOptions {
  /** Replayed verbatim on every connection this peer ever makes (WSM-RCN-020). */
  hello?: unknown;
  helloHeaders?: Record<string, unknown>;
  reconnect?: Reconnect;
  pingIntervalMs?: number;
  pingTimeoutMs?: number;
  helloTimeoutMs?: number;
  codec?: Codec;
  onStream?: StreamHandler;
  onClose?: (reason: CloseReason) => void;
  onReconnect?: (attempt: number, peer: Peer) => void;
  /** Appended **after** the muxws entry in the subprotocol offer (WSM-CDC-020/021). */
  subprotocols?: readonly string[];
  errorSerializer?: ErrorSerializer;
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
  maxFrameBytes?: number;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `hello` | `unknown` | none sent | The opening payload replayed verbatim on every connection. Deep-copied in at `connect()` and deep-copied out for every send, so neither your object nor the codec's can change a later replay. A value `structuredClone` refuses is rejected here rather than three hours into a reconnect storm. |
| `helloHeaders` | `Record<string, unknown>` | none | Headers on the hello's `open` frame, captured the same way. Supplying `hello` or `helloHeaders` makes the peer hello-configured. |
| `reconnect` | `Reconnect` | `new Reconnect()` | The backoff schedule. |
| `pingIntervalMs` | `number` — milliseconds | `20_000` | Idle time before the heartbeat sends a `ping` frame. `0` or less disables the heartbeat. |
| `pingTimeoutMs` | `number` — milliseconds | `10_000` | How long the heartbeat waits for the `pong`. Detection is bounded by `pingIntervalMs + pingTimeoutMs` milliseconds. Distinct from `peer.ping()`'s own default of 5000 milliseconds. |
| `helloTimeoutMs` | `number` — milliseconds | `10_000` | How long the hello may go unacknowledged before the attempt fails. |
| `codec` | `Codec` | `getCodec(settings.codec)` | An explicit codec, which wins over the configured name. |
| `onStream` | `StreamHandler` | none | Registered **before** the hello goes out, so an acceptor pushing a stream at the hello is not answered `reset(REFUSED)`. |
| `onClose` | `(reason: CloseReason) => void` | none | Registered **after** the first connection stands, which is what makes a failed `connect()` report itself exactly once, through the rejection. |
| `onReconnect` | `(attempt: number, peer: Peer) => void` | none | Fires once per re-established connection, after the hello acknowledgement. |
| `subprotocols` | `readonly string[]` | `[]` | Extra entries appended after the muxws entry in the offer. |
| `errorSerializer` | `ErrorSerializer` | `defaultErrorSerializer` | Turns a handler's failure into the `reset(APPLICATION_ERROR)` payload. The default puts the error's `name` and `message` **on the wire**; replace it with a redacting one on a public-facing connection. |
| `maxPayloadBytes` | `number` | `67_108_864` | This peer's own reassembly limit in bytes. Never announced. |
| `maxConcurrentStreams` | `number` | `100` | How many streams the remote may hold open here. Never announced. |
| `maxFrameBytes` | `number` | `MAX_FRAME_BYTES` (65536) | Test-only lowering of the protocol frame cap. |

### Return

None — `ConnectOptions` is an interface, not a call.

### Raises

Raises: nothing. It is a type; the values in it are validated by the calls that read them.

### Example

```ts
import { type ConnectOptions, JsonCodec, Reconnect, registerCodec } from 'muxws';

registerCodec('json', new JsonCodec());

const options: ConnectOptions = {
  hello: { client: 'docs' },
  reconnect: new Reconnect({ initialDelayMs: 250, maxDelayMs: 5_000 }),
  pingIntervalMs: 20_000,
  pingTimeoutMs: 10_000,
  helloTimeoutMs: 10_000,
  maxConcurrentStreams: 100,
  onClose: (reason) => {
    console.log('closed', reason.code, 'willRetry', reason.willRetry);
  },
};

console.log(options.pingIntervalMs, options.reconnect?.maxDelayMs);
```

## `NodeConnectOptions` (TypeScript)

`ConnectOptions` plus the one field only Node can honour.

### Signature

```ts
export interface NodeConnectOptions extends ConnectOptions {
  headers?: Record<string, string>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `headers` | `Record<string, string>` | none | HTTP headers for the upgrade request, handed to `new WebSocket(url, protocols, { headers })`. Authentication belongs here - at the upgrade - and not in the hello. |
| *(inherited)* | `ConnectOptions` | — | Every field of [`ConnectOptions`](#connectoptions-typescript), unchanged. |

### Return

None — `NodeConnectOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, registerCodec } from 'muxws';
import type { NodeConnectOptions } from 'muxws/node';

registerCodec('json', new JsonCodec());

const options: NodeConnectOptions = {
  headers: { authorization: 'Bearer docs-token' },
  helloTimeoutMs: 10_000,
  subprotocols: ['my-app.v1'],
};

console.log(options.headers?.authorization, options.helloTimeoutMs);
```

## See also

- [`accept()`](./accept.md) — the other end of the same connection.
- [`Peer`](./peer.md) — what `connect()` returns.
- [`Reconnect`](./reconnect.md) — the backoff schedule, the heartbeat and the hello replay.
- [Errors](./errors.md) — every class named above.
