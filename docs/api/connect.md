---
outline: deep
---

# connect

`connect()` dials a URL, asserts the codec at the WebSocket handshake, sends the hello if one is
configured, starts the read loop, and returns a `Peer` that is already serving. It is the dialer's
only entry point; all further work is done on the returned `Peer`.

Behaviour common to every `connect()`:

- **A failed first attempt raises.** Reconnection applies only to a connection that was established
  and then lost. The first dial is never retried, and no option changes this, so an unreachable or
  mistyped URL is reported as an exception from `connect()`.
- **`max_payload_bytes`, `max_concurrent_streams` and `error_serializer` apply to this peer only.**
  None of them is sent to or negotiated with the remote. The remote observes a limit only as the
  reset it receives when it exceeds the limit.
- **Durations are seconds as floats in Python and milliseconds as integers in TypeScript.** The
  defaults are the same in both ports, each expressed in its own unit.

The Python package dials with the `websockets` library, version 14 or newer; `connect()` requires
`pip install muxws[websockets]`. In TypeScript there are two
`connect()` functions: the one exported from `muxws` dials with the platform `WebSocket` (the browser,
and Node 22+ where that global exists), and the one exported from `muxws/node` dials with the `ws`
package and can set handshake headers.

A `ws+unix://` URL dials a Unix domain socket. Python's `connect()` and the one from `muxws/node` both
open one: each parses the scheme itself and hands its WebSocket library a socket path plus a logical
`ws://` URL to put on the request line. The browser `connect()` refuses the scheme up front with
`UnixSocketsUnsupportedError`, whose message names `muxws/node`, because neither a browser nor Node's
global `WebSocket` can open a filesystem socket. In all three, the handshake, the subprotocol offer,
the HTTP 400 refusal and every option below behave as they do over TCP. See [Unix domain sockets](/guide/transports#unix-domain-sockets).

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
    open_interceptors: Sequence[OpenInterceptor] = (),
    reply_headers_interceptors: Sequence[ReplyHeadersInterceptor] = (),
    incoming_headers_interceptors: Sequence[IncomingHeadersInterceptor] = (),
) -> Peer:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `str` | required | The WebSocket URL to dial: `ws://`, `wss://`, or `ws+unix://<socket path>:<request target>` for a Unix domain socket. In the `ws+unix:` form the URL's path and query are split on the **first** colon: what is in front is the file to open, what is behind is the HTTP request target, and the target is `/` both when there is no colon and when nothing follows it. The scheme is case-insensitive and `ws+unix:/run/api.sock:/ws` names the same file as `ws+unix:///run/api.sock:/ws`. An authority, if one is given, becomes the `Host` header and nothing else; with none, the header is `localhost`. There is no `wss+unix://`. Re-dialled unchanged on every reconnection. |
| `headers` | `dict[str, str] \| None` | `None` | Extra HTTP headers for the upgrade request, passed to `websockets.connect(additional_headers=...)`. Credentials go here, not in the hello. |
| `subprotocols` | `list[str] \| None` | `None` | Extra subprotocol entries appended **after** the `muxws.v1.<codec>` entry. A muxws acceptor never selects them; they are available to application-level handshake logic. |
| `hello` | `Any` | `None` | The opening payload replayed verbatim on every connection this peer ever makes. Deep-copied at this call and never re-read; later changes to the passed object do not affect the replay. |
| `hello_headers` | `dict[str, Any] \| None` | `None` | Headers for the hello's `open` frame, captured by value alongside `hello`. Supplying either one makes the peer "hello-configured": it is not established until the acceptor's handler ends that stream. |
| `reconnect` | `Reconnect \| None` | `None` | The backoff schedule. `None` means `Reconnect()` - the defaults, with unlimited attempts. Reconnection applies only after a connection has been established once. |
| `ping_interval` | `float` — seconds | `20.0` | How long the socket may be **idle** before the heartbeat sends a `ping` frame. Idle means no frame in either direction. `0` or less disables the heartbeat. |
| `ping_timeout` | `float` — seconds | `10.0` | How long the heartbeat waits for the matching `pong` before treating the socket as lost. Detection is bounded by `ping_interval + ping_timeout` seconds. `peer.ping()` has a separate default of 5.0 seconds. |
| `hello_timeout` | `float` — seconds | `10.0` | How long a hello may go unacknowledged before the attempt fails. On the first connection, `connect()` raises `StreamTimeout`. |
| `max_payload_bytes` | `int` | `67_108_864` | The largest reassembled payload **this** peer accepts, in bytes of encoded output. Local, never announced; a sender that crosses it gets `reset(PAYLOAD_TOO_LARGE)` on the fragment that crossed it. |
| `max_concurrent_streams` | `int` | `100` | How many streams the **remote** may have open here at once. Local, never announced; the excess open is refused without the handler running. |
| `error_serializer` | `ErrorSerializer \| None` | `None` | Turns a handler's exception into the payload of the `reset(APPLICATION_ERROR)` frame. `None` means `default_error_serializer`, which puts the exception's class name and `str(exc)` **on the wire** - replace it with a redacting one on any public-facing connection. |
| `codec` | `Codec \| None` | `None` | An explicit codec, which wins over `settings.codec`. `None` looks the configured name up in the registry and never falls back to JSON. |
| `max_frame_bytes` | `int` | `MAX_FRAME_BYTES` (65536) | For tests only. The frame cap is a protocol constant; a lower value makes small payloads fragment, which the conformance runner uses. |
| `on_stream` | `StreamHandler \| None` | `None` | The incoming-stream handler, registered **before** the hello goes out, so it receives a stream the acceptor opens as soon as it sees the hello. A handler registered with `peer.on_stream(...)` after `connect()` returns is too late for such a stream, which is answered `reset(REFUSED, "no on_stream handler")`. |
| `on_close` | `Callable[[Any], None] \| None` | `None` | Called with a `CloseReason` on every socket loss after the first connection is established. When the first connection fails, `connect()` raises and this handler is not called. |
| `on_reconnect` | `Callable[[int, Any], None] \| None` | `None` | Called `(attempt, peer)` once per **re**-established connection, after the hello is acknowledged. The first connection does not fire it. |
| `open_interceptors` | `Sequence[OpenInterceptor]` | `()` | Registered in order with [`peer.intercept_open()`](./peer.md#peer-intercept-open-python) before the hello goes out. The hello itself does not pass through them. |
| `reply_headers_interceptors` | `Sequence[ReplyHeadersInterceptor]` | `()` | Registered in order with [`peer.intercept_reply_headers()`](./peer.md#peer-intercept-reply-headers-python) before the hello goes out. |
| `incoming_headers_interceptors` | `Sequence[IncomingHeadersInterceptor]` | `()` | Registered in order with [`peer.intercept_incoming_headers()`](./peer.md#peer-intercept-incoming-headers-python) before the hello goes out, so a stream the acceptor pushes in answer to the hello passes them. |

### Return

`Peer` — a dialer peer whose read loop is already running as a background task, whose hello (if any)
has been acknowledged, and whose reconnect supervisor is running. `peer.is_open` is true when this
returns.

### Raises

- `CodecNotRegistered` — the configured codec name was never registered. Raised before any socket is
  touched.
- `CodecMismatch` — the acceptor refused the upgrade with HTTP 400, or completed it having negotiated
  something other than `muxws.v1.<codec>`. The socket is closed with 1008 in the second case. A
  `ws+unix://` dial takes the same path: the refusal is an HTTP status on an ordinary upgrade.
- `ProtocolError` — `max_frame_bytes` is too small to hold an envelope plus one indivisible unit.
  Raised before the dial.
- The `copy.deepcopy` error (typically `TypeError`) — `hello` or `hello_headers` cannot be deep-copied.
  Raised before the dial.
- `StreamTimeout` — the hello was not acknowledged within `hello_timeout` seconds.
- `StreamReset` (or a subclass: `StreamRefused`, `RemoteError`, `ConnectionLost`) — the acceptor reset
  the hello stream, or the socket closed before the hello was acknowledged.
- `muxws.transports.websockets_.WebsocketsNotInstalledError` — `websockets` is not installed. A
  `TransportUnsupportedError`, so it is a `MuxwsError` and a `RuntimeError`. Its message contains
  `pip install muxws[websockets]`. Raised for `ws://`, `wss://` and `ws+unix://` URLs alike.
- Whatever the dial itself raised — `OSError` for a refused TCP connection, `websockets`'
  `InvalidHandshake` family for a broken upgrade, `ValueError` for a `subprotocols` entry that is not
  a valid HTTP token. It is propagated unaltered. Over `ws+unix://` the same clause covers a socket
  file that is not there (`FileNotFoundError`) and one no process is listening on
  (`ConnectionRefusedError`), both of them `OSError` subclasses. None of these is wrapped in a
  `TransportUrlError`.
- `muxws.transports.websockets_.WebsocketUrlError` — a URL `websockets` cannot parse into a dialable
  target: no hostname, a scheme that is not `ws`/`wss` (`http://example.com/x`), a port that is not an
  integer, or a string that is not a URL at all. A `TransportUrlError`, so a `MuxwsError` and a
  `ValueError`; the `InvalidURI` or `ValueError` from `websockets.uri.parse_uri` is chained as
  `__cause__`. It also covers a `ws+unix://` URL whose *authority* is unusable, because the string
  checked for that arm is the logical `ws://` URI the dial synthesises: `ws+unix://user@/a.sock:/y`
  raises `WebsocketUrlError` about `ws://user@/y`. Raised **before** the dial, so a URL whose text
  contains `HTTP 400` is not reported as a `CodecMismatch`.
- A bare `ValueError` — an authority with an unbalanced `[`, as in `ws://[::1`. `urllib.parse.urlsplit`
  rejects it at the start of the scheme check, before both URL checks below. It is not a
  `TransportUrlError` and not a `MuxwsError`.
- `muxws.transports.unix.UnixUrlError` — a `ws+unix://` URL naming no socket file, or one whose request
  target does not begin with `/`, or a `wss+unix://` URL, which does not exist. Also a
  `TransportUrlError`, and raised before any socket is touched. Checked before the `websockets` URL
  parse and with stdlib alone, so it answers the same whether or not `websockets` is installed.
- `muxws.transports.unix.UnixSocketsUnsupportedError` — a `ws+unix://` URL on a platform with no
  `AF_UNIX`, which is Windows. A `TransportUnsupportedError`, raised by the URL parse inside the
  `connect()` call; its message names the scheme and the missing `socket.AF_UNIX`. The platform check
  runs before the two grammar checks, so on Windows a malformed `ws+unix://` URL raises this class
  rather than `UnixUrlError`. `wss+unix://` raises `UnixUrlError` on every platform, because the
  scheme check runs first.

The four transport classes above — `WebsocketsNotInstalledError`, `WebsocketUrlError`, `UnixUrlError`
and `UnixSocketsUnsupportedError` — live in their transport's module and are not exported from
`muxws`. An application that does not want to import a transport to catch its failures writes
`except TransportUrlError` or `except TransportUnsupportedError`, both exported from `muxws`.
`TransportUrlError` means the address is unusable; `TransportUnsupportedError` means this process or
platform cannot dial it.

**The first attempt is not retried.** For every `reconnect` value, including `Reconnect()`, a failure
of the first dial or of the first hello raises from `connect()`. An application that needs the first
dial retried calls `connect()` in its own loop.

**No socket stays open after a failure.** An exception raised after the dial closes the dialled
socket before it reaches the caller.

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
it also works on any Node that exposes a global `WebSocket`. It takes no handshake headers, because
the browser `WebSocket` API cannot set them.

### Signature

```ts
export async function connect(url: string, options: ConnectOptions = {}): Promise<Peer>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `string` | required | The WebSocket URL to dial. Re-dialled unchanged, with the same subprotocol offer, on every reconnection. An `http://` or `https://` URL is upgraded to `ws:`/`wss:` by a spec-conformant platform `WebSocket` — browsers, and node's undici — while jsdom rejects the scheme instead; Python answers `WebsocketUrlError` for both. A `ws+unix://` URL is **rejected before any dial**, with a `UnixSocketsUnsupportedError` naming `muxws/node`: the platform `WebSocket` cannot open a filesystem socket. |
| `options` | `ConnectOptions` | `{}` | Everything else. Each field is documented under [`ConnectOptions`](#connectoptions-typescript) below. |

### Return

`Promise<Peer>` — resolves with a dialer peer that is already serving and, if a hello was configured,
already acknowledged. `peer.isOpen` is true when it resolves.

### Raises

Rejects with:

- `CodecNotRegistered` — the configured codec name is not in the registry. Thrown before any socket is
  touched. Importing only `muxws/node` registers no codec; JSON is registered as a side effect of
  importing `muxws`.
- `CodecMismatch` — the acceptor refused the handshake, or negotiated something other than
  `muxws.v1.<codec>`.
- `StreamTimeout` — the hello was not acknowledged within `helloTimeoutMs` milliseconds.
- `StreamReset` (or `StreamRefused`, `RemoteError`, `ConnectionLost`) — the acceptor reset the hello
  stream, or the socket closed before the hello was acknowledged.
- `ProtocolError` — `maxFrameBytes` is too small to hold an envelope plus one indivisible unit.
  Thrown before the dial.
- The `structuredClone` error (a `DOMException` named `DataCloneError`) — `hello` or `helloHeaders`
  holds a value `structuredClone` cannot copy, such as a function. Thrown before the dial.
- `UnixSocketsUnsupportedError` — the URL's scheme is `ws+unix:` (compared case-insensitively, after
  trimming leading whitespace). Rejected before the codec lookup and before any socket is opened.
  `connect()` is an `async function`, so the error arrives as a rejected promise, not as a synchronous
  throw. The message names `muxws/node` as the entry point that dials this scheme; the platform's own
  URL parsers also reject the scheme, with a message that names neither. A `TransportUnsupportedError`:
  the URL is valid, and this entry point has no filesystem transport (WSM-API-022).
- Whatever the underlying `WebSocket` failed with. An unreachable host surfaces as the platform's own
  error, and so does a **malformed URL**: the platform's constructor throws a `DOMException` named
  `SyntaxError` (`The URL 'nonsense' is invalid.` under jsdom, `TypeError: Invalid URL` under undici),
  which is not a `MuxwsError`. This entry point wraps no URL error. `muxws/node` wraps them in
  `WsUrlError`, and Python's `connect()` wraps them in `WebsocketUrlError` except for an unbalanced `[`
  in the authority (`ws://[::1`), which `urllib.parse.urlsplit` rejects as a bare `ValueError`.

`UnixSocketsUnsupportedError` is exported from `muxws`, the entry point that ships this transport, and
from nowhere else; `TransportUrlError` and `TransportUnsupportedError` are the root-exported bases to
catch when the concrete class does not matter.

**The first attempt is not retried**, for every `reconnect` value; its failure rejects the promise.

**No socket stays open after a failure.** An error thrown after the dial closes the dialled socket
before the promise rejects.

### Example

```ts
import { connect, JsonCodec, registerCodec, type Stream } from 'muxws';
import { accept, handleProtocols, refuseMismatchedUpgrade } from 'muxws/node';
import { WebSocketServer } from 'ws';

// `muxws/node` registers no codec; importing `muxws` registers JSON.
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

Dial `url` over the `ws` package and return a serving peer. The behaviour is that of the browser
`connect()`, with two additions: handshake headers, and `ws+unix://` URLs.

### Signature

```ts
export async function connect(url: string, options: NodeConnectOptions = {}): Promise<Peer>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `url` | `string` | required | The WebSocket URL to dial, over the `ws` package - including `ws+unix://<socket path>:<request target>`, whose path and query are split on the **first** colon, exactly as in Python, and whose authority becomes the `Host` header (`localhost` when there is none). `ws` splits on every colon, so `muxws/node` parses the scheme itself and passes `ws` the synthesised `ws://` URL; see [Unix domain sockets](../guide/transports.md#unix-domain-sockets). An `http://` or `https://` URL is converted to `ws:`/`wss:` by `ws` and dialled. |
| `options` | `NodeConnectOptions` | `{}` | `ConnectOptions` plus `headers`. See [`NodeConnectOptions`](#nodeconnectoptions-typescript). |

### Return

`Promise<Peer>` — as for the browser entry point.

### Raises

The same set as the browser `connect()`, plus:

- `Error` with the message `unexpected server response: <status>` — the upgrade was answered with a
  status other than 101 and other than the 400 that means a refused muxws handshake.
- A `ws` transport error — an unreachable host, a TLS failure, a socket reset during the upgrade.
  Propagated unchanged, not wrapped in a `TransportUrlError`.
- `SyntaxError` from `ws` — `An invalid or duplicated subprotocol was specified`, for a `subprotocols`
  entry that is not a valid HTTP token. Rethrown unchanged, because the URL itself is valid; it is not
  a `MuxwsError`. Python's `connect()` raises `ValueError: invalid subprotocol: …` for the same input.
- `UnixUrlError` — a `ws+unix://` URL this module's grammar refuses: a request target that does not
  begin with `/`, a URL naming no socket file, or `wss+unix://`. Rejected before the dial. Without
  this check a relative target would be appended to the authority: `…/a.sock:ws` would synthesise
  `ws://localhostws`, a valid URL, and the dial would open the socket file and request `/` instead of
  the given target. The Python class has the same name, `muxws.transports.unix.UnixUrlError`; both
  are `TransportUrlError`s.
- `WsUrlError` — a URL `ws` itself cannot parse. A `TransportUrlError` framing `ws`'s own
  `SyntaxError: Invalid URL: …`, which is kept on `cause`.
- `WsNotInstalledError` — the optional peer dependency `ws` is not installed. A
  `TransportUnsupportedError` whose message contains `npm install ws`, raised in place of Node's
  `ERR_MODULE_NOT_FOUND`. Any other failure to import `ws` is rethrown unchanged.

`ws` is an optional peer dependency imported inside the dial closure, so `accept()` and
`handleProtocols()` keep working in a process that never dials; only a dial needs it, and only a dial
can raise `WsNotInstalledError`. These three classes are exported from `muxws/node` and from nowhere
else; the `TransportUrlError` / `TransportUnsupportedError` bases come from `muxws`.

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
  /** Registered before the hello, in order, as `peer.interceptOpen()` would (WSM-API-026). */
  openInterceptors?: readonly OpenInterceptor[];
  /** Registered before the hello, in order, as `peer.interceptReplyHeaders()` would (WSM-API-026). */
  replyHeadersInterceptors?: readonly ReplyHeadersInterceptor[];
  /** Registered before the hello, in order, as `peer.interceptIncomingHeaders()` would (WSM-API-026). */
  incomingHeadersInterceptors?: readonly IncomingHeadersInterceptor[];
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
| `hello` | `unknown` | none sent | The opening payload replayed verbatim on every connection. Deep-copied in at `connect()` and deep-copied out for every send, so neither your object nor the codec's can change a later replay. A value `structuredClone` cannot copy makes `connect()` reject with the `structuredClone` error. |
| `helloHeaders` | `Record<string, unknown>` | none | Headers on the hello's `open` frame, captured the same way. Supplying `hello` or `helloHeaders` makes the peer hello-configured. |
| `reconnect` | `Reconnect` | `new Reconnect()` | The backoff schedule. |
| `pingIntervalMs` | `number` — milliseconds | `20_000` | Idle time before the heartbeat sends a `ping` frame. `0` or less disables the heartbeat. |
| `pingTimeoutMs` | `number` — milliseconds | `10_000` | How long the heartbeat waits for the `pong`. Detection is bounded by `pingIntervalMs + pingTimeoutMs` milliseconds. Distinct from `peer.ping()`'s own default of 5000 milliseconds. |
| `helloTimeoutMs` | `number` — milliseconds | `10_000` | How long the hello may go unacknowledged before the attempt fails. |
| `codec` | `Codec` | `getCodec(settings.codec)` | An explicit codec, which wins over the configured name. |
| `onStream` | `StreamHandler` | none | Registered **before** the hello goes out, so an acceptor pushing a stream at the hello is not answered `reset(REFUSED)`. |
| `onClose` | `(reason: CloseReason) => void` | none | Registered **after** the first connection is established, so it is not called when `connect()` rejects. |
| `onReconnect` | `(attempt: number, peer: Peer) => void` | none | Fires once per re-established connection, after the hello acknowledgement. |
| `openInterceptors` | `readonly OpenInterceptor[]` | `[]` | Registered in order with [`peer.interceptOpen()`](./peer.md#peer-interceptopen-typescript) before the hello goes out. The hello itself does not pass through them. |
| `replyHeadersInterceptors` | `readonly ReplyHeadersInterceptor[]` | `[]` | Registered in order with [`peer.interceptReplyHeaders()`](./peer.md#peer-interceptreplyheaders-typescript) before the hello goes out. |
| `incomingHeadersInterceptors` | `readonly IncomingHeadersInterceptor[]` | `[]` | Registered in order with [`peer.interceptIncomingHeaders()`](./peer.md#peer-interceptincomingheaders-typescript) before the hello goes out, so a stream the acceptor pushes in answer to the hello passes them. |
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

`ConnectOptions` plus `headers`, which only the `ws` dial can send.

### Signature

```ts
export interface NodeConnectOptions extends ConnectOptions {
  headers?: Record<string, string>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `headers` | `Record<string, string>` | none | HTTP headers for the upgrade request, handed to `new WebSocket(url, protocols, { headers })`. Credentials go here, not in the hello. |
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
