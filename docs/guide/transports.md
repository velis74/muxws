# Transports

The muxws peer does not open sockets. It uses a `SocketAdapter` with four methods (`send_text`,
`send_bytes`, `receive`, `close`), and the adapters are the **only** transport-specific code in the
library.

Shipped adapters: Starlette/FastAPI and the `websockets` library in Python, the browser's global
`WebSocket` and the `ws` package in TypeScript, and an in-memory pair in each language for tests.

Every snippet below is a complete file and runs as shown, unless it is marked `# fragment` /
`// fragment`.

## FastAPI / Starlette acceptor

```python
import uvicorn

from fastapi import FastAPI, WebSocket
from muxws import Stream, serve

app = FastAPI()


async def on_stream(payload, stream: Stream) -> None:
    """One handler for every inbound stream on this connection."""
    if payload == {"op": "count"}:
        for n in range(3):
            await stream.send({"n": n})
        await stream.end()
        return
    await stream.reply({"echo": payload})


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    # Do NOT call websocket.accept() first: muxws performs the upgrade, because it selects the
    # subprotocol.
    await serve(websocket, handler=on_stream)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
```

Needs `pip install "muxws[starlette]" fastapi uvicorn`.

**Do not accept the WebSocket in the application.** `accept()`, and `serve()`, which calls it,
performs the upgrade, because selecting the `muxws.v1.<codec>` subprotocol is part of the upgrade. If
the application has already accepted the WebSocket, `accept()` raises `ProtocolError`.

If the offered codec does not match the acceptor's, the upgrade is **denied with HTTP 400** and no
accept takes place. `accept()` then raises `CodecMismatch`; `serve()` catches it and returns without
an error, because the 400 response has already been sent.

Use `accept()` instead of `serve()` when the application needs the peer object, for example to tag
it, register it or push to it:

```python
# fragment
from muxws import PeerRegistry, accept

registry = PeerRegistry()


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    peer = await accept(websocket)
    peer.on_stream(on_stream)
    peer.tags["tenant"] = websocket.query_params.get("tenant", "")
    with registry.registered(peer):
        await peer.serve()
```

## `websockets` acceptor, with `select_subprotocol`

```python
import asyncio

import websockets

from muxws import Stream, accept, select_subprotocol


async def on_stream(payload, stream: Stream) -> None:
    await stream.reply({"echo": payload})


async def handle(connection) -> None:
    peer = await accept(connection)
    peer.on_stream(on_stream)
    await peer.serve()


async def main() -> None:
    async with websockets.serve(
        handle,
        "127.0.0.1",
        8000,
        select_subprotocol=select_subprotocol,
    ) as service:
        print(f"listening on {service.sockets[0].getsockname()}")
        await asyncio.Future()


asyncio.run(main())
```

Needs `pip install "muxws[websockets]"`.

`select_subprotocol` is installed as the `websockets` handshake hook and is called with
`(connection, subprotocols)`. Pass the function itself; do not call it, and do not wrap it.

::: warning It raises rather than returning `None`
`muxws.select_subprotocol` **raises** `NegotiationError` on a mismatch. A hook that returns `None`
causes `websockets` to answer 101 without a `Sec-WebSocket-Protocol` header: a *successful* handshake
with no subprotocol selected, and the mismatch is then detected only on the open socket. `websockets`
converts an `InvalidHandshake` raised by this hook into HTTP 400 and any other exception into 500, so
`NegotiationError` produces the 400 the protocol requires.

To install your own hook, call muxws's from it instead of reimplementing it:

```python
def select(connection, subprotocols):  # fragment
    # application checks first; then delegate, and let it raise
    return select_subprotocol(connection, subprotocols)
```

:::

`accept()` takes the `websockets` connection directly: it recognises the type, checks that the
negotiated subprotocol matches this peer's codec, and wraps it. The handshake is already complete when
the handler is called, so this check cannot produce a 400; only the hook above can. On a mismatch it
logs one error line, closes the socket with 1008 and raises `CodecMismatch`.

## `websockets` dialer

```python
import asyncio

from muxws import Reconnect, connect


async def main() -> None:
    peer = await connect(
        "ws://127.0.0.1:8000/ws",
        hello={"client": "reporting"},
        reconnect=Reconnect(),
        ping_interval=20.0,  # seconds
        ping_timeout=10.0,  # seconds
    )
    print(await peer.request({"hello": "world"}, timeout=5.0))  # seconds
    await peer.close()


asyncio.run(main())
```

Needs `pip install "muxws[websockets]"`. It can dial any of the acceptors on this page. Without the
extra, the first `connect()` raises `muxws.transports.websockets_.WebsocketsNotInstalledError`, whose
message contains that install command.

`connect()` uses `websockets`; Python has no other dialer and no adapter option. It offers
`muxws.v1.<codec>` first in its subprotocol list, verifies the selected subprotocol, and passes the
connection to a `Peer` that is already serving. Application subprotocol entries go in
`subprotocols=[...]`, after the muxws entry; the acceptor ignores them. The URL alone selects the
underlying socket: a `ws+unix://` URL dials a socket file through the same function, with the same
handshake and the same options; see [Unix domain sockets](#unix-domain-sockets).

`connect()` **raises if the first attempt fails**, regardless of `reconnect=`. See
[Reconnect](/guide/reconnect#the-first-dial-is-not-retried).

## Browser dialer

```ts
import { connect, type Stream } from 'muxws';

const peer = await connect('wss://example.test/ws', {
  hello: { tab: sessionStorage.getItem('tab') },
  pingIntervalMs: 20_000,
  pingTimeoutMs: 10_000,
  onStream: async (payload: unknown, stream: Stream) => {
    // Streams the server opens (server push) arrive here.
    console.log('pushed', payload);
    await stream.end();
  },
  onReconnect: (attempt: number) => {
    console.log(`reconnected (${attempt}); resubscribing`);
  },
});

console.log(await peer.request({ hello: 'world' }, { timeoutMs: 5000 }));
```

Needs `npm install muxws` only: the browser entry point has no runtime dependencies, and
`BrowserSocket`, the adapter over the platform's global `WebSocket`, is the only WebSocket-specific
file in it.

`onStream` is passed to `connect()` rather than registered after it returns. The acceptor may open a
stream as soon as it receives the hello, and a peer whose handler is registered one `await` later
answers that stream with `reset(REFUSED, "no on_stream handler")`. The same applies to `onClose` and
`onReconnect`.

A browser cannot set request headers on a WebSocket handshake, so this `connect()` has no `headers`
option. Credentials are sent as a cookie, in the URL, or as an extra subprotocol entry; see below.

A browser cannot open a filesystem socket, so this `connect()` rejects a `ws+unix:` URL immediately
with a message that names `muxws/node`. See [Unix domain sockets](#unix-domain-sockets).

## Node `ws` acceptor, via `muxws/node`

```ts
import { WebSocketServer } from 'ws';

// `muxws/node` does not register the JSON codec; the main entry point does, on import.
import 'muxws';
import { handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';
import type { Stream } from 'muxws';

async function onStream(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ echo: payload });
}

// Both hooks. `handleProtocols` selects; `refuseMismatchedUpgrade` refuses.
const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 8000, handleProtocols }));

server.on('connection', (socket) => {
  void serve(socket, { handler: onStream }).catch((error: unknown) => {
    console.error('connection ended', error);
  });
});
```

Needs `npm install muxws ws`. `ws` is an optional peer dependency, imported only from this subpath,
so a browser bundle does not contain it. It is imported dynamically and only on the dial path, so a
process that only accepts connections runs without it; a process that dials without it gets
`WsNotInstalledError` from `muxws/node`, a `TransportUnsupportedError` whose message is `npm install
ws`.

The snippet has three requirements:

**`import 'muxws';` is required.** The main entry point registers the JSON codec, and `muxws/node`
does not, because a module with import-time side effects cannot be removed by tree-shaking; the
registration is in the entry point a browser loads. A Node acceptor that imports only from
`muxws/node` fails on its first connection with `CodecNotRegistered: codec 'json' is not registered
… registered codecs are []`. Alternatively, call `registerCodec('json', new JsonCodec())` from
`muxws` during bootstrap.

**Both hooks are required.** `handleProtocols` only *selects* a value: when it returns `false`, `ws`
still answers 101, without a `Sec-WebSocket-Protocol` header. `refuseMismatchedUpgrade` wraps the
server's `shouldHandle`, the hook that can reject an upgrade with a status, so a mismatched codec
receives HTTP 400. The wrapper calls the inherited `shouldHandle` first, so a server constructed with `path`
keeps that check.

Without `refuseMismatchedUpgrade`, the upgrade is answered with 101 instead of 400 and the connection
completes. `accept()` then finds no matching subprotocol on the open socket, logs one error line,
closes the socket with 1008 and throws `CodecMismatch`; `serve()` resolves. The dialer's error depends
on its language: a Python dialer detects the mismatch on the open socket and raises `CodecMismatch`,
while a browser or Node dialer is rejected by its own WebSocket client before muxws runs, with a
message such as `Server sent no subprotocol` that names neither codec. If a cross-language dial fails
this way, check this hook first.

**Use `accept()` to get the peer.** `serve` is `accept` plus a handler plus the read loop;
`accept(socket, options)` returns the `Peer` for tagging and registration.

## Unix domain sockets

A muxws connection can be dialled over a socket **file** instead of a TCP port. The protocol is the
same WebSocket: the same HTTP GET + Upgrade, the same `muxws.v1.<codec>` subprotocol answered 101 or
400, carried by the same `WebsocketsSocket` and `WsSocket` adapters. Frames, close codes and
`connect()` arguments are unchanged. Only the URL differs:

```text
ws+unix:///run/muxws/api.sock:/ws
          └────────┬────────┘ └┬┘
              socket file     request target
```

The URL's path, including the query, is split on the **first** colon. The part before it is the
filesystem path to open; the part after it is the HTTP request target for the `GET` line of the
upgrade, and must begin with `/`. Both ports reject a target that does not, before opening a socket.
Without that check, an unrooted target would be merged into the authority: `…api.sock:ws` would become
`ws://localhostws`, and the dial would open the correct file and request `/` with an unintended `Host`
header. An acceptor that does not route on the request target (`unix_serve`, or a `WebSocketServer`
without `path`) would accept that handshake.

With no colon, or with a trailing colon and nothing after it, the request target is `/`. The query is
part of the split string, so it belongs to the target when a colon precedes it
(`ws+unix:///run/muxws/api.sock:/ws?tenant=42` opens that file and requests `/ws?tenant=42`) and to
the *filename* when no colon precedes it. An authority between the `//` and the path is used only as
the `Host` header; without an authority, the header is `localhost`. The scheme is case-insensitive,
and a single slash (`ws+unix:/run/muxws/api.sock:/ws`) parses the same way as two.

The socket path is taken from the URL as written: it is neither percent-encoded nor percent-decoded.
`ws+unix:///tmp/my dir/api.sock` opens `/tmp/my dir/api.sock`, and `ws+unix:///tmp/a%20b.sock` opens
a file whose name contains the three characters `%20`. Both ports apply this rule.

There is no `wss+unix://`. A socket file cannot be reached from another machine and has no host name
for certificate verification, so it is protected by its directory's permissions, not by TLS. Dialling
`wss+unix:` raises `UnixUrlError`; it is not downgraded to plaintext.

The dialer is the public `connect()`, unchanged:

<<< @/examples/uds_client.py

The acceptor is `websockets.asyncio.server.unix_serve` with the same `select_subprotocol` hook the TCP
acceptor installs, and the same `accept()` afterwards:

<<< @/examples/uds_server.py

Both need `pip install "muxws[websockets]"`. Start the acceptor, then run the dialer; the dialer
prints:

::: tip Running the pair without two terminals
`python demo.py --uds` from the repository root starts this acceptor on a socket file in a temporary
directory, runs this dialer against it, and removes both afterwards. It runs these two files, not
copies of them.
:::

<!-- expected-output: unix-socket -->

```text
greeting: hello over a unix socket
chunk 1 of 3
chunk 2 of 3
chunk 3 of 3
ping: answered
```

These are the quick start's two call shapes, one unary request and one streamed response, over a
socket file instead of a port, followed by a `ping` round trip.

**The Node acceptor uses `{ server }` rather than `{ port }`.** `ws` cannot bind a socket file, so an
`http.Server` binds it and the `WebSocketServer` attaches to that server's upgrade event.
`refuseMismatchedUpgrade` wraps `shouldHandle` in both configurations, so a codec mismatch still
receives HTTP 400:

```ts
import { statSync, unlinkSync } from 'node:fs';
import { createServer } from 'node:http';
import { connect as netConnect } from 'node:net';
import { WebSocketServer } from 'ws';

// Required: the main entry point registers the JSON codec, and `muxws/node` does not.
import 'muxws';
import { handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';
import type { Stream } from 'muxws';

const SOCKET = process.env.MUXWS_SOCKET ?? '/tmp/muxws.sock';

async function onStream(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ echo: payload });
}

/**
 * Remove the socket file only if it is stale, left by a process that exited without closing it.
 *
 * A socket file remains after the process that bound it exits, and `listen()` on the path fails with
 * EADDRINUSE until it is removed. Unlinking unconditionally would remove the address of a running
 * acceptor, and a path that is not a socket must not be removed: reject a non-socket path, probe a
 * socket path, and unlink it only if the probe connection is refused.
 */
async function clearStaleSocket(path: string): Promise<void> {
  let info;
  try {
    info = statSync(path);
  } catch {
    return; // No file at the path.
  }
  if (!info.isSocket()) throw new Error(`${path} exists and is not a socket file; refusing to remove it`);

  const alive = await new Promise<boolean>((resolve) => {
    const probe = netConnect(path);
    probe.on('connect', () => {
      probe.destroy();
      resolve(true);
    });
    probe.on('error', () => resolve(false));
  });
  if (alive) throw new Error(`${path} already has an acceptor listening on it`);
  unlinkSync(path);
}

await clearStaleSocket(SOCKET);

const httpServer = createServer();
const server = refuseMismatchedUpgrade(new WebSocketServer({ server: httpServer, handleProtocols }));

server.on('connection', (socket) => {
  void serve(socket, { handler: onStream }).catch((error: unknown) => {
    console.error('connection ended', error);
  });
});

// SIGINT's default action ends the process without running `close()` and leaves the socket file,
// which `clearStaleSocket` removes on the next start.
process.on('SIGINT', () => {
  httpServer.close(() => process.exit(0));
});

// Listen on the HTTP server, not on the WebSocketServer: only `http.Server` has a `listen()` that
// binds a path.
httpServer.listen(SOCKET, () => {
  console.log(`listening on ${SOCKET}`);
});
```

**The Node dialer is also the same `connect()`**, with the URL passed unchanged:

```ts
import 'muxws';
import { connect } from 'muxws/node';

const peer = await connect('ws+unix:///tmp/muxws.sock:/ws');
console.log(await peer.request({ say: 'hello' }, { timeoutMs: 5000 }));
await peer.close();
```

**`muxws/node` splits on the first colon itself.** `ws` supports `ws+unix:`, but it splits the path
on *every* colon and keeps the second field: given the whole URL, it would dial
`ws+unix:///run/api.sock:/ws:v2` with the target `/ws` and
`ws+unix:///run/api.sock:/ws?since=2026-08-14T10:00:00Z` with `/ws?since=2026-08-14T10`, while Python
requests the whole target for both URLs. Neither end would detect the truncation: the acceptor routes
on the target it receives, and the dialer does not report it. `muxws/node` therefore performs the split
and passes `ws` a plain `ws:` URL plus a connector that opens the file. `ws` still performs the dial,
and everything after the split is handled by `ws`: the subprotocol offer, the events, the HTTP 400.

The browser entry point rejects this scheme: neither a browser nor Node's global `WebSocket` can open
a filesystem socket, so `connect()` from `muxws` rejects a `ws+unix:` URL immediately with a
`UnixSocketsUnsupportedError` naming `muxws/node`. It is a `TransportUnsupportedError`: the URL is
valid, and the application must dial through a different entry point.

**On POSIX, both ports raise the same class for the same three invalid URLs.** A `ws+unix:` URL with a
request target that does not begin with `/`, a `ws+unix:` URL naming no socket file, and a `wss+unix:`
URL raise `UnixUrlError`: `from muxws.transports.unix import UnixUrlError` in Python,
`import { UnixUrlError } from 'muxws/node'` in TypeScript. Both are `TransportUrlError`s, so
`except TransportUrlError` / `instanceof TransportUrlError` catches an invalid address for *any*
transport, `ws://` included, without importing a transport module. See
[Errors](/guide/errors#errors-a-transport-reports-and-the-two-you-catch).

**A Unix domain socket identifies the caller.** The socket file has an owner, a group and a mode, so
the kernel checks who may open the connection before any HTTP is sent; placing the socket in a
directory that only the intended callers can traverse restricts access with `chmod`. Once a connection
is open, `SO_PEERCRED` on Linux gives the acceptor the caller's pid, uid and gid from the kernel. The
remote process cannot forge these values, and they are not sent over the connection. This is
authentication at the upgrade, before `accept()`, as described in
[Where authentication belongs](#where-authentication-belongs), with no credential on the wire and no
cookie, header or token to rotate. `uds_server.py` above reads it on Linux and prints
`unavailable on this platform` elsewhere: macOS and the BSDs have `getpeereid()`, which returns the uid
and the gid but not the pid and which CPython does not expose, and Node's standard library exposes
neither call. On those platforms, the file permissions are the only access control.

**Path length is limited.** A socket path is copied into `sockaddr_un.sun_path`, which is about 108
bytes on Linux and smaller on some other systems, so keep both the directory and the basename short.
An overlong path raises an error (CPython raises `OSError: AF_UNIX path too long`, and libuv returns
`EINVAL` with the whole path), but neither message names the component to shorten. A path built from
a long `TMPDIR` commonly exceeds the limit.

**`AF_UNIX` does not exist on Windows**, and the two ports behave differently there. In Python,
`connect()` raises `muxws.transports.unix.UnixSocketsUnsupportedError` (a `TransportUnsupportedError`,
so both a `MuxwsError` and a `RuntimeError`) from the URL parse, before opening a socket and before
the first attempt, so a background reconnection never raises it. This check runs before the two URL
shape checks, so on Windows an invalid request target and a URL naming no socket file also raise
`UnixSocketsUnsupportedError`; only `wss+unix:` raises `UnixUrlError` on every platform. `muxws/node`
raises no equivalent error: on Windows, `net.connect({ path })` opens a *named pipe*, but no `ws+unix:`
URL can address one, because `new URL()` rejects the backslashes in `\\.\pipe\name` on every
platform. A Windows dial of a POSIX-style path therefore fails with a connect error naming the path.

## In-memory, for tests

Both languages ship a pair of adapters connected to each other without a socket. The library's tests
and the conformance runner use them to run two peers.

```python
from muxws import Peer, get_codec
from muxws.transports.memory import memory_pair

left_socket, right_socket = memory_pair()
dialer = Peer(left_socket, codec=get_codec("json"), is_dialer=True)
acceptor = Peer(right_socket, codec=get_codec("json"), is_dialer=False)
```

```ts
import { Peer, getCodec, memoryPair } from 'muxws';

const [leftSocket, rightSocket] = memoryPair();
const dialer = new Peer(leftSocket, { codec: getCodec('json'), isDialer: true });
const acceptor = new Peer(rightSocket, { codec: getCodec('json'), isDialer: false });
```

The memory pair needs no handshake: a muxws connection is established when the socket is open with
the subprotocol accepted, and a memory pair is created in that state.

## Where authentication belongs

**At the upgrade, before `accept()`.** muxws does not interpret credentials anywhere.

The upgrade is an ordinary HTTP request, so use HTTP mechanisms: a cookie, an `Authorization` header,
a signed query parameter, a mutual-TLS client certificate, or, for browsers, an extra subprotocol
entry, because a browser can set subprotocols but not headers.

```python
# fragment
@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    identity = authenticate(websocket.headers, websocket.cookies)
    if identity is None:
        # Closed before accept: the upgrade is denied and no muxws frame is sent.
        await websocket.close(code=1008)
        return
    peer = await accept(websocket)
    peer.tags["user"] = identity.user_id
    peer.on_stream(on_stream)
    await peer.serve()
```

**Check `Origin` when the credential is a cookie.** A browser sends the site's cookies with a
WebSocket handshake whatever page opened it, and the WebSocket protocol has no CSRF protection of its
own, so a page on another site can open an authenticated socket. The `Origin` header names the page's
origin. `accept()` checks it when given `allowed_origins` (Python) or `allowedOrigins` (TypeScript):
a list of exact origins, `SameOrigin` for pages served by the host the handshake is addressed to, or a
function of the header value for any other rule. With a list or `SameOrigin`, a request without
`Origin` is refused; a dialer that is not a browser sends none, so an endpoint that serves such
dialers as well passes a function.

```python
# fragment
@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    # Refused origins get HTTP 403; serve() returns without raising.
    await serve(websocket, handler=on_stream, allowed_origins=["https://app.example.com"])
```

```ts
// fragment
server.on('connection', (socket, request) => {
  // `ws` hands the upgrade request to the connection event; accept() reads its Origin header.
  void serve(socket, { handler: onStream, allowedOrigins: ['https://app.example.com'], request });
});
```

`SameOrigin()` compares `Origin` with the request's `Host` header. Behind a proxy that rewrites `Host`,
`SameOrigin(trust_forwarded_host=True)` / `new SameOrigin({ trustForwardedHost: true })` compares with
`X-Forwarded-Host`, which the proxy sets to the host the browser addressed. Its `also` argument lists
further origins, such as a development server on another port. See [`SameOrigin`](/api/accept#sameorigin).

A Starlette socket with a refused origin is answered with HTTP 403. A `websockets` or `ws` connection
has completed its handshake when `accept()` runs, and is closed with 1008. `accept()` raises
`OriginRefused` in both cases. See [`accept()`](/api/accept#accept-python).

**Not in the hello.** The hello is an ordinary stream: it arrives after the socket is open and the
acceptor has allocated resources for it, it is sent unchanged on every reconnection for the life of
the process (so the credential cannot be rotated), and it is passed as an application payload to every
frame observer on the connection. The hello identifies a client; it does not authorize the connection.

**Not in per-stream `headers`.** muxws does not interpret them. They carry application metadata, such
as a trace id, a content type or an idempotency key. muxws does not route on the opening payload
either; see [What muxws does not do](/guide/rationale#what-muxws-does-not-do).

**When a credential expires during a connection**, close the connection with `goaway`. Streams at or
below the cut-off finish, no new streams are accepted, and a dialer with a reconnect helper dials again
and authenticates at the next upgrade.

```python
# fragment
await peer.close(reason="credential expired")
```

## Writing your own adapter

An adapter implements four methods. No other part of the library changes.

```python
class MySocket:  # fragment
    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

```ts
interface SocketAdapter { // fragment
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
```

Two requirements are not visible in the signatures:

- **`receive()` must raise `ConnectionClosed` when the socket dies**; it must not return or block. The
  exception tells the peer that the connection ended and starts socket-death handling: every live
  stream is failed, `on_close` fires, and the reconnect helper is notified.
- **Text and binary are separate methods.** The peer chooses between them from the codec's declared
  `binary` flag and does not inspect the data it sends. A single `send` method that accepted both
  would hide a mismatch between a codec's declaration and its output.

In Python, pass an adapter instance to `accept()`, which accepts any object implementing the
interface. In TypeScript, `accept()` takes a `ws` connection and wraps it in `WsSocket`, so a custom
adapter is passed directly to the peer: `new Peer(adapter, { codec, isDialer: false })`.

## See also

- [`api/transports`](/api/transports) — `SocketAdapter`, the Starlette adapter, the `websockets` adapter, the in-memory adapter
- [`api/accept`](/api/accept) — `accept`, `serve`, `select_subprotocol`
- [`api/connect`](/api/connect) — `connect` and `ConnectOptions`
- [`api/peer`](/api/peer) — `on_stream`, `serve`, `tags`, `close`
- [`api/codec`](/api/codec) — `registerCodec` and the codec the subprotocol names
- [`api/errors`](/api/errors) — `CodecMismatch`, `CodecNotRegistered`, `ConnectionClosed`
