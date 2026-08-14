# Transports

muxws does not open sockets. It speaks to a `SocketAdapter` — four methods, `send_text`,
`send_bytes`, `receive`, `close` — and that adapter is the **only** transport-specific code in the
library. Everything else, in both languages, is written against those four methods and knows nothing
about WebSockets.

The adapters that ship: Starlette/FastAPI and the `websockets` library in Python, the browser's global
`WebSocket` and the `ws` package in TypeScript, plus an in-memory pair in each language for tests.

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
    # Do NOT call websocket.accept() first: muxws performs the upgrade itself, because it is the
    # only party that knows which subprotocol to select.
    await serve(websocket, handler=on_stream)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
```

Needs `pip install "muxws[starlette]" fastapi uvicorn`.

The one thing to get right is the comment: **do not accept the WebSocket yourself**. `accept()` — and
`serve()`, which calls it — performs the upgrade, because selecting the `muxws.v1.<codec>` subprotocol
is part of that upgrade and an application that accepted first has already taken the decision away.
Accepting first raises `ProtocolError` saying so.

On a codec the acceptor does not speak, the upgrade is **denied with HTTP 400** before any accept
happens. `serve()` returns quietly in that case: the 400 has already gone out, and raising would turn
an ordinary misconfiguration into a traceback out of your endpoint.

Use `accept()` instead of `serve()` when you want the peer object — to tag it, to register it, to push
to it:

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
`muxws.select_subprotocol` **raises** `NegotiationError` on a mismatch. Returning `None` — which is
how a hand-written hook usually says "no thanks" — answers 101 with no `Sec-WebSocket-Protocol`
header, which is a *successful* handshake with no subprotocol selected. The mismatch would then have
to be discovered on an already-open socket, which is exactly what refusing at the handshake exists to
avoid. `websockets` turns an `InvalidHandshake` out of this hook into HTTP 400 and anything else into
500, so `NegotiationError` is the one exception that produces the status the protocol asks for.

If you install your own hook, wrap muxws's rather than reimplementing it:

```python
def select(connection, subprotocols):  # fragment
    # your own checks first — then delegate, and let it raise
    return select_subprotocol(connection, subprotocols)
```
:::

`accept()` takes the `websockets` connection directly: it recognises one, checks that the negotiated
subprotocol is the one this peer's codec asks for, and wraps it. By the time your handler is called
the handshake is already complete, so this second check is the last line of defence rather than the
first — the hook above is what produces the 400.

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

Needs `pip install "muxws[websockets]"`. It dials any of the acceptors on this page. Without it the
first `connect()` raises `muxws.transports.websockets_.WebsocketsNotInstalledError`, whose message is
that install line.

`connect()` is the `websockets` dialer — there is no second Python dialer and no adapter to choose. It
offers `muxws.v1.<codec>` first in its subprotocol list, verifies what came back, and hands the
connection to a `Peer` that is already serving. Extra subprotocol entries of your own go in
`subprotocols=[...]`, appended after the muxws one, and the acceptor ignores every one of them. The URL
is the only thing that selects the socket underneath it: a `ws+unix://` URL dials a socket file through
this same function, with the same handshake and the same options — see
[Unix domain sockets](#unix-domain-sockets).

Remember that it **raises if the first attempt fails**, whatever `reconnect=` says. See
[Reconnect](/guide/reconnect#the-first-thing-it-does-not-do).

## Browser dialer

```ts
import { connect, type Stream } from 'muxws';

const peer = await connect('wss://example.test/ws', {
  hello: { tab: sessionStorage.getItem('tab') },
  pingIntervalMs: 20_000, // milliseconds
  pingTimeoutMs: 10_000, // milliseconds
  onStream: async (payload: unknown, stream: Stream) => {
    // Server push arrives here — the same mechanism, with the roles swapped.
    console.log('pushed', payload);
    await stream.end();
  },
  onReconnect: (attempt: number) => {
    console.log(`reconnected (${attempt}); resubscribing`);
  },
});

console.log(await peer.request({ hello: 'world' }, { timeoutMs: 5000 })); // milliseconds
```

Needs `npm install muxws`. Nothing else: the browser entry point has no runtime dependencies, and
`BrowserSocket` — the adapter over the platform's global `WebSocket` — is the only file in it that
knows what a WebSocket is.

Note `onStream` passed to `connect()` rather than registered afterwards. That is not style: the
acceptor may push a stream the instant it sees the hello, and a handler registered one `await` later
answers that push `reset(REFUSED, "no on_stream handler")`. The same applies to `onClose` and
`onReconnect`.

A browser cannot set request headers on a WebSocket handshake, so there is no `headers` option here.
Credentials travel as a cookie, in the URL, or as an extra subprotocol entry — see below.

It cannot open a filesystem socket either, so this `connect()` rejects a `ws+unix:` URL immediately and
says to use `muxws/node`. See [Unix domain sockets](#unix-domain-sockets).

## Node `ws` acceptor, via `muxws/node`

```ts
import { WebSocketServer } from 'ws';

// `muxws/node` deliberately does not register the JSON codec; the main entry point does, on import.
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

Needs `npm install muxws ws`. `ws` is an optional peer dependency, reached only from this subpath, so
a browser bundle never sees it. It is imported dynamically and only on the dial path, so a process
that only accepts connections runs without it; a process that dials without it gets
`WsNotInstalledError` from `muxws/node` — a `TransportUnsupportedError` whose message is `npm install
ws`.

Three things this snippet is carrying:

**`import 'muxws';` is required.** The main entry point is what registers the JSON codec, and
`muxws/node` deliberately does not — a side-effecting import can never be tree-shaken out, so the
registration lives in the entry point a browser already loads. A Node acceptor that imports only from
`muxws/node` fails on its first connection with `CodecNotRegistered: codec 'json' is not registered
… registered codecs are []`. The alternative, if you would rather be explicit, is
`registerCodec('json', new JsonCodec())` from `muxws` during bootstrap.

**Both hooks, not one.** `handleProtocols` only *picks* a value: returning `false` selects nothing, and
`ws` still answers 101 without a `Sec-WebSocket-Protocol` header. `refuseMismatchedUpgrade` wraps the
server's `shouldHandle`, which is the hook that can abort an upgrade with a status, so a mismatched
codec gets HTTP 400. A server that installs only the first one completes handshakes it should have
refused. The wrapper calls the inherited `shouldHandle` first, so a server constructed with `path`
keeps that check.

What you see when you get this wrong is not an error at the acceptor at all — it connects. The dialer
is the one that suffers, and what it reports depends on which language it is: a Python dialer notices
on the open socket and raises `CodecMismatch`, while a browser or Node dialer is rejected by its own
WebSocket client before muxws sees anything, with a message like `Server sent no subprotocol` that
names neither codec. If a cross-language dial fails that way and your acceptor looks healthy, this
hook is the first thing to check.

**`accept()` if you want the peer.** `serve` is `accept` plus a handler plus the read loop;
`accept(socket, options)` gives you the `Peer` to tag and register instead.

## Unix domain sockets

A muxws connection can be dialled over a socket **file** instead of a TCP port. It is the same
WebSocket: the same HTTP GET + Upgrade, the same `muxws.v1.<codec>` subprotocol answered 101 or 400,
carried by the same `WebsocketsSocket` and `WsSocket` adapters. No frame changes, no close code
changes, no new argument to `connect()`. The URL is the whole of the difference:

```text
ws+unix:///run/muxws/api.sock:/ws
          └────────┬────────┘ └┬┘
              socket file     request target
```

Take the URL's path, query included, and split it on the **first** colon. What is in front is the
filesystem path to open; what is behind is the HTTP request target that goes into the `GET` line of the
upgrade, and it must begin with `/`. Both ports refuse a target that does not, before any socket is
touched: an unrooted target folds into the authority instead, so `…api.sock:ws` becomes
`ws://localhostws`, and the dial opens the right file and asks for `/` with a `Host` nobody chose.
Against an acceptor that does not route on the request target — `unix_serve`, or a `WebSocketServer`
with no `path` — that handshake **succeeds**, so the misdial connects rather than failing.

With no colon at all, or a trailing colon and nothing after it, the request target is `/`. The query
is part of the string that gets split, so it travels with the target when a colon precedes it —
`ws+unix:///run/muxws/api.sock:/ws?tenant=42` opens that file and asks for `/ws?tenant=42` — and is
part of the *filename* when there is none. An authority may sit between the `//` and the path: it
becomes the `Host` header and nothing else, and with no authority the header is `localhost`. The
scheme is case-insensitive, and a single slash — `ws+unix:/run/muxws/api.sock:/ws` — parses the same
way as the usual two.

There is no `wss+unix://`. A socket file is unreachable from another machine and has no host name for
a certificate to be checked against, so what protects it is its directory's permissions, not TLS;
dialling the scheme is a `UnixUrlError` rather than a silent downgrade to plaintext.

The dialer is the public `connect()`, unchanged:

<<< @/examples/uds_client.py

The acceptor is `websockets.asyncio.server.unix_serve` with the same `select_subprotocol` hook the TCP
acceptor installs, and the same `accept()` afterwards:

<<< @/examples/uds_server.py

Both need `pip install "muxws[websockets]"`. Start the acceptor, run the dialer, and the dialer prints:

::: tip Running the pair without two terminals
`python demo.py --uds` from the repository root starts this acceptor on a socket file in a temporary
directory, runs this dialer against it, and cleans both up. It runs these two files rather than a copy
of them.
:::

<!-- expected-output: unix-socket -->

```text
greeting: hello over a unix socket
chunk 1 of 3
chunk 2 of 3
chunk 3 of 3
ping: answered
```

Those are the quick start's two call shapes — one unary request, one streamed response — over a socket
file instead of a port, with a heartbeat added at the end to show that a `ping` frame travels the same
way.

**The Node acceptor is `{ server }` rather than `{ port }`.** `ws` cannot bind a socket file itself, so
an `http.Server` binds it and the `WebSocketServer` attaches to that server's upgrade event.
`refuseMismatchedUpgrade` wraps `shouldHandle` either way, so the HTTP 400 on a codec mismatch survives
the change of attachment:

```ts
import { statSync, unlinkSync } from 'node:fs';
import { createServer } from 'node:http';
import { connect as netConnect } from 'node:net';
import { WebSocketServer } from 'ws';

// Still required: the main entry point is what registers the JSON codec, and `muxws/node` does not.
import 'muxws';
import { handleProtocols, refuseMismatchedUpgrade, serve } from 'muxws/node';
import type { Stream } from 'muxws';

const SOCKET = process.env.MUXWS_SOCKET ?? '/tmp/muxws.sock';

async function onStream(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ echo: payload });
}

/**
 * Remove the socket file only if it is the corpse a killed run left behind.
 *
 * A socket file outlives the process that bound it, so the next `listen()` gets EADDRINUSE until
 * somebody removes it. Unlinking unconditionally would take the address from an acceptor that is
 * alive and serving, and a path that is not a socket must never be unlinked by this process at all:
 * refuse anything that is not a socket, probe the rest, unlink only what refuses the probe.
 */
async function clearStaleSocket(path: string): Promise<void> {
  let info;
  try {
    info = statSync(path);
  } catch {
    return; // Nothing there, which is the ordinary case.
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

// SIGINT's default action kills the process without running `close()`, which is what leaves a
// socket file behind for the block above to clean up next time.
process.on('SIGINT', () => {
  httpServer.close(() => process.exit(0));
});

// Listen on the HTTP server, not on the WebSocketServer: only the `http.Server` has a `listen()` to
// bind the path with.
httpServer.listen(SOCKET, () => {
  console.log(`listening on ${SOCKET}`);
});
```

**The Node dialer is the same `connect()` too.** The URL goes in as it stands:

```ts
import 'muxws';
import { connect } from 'muxws/node';

const peer = await connect('ws+unix:///tmp/muxws.sock:/ws');
console.log(await peer.request({ say: 'hello' }, { timeoutMs: 5000 })); // milliseconds
await peer.close();
```

**The first-colon split is muxws's own.** `ws` has dialled `ws+unix:` for years, but it splits the
path on *every* colon and keeps the second field: handed the URL whole, it would dial
`ws+unix:///run/api.sock:/ws:v2` as the target `/ws` and
`ws+unix:///run/api.sock:/ws?since=2026-08-14T10:00:00Z` as `/ws?since=2026-08-14T10`, while the same
two URLs in Python ask for the whole target. Neither end can see that happen — the acceptor routes on
the target it was handed and the dialer never learns it was cut — so `muxws/node` performs the split
itself and hands `ws` an ordinary `ws:` URL plus a connector that opens the file. `ws` is still what
dials, and everything after the split is `ws`'s: the offer, the events, the HTTP 400.

The browser entry point is the one place this scheme is refused rather than dialled: neither a browser
nor Node's global `WebSocket` can open a filesystem socket, so `connect()` from `muxws` rejects a
`ws+unix:` URL up front with a `UnixSocketsUnsupportedError` naming `muxws/node`. It is a
`TransportUnsupportedError`: the URL is good, and what has to change is the entry point doing the
dialling.

**On POSIX, both ports name the same three refusals the same way.** A `ws+unix:` URL with a request
target that does not begin with `/`, one naming no socket file, and `wss+unix:` are all `UnixUrlError` —
`from muxws.transports.unix import UnixUrlError` in Python, `import { UnixUrlError } from 'muxws/node'`
in TypeScript. Both are `TransportUrlError`s, so an application that does not want to import a
transport module to catch its failures writes `except TransportUrlError` / `instanceof
TransportUrlError` and gets a bad address over *any* transport, `ws://` included. See
[Errors](/guide/errors#errors-a-transport-reports-and-the-two-you-catch).

**What this transport buys, that no other one does, is who is calling.** The socket file has an owner,
a group and a mode, so the kernel decides who may open the connection at all before a single byte of
HTTP is written — put the socket in a directory only the intended callers can traverse and the
question is settled by `chmod`. Once a connection is open, `SO_PEERCRED` on Linux hands the acceptor
the caller's pid, uid and gid straight from the kernel: a credential the process on the other end
cannot forge and which never travels over the connection.
That is authentication happening exactly where [Where authentication belongs](#where-authentication-belongs)
says it does — at the upgrade, before `accept()` — with nothing on the wire and no cookie, header or
token to rotate. `uds_server.py` above reads it on Linux and prints `unavailable on this platform`
elsewhere: macOS and the BSDs have `getpeereid()`, which gives the uid and the gid but not the pid
and which CPython does not expose, and Node's standard library exposes neither call. On those the
file's permissions are the whole of the gate.

**Two limits worth knowing before you deploy one.** A socket path is copied into `sockaddr_un.sun_path`,
which is about 108 bytes on Linux and smaller on some other systems, so keep the directory short and
the basename shorter. An overrun is not silent — CPython raises `OSError: AF_UNIX path too long` and
libuv returns `EINVAL` quoting the whole path — but neither message says which component to shorten,
and a path assembled from a long `TMPDIR` is the usual way to cross the line without meaning to.

And `AF_UNIX` does not exist on Windows, where the two ports answer differently. Python refuses:
`connect()` raises `muxws.transports.unix.UnixSocketsUnsupportedError` — a `TransportUnsupportedError`,
so a `MuxwsError` and a `RuntimeError` both — from the URL parse, before any socket is touched, and
therefore before the first attempt, so it can never resurface from a background reconnection. That
check runs ahead of the two shape checks, so a bad request target and a URL naming no socket file are
`UnixSocketsUnsupportedError` there as well; only `wss+unix:` stays a `UnixUrlError` on every platform.
`muxws/node` has nothing equivalent to raise: `net.connect({ path })` opens a *named pipe* on Windows
instead of failing, so the call is meaningful there — but no `ws+unix:` URL can address one, because
`new URL()` rejects the backslashes in `\\.\pipe\name` on every platform, this one included. A Windows
dial of a POSIX-looking path therefore fails with a connect error naming the path it tried.

## In-memory, for tests

Both languages ship a pair of adapters wired to each other with no socket anywhere, which is how the
library's own tests and the conformance runner drive two peers.

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

There is no handshake to fake, because there is no handshake: a muxws connection is established the
moment the socket is open with the subprotocol accepted, and a memory pair simply asserts that.

## Where authentication belongs

**At the upgrade, before `accept()`.** That is the only place, and muxws interprets credentials
nowhere.

The upgrade is an ordinary HTTP request, so use what HTTP already has: a cookie, an `Authorization`
header, a signed query parameter, a mutual-TLS client certificate — or, for browsers, an extra
subprotocol entry, since a browser can set that and cannot set a header.

```python
# fragment
@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    identity = authenticate(websocket.headers, websocket.cookies)
    if identity is None:
        # Refused before accept, so the upgrade is denied and no muxws frame ever exists.
        await websocket.close(code=1008)
        return
    peer = await accept(websocket)
    peer.tags["user"] = identity.user_id
    peer.on_stream(on_stream)
    await peer.serve()
```

**Never in the hello.** The hello is an ordinary stream: it arrives after the socket is open and the
acceptor has already spent resources on it, it is replayed verbatim on every reconnection for the life
of the process (so the credential can never be rotated), and it travels as an application payload
through whatever frame observers the connection carries. The hello answers "who is this, again"; it
does not answer "should this connection exist". See [Reconnect](/guide/reconnect#the-hello).

**Never in per-stream `headers`.** muxws does not interpret them, at all. They exist for the
application — a trace id, a content type, an idempotency key — and using them for re-authentication
would mean re-checking a credential per stream on a socket that was already authenticated once, on a
field the library will not help you with. muxws also does not look at the opening payload for routing:
no path matching, no method dispatch, no handler table. There is one `on_stream` handler per peer and
what to do with a payload is entirely the application's decision.

**When a credential expires mid-connection**, close the connection with `goaway`. Streams already
below the cut-off finish; nothing new is accepted; and a dialer with a reconnect helper re-dials and
re-authenticates at the next upgrade, which is where authentication happens anyway.

```python
# fragment
await peer.close(reason="credential expired")
```

## Writing your own adapter

Four methods. Nothing else in the library needs changing.

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

Two obligations that are not visible in the signatures:

- **`receive()` must raise `ConnectionClosed` when the socket dies**, rather than returning or hanging.
  That exception is how the peer learns the connection ended, and it is what starts the whole
  socket-death path — failing every live stream, firing `on_close`, and waking the reconnect helper.
- **Text and binary are separate methods on purpose.** The peer chooses between them from the codec's
  declared `binary` flag and never by sniffing what it is about to send. An adapter that collapsed both
  into one polymorphic `send` would work until a codec's declaration and its output disagreed.

In Python, pass an instance to `accept()`, which takes anything already implementing the port as it
is. In TypeScript, `accept()` takes a `ws` connection and wraps it in `WsSocket`, so an adapter of
your own is handed straight to the peer: `new Peer(adapter, { codec, isDialer: false })`.

## See also

- [`api/transports`](/api/transports) — `SocketAdapter`, the Starlette adapter, the `websockets` adapter, the in-memory adapter
- [`api/accept`](/api/accept) — `accept`, `serve`, `select_subprotocol`
- [`api/connect`](/api/connect) — `connect` and `ConnectOptions`
- [`api/peer`](/api/peer) — `on_stream`, `serve`, `tags`, `close`
- [`api/codec`](/api/codec) — `registerCodec` and the codec the subprotocol names
- [`api/errors`](/api/errors) — `CodecMismatch`, `CodecNotRegistered`, `ConnectionClosed`
