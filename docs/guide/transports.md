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

Needs `pip install "muxws[websockets]"`. It dials any of the acceptors on this page.

`connect()` is the `websockets` dialer — there is no second Python dialer and no adapter to choose. It
offers `muxws.v1.<codec>` first in its subprotocol list, verifies what came back, and hands the
connection to a `Peer` that is already serving. Extra subprotocol entries of your own go in
`subprotocols=[...]`, appended after the muxws one, and the acceptor ignores every one of them.

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
a browser bundle never sees it.

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

Pass an instance to `accept()`, which takes anything already implementing the port as it is.

## See also

- [`api/transports`](/api/transports) — `SocketAdapter`, the Starlette adapter, the `websockets` adapter, the in-memory adapter
- [`api/accept`](/api/accept) — `accept`, `serve`, `select_subprotocol`
- [`api/connect`](/api/connect) — `connect` and `ConnectOptions`
- [`api/peer`](/api/peer) — `on_stream`, `serve`, `tags`, `close`
- [`api/codec`](/api/codec) — `registerCodec` and the codec the subprotocol names
- [`api/errors`](/api/errors) — `CodecMismatch`, `CodecNotRegistered`, `ConnectionClosed`
