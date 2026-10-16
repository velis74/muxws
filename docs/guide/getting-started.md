# Getting Started

## Installation

muxws has **no required runtime dependencies** in either language. Transports, codecs and web
framework integrations are optional extras or optional peer dependencies.

```bash
# Python. The extras are transports and codecs, and each one is optional:
#   muxws[starlette]   - the FastAPI / Starlette acceptor
#   muxws[websockets]  - the `websockets` dialer and acceptor
#   muxws[msgpack]     - the msgpack codec
# The quick start below runs a FastAPI server under uvicorn, so it asks for those two as well.
pip install "muxws[starlette,websockets]" fastapi uvicorn
```

```bash
# TypeScript. `ws` and `@msgpack/msgpack` are optional peer dependencies: `ws` is needed only for a
# Node dialer or acceptor (a browser has WebSocket already), `@msgpack/msgpack` only for the msgpack
# codec. `tsx` is here to run the .ts file below without a build step.
npm install muxws ws
npm install --save-dev tsx
# The client below is an ES module (it uses top-level `await`), and the package.json created by npm
# does not declare `"type": "module"`. Without this line `npx tsx` rejects the file with
# "Top-level await is currently not supported with the cjs output format".
npm pkg set type=module
```

## Quick Start

Three files, one socket, two call shapes: a unary request that receives exactly one answer, and a
streaming response that arrives in several payloads. The server is Python; the client is Python **or**
TypeScript. Both clients print the same output, because both use the same wire protocol.

### 1. The server

The route passes the socket to `accept()`; after that, the route contains no transport-specific code.
`accept()` performs the WebSocket upgrade itself, because it selects the `muxws.v1.<codec>`
subprotocol.

Each peer has one `on_stream` handler, and every stream the other end opens is delivered to it.
`stream.reply()` sends one payload and ends the stream; `stream.send()` any number of times followed by
`stream.end()` is the streaming shape.

<<< @/examples/quickstart_server.py

Run it:

```bash
python quickstart_server.py
```

It listens on `127.0.0.1:8000`. Set `MUXWS_PORT` to use another port.

### 2. The Python client

`connect()` returns a peer that is already serving. `request()` is the unary shape: one payload out,
exactly one payload back. `open()` is the general shape. It is **synchronous** and returns the
`Stream` in the same call that allocates the stream id; `end=True` means this side sends nothing
more.

<<< @/examples/quickstart_client.py

Run it in a second terminal:

```bash
python quickstart_client.py
```

### 3. The TypeScript client

The same two calls, against the same server. There are two imports: `muxws` is the package, and
importing it registers the built-in `json` codec; `muxws/node` is the subpath that provides the `ws`
socket adapter, and it is the only part of the package that imports `ws`. A browser dialer imports
`connect` from `muxws` instead; the rest of the code is unchanged.

Units: every duration in the TypeScript API is **milliseconds as an integer**; in the Python API it is
**seconds as a float**. Neither call below takes a duration; the rule applies to
`request(..., { timeoutMs })` and `result({ timeoutMs })`.

<<< @/examples/quickstart-client.ts

Run it in a second terminal:

```bash
npx tsx quickstart-client.ts
```

Set `MUXWS_URL` to point either client somewhere other than `ws://127.0.0.1:8000/ws`.

### 4. What you see

Either client prints exactly this. The documentation test compares the output of both clients with
this block byte for byte:

<!-- expected-output: quickstart -->

```text
greeting: hello, muxws
chunk 1 of 3
chunk 2 of 3
chunk 3 of 3
```

The first line arrived on one stream and the other three on a second stream, over the same socket,
without a second connection or an application-defined correlation id. This client completes the
request before it opens the second stream, but this is not required. If both streams were open at
once, the writer would interleave their frames, because a stream holds at most one unsent fragment
at a time and the writer serves streams round-robin. For the same reason a 1 MB export does not delay
a 200-byte progress update on another stream.

## Next: server push

muxws has no separate push API. `Peer` is a single symmetric type: the acceptor calls `peer.open()`
in the same way as the dialer, on the existing socket, and the dialer receives the stream in its own
`on_stream` handler. Correlation and cancellation work the same way in both directions.

The acceptor side. `peer` is in scope in the route, and the handler uses it:

<<< @/examples/push_server.py

The dialer side. The handler is passed to `connect()` rather than registered after `connect()`
returns. The acceptor may open a stream as soon as the connection is accepted, and a stream that
arrives before a handler is registered is answered with `reset(REFUSED)`:

<<< @/examples/push_client.py

Or the same dialer in TypeScript:

<<< @/examples/push-client.ts

Start `python push_server.py` (127.0.0.1:8001) and run either client. Both print:

<!-- expected-output: push -->

```text
push: ticks
tick 1
tick 2
tick 3
```

## Next: reconnecting after a disconnect

Reconnection is a **dialer-only** helper, enabled by passing two more arguments to `connect()`: a
`hello` and a `Reconnect`.

<<< @/examples/reconnect_client.py

When an established connection is lost, the dialer redials on a jittered exponential schedule
(`initial_delay` 0.25 seconds, `factor` 2.0, capped at `max_delay` 30.0 seconds, `jitter` ±30 %) and
replays the `hello` verbatim on every new socket. The acceptor receives the hello as an ordinary stream
in its `on_stream` handler and acknowledges it by returning, so the dialer's identity is re-established
without reconnect-specific code on the acceptor.

**No stream survives a reconnect.** Every stream that was open when the socket closed has failed,
nothing is buffered while the peer has no socket, stream ids start again at 1, and `peer.id` changes,
so the log shows a reconnected peer as a new connection. A reconnect restores a socket and the
identity established by the hello, and nothing else.

If the **first** `connect()` fails, it raises regardless of `reconnect=`, so a wrong URL is reported
immediately instead of being retried indefinitely.

Run it against the same `quickstart_server.py`. It connects, sends one request and exits, so on a
working connection the `on_reconnect` line is never printed; the file contains it to show where a
reconnection is reported:

<!-- expected-output: reconnect -->

```text
greeting: hello, muxws
```

## See also

[`connect`](/api/connect) · [`accept`](/api/accept) · [`Peer`](/api/peer) ·
[`Stream`](/api/stream) · [`Reconnect`](/api/reconnect)
