# Getting Started

## Installation

muxws has **no required runtime dependencies** in either language. A transport, a codec or a web
framework is something you already have, or something you ask for by name.

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
# The client below is an ES module - it uses `await` at the top level - and the package.json npm
# just wrote for you does not say so. Without this line `npx tsx` refuses the file with
# "Top-level await is currently not supported with the cjs output format".
npm pkg set type=module
```

## Quick Start

Three files, one socket, two call shapes: a unary request that gets exactly one answer, and a
streaming response that arrives in pieces. The server is Python; the client is Python **or**
TypeScript, and both print the same thing, because both speak the same wire.

### 1. The server

The route hands the socket to `accept()` and then does nothing transport-specific ever again.
`accept()` performs the WebSocket upgrade itself, because it is the only party that knows which
`muxws.v1.<codec>` subprotocol to select.

There is one `on_stream` handler per peer, and every stream the other end opens arrives at it — the
first one, and every one after that. `stream.reply()` is one payload plus the end of the stream;
`stream.send()` as many times as you like, then `stream.end()`, is the streaming shape.

<<< @/examples/quickstart_server.py

Run it:

```bash
python quickstart_server.py
```

It listens on `127.0.0.1:8000`. Set `MUXWS_PORT` to move it.

### 2. The Python client

`connect()` returns a peer that is already serving. `request()` is the unary shape: one payload out,
exactly one payload back. `open()` is the general one — it is **synchronous**, it hands back the
`Stream` in the same turn it allocated the id, and `end=True` says this side has nothing more to
send.

<<< @/examples/quickstart_client.py

Run it in a second terminal:

```bash
python quickstart_client.py
```

### 3. The TypeScript client

The same two calls, against the same server. Two imports: `muxws` is the package, and importing it
is what registers the built-in `json` codec; `muxws/node` is the subpath that owns the `ws` socket,
and it is the only part of the package that touches `ws`. A browser dialer imports `connect` from
`muxws` instead and changes nothing else.

Note the units: every duration in the TypeScript port is **milliseconds as an integer**, where its
Python twin is **seconds as a float**. Neither call below takes one, but that is the rule when you
reach for `request(..., { timeoutMs })` or `result({ timeoutMs })`.

<<< @/examples/quickstart-client.ts

Run it in a second terminal:

```bash
npx tsx quickstart-client.ts
```

Set `MUXWS_URL` to point either client somewhere other than `ws://127.0.0.1:8000/ws`.

### 4. What you see

Either client prints exactly this, and the documentation test asserts it byte for byte against both:

<!-- expected-output: quickstart -->

```text
greeting: hello, muxws
chunk 1 of 3
chunk 2 of 3
chunk 3 of 3
```

The first line came back on one stream and the last three on another, both over the same socket —
no second connection, and no correlation id you had to invent. This client finishes the request
before it opens the second stream, but nothing requires that: had both been open at once, the writer
would have interleaved their frames, because a stream holds at most one unsent fragment at a time and
the writer takes the streams round-robin. That is what stops a 1 MB export from stalling a 200-byte
progress update on another stream.

## Next: server push

There is no push API, because there does not need to be one. `Peer` is a single symmetric type: the
acceptor calls `peer.open()` exactly as the dialer did, on the socket that is already there, and the
dialer receives it in **its** `on_stream` handler. Same correlation, same cancellation, same
everything.

The acceptor side — `peer` is in scope in the route, so the handler closes over it:

<<< @/examples/push_server.py

The dialer side. The handler is passed to `connect()` rather than registered after it returns,
because the acceptor may push a stream the instant it sees this dialer, and a handler registered one
`await` later would meet that push with `reset(REFUSED)`:

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

## Next: surviving a disconnect

Reconnection is a **dialer-only** helper, and you get it by passing two more arguments to
`connect()`: a `hello` and a `Reconnect`.

<<< @/examples/reconnect_client.py

What that buys you: when an established connection is lost, the dialer redials on a jittered
exponential schedule — `initial_delay` 0.25 seconds, doubling, capped at `max_delay` 30.0 seconds,
±30 % jitter — and replays the `hello` verbatim on every socket it ever gets. The acceptor sees the
hello as an ordinary stream in its ordinary `on_stream` handler and acknowledges it by returning, so
your identity is re-established without any code on the accepting side.

What it does **not** buy you: **no stream survives**. Every stream that was open when the socket died
is already dead, nothing was buffered while the peer was between sockets, the stream id space starts
over at 1, and `peer.id` changes — a reconnected peer reads as a new connection in the log, on
purpose. A reconnect restores a live socket and an accepted identity, and that is the whole list.

One thing it deliberately does not do either: if the **first** `connect()` fails, it raises, whatever
`reconnect=` says. A typo in the URL that retried forever would never surface.

Run it against the same `quickstart_server.py`. It connects, asks once and exits, so on a healthy
socket the `on_reconnect` line never appears — it is in the file to show you where a reconnection
surfaces, not because a two-second run will see one:

<!-- expected-output: reconnect -->

```text
greeting: hello, muxws
```

## See also

[`connect`](/api/connect) · [`accept`](/api/accept) · [`Peer`](/api/peer) ·
[`Stream`](/api/stream) · [`Reconnect`](/api/reconnect)
