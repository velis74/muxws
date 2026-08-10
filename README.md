# muxws

**HTTP/3 stream semantics over a WebSocket.** Many independent, cancellable, bidirectional streams on
one socket, either end able to open one - the model HTTP/2 and HTTP/3 already settled, without needing
QUIC or an HTTP/3 stack. The wire format is the product; Python and TypeScript are the two ports that
exist today, and [`SPEC.md`](SPEC.md) plus [`conformance/`](conformance/README.md) are what a third is
written from.

The mimicry is of *semantics*, not transport: one TCP connection means one global message order and no
per-stream loss recovery - [`docs/guide/rationale.md`](docs/guide/rationale.md) says what is and is not
copied. muxws is not a router, not a serializer of domain objects, not an authentication mechanism, not a
durable store and not an RPC framework.

## Install

```bash
pip install muxws                       # extras: [starlette], [websockets], [msgpack]
npm install muxws                       # optional peers: ws, @msgpack/msgpack
```

## Documentation

**The documentation site is the manual**: build it with `npm run docs:dev`, or read `docs/`.

| | |
|---|---|
| Server and client in three files | [`docs/guide/getting-started.md`](docs/guide/getting-started.md) |
| Every public symbol, both languages | [`docs/api/`](docs/api/) |
| The normative rules, and the fixtures a third port is written from | [`SPEC.md`](SPEC.md), [`conformance/`](conformance/README.md) |

## Status

1.0. The wire is frozen: `muxws.v1.<codec>` is the subprotocol, and `v1` is the only generation until
a breaking change earns a new one. Python package `muxws` (PyPI); npm package `muxws`, browser entry
plus the `muxws/node` and `muxws/msgpack` subpaths.

## Licence

MIT. Copyright © 2025 Jure Erznožnik.
