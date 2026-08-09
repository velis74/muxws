# muxws

Multiplexed, cancellable, bidirectional streams over one WebSocket - a framing, multiplexing,
stream-lifecycle, cancellation and connection-lifecycle protocol, with reference implementations in
Python and TypeScript that share one wire format. It is not a router, not a serializer of domain
objects, not an authentication mechanism, not a durable store and not an RPC framework.

## Install

```bash
pip install muxws                       # extras: [starlette], [websockets], [msgpack]
npm install muxws                       # optional peers: ws, @msgpack/msgpack
```

## Documentation

**The documentation site is the manual**: build it with `npm run docs:dev`, or read `docs/`.

| | |
|---|---|
| Why one socket with many streams | [`docs/guide/rationale.md`](docs/guide/rationale.md) |
| Server and client in three files | [`docs/guide/getting-started.md`](docs/guide/getting-started.md) |
| Every public symbol, both languages | [`docs/api/`](docs/api/) |
| The normative rules, with `WSM-*` ids | [`SPEC.md`](SPEC.md) |
| Fixtures for a third implementation | [`conformance/README.md`](conformance/README.md) |

## Status

1.0. The wire format is frozen: `muxws.v1.<codec>` is the WebSocket subprotocol, and `v1` is the
only generation there will be until a breaking change earns a new one.

| | |
|---|---|
| Python package | `muxws` (PyPI), import `muxws` |
| npm package | `muxws`, browser entry plus the `muxws/node` and `muxws/msgpack` subpaths |

## Licence

MIT. Copyright © 2025 Jure Erznožnik.
