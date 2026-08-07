# muxws

Multiplexed, cancellable, bidirectional streams over one WebSocket - a framing, multiplexing,
stream-lifecycle, cancellation and connection-lifecycle protocol, with reference implementations in
Python and TypeScript that share one wire format.

muxws is not a router, not a serializer of domain objects, not an authentication mechanism, not a
durable store and not an RPC framework.

## Install

```bash
pip install muxws                       # extras: [starlette], [websockets], [msgpack]
npm install muxws                       # optional peers: ws, @msgpack/msgpack
```

## Status

Pre-1.0 and under active construction, milestone by milestone. The wire format freezes at 1.0.

| | |
|---|---|
| Python package | `muxws` (PyPI), import `muxws` |
| npm package | `muxws`, browser entry plus `muxws/node` and `muxws/msgpack` subpaths |
| Wire generation | `muxws.v1.<codec>` WebSocket subprotocol |

## Documentation

The documentation site is built from `docs/` and is the place to start once it lands (milestone M7).
The normative rules live in `SPEC.md` from milestone M6; until then they are in
`docs/design/muxws-spec.md`, and `docs/design/README.md` maps the implementation milestones.

## Licence

MIT. Copyright © 2025 Jure Erznožnik.
