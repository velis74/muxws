# <img src="muxws-icon.svg" alt="" style="width:2.5em; height: 2.5em; vertical-align: middle"> muxws

**Multiplexed streams over one WebSocket, with HTTP/2 and HTTP/3 stream semantics.** muxws carries many
independent, cancellable, bidirectional streams on one socket; either end can open a stream. It needs no
QUIC or HTTP/3 stack. muxws is defined by its wire format. Python and TypeScript implementations are in
this repository; [`SPEC.md`](https://github.com/velis74/muxws/blob/main/SPEC.md) and
[`conformance/`](https://github.com/velis74/muxws/blob/main/conformance/README.md) specify the protocol
for further implementations.

muxws copies the stream semantics of HTTP/2 and HTTP/3, not their transport. A connection runs over one
TCP socket or one Unix domain socket, so all streams share one global message order and there is no
per-stream loss recovery. [Rationale](https://docs.velis.si/muxws/guide/rationale) lists which parts of the
HTTP model muxws copies and which it does not;
[Comparison](https://docs.velis.si/muxws/guide/comparison) compares the two protocols frame by frame.
muxws does not route requests, serialize domain objects, authenticate peers, store messages durably or
define RPC services.

## Install

```bash
pip install muxws                       # extras: [starlette], [websockets], [msgpack]
npm install muxws                       # optional peers: ws, @msgpack/msgpack
```

## Documentation

The manual is at **[docs.velis.si/muxws](https://docs.velis.si/muxws/)**. `npm run docs:dev` builds and
serves it locally.

| | |
|---|---|
| A server and a client in three files | [Getting started](https://docs.velis.si/muxws/guide/getting-started) |
| A browser demo, a Unix-socket demo and a throughput benchmark | [Demos](https://docs.velis.si/muxws/guide/demos) |
| Every public symbol in both languages | [API reference](https://docs.velis.si/muxws/api/) |
| The normative rules and the conformance fixtures for new implementations | [`SPEC.md`](https://github.com/velis74/muxws/blob/main/SPEC.md), [`conformance/`](https://github.com/velis74/muxws/blob/main/conformance/README.md) |
| Changes per release, and the changes required when upgrading | [Changelog](https://docs.velis.si/muxws/guide/changelog), [Migration guide](https://docs.velis.si/muxws/guide/migration) |

## Status

Alpha. The wire protocol and the packages are versioned independently:

- **Wire protocol:** generation 1, frozen. The WebSocket subprotocol is `muxws.v1.<codec>`. The `v1`
  changes only with a breaking wire change.
- **Packages:** version 0.x. Version 1.0 follows the first confirmed production deployment.

Packages: `muxws` on PyPI; `muxws` on npm, with the browser entry point and the `muxws/node` and
`muxws/msgpack` subpaths.

## Licence

MIT. Copyright © 2025 Jure Erznožnik.
