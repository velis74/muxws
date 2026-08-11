# muxws

**HTTP/2 and HTTP/3 stream semantics over a WebSocket.** Many independent, cancellable, bidirectional streams on
one socket, either end able to open one - the model HTTP/2 and HTTP/3 already settled, without needing
QUIC or an HTTP/3 stack. The wire format is the product; Python and TypeScript are the two ports that
exist today, and [`SPEC.md`](https://github.com/velis74/muxws/blob/main/SPEC.md) plus
[`conformance/`](https://github.com/velis74/muxws/blob/main/conformance/README.md) are what a third is
written from.

The mimicry is of *semantics*, not transport: one TCP connection means one global message order and no
per-stream loss recovery - [the rationale](https://docs.velis.si/muxws/guide/rationale) says what is and
is not copied, and [the comparison](https://docs.velis.si/muxws/guide/comparison) accounts for it frame
by frame. muxws is not a router, not a serializer of domain objects, not an authentication mechanism,
not a durable store and not an RPC framework.

## Install

```bash
pip install muxws                       # extras: [starlette], [websockets], [msgpack]
npm install muxws                       # optional peers: ws, @msgpack/msgpack
```

## Documentation

**[docs.velis.si/muxws](https://docs.velis.si/muxws/) is the manual** - or build it yourself with
`npm run docs:dev`.

| | |
|---|---|
| Server and client in three files | [Getting started](https://docs.velis.si/muxws/guide/getting-started) |
| Every public symbol, both languages | [API reference](https://docs.velis.si/muxws/api/) |
| The normative rules, and the fixtures a third port is written from | [`SPEC.md`](https://github.com/velis74/muxws/blob/main/SPEC.md), [`conformance/`](https://github.com/velis74/muxws/blob/main/conformance/README.md) |

## Status

0.1.0, alpha - the first release. Two versions that move independently: the *wire* is already at its
first generation and frozen there (`muxws.v1.<codec>` is the subprotocol, and `v1` stays until a
breaking change earns a new one), while the *package* carries a leading zero because none of this has
been through a real deployment yet. 1.0 is what the first confirmed production use earns. Python
package `muxws` (PyPI); npm package `muxws`, browser entry plus the `muxws/node` and `muxws/msgpack`
subpaths.

## Licence

MIT. Copyright © 2025 Jure Erznožnik.
