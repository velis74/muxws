# API Reference

This reference documents every public symbol in both languages: the signature as the source declares
it, every parameter, the return value, the exceptions raised, and one runnable example. The test suite
executes the examples; an example that fails fails the build.

Conventions used on every page:

- **Durations state their unit.** Python takes **seconds as floats** (`timeout=5.0`); TypeScript
  takes **milliseconds as integers** (`{ timeoutMs: 5000 }`). The two ports use the same names
  otherwise, in each language's casing: `on_stream` / `onStream`, `is_open` / `isOpen`.
- **Limits are local to the peer that sets them.** `max_payload_bytes`, `max_concurrent_streams` and
  `error_serializer` are configured per connection and are never sent to or negotiated with the
  remote. The remote observes a limit only as the reset it receives when it exceeds the limit.
- **`muxws/node` re-exports part of `muxws`.** `Reconnect`, `ReconnectOptions`, `Hello`,
  `HelloOptions`, `ConnectOptions`, `backoffDelay`, `shouldRetry`, `unjitteredDelay`, `PeerRegistry`,
  `TagValue` and `VERSION` are exported from both entry points. `WsSocket` is exported from
  `muxws/node` only; no module reachable from `muxws` may import it (WSM-API-022). The codecs, the
  errors, `Peer` and the frame helpers are exported from `muxws` only, and importing `muxws` registers
  the JSON codec.

The Python package requires Python 3.10 or newer. The `websockets` extra requires `websockets` 14 or
newer, the first release whose top-level `connect` is `websockets.asyncio.client.connect`, which takes
`additional_headers`.

## The pages

| Page | What it covers |
|---|---|
| [connect](/api/connect) | Dialling a peer, in Python, in the browser and under Node, with every connection option. |
| [accept](/api/accept) | The acceptor side: `accept`, `serve`, the `Origin` policies, and the subprotocol hooks that refuse a codec you do not speak. |
| [Peer](/api/peer) | The object both ends hold: opening streams, the handler and lifecycle callbacks, header interceptors, `ping`, `close`, `serve`. |
| [Stream](/api/stream) | One call: sending, ending, replying, awaiting, iterating, cancelling and resetting. |
| [Reconnect](/api/reconnect) | The dialer-only helper: backoff, the hello, and the three functions that compute its schedule. |
| [Errors](/api/errors) | The exception tree, the nine reset codes, and how a reset becomes an exception. |
| [Codec](/api/codec) | The codec protocol, the two codecs that ship, the registry, and the settings that select one. |
| [Registry](/api/registry) | `PeerRegistry`: tagging live peers and looking them up again. |
| [Transports](/api/transports) | `SocketAdapter` and every adapter that implements it, in both languages, plus the `ws+unix:` URL grammar and the `websockets` dial gates. |
| [Types](/api/types) | `Frame` and its envelope, the frame helpers, `CloseReason`, the two callback aliases and the three interceptor aliases. |

For an introduction, see the [rationale](/guide/rationale) and the
[quick start](/guide/getting-started). This reference assumes an open connection.
