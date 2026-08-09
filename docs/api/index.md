# API Reference

Every public symbol in both languages, each with its signature exactly as the source declares it,
every parameter, the return value, the exceptions it raises, and one example that runs. The examples
are executed by the test suite, so a block that stops working stops the build.

Two conventions worth knowing before you read a page:

- **Durations state their unit.** Python takes **seconds as floats** (`timeout=5.0`); TypeScript
  takes **milliseconds as integers** (`{ timeoutMs: 5000 }`). The two ports use the same names
  otherwise, in each language's casing: `on_stream` / `onStream`, `is_open` / `isOpen`.
- **Limits belong to the peer that sets them.** `max_payload_bytes`, `max_concurrent_streams` and
  `error_serializer` are configured per connection, are never announced and are never negotiated.
  The other end finds out about one only from the reset it provokes.

## The pages

| Page | What it covers |
|---|---|
| [connect](/api/connect) | Dialling a peer, in Python, in the browser and under Node, with every connection option. |
| [accept](/api/accept) | The acceptor side: `accept`, `serve`, and the subprotocol hooks that refuse a codec you do not speak. |
| [Peer](/api/peer) | The object both ends hold: opening streams, the handler and lifecycle callbacks, `ping`, `close`, `serve`. |
| [Stream](/api/stream) | One call: sending, ending, replying, awaiting, iterating, cancelling and resetting. |
| [Reconnect](/api/reconnect) | The dialer-only helper - backoff, the hello, and the three functions that describe its schedule. |
| [Errors](/api/errors) | The exception tree, the nine reset codes, and how a reset becomes an exception. |
| [Codec](/api/codec) | The codec protocol, the two codecs that ship, the registry, and the settings that select one. |
| [Registry](/api/registry) | `PeerRegistry`: tagging live peers and looking them up again. |
| [Transports](/api/transports) | `SocketAdapter` and every adapter that implements it, in both languages. |
| [Types](/api/types) | `Frame` and its envelope, the frame helpers, `CloseReason`, and the two callback aliases. |

New to muxws? Start with the [rationale](/guide/rationale) and the
[quick start](/guide/getting-started); this reference assumes you have a connection open.
