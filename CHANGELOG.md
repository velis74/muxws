# Changelog

All notable changes to `muxws` will be documented in this file. One version number covers the Python package and
the npm package; the wire subprotocol is versioned separately (`muxws.v1.<codec>`) and is unchanged by every release
below.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - Unreleased

### Added

- Header interceptors on `Peer`, each registration returning its unregister function:
  - `peer.intercept_open(interceptor)` / `peer.interceptOpen(interceptor)`: a synchronous
    `(headers, payload)` callback that `open()`, `notify()` and `request()` run before a stream id is
    allocated. It mutates a copy of the caller's headers, and the result is what the `open` frame carries.
  - `peer.intercept_reply_headers(interceptor)` / `peer.interceptReplyHeaders(interceptor)`: a synchronous
    `(headers, stream)` callback run by the call that sends this peer's first frame on a stream the remote
    opened, including the implicit end of a handler that returns. It mutates a copy of that call's headers.
  - `peer.intercept_incoming_headers(interceptor)` / `peer.interceptIncomingHeaders(interceptor)`: a
    `(headers, stream)` callback that runs once per stream with the remote's leading headers, read-only.
- `connect()`, `accept()` and `serve()` take `open_interceptors`, `reply_headers_interceptors` and
  `incoming_headers_interceptors` (`openInterceptors`, `replyHeadersInterceptors`,
  `incomingHeadersInterceptors` in TypeScript) and register them before the peer sends or reads a frame.
- New exported types: `OpenInterceptor`, `ReplyHeadersInterceptor`, `IncomingHeadersInterceptor`.
- `ws+unix:///path/to.sock:/request-target` URLs dial a WebSocket over a Unix domain socket, in Python and from
  `muxws/node`. The URL is split on the first `:` of its path in both languages.
- `TransportUrlError` and `TransportUnsupportedError`, exported from the package root: the bases of every error a
  transport raises for an address it cannot open or a runtime that cannot provide it. Concrete classes live in
  their transport's module: Python `WebsocketUrlError`, `WebsocketsNotInstalledError`, `UnixUrlError`,
  `UnixSocketsUnsupportedError`; TypeScript `WsUrlError`, `WsNotInstalledError`, `UnixUrlError` (`muxws/node`) and
  `UnixSocketsUnsupportedError` (`muxws`).
- Python: `parse_unix_url` and `UnixTarget` in `muxws.transports.unix`; `require_websockets`, `verify_dialable_url`
  and `INSTALL_HINT` in `muxws.transports.websockets_`.

### Changed

- A malformed `ws:` / `wss:` URL raises `WebsocketUrlError` (Python) or `WsUrlError` (`muxws/node`) instead of the
  transport library's own exception, which is chained as the cause. A missing `websockets` / `ws` package raises
  `WebsocketsNotInstalledError` / `WsNotInstalledError`, naming the install command. See the migration guide.
- The `websockets` extra requires `websockets>=14`. With 12 and 13 the TCP dial failed with a `TypeError` from
  `create_connection`.
- Fragmentation slices the UTF-8 encoding of a payload on byte offsets. Fragment boundaries are unchanged; splitting
  a large payload is an order of magnitude faster in both ports.

### Fixed

- TypeScript: `peer.close()` or `stop()` while the reconnect helper was dialling or waiting for the hello no longer
  lets that attempt establish. A socket the dial returns afterwards is closed, `isOpen` stays false and
  `onReconnect` does not fire. Python: a `stop()` during the hello closes the socket of that attempt.
- Python: cancelling the task awaiting `peer.request(..., timeout=...)` sends `reset(CANCELLED)` and closes the
  stream, as it does without a timeout.
- Python: `backoff_delay()` and `unjittered_delay()` return the cap after about 1024 failed attempts instead of
  raising `OverflowError`, which stopped the reconnect helper.

## [0.3.1] - 2026-08-13

### Changed

- Build only: `vite.config.ts` resolves its entry points through `import.meta.dirname`. The library code is that of
  0.3.0.

## [0.3.0] - 2026-08-13

### Added

- Leading headers from the answering side. `stream.send_headers()` / `stream.sendHeaders()` sends them on a
  payload-less `data` frame, and `send()`, `end()` and `reply()` take `headers` for the same purpose. The opener
  reads them as `stream.reply_headers` / `stream.replyHeaders`; `reply_headers_arrived` / `replyHeadersArrived`
  settles once they can no longer change, including on a stream that ends without an answer.

### Changed

- Headers ride only the first stream-level frame a peer sends on a stream. Headers on a later frame reset that
  stream with `PROTOCOL_ERROR`; the connection stays open. A second `send_headers()` raises.
- TypeScript: the handshake refusals of `subprotocol.select` and `BrowserSocket.connect` are written through
  `logger`, so `logger.level` governs them.

## [0.2.0] - 2026-08-10

### Changed

- Package metadata only. The library code is that of 0.1.0.

## [0.1.0] - 2026-08-10

### Added

- First release: the Python package `muxws` and the npm package `muxws`, speaking the `muxws.v1` wire with the
  `json` and `msgpack` codecs.

[0.4.0]: https://github.com/velis74/muxws/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/velis74/muxws/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/velis74/muxws/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/velis74/muxws/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/velis74/muxws/releases/tag/v0.1.0
