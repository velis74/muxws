# Changelog

All notable changes to `muxws` will be documented in this file. One version number covers the Python package and
the npm package; the wire subprotocol is versioned separately (`muxws.v1.<codec>`) and is unchanged by every release
below.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.3] - 2026-10-19

### Added

- `SameOrigin` (Python and TypeScript), an `allowed_origins` / `allowedOrigins` policy that allows an `Origin`
  with the host and port of the handshake's `Host` header, plus the origins listed in `also`. With
  `trust_forwarded_host` / `trustForwardedHost`, the first entry of `X-Forwarded-Host` takes the place of `Host`
  when present.
- `parse_origin` (Python) and `parseOrigin` (TypeScript) are public, exported from the package root.

### Fixed

- TypeScript: the type declarations resolve under every `moduleResolution`. `exports` carries a `types` condition
  per entry point and format (`.d.ts` for `import`, `.d.cts` for `require`), relative imports in the declarations
  name their file as `node16` / `nodenext` require, and `typesVersions` maps `muxws/node` and `muxws/msgpack` for
  `node10`. Previously only `bundler` resolution found the types.

## [0.4.2] - 2026-10-18

### Added

- `accept()` checks the handshake's `Origin` header when given `allowed_origins` (Python) /
  `allowedOrigins` (TypeScript): an iterable of exact origins (scheme and host compared case-insensitively,
  default ports filled in) or a function of the header value. With a list, a request without `Origin` is
  refused. A refused Starlette socket is answered with HTTP 403; a `websockets` or `ws` connection is closed
  with 1008. `accept()` raises the new `OriginRefused`, and `serve()` returns. The default checks nothing.
- TypeScript: `AcceptOptions.request`, the upgrade request `ws` emits with the connection, from which
  `accept()` reads `Origin`.

## [0.4.1] - 2026-10-16

### Fixed

- `Peer.close()` and a connection-level protocol error send every queued frame, including every remaining fragment
  of a fragmented payload, before closing the socket, bounded by the drain deadline. The socket was previously
  closed after a fixed number of event-loop turns, which could truncate a large reply or lose `goaway` (Python and
  TypeScript).
- TypeScript: the outbound frame accounting no longer goes negative after a fragmented send, which let `close()`
  close the socket before `goaway` was sent.
- Python: an exception from the socket adapter's `close()` during `Peer.close()` or a connection failure is logged
  as a warning and the peer is still marked closed, failing its live streams and calling `on_close`, as in
  TypeScript.
- After a connection-level protocol error or a read-loop failure, `open()` raises `ConnectionGoingAway` while the
  queued frames are being sent, and `close()` does not send a second `goaway` (WSM-CON-021; Python and TypeScript).
- An `open` larger than the frame cap has its id checked for parity and monotonicity and recorded as the remote's
  highest open before the `PAYLOAD_TOO_LARGE` reset. A later `data` or `reset` on that id is ignored instead of
  ending the connection, and an over-cap `open` with a wrong-parity or non-increasing id ends the connection with
  `goaway(PROTOCOL_ERROR)` (Python and TypeScript).
- An over-cap `data` or `reset` naming an id that is not live is no longer answered with a `reset`. At or below the
  high-water mark it is ignored (WSM-STM-002), and above it the connection ends with `goaway(PROTOCOL_ERROR)`
  (WSM-STM-003) (Python and TypeScript).
- A peer whose read loop raises sends `goaway(INTERNAL_ERROR)`, reports close code 1011 and closes its socket.
  Previously it marked itself closed and left the WebSocket open. In Python this also covers exceptions raised
  outside frame dispatch, which used to escape `serve()` with `is_open` still true.
- A frame the writer cannot encode or send, when no live stream is left to fail, closes the socket with 1011
  instead of only marking the peer closed (Python and TypeScript).
- `from_mapping` / `fromMapping` reject envelope fields of the wrong type (for example a string `stream`, a
  non-boolean `end` or a non-string `type`) with `ProtocolError`, so such a message is a connection-level protocol
  error (`goaway(PROTOCOL_ERROR)`, close 1002) in both ports. A non-string `type` with frame logging at debug level
  previously stopped the Python read loop while the peer reported itself open, and closed the TypeScript
  connection with 1011.
- A reassembled payload the codec cannot decode, or fragments of one payload that mix text and binary values, reset
  only that stream with `PROTOCOL_ERROR`; the connection stays open (Python and TypeScript).
- The WebSocket close sent by `Peer.close()` or at the end of a connection-level protocol error carries at most 123
  UTF-8 bytes of reason, cut at a character boundary. A longer reason made `ws`, browser `WebSocket` and
  `websockets` reject the close and leave the socket open. The `goaway` frame and the close report still carry the
  full reason.
- Resetting or cancelling a stream drops the stream's queued frames that have not been sent, including the
  remaining fragments of a payload that is partly sent; the `reset` ends the fragment sequence on the receiver. A
  `reset` received from the remote drops this peer's queued frames for that stream in the same way. An `open` of
  which nothing has been sent yet is still sent before the `reset` (Python and TypeScript).
- A frame that fails to encode when the writer advances past the previous frame on its stream resets only that
  stream, with the codec's error as the reason, and drops the frames queued behind it, instead of sending them
  after the stream has failed (Python and TypeScript).
- An `open` whose payload the codec cannot encode fails its stream with `INTERNAL_ERROR` and sends nothing for the
  id. A `reset` for that id was sent before, and the remote, which had not seen the id, ended the connection
  (WSM-STM-003) (Python and TypeScript).
- `connect()` checks `max_frame_bytes` / `maxFrameBytes` and copies the hello before dialing, so an unusable frame
  cap or an uncopyable hello raises without opening a socket; any other failure after the dial and before the hello
  closes the dialled socket (Python and TypeScript).
- Python: `accept()` and `serve()` close a `websockets` connection whose negotiated subprotocol is not
  `muxws.v1.<codec>` with 1008 before raising `CodecMismatch`.
- TypeScript: `muxws/node` `accept()` and `connect()` log the negotiated-subprotocol error line on a codec mismatch
  detected on an open socket, as the browser `connect()` and Python do.
- The negotiated-subprotocol error line logged on an open-socket codec mismatch names the `VITE_MUXWS_CODEC` and
  `MUXWS_CODEC` environment variables (Python and TypeScript, WSM-CDC-029).
- `PeerRegistry` keeps one close handler per peer across reconnects; re-registering a peer after each reconnect no
  longer adds another handler to it (Python and TypeScript).
- Python: `Peer.ping()` removes its pending entry when the caller is cancelled, not only on a pong or a timeout.
- TypeScript: `muxws/node` opens a `ws+unix:` socket path as written in the URL, as Python does: a space or
  non-ASCII character in the path is no longer percent-encoded, `%XX` is not decoded, and a trailing space stays
  part of the path.
- Python: `MemorySocket.receive()` raises `ConnectionClosed` on every call after the socket closes, instead of
  waiting forever on the second call.
- TypeScript: `ConnectionClosed` with no message (or an empty one) takes its message from `reason`, then
  `"connection closed"`, as in Python.
- TypeScript: `Peer.onError` handlers are called in isolation: a handler that throws is logged and the remaining
  handlers still run, and a throwing handler on the handler-teardown path no longer produces an unhandled
  rejection.
- `Reconnect` rejects a NaN initial delay, factor or max delay at construction (`ValueError` in Python, `Error` in
  TypeScript).
- An unknown frame type is logged once per peer instead of once per frame, for at most 32 distinct types
  (WSM-FRM-002; Python and TypeScript).

## [0.4.0] - 2026-10-16

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

[0.4.3]: https://github.com/velis74/muxws/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/velis74/muxws/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/velis74/muxws/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/velis74/muxws/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/velis74/muxws/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/velis74/muxws/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/velis74/muxws/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/velis74/muxws/releases/tag/v0.1.0
