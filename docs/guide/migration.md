# Migration guide

The sections are newest first, one per release that changes behaviour an application can depend on. A project
crossing a single release reads that release's section; one crossing several reads them oldest first, from the
release it is on up to the one it upgrades to. A release with no section here needs no change in the application.

This is the only page that names superseded behaviour; everywhere else in this documentation only the current one
exists. The full list of changes per release is the [changelog](./changelog.md).

<!-- New releases go directly below this comment, above the previous one, as `## Upgrading to vX.Y.Z (from vA.B.x)`. -->

## Upgrading to v0.4.0 (from v0.3.x)

Nothing changes on the wire, and a peer that registers no header interceptors behaves as in 0.3. The changes below
concern only code that catches the exceptions a dial raises before a connection exists.

### A malformed URL raises a `MuxwsError`

`connect()` with a `ws:` / `wss:` URL the transport library cannot parse raises `WebsocketUrlError` in Python and
`WsUrlError` from `muxws/node`. Both extend `TransportUrlError`, which is a `MuxwsError`; the Python class is also a
`ValueError`. The library's own exception is chained: `__cause__` in Python, `cause` in TypeScript.

```python
# fragment
try:
    peer = await connect(url)
except websockets.exceptions.InvalidURI:  # 0.3; in 0.4 this no longer matches
    ...
except TransportUrlError as error:        # 0.4; error.__cause__ is the InvalidURI
    ...
```

```typescript
// fragment
try {
  peer = await connect(url);
} catch (error) {
  if (error instanceof SyntaxError) { /* 0.3: what `ws` threw; in 0.4 this no longer matches */ }
  if (error instanceof TransportUrlError) { /* 0.4: error.cause is the SyntaxError */ }
}
```

### A missing transport package raises a `MuxwsError`

A dial with the `websockets` package (Python) or the `ws` package (`muxws/node`) not installed raises
`WebsocketsNotInstalledError` / `WsNotInstalledError`, whose message names the install command. Both extend
`TransportUnsupportedError`, which is a `MuxwsError`; the Python class is a `RuntimeError` and not an `ImportError`.
Code that caught `ImportError` in Python, or checked `code === 'ERR_MODULE_NOT_FOUND'` in TypeScript, catches
`TransportUnsupportedError` instead. A package that is installed and fails while loading still raises its own error.

### Checklist for 0.4.0

- Replace `except websockets.exceptions.InvalidURI` and `instanceof SyntaxError` around `connect()` with
  `TransportUrlError`.
- Replace `except ImportError` and `ERR_MODULE_NOT_FOUND` checks around `connect()` with
  `TransportUnsupportedError`.
- Python: install `websockets>=14` (the `[websockets]` extra requires it).
