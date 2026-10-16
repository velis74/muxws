# Errors

This page lists every error muxws raises and, for each, whether to **retry now**, **back off**, or
**not retry**.

## The exception tree

Every error muxws raises is a subclass of `MuxwsError`, so `except MuxwsError` / `catch (e) {
if (e instanceof MuxwsError) ... }` catches all of them. The class names are the same in both languages;
the Python classes inherit from `Exception`, the TypeScript classes from `Error`.

```text
MuxwsError                     everything below is one of these
├── ProtocolError              this peer or the remote violated the protocol
├── ConnectionClosed           the socket died — connection-level, never per-stream
├── ConnectionGoingAway        goaway has been sent or received; no new stream here
├── StreamAlreadyConsumed      a stream was awaited and iterated, or iterated twice
├── StreamClosed               send/end/reply on a stream that closed normally
├── CodecError                 configuration failure, not a stream failure
│   ├── CodecNotRegistered     the configured codec name was never registered
│   └── CodecMismatch          the two ends disagree; the handshake was rejected
├── TransportUrlError          this transport cannot open that address — retype the URL
├── TransportUnsupportedError  this runtime has no such transport — change where you run
└── StreamReset                a stream ended early — carries code, reason, stream_id
    ├── RemoteError            code 2  the remote handler raised
    ├── StreamRefused          code 4  not accepted and definitively not processed
    ├── StreamTimeout          code 6  a deadline expired
    └── ConnectionLost         code 9  synthesised locally when the socket died
```

`StreamReset` itself is raised for every other code: `NO_ERROR`, `CANCELLED`, `PROTOCOL_ERROR`,
`PAYLOAD_TOO_LARGE`, `INTERNAL_ERROR`, and any code this generation does not define.

The two `Transport…` classes are **base classes** and are never raised directly. The raised class is a
subclass named after the transport that failed, for example `UnixUrlError` for an invalid `ws+unix://`
URL or `WsNotInstalledError` for a `muxws/node` dial in a process without the `ws` package. These
subclasses are defined in their transports' modules, not in the shared errors module. Catch the base
class to handle the failure regardless of which transport the URL selected; see
[Writing an adapter of your own](#writing-an-adapter-of-your-own) for the convention a third-party
transport follows.

Three attributes are on every `StreamReset`:

| Attribute | Type | Meaning |
|---|---|---|
| `code` / `code` | `ResetCode` or a bare `int` | The wire code. A bare integer when the remote sent a number this generation does not define. |
| `reason` / `reason` | `str \| None` | Free text from the peer that reset the stream. Diagnostic only. |
| `stream_id` / `streamId` | `int \| None` | Which stream. |

`RemoteError` adds `payload`: the structured error object produced by the remote's `error_serializer`,
if the remote sent one.

## The nine reset codes

Codes are numeric on the wire and named in both APIs. The same codes are used by `reset` and
`goaway`. **The reaction column is normative**: the other end may assume this peer reacts as stated.

| Code | Name | Raised when | Required reaction |
|---|---|---|---|
| 0 | `NO_ERROR` | Graceful. On `goaway`, orderly shutdown; on `reset`, "done and no longer interested". | None. Not a failure. |
| 1 | `CANCELLED` | The initiator asked for the operation to stop. | Stop producing; do not retry. |
| 2 | `APPLICATION_ERROR` | The remote handler raised. `reason` carries a message; an optional `payload` carries a structured error object. | Surface to the caller. Retry is the application's call. |
| 3 | `PROTOCOL_ERROR` | The peer violated this specification. | Fix the implementation. Never retried automatically. |
| 4 | `REFUSED` | Not accepted and definitively not processed: no registered handler, an open after `goaway`, or an open beyond the receiver's own concurrency limit. | Retry — elsewhere if another connection is available, otherwise after a delay. |
| 6 | `TIMEOUT` | A deadline expired locally; the reset tells the remote to stop working. | Stop producing. |
| 7 | `PAYLOAD_TOO_LARGE` | An encoded message exceeded what the receiver accepts, or a payload exceeded the receiver's `max_payload_bytes`. | Do not retry unchanged; fragment or shrink. |
| 8 | `INTERNAL_ERROR` | A bug in the peer implementation itself, not in the application handler. | Surface and log. |
| 9 | `CONNECTION_CLOSED` | Synthesised locally when the socket dies, on every stream live at that instant. **Never appears on the wire.** | Do not retry on this peer now; rebuild from `on_reconnect`. |

**Code 5 is retired.** No name maps to it, and a peer must not send it. A peer that *receives* it
treats it as any unknown code.

**An unrecognised code resets that stream and leaves the connection open.** A future generation's
code, or 5, is raised as a plain `StreamReset` whose `code` is the raw integer. For this reason `code`
is typed as "a `ResetCode` *or* an `int`": converting a wire value directly into a closed enumeration
would raise in the read loop, after which the peer would report itself open while every await hangs.

### Reading the table as a decision

- **Retry now, elsewhere or on a new connection**: `REFUSED` (4). It guarantees that the handler did
  not run.
- **Back off, then retry**: `REFUSED` (4) when no other connection is available; `CONNECTION_CLOSED`
  (9), where the reconnect helper performs the backoff and the application retries from
  `on_reconnect`.
- **Do not retry**: `CANCELLED` (1) and `TIMEOUT` (6), because the operation was asked to stop.
  `PROTOCOL_ERROR` (3) and `INTERNAL_ERROR` (8), because a retry reproduces the bug.
  `PAYLOAD_TOO_LARGE` (7), because a retry with the same payload fails the same way; reduce the payload
  first.
- **Application decides**: `APPLICATION_ERROR` (2). muxws cannot determine whether the remote
  handler's failure was transient.
- **No action**: `NO_ERROR` (0).

A handler that raises **always** produces `APPLICATION_ERROR`, never `REFUSED`, even when it raises
immediately. `REFUSED` guarantees that the operation did not run; a handler that debits an account and
then raises has run, and a `REFUSED` would cause the client to retry the debit.

## Three ways a send can fail

`stream.send()`, `stream.end()` and `stream.reply()` on a stream that is no longer open raise one of
three classes, one for each cause.

**`StreamClosed`: the stream ended normally.** Both sides had finished before the `send`. This is an
**expected race, not a bug**: in a bidirectional exchange, the remote handler can return while the last
`send` is in transit. `StreamClosed` is neither a `StreamReset` (no reset occurred) nor a
`ProtocolError` (no peer violated the protocol), so a handler that catches either of those does not
treat a normal end as a failure. It can usually be ignored.

`StreamClosed` is also raised by a second `end()` on the same stream, and by `send()` after `end` has
been sent. That case is a caller bug, but the class is the same; to distinguish the two, check the
stream state rather than the exception.

**The stream's own `StreamReset`: the stream was reset.** The stream ended early with a reset code.
The raised class is the subclass the code maps to, and it carries the remote's `reason`. Every
subsequent call on that stream raises the same failure again, as a new instance each time, so
repeated calls do not extend the traceback of a single exception object.

**`ConnectionLost`: the socket died.** The failure is not specific to this stream; every other live
stream received `ConnectionLost` at the same time.

### Why `ConnectionLost` is a `StreamReset` and `ConnectionClosed` is not

Both report a closed socket, at different levels.

`ConnectionLost` is **per stream**. When the socket dies, every live stream is failed individually,
and each receives its own `ConnectionLost` with its own `stream_id`. It is raised from the same places
as any other stream failure (an `await`, an `async for`, a `send`), so a call site that handles
`StreamReset` also handles a closed socket. Its code is `CONNECTION_CLOSED` (9). The code is generated
locally and is never sent on the wire.

`ConnectionClosed` is **per connection**. Peer-level calls raise it, and it means the socket is
closed, not that one exchange failed. It is not a `StreamReset`, because it names no stream and has no
reset code. If `except StreamReset` caught it, a per-stream handler would handle a connection-level
failure, and the application would continue to use a peer without a socket.

`peer.ping()` raises both. It raises `ConnectionClosed` when the pong does not arrive within the
deadline (`timeout=5.0` seconds / `timeoutMs = 5000` milliseconds by default), because a missing pong
is a connection-level result. It raises `ConnectionLost` when the peer is already between sockets,
which is the condition every stream call reports. Catch `MuxwsError` around a ping to handle both.

## What happens when the socket dies

Every live stream is failed with `ConnectionLost` **before** `on_close` fires, so a close handler
already sees a peer with no live streams. Then:

| Call | Python | TypeScript |
|---|---|---|
| `await stream` / `stream.result(timeout=` seconds `)` / `result({ timeoutMs })` milliseconds | raises `ConnectionLost` | rejects with `ConnectionLost` |
| `async for … in stream` | raises `ConnectionLost` | rejects with `ConnectionLost` |
| `stream.send()` / `.end()` / `.reply()` | raises `ConnectionLost` | rejects with `ConnectionLost` |
| `stream.cancel()` / `.reset()` | returns; no-op on a closed stream | resolves; no-op on a closed stream |
| `peer.open()` | raises `ConnectionLost`, synchronously | throws `ConnectionLost`, synchronously |
| `peer.request()` / `peer.notify()` | raises `ConnectionLost` | rejects with `ConnectionLost` |
| `peer.ping()` | raises `ConnectionLost` | rejects with `ConnectionLost` |
| `peer.close()` | returns; already closed | resolves; already closed |
| `peer.serve()` | returns | resolves |
| an `on_stream` handler mid-flight | its stream is failed and the handler task is cancelled | its stream is failed and `stream.signal` aborts |

**Nothing is buffered while the peer is between sockets.** The writer's queues are discarded when
the socket dies; nothing is kept for the next socket. For this reason `open()` raises instead of
queueing: a queued frame would be delivered on a later connection, possibly minutes later, after a
different hello and in a different id space. To open a stream on the next connection, open it from
`on_reconnect`.

`ConnectionGoingAway` is also raised synchronously, under a different condition: the socket is open,
but `goaway` has been received (the remote is stopping) or sent (this peer is stopping), so no new
stream can be opened on this connection. Streams at or below the `goaway`'s `last_stream` run to
completion; streams above it were not processed and are failed with `REFUSED`, so they can be retried
on a new connection. Dial again to open new streams.

## `error_serializer` is per peer

When an `on_stream` handler raises, muxws resets the stream with `APPLICATION_ERROR` and calls the
peer's `error_serializer` to build the optional structured payload sent with the reset. The default:

```python
# fragment
def default_error_serializer(exc: BaseException) -> Any:
    return {"type": type(exc).__name__, "message": str(exc)}
```

It is set **per peer**, on `connect()` and on `accept()`, not per process. One process can therefore
redact errors on its browser-facing connections and send full errors on its internal ones:

```python
# fragment
def redacted(exc: BaseException) -> Any:
    return {"type": "Error"}

# The browser-facing acceptor: nothing about the exception leaves the process.
public_peer = await accept(websocket, error_serializer=redacted)

# The internal acceptor, on a different route: the default serializer, which includes the message.
internal_peer = await accept(websocket)
```

The reset is always sent. If the serializer raises, the exception is logged and the reset is sent
without a payload. Without the reset, the caller would wait indefinitely.

::: warning The default puts exception text on the wire
`default_error_serializer` sends `str(exc)`, which for many exceptions contains a file path, a SQL
fragment, a key name or a value the application never meant to expose. Replace it on any peer facing
an untrusted remote. See [Observability](/guide/observability#what-does-leave-the-process).
:::

## Errors that are not about a connection

`CodecNotRegistered` and `CodecMismatch` are subclasses of `CodecError`, which is not a
`StreamReset`: they are configuration failures, not stream failures, and neither is retryable.

- **`CodecNotRegistered`** is raised **before any socket is opened**. The message names the configured
  codec and the registered codecs. It means this process's configuration is wrong. muxws never falls
  back to JSON.
- **`CodecMismatch`** means the two ends are configured for different codecs and the handshake was
  rejected. A retry fails the same way. Fix the configuration on one end.

`StreamAlreadyConsumed` is always a caller bug, never a race: a `Stream` has exactly one consumer, the
first use claims it, and a second use (awaiting a stream that is being iterated, or iterating it
twice) raises. See [Call shapes](/guide/call-shapes).

`ProtocolError` means a peer violated the protocol. When the remote violates it, this peer ends the
connection with `goaway(PROTOCOL_ERROR)`: it sends the frames already queued, for at most 10 seconds,
then closes the socket, and from the moment `goaway` is queued `open()` raises `ConnectionGoingAway`.
When raised locally, it means the call is invalid, for example resetting a stream with
`CONNECTION_CLOSED`, which must not be sent, or with a number this generation does not define.

## Errors a transport reports, and the two you catch

A transport can fail in two ways unrelated to the protocol. muxws defines a base class for each, so
an application can distinguish them by class without parsing the message.

**`TransportUrlError`: the address cannot be opened.** The URL is invalid: a `ws+unix:` URL whose
request target does not start with `/`, a `ws+unix:` URL naming no socket file, or a `wss+unix:` URL
(the scheme is not supported). For a `ws:`/`wss:` URL, the URL parser of the dialing library decides:
Python's `WebsocketUrlError` covers a missing hostname or a non-numeric port, `muxws/node`'s
`WsUrlError` covers every URL `ws` rejects, and the browser entry point lets the platform's
`WebSocket` constructor throw. It is always raised **before** the dial, so a background reconnection
never raises it, and it is distinct from a refused handshake. Fix the URL. In Python it is also a
`ValueError`, so code that wraps its configuration parsing in `except ValueError` catches it.

**`TransportUnsupportedError`: the runtime cannot provide the transport.** The URL is valid, and the
same program would work in another environment: `websockets` or `ws` is not installed, the interpreter
has no `AF_UNIX`, or the imported entry point does not include the transport the scheme selects.
Change the environment, not the URL. A class raised for a missing package **names the install
command** (`pip install muxws[websockets]`, `npm install ws`) in its message. In Python it is also a
`RuntimeError`. A retry does not help.

Neither base class is raised directly. The raised class is a concrete subclass defined by the
transport that failed, and these subclasses are **not** exported from `muxws`:

```python
# fragment
from muxws import connect, TransportUnsupportedError, TransportUrlError
from muxws.transports.unix import UnixUrlError  # the concrete one, from its transport

try:
    peer = await connect(url)
except UnixUrlError:
    ...  # only a ws+unix: URL can be this
except TransportUrlError:
    ...  # any transport, any scheme: the URL is unusable
except TransportUnsupportedError:
    ...  # the URL is fine; this process or this platform cannot dial it
```

Most applications need only the two base classes. They are exported from the package root in both
languages and have no dependencies, so `except TransportUrlError` works in a process that cannot
import the transport that raised.

### Writing an adapter of your own

The socket adapter interface is public: an adapter is any object with the four members
`SocketAdapter` defines. Transports muxws does not ship, such as a QUIC datagram carrier, an SSH
channel or a test double that fails on demand, are written outside this repository. An adapter that
reports failures of its own follows the convention below, so that an application catches its errors
the same way as those of a shipped adapter.

1. **Subclass one of the two bases.** `TransportUrlError` if the address is unusable;
   `TransportUnsupportedError` if the transport cannot exist here at all. Both are exported from the
   package root in both languages — `from muxws import TransportUrlError` and
   `import { TransportUrlError } from 'muxws'`. Every error an application catches must be a
   `MuxwsError`, and these are the two `MuxwsError` bases for failures not related to a stream.
2. **In TypeScript, set `name` on the subclass.** `name` identifies the class in log output and in
   comparisons between the two ports. A subclass that does not assign it inherits
   `'TransportUrlError'`, and log lines and cross-language comparisons then show the base class name.
   `UnixUrlError` and `WsUrlError` set it in their constructors:

   ```ts
   export class MyQuicUrlError extends TransportUrlError {
     constructor(message?: string, options: { cause?: unknown } = {}) {
       super(message, options);
       this.name = 'MyQuicUrlError';
     }
   }
   ```

   Python requires nothing here: `type(exc).__name__` is already the subclass name.
3. **Define the class in your own module, next to the code that raises it.** Do not put it in a shared
   errors module or re-export it from a package root that also exports muxws names. Callers import it
   as `from your_package.transport import YourUrlError`, the same way they import `UnixUrlError`.
4. **Keep the module importable without the dependency it adapts.** If the class can only be defined
   after `import your_dependency` succeeds, then `except YourDependencyMissingError` itself raises
   `ImportError`. Import the dependency lazily, inside the dial.
5. **Chain the original exception.** `raise YourUrlError(...) from exc` in Python, `{ cause: error }`
   in TypeScript. The class states the category of the failure; the chained original states the
   specific cause.
6. **State the fix when there is exactly one.** For a missing package, include the install command
   verbatim in the message.
7. **Define a class only for a failure that can occur.** If no input or environment produces the
   failure, do not define a class for it.

The shared errors module contains only the errors the `SocketAdapter` contract requires.
`ConnectionClosed` is defined there because the contract requires every adapter, including
third-party ones, to raise it. Adapter-specific errors are defined in the adapter's module.

## See also

- [`api/errors`](/api/errors) — every class above, `ResetCode`, `default_error_serializer`
- [`api/stream`](/api/stream) — `send`, `end`, `reply`, `cancel`, `reset`, `result`, `closed`
- [`api/peer`](/api/peer) — `open`, `request`, `notify`, `ping`, `close`, `on_close`, `on_reconnect`
- [`api/connect`](/api/connect) and [`api/accept`](/api/accept) — the `error_serializer` parameter
- [`api/transports`](/api/transports) — `SocketAdapter`, the interface an adapter of your own implements
- [`api/types`](/api/types) — `ErrorSerializer`, `CloseReason`
