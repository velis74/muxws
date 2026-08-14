# Errors

This page is meant to be enough on its own. If something failed, you should be able to decide from
here — without opening anything else — whether to **retry now**, **back off**, or **not retry**.

## The exception tree

Every error muxws raises descends from one root, so `except MuxwsError` / `catch (e) {
if (e instanceof MuxwsError) ... }` is a complete net. The names are identical in both languages;
Python's inherit from `Exception`, TypeScript's from `Error`.

```
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

`StreamReset` itself is what you get for every other code — `NO_ERROR`, `CANCELLED`,
`PROTOCOL_ERROR`, `PAYLOAD_TOO_LARGE`, `INTERNAL_ERROR`, and any code this generation does not define.

The two `Transport…` classes are **bases**, never raised as themselves. What arrives is a subclass
named after the transport that could not do the job — `UnixUrlError` for a bad `ws+unix://` URL,
`WsNotInstalledError` for a `muxws/node` dial in a process without the `ws` package — and those
subclasses live in their transports' own modules rather than in the shared one. Catch the base if you
do not want to know which transport a configured URL named; see
[Writing an adapter of your own](#writing-an-adapter-of-your-own) for why the split is where it is.

Three attributes are on every `StreamReset`:

| Attribute | Type | Meaning |
|---|---|---|
| `code` / `code` | `ResetCode` or a bare `int` | The wire code. A bare integer when the remote sent a number this generation does not define. |
| `reason` / `reason` | `str \| None` | Free text from whoever reset the stream. Diagnostic only. |
| `stream_id` / `streamId` | `int \| None` | Which stream. |

`RemoteError` adds `payload`: the structured error object the remote's `error_serializer` produced, if
it sent one.

## The nine reset codes

Numeric on the wire, named in both APIs. The same table serves `reset` and `goaway`. **The reaction
column is normative** — it is what the other end is entitled to assume you will do.

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

**5 is a retired number and never appears.** It was `STREAM_LIMIT`, for rejection against an announced
concurrency limit; there is no announced limit, so there is nothing to reject against and no name maps
to 5. It must never be sent. A peer that *receives* it treats it as it treats any unknown code.

**An unrecognised code resets that stream and leaves the connection alive.** A future generation's
code, or the retired 5, arrives as a plain `StreamReset` whose `code` is the raw integer. This is why
`code` is typed as "a `ResetCode` *or* an `int`": converting a wire value straight into a closed
enumeration raises out of the read loop, after which the peer looks open with every await hanging.

### Reading the table as a decision

- **Retry now, elsewhere or on a new connection** — `REFUSED` (4). It is a promise that nothing ran.
- **Back off, then retry** — `REFUSED` (4) when there is nowhere else to go; `CONNECTION_CLOSED` (9),
  where the backing off is the reconnect helper's and the retry is yours from `on_reconnect`.
- **Do not retry** — `CANCELLED` (1) and `TIMEOUT` (6): somebody asked for this to stop.
  `PROTOCOL_ERROR` (3) and `INTERNAL_ERROR` (8): retrying reproduces the bug. `PAYLOAD_TOO_LARGE` (7):
  retrying *unchanged* fails identically; shrink first.
- **Ask the application** — `APPLICATION_ERROR` (2). muxws cannot know whether the remote handler's
  failure was transient.
- **Nothing to do** — `NO_ERROR` (0).

The one that catches people is `APPLICATION_ERROR` versus `REFUSED`. A handler that raises **always**
produces `APPLICATION_ERROR`, never `REFUSED`, even when the failure was immediate — because `REFUSED`
promises the operation definitively did not happen, and a handler that debits an account and then
raises would, under `REFUSED`, be inviting the client to retry the debit.

## Three ways a send can fail

`stream.send()`, `stream.end()` and `stream.reply()` on a stream that is no longer open raise one of
three things, and they are three different classes because they are three different situations.

**`StreamClosed` — it ended normally.** Both sides had finished; your `send` arrived after. This is an
**expected race, not a bug**: a remote handler returning and your last `send` crossing on the wire is
an ordinary outcome of a bidirectional exchange. `StreamClosed` is deliberately neither a
`StreamReset` (nothing was reset) nor a `ProtocolError` (nobody misbehaved), so a handler that catches
either of those does not accidentally treat a clean finish as a failure. You may usually ignore it.

`StreamClosed` is also what a second `end()` on the same stream raises, and what `send()` raises after
you have already sent `end`. That one *is* a caller bug, but it is the same class, so check the state
rather than the exception if you need to tell them apart.

**That stream's own `StreamReset` — the remote objected.** The stream was killed early and there is a
code saying why. The exact subclass is the one the code maps to, and the reset carries the remote's
`reason`. Every subsequent call on that stream raises the same failure again — a fresh instance each
time, so a caller that keeps trying does not grow one exception object with an ever-longer traceback.

**`ConnectionLost` — the socket died.** Not this stream's problem: the wire under it disappeared, and
every other live stream got the same thing at the same instant.

### Why `ConnectionLost` is a `StreamReset` and `ConnectionClosed` is not

They describe the same event from two altitudes.

`ConnectionLost` is **per stream**. When the socket dies, every live stream is failed individually,
and each one gets its own `ConnectionLost` carrying its own `stream_id`. It comes out of exactly the
places any other stream failure comes out of — an `await`, an `async for`, a `send` — so a call site
that already handles `StreamReset` handles a dead socket for free, without a second branch. Its code
is `CONNECTION_CLOSED` (9), and that code is synthesised locally and never travels: telling a remote
that *its* connection had died would be both false and unfalsifiable.

`ConnectionClosed` is **per connection**. It is what peer-level calls raise, and it says the socket
itself is gone rather than that one exchange failed. It is not a `StreamReset` because there is no
stream to name and no reset code to react to. Catching `StreamReset` must not catch it, or a
connection-level failure would be swallowed by a per-stream handler and the application would keep
trying to work on a peer that has none.

One call sits across the line: `peer.ping()` raises `ConnectionClosed` when the pong does not come
back within its deadline (`timeout=5.0` seconds / `timeoutMs = 5000` milliseconds by default) — a lost
pong is a connection-level answer — but `ConnectionLost` when the peer is *already* between sockets,
because that is the same "there is no wire" condition every stream call reports. Catch `MuxwsError`
around a ping if you care about neither distinction.

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

**Nothing is buffered while the peer is between sockets.** The writer's queues are discarded the
moment the socket dies; nothing is held for the next one. This is why `open()` raises rather than
queueing: a frame accepted now and delivered on a connection established four minutes later, after a
different hello and with a different id space, is not the frame the caller meant to send. A stream you
want on the next connection is one you open from `on_reconnect`.

The related synchronous raise is `ConnectionGoingAway`, which is different: the socket is *fine*, but
`goaway` has been received (the remote is stopping) or sent (this peer is stopping), so no new stream
may be opened on this connection. Streams already at or below the `goaway`'s `last_stream` cut-off run
to completion; streams above it were never processed and are failed `REFUSED`, which is safe to retry
on a new connection. Dial again to get a connection that can open streams.

## `error_serializer` is per peer

When an `on_stream` handler raises, muxws resets the stream with `APPLICATION_ERROR` and calls this
peer's `error_serializer` to build the optional structured payload that rides the reset. The default:

```python
# fragment
def default_error_serializer(exc: BaseException) -> Any:
    return {"type": type(exc).__name__, "message": str(exc)}
```

It is chosen **per peer**, on `connect()` and on `accept()` — not per process, not globally. That is
what lets one process redact on its browser-facing connections and not on its internal ones:

```python
# fragment
def redacted(exc: BaseException) -> Any:
    return {"type": "Error"}

# The browser-facing acceptor: nothing about the exception leaves the process.
public_peer = await accept(websocket, error_serializer=redacted)

# The internal acceptor, on a different route: the default, because the message is worth having.
internal_peer = await accept(websocket)
```

The reset itself is unconditional. A serializer that raises is logged and the reset goes out with no
payload — a handler failure reaching the opener as *silence* would leave the caller waiting forever,
which is worse than a reset with less detail in it.

::: warning The default puts exception text on the wire
`default_error_serializer` sends `str(exc)`, which for many exceptions contains a file path, a SQL
fragment, a key name or a value the application never meant to expose. Replace it on any peer facing
an untrusted remote. See [Observability](/guide/observability#what-does-leave-the-process).
:::

## Errors that are not about a connection

`CodecNotRegistered` and `CodecMismatch` are both `CodecError`, which is outside `StreamReset` on
purpose: they are configuration failures, not stream failures, and neither is retryable.

- **`CodecNotRegistered`** is raised **before any socket is opened**, naming the configured name and
  the set that *is* registered. It means this process's configuration is wrong. There is no fallback
  to JSON, ever.
- **`CodecMismatch`** means the two ends are configured for different codecs and the handshake was
  rejected. Retrying dials the same mismatch. Fix the configuration on one end.

`StreamAlreadyConsumed` is a caller bug and never a race: a `Stream` has exactly one consumer, the
first use claims it, and the second use — awaiting one that is being iterated, or iterating one twice
— raises. See [Call shapes](/guide/call-shapes).

`ProtocolError` means somebody violated the protocol. From the remote, it kills the connection with
`goaway(PROTOCOL_ERROR)`. Raised locally, it means the call was impossible as written — resetting a
stream with `CONNECTION_CLOSED`, which may never be sent, or with a number this generation does not
define.

## Errors a transport reports, and the two you catch

A transport can fail in two ways that have nothing to do with the protocol, and muxws gives each of
them a base class so that you can act on the difference without reading a message.

**`TransportUrlError` — the address could not be opened.** The URL is wrong: a `ws:` URL with no
hostname, a port that is not a number, a `ws+unix:` URL whose request target does not start with `/`,
a `wss+unix:` URL, which does not exist. Always raised **before** the dial, so it can never resurface
later out of a background reconnection, and never confused with a refused handshake. The action is to
fix the URL. In Python it is a `ValueError` too, so a caller who never heard of muxws and wrapped its
own configuration parsing in `except ValueError` still catches it.

**`TransportUnsupportedError` — this runtime cannot provide that transport at all.** The URL is fine
and the same program would work elsewhere: `websockets` or `ws` is not installed, the interpreter has
no `AF_UNIX`, the entry point you imported does not ship the transport the scheme names. The action
is to change the environment, not the URL, and a class raised for a missing package **names the
install** — `pip install muxws[websockets]`, `npm install ws` — because "no module named …" is what
the runtime already told you. In Python it is a `RuntimeError` too, for the same reason the other one
is a `ValueError`. Retrying never helps.

Neither is ever raised as itself. What you receive is a concrete subclass belonging to the transport
that failed, and those are **not** importable from `muxws`:

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

Most applications want only the two bases. They are exported from the package root in both languages
and carry no dependency of their own, so `except TransportUrlError` is writable in a process that
cannot even import the transport that raised.

### Writing an adapter of your own

The socket seam is public: an adapter is any object with the four members `SocketAdapter` names, and
transports muxws does not ship — a QUIC datagram carrier, an SSH channel, a test double that fails on
demand — are expected to be written outside this repository. If yours reports a failure of its own,
this is the convention, and it exists so that an application cannot tell your adapter from a shipped
one by the shape of what it catches.

1. **Subclass one of the two bases.** `TransportUrlError` if the address is unusable;
   `TransportUnsupportedError` if the transport cannot exist here at all. Both are exported from the
   package root in both languages — `from muxws import TransportUrlError` and
   `import { TransportUrlError } from 'muxws'`. Everything an application catches must be a
   `MuxwsError`, and these are the two doors into it that are not about a stream.
2. **In TypeScript, set `name` on the subclass.** JavaScript has one prototype chain, so `name` is
   the discriminator that carries the class's identity across the two ports, and a subclass that does
   not assign it inherits `'TransportUrlError'` — every log line and every cross-language comparison
   then reads the base instead of your class. It is three lines, and it is what `UnixUrlError` and
   `WsUrlError` already are:

   ```ts
   export class MyQuicUrlError extends TransportUrlError {
     constructor(message?: string, options: { cause?: unknown } = {}) {
       super(message, options);
       this.name = 'MyQuicUrlError';
     }
   }
   ```

   Python needs nothing here: `type(exc).__name__` is the class's own name already.
3. **Put the class in your own module, next to the code that raises it.** Not in a shared errors
   module, not re-exported from a package root that also exports muxws's own names. Callers reach it
   as `from your_package.transport import YourUrlError`, exactly the way they reach `UnixUrlError`.
4. **Keep the module importable without the dependency it adapts.** If your class only exists after
   `import your_dependency` succeeds, then `except YourDependencyMissingError` raises the very
   `ImportError` it was written to replace. Import the dependency lazily, inside the dial.
5. **Chain, do not replace.** `raise YourUrlError(...) from exc` in Python, `{ cause: error }` in
   TypeScript. Your class says which category the failure is in; the original still says what
   actually went wrong, and it is usually the more specific of the two.
6. **Name the remedy when there is exactly one.** A missing package has one fix and your message
   should contain it verbatim, in a form the reader can paste.
7. **Do not invent a class for a failure you cannot reach.** If you cannot write the command that
   produces it, the class is decoration, and it will be the one an application branches on.

The one thing you cannot do is add a class to `muxws/errors.py` — which is exactly why the rule is
"subclass the base, keep the class local" and not "everything lives in the shared module". The
shared module holds what the *contract* mandates. `ConnectionClosed` is the case that fixes that
boundary: every adapter raises it, including yours, but it stays shared because the `SocketAdapter`
protocol requires it of every adapter. Ownership decides where an error lives, not who raises it.

## See also

- [`api/errors`](/api/errors) — every class above, `ResetCode`, `default_error_serializer`
- [`api/stream`](/api/stream) — `send`, `end`, `reply`, `cancel`, `reset`, `result`, `closed`
- [`api/peer`](/api/peer) — `open`, `request`, `notify`, `ping`, `close`, `on_close`, `on_reconnect`
- [`api/connect`](/api/connect) and [`api/accept`](/api/accept) — the `error_serializer` parameter
- [`api/transports`](/api/transports) — `SocketAdapter`, the seam an adapter of your own implements
- [`api/types`](/api/types) — `ErrorSerializer`, `CloseReason`
