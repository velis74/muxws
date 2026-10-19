# Streams & cancellation

A stream is a state machine with two independent halves: whether this peer may still send, and
whether it may still receive. Each half closes separately.

## The five states

Each peer tracks the state of each stream separately. The states at the two ends are symmetric: a
stream that is `half_closed_local` on one peer is `half_closed_remote` on the other.

| State | Entered by | This peer may still |
|---|---|---|
| `idle` | an id has been allocated and nothing sent yet | — |
| `open` | `open` sent or received without `end` | send and receive |
| `half_closed_local` | this peer sent `end: true` | receive only |
| `half_closed_remote` | this peer received `end: true` | send only |
| `closed` | both ends sent `end`, **or** either end sent `reset`, **or** the connection died | nothing |

`idle` is not externally observable. It exists only within the indivisible step that allocates an id
and queues the `open` frame on the writer; when `open()` returns a `Stream`, the stream is already
`open` or `half_closed_local`.

## What moves between them

| Event | From `open` | From `half_closed_local` | From `half_closed_remote` |
|---|---|---|---|
| this peer sends `end` | → `half_closed_local` | raises `StreamClosed` | → `closed` |
| this peer receives `end` | → `half_closed_remote` | → `closed` | resets the stream: data after end |
| this peer sends `data` | stays `open` | raises `StreamClosed` | stays `half_closed_remote` |
| this peer receives `data` | stays `open` | stays `half_closed_local` | resets the stream: data after end |
| either end sends `reset` | → `closed` | → `closed` | → `closed` |
| the socket dies | → `closed` | → `closed` | → `closed` |

From the table:

- **Sending after this peer's own `end` is a local error.** `send()`, `end()` and `reply()` raise at
  the call site and send no frame.
- **Receiving data after the remote's `end` is a stream-level protocol error.** That stream is reset;
  the connection and the other streams on it are not affected.
- **Socket death closes every live stream** and fails each one with `ConnectionLost` *before*
  `on_close` fires. Nothing is buffered for a later socket. See [Errors](/guide/errors).

When a stream is `closed`, the peer removes it. No state is kept per closed stream, so the memory use
of a connection does not grow with the number of streams it has served.

### Watching for the close

```python
# fragment
await stream.closed.wait()      # asyncio.Event
```

```typescript
// fragment
await stream.closed;            // Promise<void>
```

In TypeScript, `closed` **resolves and never rejects**, on every close path including a reset; the
reset is reported through the awaits and the iterator instead. `stream.signal`, an `AbortSignal`, is
aborted at the same time.

## `end` is a flag, never a frame

There is no `end` frame type and no trailers frame type. `end` is a boolean on the last `data` frame,
or on the `open` frame itself, so a unary request is a single frame in each direction.

```python
# fragment
await stream.send({"row": 1})              # data
await stream.send({"row": 2}, end=True)    # data, end: true - the last one
```

```typescript
// fragment
await stream.send({ row: 1 });
await stream.send({ row: 2 }, { end: true });
```

`end()` sets the same flag as a separate call, and may carry a final payload:

```python
# fragment
await stream.end()                  # end with no payload at all
await stream.end({"total": 42})     # end carrying one last payload
await stream.reply({"total": 42})   # send + end, for a unary handler
```

```typescript
// fragment
await stream.end();
await stream.end({ payload: { total: 42 } });
await stream.reply({ total: 42 });
```

A handler that returns without ending its stream ends it implicitly.

## Leading headers are carried on each side's first frame

`headers` on `open` carry the opener's metadata for the stream, readable before the body. The
answering peer can send headers once, on the **first frame it sends**. That frame may carry no
payload, so the answering peer can send its headers before it has produced any data.

```python
# fragment
async def handler(payload, stream):
    await stream.send_headers({"content-type": "text/csv", "rows-estimated": 40_000})
    async for row in rows():
        await stream.send(row)
    await stream.end()
```

```typescript
// fragment
acceptor.onStream(async (payload, stream) => {
  await stream.sendHeaders({ 'content-type': 'text/csv', 'rows-estimated': 40_000 });
  for await (const row of rows()) await stream.send(row);
  await stream.end();
});
```

The opener reads them from `reply_headers` / `replyHeaders`, and can wait for their arrival:

```python
# fragment
stream = peer.open({"q": "export"}, end=True)
await stream.reply_headers_arrived.wait()
print(stream.reply_headers)            # {"content-type": "text/csv", ...} - before the first row
```

```typescript
// fragment
const stream = peer.open({ q: 'export' }, { end: true });
await stream.replyHeadersArrived;
console.log(stream.replyHeaders);      // before the first row
```

If the headers are not needed before the first payload, send them with it, as one frame:
`send(payload, headers=...)`, `reply(payload, headers=...)`.

`stream.headers` always holds the `open`'s headers and `stream.reply_headers` always the answer's, with
the same values at both ends of the stream. Each peer sends **one** set per stream, on its first frame;
the first frame uses up the opportunity whether or not it carries headers. A later attempt raises
locally, and a peer that receives later headers on the wire resets that stream. muxws has no status
field, and `headers` is not used as one: a failed exchange is a `reset` with a code. muxws does not
read either set.

## Header interceptors apply to every stream a peer opens or answers

Metadata for every stream, such as a connection identifier or a trace id from the current context, is
added once per peer instead of at every `open()` call. An **open interceptor** runs synchronously
inside `open()`, `notify()` and `request()`, before the stream id is allocated, and modifies a copy of
the caller's headers:

```python
# fragment
def add_context(headers, payload):
    headers["connection"] = connection_id
    trace = current_trace.get()
    if trace is not None:
        headers.setdefault("trace", trace)

unregister = peer.intercept_open(add_context)
```

```typescript
// fragment
const unregister = peer.interceptOpen((headers, payload) => {
  headers.connection = connectionId;
  if (currentTrace !== null && headers.trace === undefined) headers.trace = currentTrace;
});
```

A **reply headers interceptor** does the same for the answering side: it runs on the first frame
this peer sends on a stream the remote opened, whichever call sends it (`send_headers()`, `send()`,
`end()`, `reply()`, or the implicit end of a handler that returns), and modifies a copy of that call's
headers:

```python
# fragment
def add_server_time(headers, stream):
    headers["server-time"] = time.time()

peer.intercept_reply_headers(add_server_time)
```

```typescript
// fragment
peer.interceptReplyHeaders((headers, stream) => {
  headers['server-time'] = Date.now() / 1000;
});
```

An **incoming interceptor** receives the leading headers the remote sends, once per stream: the
`open`'s headers on a stream the remote opened (before the handler runs), and the answer's headers on a
stream this peer opened (before the first payload is delivered). `stream.local` distinguishes the two
cases. The headers are read-only:

```python
# fragment
def note(headers, stream):
    if stream.local and "server-time" in headers:
        record_clock_skew(headers["server-time"])

peer.intercept_incoming_headers(note)
```

```typescript
// fragment
peer.interceptIncomingHeaders((headers, stream) => {
  if (stream.local && 'server-time' in headers) recordClockSkew(headers['server-time']);
});
```

Every registration returns a function that removes it. Interceptors of each kind run in registration
order. They belong to the `Peer`: a dialer keeps them across reconnects, while on the acceptor a
reconnect creates a new `Peer`. `connect()`, `accept()` and `serve()` take all three kinds as arguments
(`open_interceptors`, `reply_headers_interceptors`, `incoming_headers_interceptors`;
`openInterceptors`, `replyHeadersInterceptors`, `incomingHeadersInterceptors` in TypeScript) and
register them before the peer sends or reads a frame. These arguments are the only way to register
interceptors on the peer `serve()` creates, and the only way for a dialer's incoming interceptors to
see a stream the acceptor opens in response to the hello.

An open or reply headers interceptor that raises fails the sending call before any frame is sent; a
reply headers interceptor that raises during the implicit end of a handler fails the handler, and the
stream is reset with `APPLICATION_ERROR`. An incoming interceptor that raises is logged, and the
stream and the connection continue. Interceptors are synchronous; an interceptor that returns an
awaitable fails the sending call (outgoing) or is logged (incoming).

The reconnect helper's hello does not pass through interceptors, because it is sent with identical
bytes on every connection. Interceptors are application code; muxws itself reads no header. The
reference entries are
[`intercept_open`](/api/peer#peer-intercept-open-python),
[`intercept_reply_headers`](/api/peer#peer-intercept-reply-headers-python) and
[`intercept_incoming_headers`](/api/peer#peer-intercept-incoming-headers-python).

## Trailers are carried on the `end` frame

Trailers are metadata known only after the body is complete, such as a checksum, a row count or a
server-side duration. They are sent on the same frame as `end`, so the remote does not have to
correlate a separate message with the stream.

```python
# fragment
async for row in stream:
    write(row)
# available only after iteration completes:
print(stream.trailers)              # {"checksum": "deadbeef"} or None
```

```typescript
// fragment
for await (const row of stream) {
  write(row);
}
console.log(stream.trailers);       // { checksum: 'deadbeef' } or null
```

Sending them:

```python
# fragment
await stream.end({"row": 99}, trailers={"checksum": "deadbeef"})
await stream.reply({"total": 42}, trailers={"checksum": "deadbeef"})
```

```typescript
// fragment
await stream.end({ payload: { row: 99 }, trailers: { checksum: 'deadbeef' } });
await stream.reply({ total: 42 }, { trailers: { checksum: 'deadbeef' } });
```

`stream.trailers` is `None` / `null` until the remote's `end` arrives with trailers. Read it after the
stream has ended.

When a payload is fragmented, `headers` are carried on the **first** fragment and `end` and `trailers`
on the **last**; neither is split. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

## `cancel()` closes locally, immediately

`stream.cancel()` sends `reset(CANCELLED)` and closes the stream on this side **immediately**. muxws
has no acknowledgement frame, so `cancel()` does not wait for a round trip.

```python
# fragment
stream = peer.open({"report": "sales"}, end=True)
await stream.cancel("user navigated away")
```

```typescript
// fragment
const stream = peer.open({ report: 'sales' }, { end: true });
await stream.cancel('user navigated away');
```

When `cancel()` returns:

- the stream is `closed` on this side, and the peer has removed it;
- frames of the stream that are queued and not yet sent are dropped, including the rest of a
  payload that is partly sent; the `reset` ends that fragment sequence on the receiver. An `open`
  not yet sent at all is sent before the `reset`;
- frames still in transit from the remote are discarded: a frame for a closed id at or below the
  high-water mark is ignored without an error;
- `send()`, `await` and iteration on the stream raise `StreamReset` with the `CANCELLED` code;
- a second `cancel()` or `reset()` is a no-op.

`cancel(reason)` is equivalent to `reset(ResetCode.CANCELLED, reason)`. `reset()` accepts any code the
generation defines except `CONNECTION_CLOSED`, which is generated locally when a socket dies and must
not be sent; passing it raises `ProtocolError` at the call site and sends nothing.

### What the remote handler observes

The remote peer stops the handler of the cancelled stream.

**Python**: the handler task is cancelled, so `asyncio.CancelledError` is raised at the handler's
current `await`:

```python
# fragment
@peer.on_stream
async def export(payload, stream):
    try:
        for row in rows:
            await stream.send({"row": row})
    except asyncio.CancelledError:
        release_the_database_cursor()
        raise                       # re-raise; do not suppress it
```

**TypeScript**: a running function cannot be interrupted, so the handler must check for
cancellation. `stream.signal` is aborted, and every later `await stream.send(...)` rejects with a
`StreamReset`:

```typescript
// fragment
peer.onStream(async (payload, stream) => {
  for (const row of rows) {
    if (stream.signal.aborted) {
      releaseTheDatabaseCursor();
      return;
    }
    await stream.send({ row });
  }
});
```

`stream.signal` is a standard `AbortSignal` and can be passed to any API that accepts one, such as
`fetch`, a timer or another `AbortController`.

A handler that ignores the signal does not violate the protocol: its sends fail, its stream is
already closed, and no frame is sent. The resources it holds are not released until it returns.

## Local cancellation propagates outward

When the code waiting on a stream is cancelled, muxws sends `reset(CANCELLED)` for the stream and
then propagates the cancellation. The cancellation is not suppressed, so the cancelled task ends.

**Python.** Cancelling a task that is blocked in `await stream`, `await stream.result(...)` or an
`async for` sends `reset(CANCELLED)` and re-raises `CancelledError`:

```python
# fragment
task = asyncio.create_task(consume(peer.open({"report": "sales"}, end=True)))
task.cancel()       # the remote is told to stop; CancelledError still propagates out of `task`
```

Without the reset, the remote would continue producing data for the cancelled consumer until the
connection closed.

Python does **not** detect a `break` out of an `async for`. Leaving the loop early does not close the
async generator at that point, so no reset is sent and the remote continues producing. A consumer that
stops early cancels the stream explicitly:

```python
# fragment
async for chunk in stream:
    if chunk["enough"]:
        break
await stream.cancel("that is enough")   # required in Python; TypeScript does it for you
```

**TypeScript.** JavaScript cannot detect an abandoned promise, so dropping a promise sends no reset.
muxws sends `reset(CANCELLED)` in the two cases JavaScript can detect:

```typescript
// fragment
// 1. A `for await` that breaks, returns or throws finalises the generator, which resets the stream.
for await (const chunk of peer.open({ report: 'sales' }, { end: true })) {
  if (chunk.enough === true) break;       // sends reset(CANCELLED)
}

// 2. An AbortSignal handed to result().
const controller = new AbortController();
const stream = peer.open({ report: 'sales' }, { end: true });
setTimeout(() => controller.abort(), 100); // 100 milliseconds
// Aborting sends reset(CANCELLED) and then rejects this await.
const answer = await stream.result({ signal: controller.signal, timeoutMs: 30_000 });
```

A deadline uses a different code from a cancellation: when `timeout=` in seconds or `timeoutMs` in
milliseconds expires, the reset carries `TIMEOUT` and the call raises `StreamTimeout`. The remote
receives `TIMEOUT`, not `CANCELLED`; the two codes have different required reactions. See
[Errors](/guide/errors).

## See also

[`Stream.send`, `.end`, `.reply`, `.cancel`, `.reset`, `.trailers`, `.closed`, `.signal`](/api/stream)
&middot; [`Peer.open`, `Peer.on_stream`](/api/peer) &middot;
[`ResetCode`, `StreamReset`, `StreamClosed`, `StreamTimeout`, `ConnectionLost`](/api/errors)
