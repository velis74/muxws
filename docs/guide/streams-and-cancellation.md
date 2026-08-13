# Streams & cancellation

A stream is a small state machine with two independent halves: what this peer may still send, and
what it may still receive. Almost everything surprising about streams follows from the halves being
independent.

## The five states

Each peer tracks the state of each stream for itself. The two ends are mirror images: a stream that
is `half_closed_local` here is `half_closed_remote` there.

| State | Entered by | This peer may still |
|---|---|---|
| `idle` | an id has been allocated and nothing sent yet | — |
| `open` | `open` sent or received without `end` | send and receive |
| `half_closed_local` | this peer sent `end: true` | receive only |
| `half_closed_remote` | this peer received `end: true` | send only |
| `closed` | both ends sent `end`, **or** either end sent `reset`, **or** the connection died | nothing |

`idle` is not externally observable. It exists only inside the indivisible step that allocates an id
and puts the `open` frame on the writer, and by the time `open()` hands you a `Stream` it is already
`open` or `half_closed_local`. You will never see it.

## What moves between them

| Event | From `open` | From `half_closed_local` | From `half_closed_remote` |
|---|---|---|---|
| this peer sends `end` | → `half_closed_local` | raises `StreamClosed` | → `closed` |
| this peer receives `end` | → `half_closed_remote` | → `closed` | resets the stream: data after end |
| this peer sends `data` | stays `open` | raises `StreamClosed` | stays `half_closed_remote` |
| this peer receives `data` | stays `open` | stays `half_closed_local` | resets the stream: data after end |
| either end sends `reset` | → `closed` | → `closed` | → `closed` |
| the socket dies | → `closed` | → `closed` | → `closed` |

Three things to read out of that table:

- **Sending after your own `end` is a local error, not a wire event.** `send()`, `end()` and
  `reply()` raise at the call site and put nothing on the wire.
- **Receiving after the remote's `end` is a stream-level protocol error.** That stream is reset; the
  connection and every other stream on it are untouched.
- **Socket death closes every live stream** and fails each one with `ConnectionLost` *before*
  `on_close` fires. Nothing is buffered for a later socket. See [Errors](/guide/errors).

Once a stream is `closed` the peer forgets it entirely. Nothing is retained per closed stream, which
is why a long-lived connection that has served a million streams costs the same as a fresh one.

### Watching for the close

```python
# fragment
await stream.closed.wait()      # asyncio.Event
```

```typescript
// fragment
await stream.closed;            // Promise<void>
```

In TypeScript `closed` **resolves and never rejects**, on every close path including a reset - a
stream that closed by being reset still closed, and the reset reaches the awaits and the iterator
instead. `stream.signal`, an `AbortSignal`, is aborted at the same instant.

## `end` is a flag, never a frame

There is no `end` frame type and no trailers frame type. Ending is a boolean on the last `data`
frame - or on the `open` frame itself, which is what makes a unary request a single frame in each
direction.

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

`end()` is the same thing spelled as its own call, and it may carry a final payload:

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

A handler that returns without having ended its stream ends it implicitly, so the common case needs
no ceremony.

## Leading headers ride each side's first frame

`headers` on `open` is how the opener says what a stream is about before the body is read. The peer
answering has the same one chance, on the **first frame it sends** - which may carry no payload at
all, so it can announce what is coming before it has produced any of it.

```python
# fragment
async def handler(payload, stream):
    await stream.send_headers({"content-type": "text/csv", "rows-estimated": 40_000})
    async for row in rows():           # minutes later, perhaps
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

The opener reads them off `reply_headers` / `replyHeaders`, and waits for them where waiting is the
point:

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

If there is nothing to announce early, they can ride the first payload instead -
`send(payload, headers=...)`, `reply(payload, headers=...)` - and that is one frame rather than two.

Two attributes, not one: `stream.headers` is always the `open`'s and `stream.reply_headers` always
the answer's, and both read the same from either end of the stream. Each peer gets **one** set per
stream, spent by its first frame whether or not that frame carried any; a later one raises locally
and, if a peer puts one on the wire anyway, resets that stream. There is no status field here and
`headers` is not one - a failed exchange is a `reset` with a code. muxws never reads either set.

## Trailers ride the `end` frame

Trailers are metadata you only know once the body is finished - a checksum, a row count, a
server-side duration. They travel on the same frame as `end`, which is why they exist at all: a
separate trailing message would be a second frame the remote has to correlate.

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

`stream.trailers` is `None` / `null` until the remote's `end` arrives carrying some. Read it after
the stream has ended, not during.

When a payload is large enough to be fragmented, `headers` ride the **first** fragment and `end` and
`trailers` ride the **last**; neither is ever itself split. See
[Sizes & fragmentation](/guide/sizes-and-fragmentation).

## `cancel()` closes locally, immediately

`stream.cancel()` sends `reset(CANCELLED)` and closes the stream on this side **at once**, without
waiting for any acknowledgement. There is no acknowledgement to wait for: muxws has no ack frame,
and a cancellation that blocked on a round trip would be useless in the case that motivates it - a
user who navigated away from a page while a report was rendering.

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

The moment `cancel()` returns:

- the stream is `closed` here, and the peer has forgotten it;
- payloads still in flight from the remote are discarded - they name a stream that is no longer
  live, and a frame for a closed id at or below the high-water mark is silently ignored;
- `send()` on it raises, `await` on it raises, and iteration over it raises - all with the same
  `StreamReset` carrying the `CANCELLED` code;
- a second `cancel()` or `reset()` is a no-op.

`cancel(reason)` is exactly `reset(ResetCode.CANCELLED, reason)`. `reset()` takes any code the
generation defines, with one exception: `CONNECTION_CLOSED` is synthesised locally when a socket
dies and must never go on the wire, so passing it raises `ProtocolError` at the call site rather
than sending anything.

### What the remote handler observes

The remote is not merely told; its handler is actively stopped.

**Python** - the handler task is cancelled, so `asyncio.CancelledError` is raised at whatever the
handler is currently awaiting:

```python
# fragment
@peer.on_stream
async def export(payload, stream):
    try:
        for row in rows:
            await stream.send({"row": row})
    except asyncio.CancelledError:
        release_the_database_cursor()
        raise                       # do not swallow it
```

**TypeScript** - there is no way to interrupt a running function, so the handler is told and
cooperates. `stream.signal` is aborted, and any later `await stream.send(...)` rejects with a
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

`stream.signal` is an ordinary `AbortSignal`, so it composes with anything else that takes one -
`fetch`, a timer, another `AbortController`.

A handler that ignores the signal entirely is not a correctness problem for the protocol: its sends
fail, its stream is already gone, and nothing reaches the wire. It is only a problem for whatever
resource it is holding.

## Local cancellation propagates outward

The mirror case: not "I cancelled the stream" but "the thing waiting on the stream was cancelled".
muxws turns that into a `reset(CANCELLED)` and then lets the cancellation keep going. It is never
swallowed - a caller who cancelled a task expects the task to end, not to return normally.

**Python.** Cancelling a task that is blocked in `await stream`, `await stream.result(...)` or an
`async for` sends `reset(CANCELLED)` and re-raises `CancelledError`:

```python
# fragment
task = asyncio.create_task(consume(peer.open({"report": "sales"}, end=True)))
task.cancel()       # the remote is told to stop; CancelledError still propagates out of `task`
```

Without this the remote would go on producing rows for a consumer that no longer exists, for as long
as the connection lived.

What Python does **not** observe is a `break` out of an `async for`. Leaving the loop early does not
close the async generator at that point, so nothing is reset and the remote keeps producing. A
consumer that stops early cancels the stream itself:

```python
# fragment
async for chunk in stream:
    if chunk["enough"]:
        break
await stream.cancel("that is enough")   # required in Python; TypeScript does it for you
```

**TypeScript.** JavaScript cannot observe an abandoned promise, so muxws does not pretend to: simply
dropping a promise resets nothing. It carries the same behaviour where the language *can* see it, in
two places:

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
const answer = await stream.result({ signal: controller.signal, timeoutMs: 30_000 }); // 30000 milliseconds
```

A deadline is not a cancellation and does not use the same code: when `timeout=` in seconds or
`timeoutMs` in milliseconds expires, the reset carries `TIMEOUT` and the call raises `StreamTimeout`.
The remote is told a deadline expired, not that the caller changed its mind - and the two deserve
different reactions. See [Errors](/guide/errors).

## See also

[`Stream.send`, `.end`, `.reply`, `.cancel`, `.reset`, `.trailers`, `.closed`, `.signal`](/api/stream)
&middot; [`Peer.open`, `Peer.on_stream`](/api/peer) &middot;
[`ResetCode`, `StreamReset`, `StreamClosed`, `StreamTimeout`, `ConnectionLost`](/api/errors)
