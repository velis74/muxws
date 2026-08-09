# Call shapes

Four shapes cover everything muxws does. They are not four mechanisms - they are one mechanism,
`open()`, consumed four ways. Which shape you are in is decided by two things: whether the opener
sends `end` immediately, and whether the responder sends one payload or several.

## The four shapes

| Shape | Wire sequence | Python | TypeScript |
|---|---|---|---|
| **Unary** | `open(end)` → `data(end)` | `await peer.request(payload)` | `await peer.request(payload)` |
| **Streaming response** | `open(end)` → `data` … `data(end)` | `async for item in peer.open(payload, end=True)` | `for await (const item of peer.open(payload, { end: true }))` |
| **Bidirectional** | `open` → `data` both ways → `data(end)` both ways | `stream = peer.open(payload)` | `const stream = peer.open(payload)` |
| **One-shot push** | `open(end)`, nothing back | `await peer.notify(payload)` | `await peer.notify(payload)` |

Every one of them works in both directions. An acceptor calls `peer.request(...)` on a client
exactly as a client calls it on a server.

### Unary

One value out, one value back. The opener sends `end` with the opening payload, so the remote knows
there is nothing more coming and can answer immediately.

```python
# fragment
answer = await peer.request({"action": "list"})
# with a deadline, in seconds:
answer = await peer.request({"action": "list"}, timeout=5.0)  # 5.0 seconds
```

```typescript
// fragment
const answer = await peer.request({ action: 'list' });
// with a deadline, in milliseconds:
const answered = await peer.request({ action: 'list' }, { timeoutMs: 5000 }); // 5000 milliseconds
```

On the handler side, `reply()` is `send()` plus `end()`, which is what a unary handler wants:

```python
# fragment
@peer.on_stream
async def handle(payload, stream):
    await stream.reply({"items": ["a", "b"]})
```

```typescript
// fragment
peer.onStream(async (payload, stream) => {
  await stream.reply({ items: ['a', 'b'] });
});
```

### Streaming response

One value out, many values back. Same opening frame as unary - the difference is entirely in how the
handler answers and how the caller consumes.

```python
# fragment
async for chunk in peer.open({"report": "sales"}, end=True):
    print(chunk)
```

```typescript
// fragment
for await (const chunk of peer.open({ report: 'sales' }, { end: true })) {
  console.log(chunk);
}
```

```python
# fragment
@peer.on_stream
async def handle(payload, stream):
    for row in rows:
        await stream.send({"row": row})
    await stream.end({"done": True})
```

### Bidirectional

Both ends send until both ends have sent `end`. The opener does **not** set `end` on the `open`,
because it intends to keep sending.

```python
# fragment
stream = peer.open({"session": "editor"})
await stream.send({"keystroke": "a"})
async for update in stream:
    print(update)
```

```typescript
// fragment
const stream = peer.open({ session: 'editor' });
await stream.send({ keystroke: 'a' });
for await (const update of stream) {
  console.log(update);
}
```

The stream closes when both directions have ended; until then either end may still send. See
[Streams & cancellation](/guide/streams-and-cancellation).

### One-shot push

Fire and forget. `notify()` is async, returns nothing, and produces no handle at all - there is no
`Stream` to await, on purpose, so there is nothing to accidentally leave pending.

```python
# fragment
await peer.notify({"event": "cache-invalidated"})
```

```typescript
// fragment
await peer.notify({ event: 'cache-invalidated' });
```

It is `open(payload, end=True)` with the result thrown away. The remote's handler still runs; you
just do not hear about it. If the remote resets that stream, nothing is raised at the call site,
because there is no call site left to raise at: the `muxws.frames` log is where it shows up. See
[Observability](/guide/observability).

## `open()` is synchronous

`peer.open()` returns a `Stream` immediately, in both languages. It is not a coroutine, it is not a
promise, and there is nothing to await:

```python
# fragment
stream = peer.open({"report": "sales"})   # no await
```

```typescript
// fragment
const stream = peer.open({ report: 'sales' }); // no await
```

The reason is ordering. Allocating the stream id and putting the `open` frame on the writer are one
indivisible step, with no suspension point between them, so the order ids are allocated in *is* the
order they reach the wire. If `open()` could suspend, two concurrent calls could interleave and put
a non-monotonic id sequence on the wire - which the remote is entitled to treat as a protocol error.

Two consequences:

- **`open()` never queues.** It raises synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` when the remote has sent `goaway` (or this peer has), and `ConnectionLost`
  while the peer is between sockets with nothing buffered for the next one. It does *not* raise for
  concurrency: the receiver's stream limit is the receiver's, and it surfaces asynchronously as
  `StreamRefused` on the pending await. See [Connection lifecycle](/guide/connection-lifecycle).
- **`open()` takes no deadline, in either language.** There is no `timeout=` in seconds on the
  Python call and no `timeoutMs` in milliseconds in `OpenOptions`. `open()` returns immediately, so
  there is nothing for a deadline on it to bound. Deadlines live on the calls that actually wait:
  `stream.result(timeout=)` in seconds
  and `result({ timeoutMs })` in milliseconds, `peer.request(timeout=)` in seconds and
  `request(..., { timeoutMs })` in milliseconds.

`open()` also takes zero mandatory arguments. With no payload it puts an explicit `"payload": null`
on the wire in both languages.

::: tip TypeScript only
A `Stream` implements `PromiseLike`, so `await` unwraps it. That means a `Stream` can never be the
resolution value of a promise: an `async function` that returns a stream silently hands its caller
the stream's **first payload** instead. Box it - `return { stream }` - if you need to pass one out
of an async function.
:::

## A `Stream` is awaitable *and* async-iterable

The same object supports both shapes:

- **Awaiting it** resolves with the remote's **first** payload.
- **Iterating it** yields **every** reassembled payload until the remote ends.

```python
# fragment
first = await peer.open({"report": "sales"}, end=True)          # first payload only

async for chunk in peer.open({"report": "sales"}, end=True):    # every payload
    print(chunk)
```

```typescript
// fragment
const first = await peer.open({ report: 'sales' }, { end: true });          // first payload only

for await (const chunk of peer.open({ report: 'sales' }, { end: true })) {  // every payload
  console.log(chunk);
}
```

**The first use claims the stream, and the other then raises `StreamAlreadyConsumed`.** Awaiting a
stream you are already iterating, iterating one you already awaited, or iterating the same stream
twice, all raise it - with an error naming both uses. There is one consumer per stream, because two
consumers would split the payloads between them and neither would see the whole answer.

```python
# fragment
stream = peer.open({"report": "sales"}, end=True)
first = await stream
async for chunk in stream:   # raises StreamAlreadyConsumed
    print(chunk)
```

Awaiting the *same* stream twice is fine and is not a second claim: the value is memoized, so the
second await returns the first await's value rather than the next payload. `stream.result(...)` is
that same memoized answer with a deadline wrapped around the wait - never a second source of the
value.

## `request()` polices a second payload; `await stream` does not

This is the one behavioural difference between the two unary-looking calls, and it is deliberate.

| | Remote sends one payload | Remote sends two |
|---|---|---|
| `await peer.request(...)` | returns it | resets the stream with a protocol error and raises `ProtocolError` |
| `await peer.open(..., end=True)` | returns it | returns the **first**; the second is simply never read |

`request()` is a unary call, and a unary call that quietly discarded extra values would hide a
handler bug rather than report it. `await stream` makes no such claim - it is documented as "the
first payload", and a caller who wanted them all would have iterated.

`request()` also raises `ProtocolError` if the stream ends without producing any payload at all.

Neither call has a default deadline. A stream lives until it ends, is reset, or the connection dies;
if you want a bound, pass one - `timeout=` in seconds, `timeoutMs` in milliseconds.

## See also

[`Peer.open`, `Peer.request`, `Peer.notify`](/api/peer) &middot;
[`Stream.result`, `Stream.send`, `Stream.reply`, `Stream.end`](/api/stream) &middot;
[`StreamAlreadyConsumed`, `ProtocolError`, `StreamRefused`](/api/errors)
