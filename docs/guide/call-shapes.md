# Call shapes

muxws supports four call shapes. All four use the same mechanism, `open()`, and differ in how the
stream is used. The shape is determined by two things: whether the opener sends `end` with the
opening payload, and whether the responder sends one payload or several.

## The four shapes

| Shape | Wire sequence | Python | TypeScript |
|---|---|---|---|
| **Unary** | `open(end)` → `data(end)` | `await peer.request(payload)` | `await peer.request(payload)` |
| **Streaming response** | `open(end)` → `data` … `data(end)` | `async for item in peer.open(payload, end=True)` | `for await (const item of peer.open(payload, { end: true }))` |
| **Bidirectional** | `open` → `data` both ways → `data(end)` both ways | `stream = peer.open(payload)` | `const stream = peer.open(payload)` |
| **One-shot push** | `open(end)`, nothing back | `await peer.notify(payload)` | `await peer.notify(payload)` |

Every shape works in both directions. An acceptor calls `peer.request(...)` on a client in the same
way a client calls it on a server.

### Unary

One value out, one value back. The opener sends `end` with the opening payload, so the remote knows
that no more payloads follow and can answer immediately.

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

On the handler side, `reply()` is `send()` followed by `end()`, which is what a unary handler needs:

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

One value out, many values back. The opening frame is the same as for unary; only the handler's
answer and the caller's consumption differ.

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
because it sends more payloads.

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

Fire and forget. `notify()` is async, returns `None` (`undefined` in TypeScript), and returns no
`Stream`, so the caller has no stream that could be left pending.

```python
# fragment
await peer.notify({"event": "cache-invalidated"})
```

```typescript
// fragment
await peer.notify({ event: 'cache-invalidated' });
```

It is `open(payload, end=True)` with the returned stream discarded. The remote's handler runs, but
the caller receives no result. If the remote resets the stream, nothing is raised to the caller; the
incoming `reset` frame appears in the `muxws.frames` log. See
[Observability](/guide/observability).

## `open()` is synchronous

`peer.open()` returns a `Stream` immediately, in both languages. It is neither a coroutine nor a
promise, and is not awaited:

```python
# fragment
stream = peer.open({"report": "sales"})   # no await
```

```typescript
// fragment
const stream = peer.open({ report: 'sales' }); // no await
```

Allocating the stream id and enqueueing the `open` frame on the writer happen in one step with no
suspension point, so ids reach the wire in the order they are allocated. If `open()` could suspend
between the two, two concurrent calls could put ids on the wire out of order, which the remote treats
as a protocol error.

This has two consequences:

- **`open()` never queues.** It raises synchronously at the call site in exactly two
  connection-state cases: `ConnectionGoingAway` when the remote has sent `goaway` (or this peer has),
  and `ConnectionLost` while a reconnecting peer has no socket (nothing is buffered for the next
  one). An [open interceptor](/api/peer#peer-intercept-open-python) that raises fails the call the
  same way. It does *not* raise when the receiver's concurrent-stream limit is reached: that limit is
  enforced by the receiver, and the rejection arrives asynchronously as `StreamRefused` on the pending
  await. See [Connection lifecycle](/guide/connection-lifecycle).
- **`open()` takes no deadline, in either language.** There is no `timeout=` in seconds on the
  Python call and no `timeoutMs` in milliseconds in `OpenOptions`, because `open()` does not wait.
  Deadlines are parameters of the calls that wait: `stream.result(timeout=)` in seconds and
  `result({ timeoutMs })` in milliseconds, `peer.request(timeout=)` in seconds and
  `request(..., { timeoutMs })` in milliseconds.

`open()` has no required arguments. With no payload it puts an explicit `"payload": null`
on the wire in both languages.

::: tip TypeScript only
A `Stream` implements `PromiseLike`, so `await` unwraps it. A `Stream` therefore cannot be the
resolution value of a promise: an `async function` that returns a stream resolves to the stream's
**first payload** instead. To return a stream from an async function, wrap it in an object:
`return { stream }`.
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

**The first use claims the stream; a use of the other kind then raises `StreamAlreadyConsumed`.**
Awaiting a stream that is being iterated, iterating a stream that was awaited, and iterating the same
stream twice all raise it, with a message naming both uses. A stream has one consumer, because two
consumers would each receive only part of the payloads.

```python
# fragment
stream = peer.open({"report": "sales"}, end=True)
first = await stream
async for chunk in stream:   # raises StreamAlreadyConsumed
    print(chunk)
```

Awaiting the *same* stream twice is allowed and is not a second claim: the value is memoized, so the
second await returns the same value as the first, not the next payload. `stream.result(...)` returns
the same memoized value, with a deadline on the wait.

## `request()` polices a second payload; `await stream` does not

This is the only behavioural difference between the two calls that return one value.

| | Remote sends one payload | Remote sends two |
|---|---|---|
| `await peer.request(...)` | returns it | resets the stream with a protocol error and raises `ProtocolError` |
| `await peer.open(..., end=True)` | returns it | returns the **first**; the second is simply never read |

`request()` is a unary call; discarding extra values silently would hide a handler bug, so it
reports them. `await stream` is defined as "the first payload"; to receive all payloads, iterate the
stream.

`request()` also raises `ProtocolError` if the stream ends without producing any payload at all.

Neither call has a default deadline. A stream stays open until it ends, is reset, or the connection
closes. To set a deadline, pass `timeout=` in seconds or `timeoutMs` in milliseconds.

## See also

[`Peer.open`, `Peer.request`, `Peer.notify`](/api/peer) &middot;
[`Stream.result`, `Stream.send`, `Stream.reply`, `Stream.end`](/api/stream) &middot;
[`StreamAlreadyConsumed`, `ProtocolError`, `StreamRefused`](/api/errors)
