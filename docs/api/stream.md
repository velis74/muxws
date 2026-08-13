---
outline: deep
---

# Stream

A `Stream` is one independently addressed, independently cancellable, bidirectional exchange on a
connection that is carrying many of them. `peer.open()` returns one synchronously; an `on_stream`
handler is handed one.

The two facts that shape the whole type:

- **A stream is both awaitable and async-iterable, and the first use claims it.** `await stream`
  gives the first payload; `async for` / `for await` gives every payload. Whichever you use first
  owns the stream, and the other then raises `StreamAlreadyConsumed`. A second `await` returns the
  *first* await's value, not the next payload - there is one memoized answer per stream, never a
  second read of the wire.
- **`closed` is not the same type in the two languages.** In Python it is an `asyncio.Event` you
  `await stream.closed.wait()`. In TypeScript it is a `Promise<void>` you `await stream.closed`, and
  it **resolves rather than rejecting** when the stream is reset - a stream that closed by being
  reset still closed. The failure reaches the awaits and the iterator, not `closed`.

Deadlines belong here rather than on `open()`: `stream.result(timeout=)` takes seconds as a float,
`stream.result({ timeoutMs })` takes milliseconds as an integer.

## `stream.id` (Python)

The stream id, unique for the life of one connection.

The dialer allocates odd ids, the acceptor even ones, and each side's ids strictly increase; that is
the whole of how two peers open streams at the same instant without colliding. Ids are **not** reused
after a stream closes, and the id space starts empty again after a reconnect.

### Signature

```python
self.id = stream_id
```

### Parameters

None — a plain instance attribute, set at construction.

### Return

`int` — odd on a dialer-opened stream, even on an acceptor-opened one.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    print(dialer.open("a").id, dialer.open("b").id, acceptor.open("c").id)


asyncio.run(main())
```

## `stream.id` (TypeScript)

### Signature

```ts
readonly id: number;
```

### Parameters

None — a readonly field, set at construction.

### Return

`number` — odd for a dialer-opened stream, even for an acceptor-opened one.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

console.log(dialer.open('a').id, dialer.open('b').id, acceptor.open('c').id);
```

## `stream.headers` (Python)

The `open` frame's headers: per-stream metadata muxws carries and never reads.

They are set on both sides - the opener sees what it passed to `open()`, the handler sees what
arrived. They are not a place for credentials: authentication belongs at the upgrade, where the
transport can refuse a connection rather than an individual stream.

### Signature

```python
self.headers: dict[str, Any] = headers or {}
```

### Parameters

None — a plain instance attribute.

### Return

`dict[str, Any]` — `{}` when the `open` frame carried none.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply({"handler saw": stream.headers})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("x", headers={"trace": "abc"}, end=True)
    print("opener sees:", stream.headers)
    print(await stream)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.headers` (TypeScript)

### Signature

```ts
readonly headers: Record<string, unknown>;
```

### Parameters

None — a readonly field.

### Return

`Record<string, unknown>` — `{}` when the `open` frame carried none. (The source comment on this
field says it is empty for locally opened streams; it is not - the opener sees what it passed.)

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reply({ handlerSaw: stream.headers });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('x', { headers: { trace: 'abc' }, end: true });
console.log('opener sees:', stream.headers);
console.log(await stream);
await dialer.close({ drainMs: 100 });
```

## `stream.reply_headers` (Python)

The **answering** side's leading headers: the other half of `headers`, and what the peer that did not
open the stream announces before or with its first frame (WSM-FRM-016).

Read the same way from either end. The peer that opened the stream sees what the answer announced;
the peer answering sees what it announced itself. `{}` until there are any - use
[`stream.reply_headers_arrived`](#stream-reply-headers-arrived-python) to know that the value you are
reading is the final one.

muxws never reads them (WSM-AUT-002). There is no status field on the wire and this is not one: a
failed exchange is a `reset` with a code (§2.4), not a header a receiver has to remember to check.

### Signature

```python
self.reply_headers: dict[str, Any] = {}
```

### Parameters

None — a plain instance attribute.

### Return

`dict[str, Any]` — `{}` when the answering side announced none.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply({"rows": 2}, headers={"content-type": "text/csv"})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("export", end=True)
    print(await stream)
    await stream.reply_headers_arrived.wait()
    print("the answer announced:", stream.reply_headers)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.replyHeaders` (TypeScript)

### Signature

```ts
replyHeaders: Record<string, unknown>;
```

### Parameters

None — a field, mutable because on the opener's side it is filled in when the answer's first frame
arrives.

### Return

`Record<string, unknown>` — `{}` when the answering side announced none.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reply({ rows: 2 }, { headers: { 'content-type': 'text/csv' } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('export', { end: true });
console.log(await stream);
await stream.replyHeadersArrived;
console.log('the answer announced:', stream.replyHeaders);
await dialer.close({ drainMs: 100 });
```

## `stream.reply_headers_arrived` (Python)

Set at the instant `reply_headers` can no longer change.

That instant is the answering side's first frame on the stream - carrying headers or not, since a
first frame without them means none are coming - or the close of a stream that was never answered.
Both, because the second is the one the remote controls: a stream it resets before answering, or a
socket that dies, would otherwise leave a wait here pending forever (WSM-INV-011).

Waiting on it is what makes announcing worth anything: a consumer that learns the content type only
after the body has started has learned it too late.

### Signature

```python
self.reply_headers_arrived = asyncio.Event()
```

### Parameters

None — a plain instance attribute.

### Return

`asyncio.Event` — `await stream.reply_headers_arrived.wait()`, or `.is_set()` for a look that does
not wait.

### Raises

Raises: nothing. It is set on every path, including the ones on which no headers ever came.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.send_headers({"content-type": "text/csv"})
        await asyncio.sleep(0.02)  # the rows take a while to produce
        await stream.end({"row": 1})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("export", end=True)
    await stream.reply_headers_arrived.wait()
    print("before the body:", stream.reply_headers)
    print(await stream)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.replyHeadersArrived` (TypeScript)

### Signature

```ts
readonly replyHeadersArrived: Promise<void>;
```

### Parameters

None — a readonly field.

### Return

`Promise<void>` — resolving when `replyHeaders` is final. Like `closed`, it resolves and never
rejects.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.sendHeaders({ 'content-type': 'text/csv' });
  await new Promise<void>((resolve) => setTimeout(resolve, 20));
  await stream.end({ payload: { row: 1 } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('export', { end: true });
await stream.replyHeadersArrived;
console.log('before the body:', stream.replyHeaders);
console.log(await stream);
await dialer.close({ drainMs: 100 });
```

## `stream.payload` (Python)

The opening payload, already reassembled.

On the receiving side it is what the `open` frame carried, and it is also the first argument the
`on_stream` handler is given - `handler(payload, stream)` and `stream.payload` are the same value. On
the opening side it is what you passed to `open()`.

It is the *opening* payload only. Everything sent afterwards arrives through the await or the
iterator, never here.

### Signature

```python
self.payload: Any = payload
```

### Parameters

None — a plain instance attribute. The peer fills it in when the last fragment of a fragmented `open`
lands, which is also when the handler is finally started.

### Return

`Any` — the decoded payload, or `None` when the `open` frame carried none.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply({"same object": payload == stream.payload, "payload": stream.payload})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open({"report": "daily"}, end=True)
    print("opener sees:", stream.payload)
    print(await stream)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.payload` (TypeScript)

### Signature

```ts
payload: unknown;
```

### Parameters

None — a mutable public field; the peer writes it when a fragmented `open` completes.

### Return

`unknown` — the decoded opening payload, `null` when the `open` frame carried none.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  await stream.reply({ sameValue: payload === stream.payload, payload: stream.payload });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open({ report: 'daily' }, { end: true });
console.log('opener sees:', stream.payload);
console.log(await stream);
await dialer.close({ drainMs: 100 });
```

## `stream.trailers` (Python)

Metadata that arrives **with** the end of the stream, rather than before it.

Trailers ride the `end` flag on the last frame; there is no trailer frame. They are the place for
anything that is only known once the response is complete - a row count, a checksum, a page token.
They are populated just before the stream closes, so read them after the iteration finishes, not
during it.

### Signature

```python
self.trailers: dict[str, Any] | None = None
```

### Parameters

None — a plain instance attribute.

### Return

`dict[str, Any] | None` — `None` unless the remote's final frame carried trailers.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.send({"row": 1})
        await stream.end({"row": 2}, trailers={"rows": 2, "complete": True})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("query", end=True)
    print("before:", stream.trailers)
    async for row in stream:
        print(row)
    print("after:", stream.trailers)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.trailers` (TypeScript)

### Signature

```ts
trailers: Record<string, unknown> | null = null;
```

### Parameters

None — a mutable public field written by the peer when the remote's final frame carries trailers.

### Return

`Record<string, unknown> | null` — `null` unless trailers arrived.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.send({ row: 1 });
  await stream.end({ payload: { row: 2 }, trailers: { rows: 2, complete: true } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('query', { end: true });
console.log('before:', stream.trailers);
for await (const row of stream) console.log(row);
console.log('after:', stream.trailers);
await dialer.close({ drainMs: 100 });
```

## `stream.closed` (Python)

An `asyncio.Event` set on **every** close path: a normal end, a local or remote reset, and socket
death.

It is an `Event` and not a future, so it carries no outcome: it says *that* the stream closed, never
*why*. The why reaches whoever was awaiting or iterating the stream, and `stream.state` is `CLOSED`
either way.

### Signature

```python
self.closed = asyncio.Event()
```

### Parameters

None — a plain instance attribute holding an `asyncio.Event`.

### Return

`asyncio.Event` — `await stream.closed.wait()` to block until the stream ends; `stream.closed.is_set()`
to test without waiting.

### Raises

Raises: nothing. Waiting on it never raises, whatever ended the stream.

### Example

```python
import asyncio

from muxws import Peer, ResetCode, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reset(ResetCode.APPLICATION_ERROR, "no thanks")

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("x", end=True)
    print("set before:", stream.closed.is_set())
    await asyncio.wait_for(stream.closed.wait(), 5.0)
    print("a reset still closes the stream:", stream.closed.is_set(), stream.state.value)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.closed` (TypeScript)

A `Promise<void>` that **resolves** on every close path - a normal end, a reset, socket death - and
**never rejects**.

That is deliberate and it is the difference a reader must not guess wrong: a stream that closed by
being reset still closed, so `await stream.closed` returns rather than throwing. The reset itself
reaches the await on the stream and the `for await` loop, and `stream.signal` is aborted with it.

### Signature

```ts
readonly closed: Promise<void>;
```

### Parameters

None — a readonly field.

### Return

`Promise<void>` — resolves once, with no value, whatever ended the stream.

### Raises

Raises: nothing. It has no rejection path at all.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode, type Stream, StreamState } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reset(ResetCode.APPLICATION_ERROR, 'no thanks');
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('x', { end: true });
await stream.closed;
console.log('a reset resolves closed rather than rejecting it:', stream.state === StreamState.CLOSED);
await dialer.close({ drainMs: 100 });
```

## `stream.signal` (TypeScript)

An `AbortSignal` aborted at the same instant `closed` resolves, on every close path.

This is what a TypeScript handler observes in place of Python's `asyncio.CancelledError`: there is no
way to interrupt a running function in JavaScript, so the handler is *told* and cooperates. A handler
doing real work should check `stream.signal.aborted` between steps, or pass the signal to whatever it
calls.

There is no Python twin: Python cancels the handler's task instead.

### Signature

```ts
get signal(): AbortSignal;
```

### Parameters

None — a getter over the stream's internal `AbortController`.

### Return

`AbortSignal` — `aborted` becomes true when the stream closes. When the close was a reset, the
signal's `reason` is the `StreamReset` that caused it.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

const observed = new Promise<string>((resolve) => {
  acceptor.onStream(async (_payload: unknown, stream: Stream) => {
    stream.signal.addEventListener('abort', () => resolve(String((stream.signal.reason as Error).message)));
    await new Promise<void>((settle) => setTimeout(settle, 1000));
  });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('long job');
await new Promise<void>((resolve) => setTimeout(resolve, 50));
await stream.cancel('the user navigated away');
console.log('the handler observed:', await observed);
await dialer.close({ drainMs: 100 });
```

## `stream.state` (Python)

Which of the five states this stream is in, from this peer's point of view.

`IDLE` before anything is sent, `OPEN` while both sides may send, `HALF_CLOSED_LOCAL` once this side
has sent `end`, `HALF_CLOSED_REMOTE` once the remote has, and `CLOSED` when both have or when
something reset it. A closed stream is dropped from `peer.streams` immediately.

### Signature

```python
self.state = StreamState.IDLE
```

### Parameters

None — a plain instance attribute, driven by the peer and by this stream's own sends.

### Return

`StreamState` — a `str`-valued enum, so `stream.state.value` is `"open"`, `"closed"` and so on.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream, StreamState
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply("done")

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    open_stream = dialer.open("still talking")
    print(open_stream.state is StreamState.OPEN, open_stream.state.value)

    unary = dialer.open("ask", end=True)
    print(unary.state is StreamState.HALF_CLOSED_LOCAL, unary.state.value)
    print(await unary, unary.state is StreamState.CLOSED)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.state` (TypeScript)

### Signature

```ts
state: StreamState = StreamState.IDLE;
```

### Parameters

None — a public field the peer reads and writes.

### Return

`StreamState` — a string enum: `'idle'`, `'open'`, `'half_closed_local'`, `'half_closed_remote'`,
`'closed'`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream, StreamState } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => stream.reply('done'));

void dialer.serve();
void acceptor.serve();

const openStream = dialer.open('still talking');
console.log(openStream.state === StreamState.OPEN, openStream.state);

const unary = dialer.open('ask', { end: true });
console.log(unary.state === StreamState.HALF_CLOSED_LOCAL, unary.state);
console.log(await unary, unary.state === StreamState.CLOSED);

await dialer.close({ drainMs: 100 });
```

## `stream.local` (Python)

Whether **this** peer opened the stream.

It is what the concurrency limit counts: `max_concurrent_streams` bounds the streams the *remote*
opened here, because the limit exists to bound work the other side can impose. A peer's own opens do
not consume it.

### Signature

```python
self.local = local
```

### Parameters

None — a plain instance attribute, set at construction.

### Return

`bool` — true on the side that called `open()`, false on the side whose handler was invoked.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reply({"local on the receiving side": stream.local})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("x", end=True)
    print("local on the opening side:", stream.local)
    print(await stream)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.local` (TypeScript)

### Signature

```ts
local: boolean;
```

### Parameters

None — a public field set at construction.

### Return

`boolean` — true on the side that called `open()`.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reply({ localOnTheReceivingSide: stream.local });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('x', { end: true });
console.log('local on the opening side:', stream.local);
console.log(await stream);
await dialer.close({ drainMs: 100 });
```

## `stream.handler_task` (Python)

The `asyncio.Task` running the `on_stream` handler for this stream, on the receiving side.

It is held here so that an incoming `reset(CANCELLED)` can cancel it: cancelling the stream is
cancelling the work, which is the whole point of per-stream cancellation. The handler observes an
`asyncio.CancelledError` at its next await. The TypeScript port holds an `AbortController` here
instead and the handler observes `stream.signal`.

An application rarely touches it; it is documented because it is public and because it is what makes
cancellation real rather than advisory.

### Signature

```python
self.handler_task: asyncio.Task[None] | None = None
```

### Parameters

None — a plain instance attribute.

### Return

`asyncio.Task[None] | None` — the task on a stream the remote opened here, `None` on a stream this
peer opened and on one that was refused before any handler ran.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    cancelled: asyncio.Future[bool] = asyncio.get_running_loop().create_future()

    async def handler(_payload: object, stream: Stream) -> None:
        print("the handler runs in its own task:", stream.handler_task is asyncio.current_task())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set_result(True)
            raise

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("long job")
    await asyncio.sleep(0.05)
    await stream.cancel("the user navigated away")
    print("the remote handler was cancelled:", await asyncio.wait_for(cancelled, 5.0))

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.send()` (Python)

Send one payload on a stream that is still open, optionally ending it in the same frame.

### Signature

```python
async def send(self, payload: Any, *, end: bool = False, headers: dict[str, Any] | None = None) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | required | The value to send. It is encoded by the codec and fragmented automatically if the encoded form exceeds the 64 KiB frame cap; a stream holds at most one unsent fragment, and the writer round-robins, so a large payload cannot starve another stream. |
| `end` | `bool` | `False` | Half-close this side in the same frame. `end` is a flag, never a frame of its own. |
| `headers` | `dict[str, Any] \| None` | `None` | This side's leading headers, if this is the **first** frame it sends on the stream (WSM-FRM-016). Later ones raise; see [`stream.send_headers()`](#stream-send-headers-python). |

### Return

`None`. The frame is enqueued for the writer; awaiting this call does not mean the bytes have reached
the socket.

### Raises

Three different outcomes with three different classes, which is the point:

- `StreamClosed` — the stream ended **normally**, or this side already sent `end`. An expected race
  when a close and a last send cross, not a bug.
- `StreamReset` (or `RemoteError`, `StreamRefused`, `StreamTimeout`) — the stream was reset. A fresh
  instance of the stored failure is raised each time.
- `ConnectionLost` — the socket died. It is itself a `StreamReset`, because the stream really did end
  early; `ConnectionClosed` is not, because it describes the connection rather than a stream.
- `ProtocolError` — `headers` was passed on a frame that is not this side's first on the stream.

### Example

```python
import asyncio

from muxws import Peer, Stream, StreamClosed
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        async for item in stream:
            print("acceptor received:", item)

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("upload")
    await stream.send({"part": 1})
    await stream.send({"part": 2}, end=True)
    await asyncio.sleep(0.05)

    try:
        await stream.send({"part": 3})
    except StreamClosed as exc:
        print("sending after end:", exc)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.send()` (TypeScript)

### Signature

```ts
async send(payload: unknown, options: SendOptions = {}): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | required | The value to send, fragmented automatically above the 64 KiB frame cap. |
| `options` | `SendOptions` | `{}` | `end` and `headers`. See [`SendOptions`](#sendoptions-typescript). |

### Return

`Promise<void>` — resolving once the frame is enqueued, not once it is on the wire.

### Raises

Rejects with `StreamClosed`, a `StreamReset` subclass, or `ConnectionLost`, exactly as Python does and
for the same three reasons — plus `ProtocolError` when `headers` rides a frame that is not this side's
first on the stream.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream, StreamClosed } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  for await (const item of stream) console.log('acceptor received:', item);
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('upload');
await stream.send({ part: 1 });
await stream.send({ part: 2 }, { end: true });
await new Promise<void>((resolve) => setTimeout(resolve, 50));

try {
  await stream.send({ part: 3 });
} catch (error) {
  if (error instanceof StreamClosed) console.log('sending after end:', error.message);
}

await dialer.close({ drainMs: 100 });
```

## `stream.send_headers()` (Python)

Announce this side's leading headers with no payload at all.

This is what a handler calls when it knows *what* is coming before it has produced any of it: the
frame it puts on the wire is a `data` with headers and nothing else, so the opener can read
`reply_headers` while the first row is still being computed. Riding them along with the first payload
instead is `send(..., headers=...)` or `reply(..., headers=...)`; the difference is only whether the
announcement waits for the body.

Each peer gets **one** chance per stream, and it is spent by that peer's first frame whether or not
that frame carried headers (WSM-FRM-016). On a stream this peer opened, the first frame was the
`open`, so the headers belong to `peer.open(headers=...)` and this call raises.

### Signature

```python
async def send_headers(self, headers: dict[str, Any]) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `headers` | `dict[str, Any]` | required | Application metadata: string keys, codec-encodable values. muxws never reads them (WSM-AUT-002). They are not fragmented, so headers alone that exceed the frame cap are an error rather than a split (WSM-FRG-021). |

### Return

`None`. The frame is enqueued for the writer.

### Raises

- `ProtocolError` — this side has already sent a frame on the stream. On a locally opened stream that
  is true from the start, and the message says to use `open()` instead.
- `StreamClosed` — the stream closed normally, or this side already sent `end`.
- `StreamReset` (or a subclass) — the stream was reset.
- `ConnectionLost` — the socket died.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.send_headers({"content-type": "text/csv", "rows-estimated": 2})
        await stream.send({"row": 1})
        await stream.end({"row": 2})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("export", end=True)
    await stream.reply_headers_arrived.wait()
    print("announced:", stream.reply_headers)
    print("rows:", [row async for row in stream])

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.sendHeaders()` (TypeScript)

### Signature

```ts
async sendHeaders(headers: Record<string, unknown>): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `headers` | `Record<string, unknown>` | required | Application metadata, carried on a `data` frame with no payload. |

### Return

`Promise<void>` — resolving once the frame is enqueued, not once it is on the wire.

### Raises

Rejects with `ProtocolError` when this side has already sent a frame on the stream, and otherwise
with the same three as `send()`: `StreamClosed`, a `StreamReset` subclass, `ConnectionLost`.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.sendHeaders({ 'content-type': 'text/csv', 'rows-estimated': 2 });
  await stream.send({ row: 1 });
  await stream.end({ payload: { row: 2 } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('export', { end: true });
await stream.replyHeadersArrived;
console.log('announced:', stream.replyHeaders);
for await (const row of stream) console.log('row:', row);
await dialer.close({ drainMs: 100 });
```

## `SendOptions` (TypeScript)

### Signature

```ts
export interface SendOptions {
  end?: boolean;
  headers?: Record<string, unknown>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `end` | `boolean` | `false` | Half-close this side in the same frame as the payload. |
| `headers` | `Record<string, unknown>` | — | This side's leading headers, if this is the **first** frame it sends on the stream (WSM-FRM-016). |

### Return

None — `SendOptions` is an interface, not a call. It is also re-exported as `StreamSendOptions`, for
callers that already have a `SendOptions` of their own.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type SendOptions, type StreamSendOptions } from 'muxws';

const last: SendOptions = { end: true };
const alias: StreamSendOptions = last;

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });
acceptor.onStream(() => undefined);

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('upload');
await stream.send({ part: 1 }, alias);
console.log('end:', last.end, 'state:', stream.state);
await dialer.close({ drainMs: 100 });
```

## `stream.end()` (Python)

End this side of the stream, optionally with a last payload and trailers.

### Signature

```python
async def end(
    self,
    payload: Any = ABSENT,
    *,
    trailers: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | `ABSENT` | A final payload. The default sentinel means "no payload": the `end` frame carries no `payload` field at all, which is different from sending `None`. |
| `trailers` | `dict[str, Any] \| None` | `None` | Metadata riding the `end` frame, read by the other side as `stream.trailers` once the stream has finished. |
| `headers` | `dict[str, Any] \| None` | `None` | This side's leading headers, if this is the **first** frame it sends on the stream (WSM-FRM-016). Later ones raise; see [`stream.send_headers()`](#stream-send-headers-python). |

### Return

`None`. If the remote had already ended, this closes the stream; otherwise the stream is
half-closed-local and the remote may still send.

### Raises

- `StreamClosed` — the stream closed normally, or this side already sent `end`; a stream cannot end
  twice.
- `StreamReset` (or a subclass) — the stream was reset.
- `ConnectionLost` — the socket died.
- `ProtocolError` — `headers` was passed on a frame that is not this side's first on the stream.

### Example

```python
import asyncio

from muxws import Peer, Stream, StreamClosed
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        async for item in stream:
            print("acceptor received:", item)
        print("trailers:", stream.trailers)

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("upload")
    await stream.send({"part": 1})
    await stream.end({"part": 2}, trailers={"parts": 2})
    await asyncio.sleep(0.05)

    try:
        await stream.end()
    except StreamClosed as exc:
        print("ending twice:", exc)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.end()` (TypeScript)

### Signature

```ts
async end(options: EndOptions = {}): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `options` | `EndOptions` | `{}` | `payload`, `trailers` and `headers`. TypeScript takes the last payload in the options object rather than positionally, so `end()` with no argument ends the stream carrying nothing. See [`EndOptions`](#endoptions-typescript). |

### Return

`Promise<void>`.

### Raises

Rejects with `StreamClosed` (closed normally, or `end` already sent), a `StreamReset` subclass, or
`ConnectionLost` — plus `ProtocolError` when `headers` rides a frame that is not this side's first on
the stream.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream, StreamClosed } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  for await (const item of stream) console.log('acceptor received:', item);
  console.log('trailers:', stream.trailers);
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('upload');
await stream.send({ part: 1 });
await stream.end({ payload: { part: 2 }, trailers: { parts: 2 } });
await new Promise<void>((resolve) => setTimeout(resolve, 50));

try {
  await stream.end();
} catch (error) {
  if (error instanceof StreamClosed) console.log('ending twice:', error.message);
}

await dialer.close({ drainMs: 100 });
```

## `EndOptions` (TypeScript)

### Signature

```ts
export interface EndOptions {
  payload?: unknown;
  trailers?: Record<string, unknown>;
  headers?: Record<string, unknown>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | absent | The final payload. Leaving it out sends an `end` frame with no `payload` field, which is not the same as sending `null`. |
| `trailers` | `Record<string, unknown>` | `null` | Metadata riding the `end` frame. |
| `headers` | `Record<string, unknown>` | — | This side's leading headers, if this is the **first** frame it sends on the stream (WSM-FRM-016). |

### Return

None — `EndOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { type EndOptions, JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const finish: EndOptions = { payload: { row: 2 }, trailers: { rows: 2 } };

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  for await (const row of stream) console.log('row:', row);
  console.log('trailers:', stream.trailers);
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('query');
await stream.end(finish);
await new Promise<void>((resolve) => setTimeout(resolve, 50));
await dialer.close({ drainMs: 100 });
```

## `stream.reply()` (Python)

`send` plus `end` in one call, which is what a unary handler wants.

It is exactly `end(payload, trailers=trailers)`, spelled the way a request/response handler reads.

### Signature

```python
async def reply(
    self,
    payload: Any,
    *,
    trailers: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `Any` | required | The response. Unlike `end()`, it is required here: a reply with nothing in it is `end()`. |
| `trailers` | `dict[str, Any] \| None` | `None` | Metadata riding the same `end` frame. |
| `headers` | `dict[str, Any] \| None` | `None` | The answer's leading headers, riding the same frame. Only legal when this is the handler's first frame on the stream (WSM-FRM-016); to announce them *before* the answer is ready, use [`stream.send_headers()`](#stream-send-headers-python). |

### Return

`None`.

### Raises

The same four as `end()`: `StreamClosed`, a `StreamReset` subclass, `ConnectionLost`, `ProtocolError`.

### Example

```python
import asyncio

from muxws import Peer, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        await stream.reply({"pong": payload}, trailers={"served_by": "docs"})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("ping", end=True)
    print(await stream)
    print("trailers:", stream.trailers)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.reply()` (TypeScript)

### Signature

```ts
async reply(payload: unknown, options: ReplyOptions = {}): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `payload` | `unknown` | required | The response. Positional here, unlike `end()`. |
| `options` | `ReplyOptions` | `{}` | `trailers` and `headers`. See [`ReplyOptions`](#replyoptions-typescript). |

### Return

`Promise<void>`.

### Raises

Rejects with `StreamClosed`, a `StreamReset` subclass, `ConnectionLost`, or `ProtocolError` when
`headers` rides a frame that is not this side's first on the stream.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  await stream.reply({ pong: payload }, { trailers: { servedBy: 'docs' } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('ping', { end: true });
console.log(await stream);
console.log('trailers:', stream.trailers);
await dialer.close({ drainMs: 100 });
```

## `ReplyOptions` (TypeScript)

### Signature

```ts
export interface ReplyOptions {
  trailers?: Record<string, unknown>;
  headers?: Record<string, unknown>;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `trailers` | `Record<string, unknown>` | `undefined` | Metadata riding the `end` frame the reply sends. |
| `headers` | `Record<string, unknown>` | `undefined` | The answer's leading headers, riding the same frame (WSM-FRM-016). |

### Return

None — `ReplyOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type ReplyOptions, type Stream } from 'muxws';

const options: ReplyOptions = { trailers: { servedBy: 'docs' } };

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => stream.reply({ pong: payload }, options));

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('ping', { end: true });
console.log(await stream, stream.trailers);
await dialer.close({ drainMs: 100 });
```

## `stream.cancel()` (Python)

Cancel the stream: `reset(CANCELLED)`, closing locally **at once** and without waiting for any
acknowledgement.

There is no acknowledgement to wait for, by design. The remote's handler is cancelled - it sees an
`asyncio.CancelledError` at its next await - and the local side is closed the instant this returns.
Cancelling one stream costs the connection nothing and affects no other stream.

### Signature

```python
async def cancel(self, reason: str | None = None) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `str \| None` | `None` | Free text carried on the `reset` frame, for the remote's logs. |

### Return

`None`. A no-op on a stream that is already closed - including after socket death, where there is
nothing to send it on.

### Raises

Raises: nothing.

### Example

```python
import asyncio

from muxws import Peer, ResetCode, Stream, StreamReset
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        for index in range(1000):
            await stream.send({"row": index})
            await asyncio.sleep(0.01)

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("big query")
    async for row in stream:
        print("row:", row)
        break

    await stream.cancel("that is enough")
    print("closed at once:", stream.closed.is_set(), stream.state.value)
    print("a second cancel is a no-op:", await stream.cancel())

    try:
        await stream.send({"anything": True})
    except StreamReset as exc:
        print("the stream is gone:", exc.code is ResetCode.CANCELLED)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.cancel()` (TypeScript)

### Signature

```ts
async cancel(reason?: string): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `reason` | `string` | `undefined` | Free text carried on the `reset` frame. |

### Return

`Promise<void>`. A no-op on an already-closed stream. The remote handler observes it as an aborted
`stream.signal` rather than as a thrown cancellation.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode, type Stream, StreamReset } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  for (let index = 0; index < 1000 && !stream.signal.aborted; index += 1) {
    await stream.send({ row: index });
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
});

void dialer.serve();
void acceptor.serve();

// Awaited rather than iterated: a `break` out of a `for await` resets the stream by itself in
// TypeScript, and then `cancel()` would have nothing left to do.
const stream = dialer.open('big query');
console.log('first row:', await stream.result({ timeoutMs: 5000 }));

await stream.cancel('that is enough');
console.log('closed at once:', stream.state);

try {
  await stream.send({ anything: true });
} catch (error) {
  if (error instanceof StreamReset) console.log('the stream is gone:', error.code === ResetCode.CANCELLED);
}

await dialer.close({ drainMs: 100 });
```

## `stream.reset()` (Python)

Terminate the stream in both directions with an explicit reset code.

`cancel()` is this with `CANCELLED`. Use `reset()` when the code carries information the remote can
act on - `TIMEOUT`, `PAYLOAD_TOO_LARGE`, `APPLICATION_ERROR` - and remember that `REFUSED` is a
promise that nothing ran.

### Signature

```python
async def reset(self, code: ResetCode, reason: str | None = None) -> None:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode` | required | The reset code. `CONNECTION_CLOSED` is rejected: it is synthesised locally when a socket dies and must never appear on the wire, because it would tell the remote that *its* connection had died. |
| `reason` | `str \| None` | `None` | Free text carried on the `reset` frame. |

### Return

`None`. A no-op once the stream is closed, including after socket death.

### Raises

- `ProtocolError` — `code` is `ResetCode.CONNECTION_CLOSED`, or a number this generation does not
  define (5 is retired and must not be reused). Raised before anything is sent, and raised even on a
  stream that is already closed.

### Example

```python
import asyncio

from muxws import Peer, ProtocolError, RemoteError, ResetCode, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.reset(ResetCode.APPLICATION_ERROR, "the account is frozen")

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    try:
        await dialer.request("withdraw", timeout=5.0)
    except RemoteError as exc:
        print("code", int(exc.code), "reason", exc.reason)

    stream = dialer.open("x")
    try:
        await stream.reset(ResetCode.CONNECTION_CLOSED, "never allowed")
    except ProtocolError as exc:
        print(str(exc)[:52])
    await stream.cancel()

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.reset()` (TypeScript)

### Signature

```ts
async reset(code: ResetCode, reason?: string): Promise<void>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `code` | `ResetCode` | required | The reset code. `CONNECTION_CLOSED` is rejected, for the reason the Python entry gives. |
| `reason` | `string` | `undefined` | Free text carried on the `reset` frame. |

### Return

`Promise<void>`. A no-op once the stream is closed.

### Raises

Rejects with `ProtocolError` for `ResetCode.CONNECTION_CLOSED` and for any number this generation
does not define, including the retired 5. The check runs before anything is sent and before the
already-closed short-circuit — but `reset` is an `async` method, so the failure arrives as a
rejection rather than as a throw at the call site.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ProtocolError, RemoteError, ResetCode, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reset(ResetCode.APPLICATION_ERROR, 'the account is frozen');
});

void dialer.serve();
void acceptor.serve();

try {
  await dialer.request('withdraw', { timeoutMs: 5000 });
} catch (error) {
  if (error instanceof RemoteError) console.log('code', error.code, 'reason', error.reason);
}

const stream = dialer.open('x');
try {
  await stream.reset(ResetCode.CONNECTION_CLOSED, 'never allowed');
} catch (error) {
  if (error instanceof ProtocolError) console.log(error.message.slice(0, 52));
}
await stream.cancel();

await dialer.close({ drainMs: 100 });
```

## `stream.__await__()` (Python)

What makes `await stream` work: it claims the stream for the await shape and resolves with the
**first** payload the remote sends.

There is one memoized answer per stream. A second `await` returns that same value rather than the
next payload - a stream is not a queue you can pull twice - and a stream that has already been
iterated raises instead.

### Signature

```python
def __await__(self):
```

### Parameters

None — it is the awaitable protocol method. `await stream` calls it with no arguments.

### Return

A generator, as the protocol requires; `await stream` evaluates to the first payload, of type `Any`.

### Raises

- `StreamAlreadyConsumed` — the stream is already being iterated.
- `ProtocolError` — the stream ended without ever producing a payload.
- `RemoteError`, `StreamRefused`, `StreamTimeout`, `StreamReset` — the stream was reset.
- `ConnectionLost` — the socket died.
- `asyncio.CancelledError` — the awaiting task was cancelled. Before it propagates, the stream is
  reset with `CANCELLED`, so the remote stops producing for a consumer that has gone away.

### Example

```python
import asyncio

from muxws import Peer, Stream, StreamAlreadyConsumed
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(_payload: object, stream: Stream) -> None:
        await stream.send({"first": True})
        await stream.end({"second": True})

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    stream = dialer.open("ask", end=True)
    print("first await:", await stream)
    print("second await is the same value:", await stream)

    try:
        async for _item in stream:
            pass
    except StreamAlreadyConsumed as exc:
        print(str(exc)[:44])

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.then()` (TypeScript)

What makes `await stream` work: `Stream` implements `PromiseLike`, so awaiting it claims it for the
await shape and resolves with the first payload.

It **implements** `PromiseLike` rather than extending `Promise`, deliberately: subclassing would make
`stream.then(...).then(...)` construct a `Stream` for every derived promise - a chain of objects each
claiming to be an addressable exchange with an id, none of which is one. `then` returns an ordinary
`Promise`.

### Signature

```ts
then<R1 = T, R2 = never>(
  onOk?: ((value: T) => R1 | PromiseLike<R1>) | null,
  onErr?: ((error: unknown) => R2 | PromiseLike<R2>) | null,
): Promise<R1 | R2>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `onOk` | `((value: T) => R1 \| PromiseLike<R1>) \| null` | `undefined` | Called with the first payload. |
| `onErr` | `((error: unknown) => R2 \| PromiseLike<R2>) \| null` | `undefined` | Called with the failure that ended the stream. |

### Return

`Promise<R1 | R2>` — an ordinary promise, not a `Stream`.

### Raises

Rejects with `StreamAlreadyConsumed` (the stream is already being iterated), `ProtocolError` (it
ended without producing a payload), a `StreamReset` subclass, or `ConnectionLost`.
`StreamAlreadyConsumed` is thrown **synchronously** out of `then` itself, before any promise exists.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream, StreamAlreadyConsumed } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.send({ first: true });
  await stream.end({ payload: { second: true } });
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('ask', { end: true });
console.log('first await:', await stream);
console.log('second await is the same value:', await stream);
console.log('then returns a plain promise:', (await stream.then((value) => value)) !== stream);

try {
  for await (const _item of stream) console.log(_item);
} catch (error) {
  if (error instanceof StreamAlreadyConsumed) console.log(error.message.slice(0, 44));
}

await dialer.close({ drainMs: 100 });
```

## `stream.catch()` (TypeScript)

`then(undefined, onErr)`, for a caller that wants only the failure path. It claims the stream for the
await shape exactly as `then` does.

### Signature

```ts
catch<R2 = never>(onErr?: ((error: unknown) => R2 | PromiseLike<R2>) | null): Promise<T | R2>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `onErr` | `((error: unknown) => R2 \| PromiseLike<R2>) \| null` | `undefined` | Called with whatever ended the stream early. |

### Return

`Promise<T | R2>` — the first payload, or the handler's value if the stream failed.

### Raises

The same set as `then`, and `StreamAlreadyConsumed` synchronously when the stream is already being
iterated.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode, type Stream, StreamReset } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  await stream.reset(ResetCode.APPLICATION_ERROR, 'no');
});

void dialer.serve();
void acceptor.serve();

const recovered = await dialer
  .open('x', { end: true })
  .catch((error: unknown) => (error instanceof StreamReset ? `reset: ${error.reason}` : 'other'));
console.log(recovered);

await dialer.close({ drainMs: 100 });
```

## `stream.finally()` (TypeScript)

`then().finally(onSettled)`, for cleanup that must run whichever way the stream ended. It claims the
stream for the await shape.

### Signature

```ts
finally(onSettled?: (() => void) | null): Promise<T>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `onSettled` | `(() => void) \| null` | `undefined` | Called once the stream has settled, either way, with no arguments. |

### Return

`Promise<T>` — the first payload, or a rejection carrying whatever ended the stream. `finally` does
not swallow the failure.

### Raises

The same set as `then`.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => stream.reply({ echo: payload }));

void dialer.serve();
void acceptor.serve();

const value = await dialer.open('ask', { end: true }).finally(() => console.log('cleanup ran'));
console.log(value);

await dialer.close({ drainMs: 100 });
```

## `stream.result()` (Python)

The same memoized answer as `await stream`, with a deadline wrapped around the wait.

It is never a second source of the value: `await stream` and `await stream.result()` resolve from one
place, so a second read returns the first read's value rather than the next payload.

### Signature

```python
async def result(self, timeout: float | None = None) -> Any:
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `timeout` | `float \| None` — seconds | `None` | The deadline, in seconds. `None` waits forever. On expiry the stream is reset with `TIMEOUT` **before** `StreamTimeout` is raised, so the remote is told to stop working rather than finishing into a caller that has gone. |

### Return

`Any` — the first payload the remote sent.

### Raises

- `StreamAlreadyConsumed` — the stream is already being iterated.
- `StreamTimeout` — `timeout` seconds expired. The remote has already been sent `reset(TIMEOUT)`.
- `ProtocolError` — the stream ended without producing a payload.
- `RemoteError`, `StreamRefused`, `StreamReset` — the stream was reset.
- `ConnectionLost` — the socket died.
- `asyncio.CancelledError` — the awaiting task was cancelled; the stream is reset with `CANCELLED`
  first.

### Example

```python
import asyncio

from muxws import Peer, ResetCode, Stream, StreamTimeout
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    codes: list[int] = []

    async def handler(payload: object, stream: Stream) -> None:
        if payload == "slow":
            # Never answers, and waits on the stream rather than on a clock: `closed` is set when the
            # caller's deadline resets it (WSM-API-023).
            await stream.closed.wait()
            return
        await stream.reply({"echo": payload})

    acceptor.on_stream(handler)
    acceptor.on_frame(lambda direction, frame, _bytes: codes.append(frame.code or 0) if frame.type == "reset" else None)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    print(await dialer.open("fast", end=True).result(timeout=5.0))

    try:
        await dialer.open("slow", end=True).result(timeout=0.2)
    except StreamTimeout as exc:
        print("timed out:", str(exc)[:34])
    await asyncio.sleep(0.05)
    print("the remote was told first:", ResetCode.TIMEOUT in codes)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream.result()` (TypeScript)

### Signature

```ts
async result(options: ResultOptions = {}): Promise<T>;
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `options` | `ResultOptions` | `{}` | `timeoutMs` and `signal`. See [`ResultOptions`](#resultoptions-typescript). With neither, it is exactly `await stream`. |

### Return

`Promise<T>` — the first payload the remote sent, from the same memoized promise `await stream` uses.

### Raises

Rejects with `StreamTimeout` (after the remote has been sent `reset(TIMEOUT)`), `ProtocolError` (the
stream ended without a payload), a `StreamReset` subclass, or `ConnectionLost`. An aborted `signal`
rejects with the signal's own `reason` after resetting the stream with `CANCELLED`. It also rejects
with `StreamAlreadyConsumed` when the stream is already being iterated: `result` is an `async`
method, so unlike `then`, which throws that one at the call site, this one hands it to you as a
rejection.

### Example

```ts
import { JsonCodec, memoryPair, Peer, ResetCode, type Stream, StreamTimeout } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

const codes: number[] = [];
acceptor.onStream(async (payload: unknown, stream: Stream) => {
  if (payload === 'slow') {
    // Never answers, and waits on the stream rather than on a clock: `closed` resolves when the
    // caller's deadline resets it (WSM-API-023). A timer would keep this process alive for ten
    // seconds after the last line had been printed.
    await stream.closed;
    return;
  }
  await stream.reply({ echo: payload });
});
acceptor.onFrame((_direction, frame) => {
  if (frame.type === 'reset') codes.push(frame.code ?? 0);
});

void dialer.serve();
void acceptor.serve();

console.log(await dialer.open('fast', { end: true }).result({ timeoutMs: 5000 }));

try {
  await dialer.open('slow', { end: true }).result({ timeoutMs: 200 });
} catch (error) {
  if (error instanceof StreamTimeout) console.log('timed out:', error.message.slice(0, 34));
}
await new Promise<void>((resolve) => setTimeout(resolve, 50));
console.log('the remote was told first:', codes.includes(ResetCode.TIMEOUT));

await dialer.close({ drainMs: 100 });
```

## `ResultOptions` (TypeScript)

### Signature

```ts
export interface ResultOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `timeoutMs` | `number` — milliseconds | none (waits forever) | The deadline in milliseconds. On expiry the stream is reset with `TIMEOUT` before `StreamTimeout` is thrown. |
| `signal` | `AbortSignal` | none | The TypeScript stand-in for cancelling the awaiting task: abandoning a promise is invisible to the promise, so a consumer that wants the stream reset when it gives up says so with a signal. An abort resets the stream with `CANCELLED` and then throws the signal's `reason`. A signal that is already aborted takes that path immediately. |

### Return

None — `ResultOptions` is an interface, not a call.

### Raises

Raises: nothing.

### Example

```ts
import { JsonCodec, memoryPair, Peer, type ResultOptions, type Stream } from 'muxws';

const controller = new AbortController();
const options: ResultOptions = { timeoutMs: 5000, signal: controller.signal };

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (_payload: unknown, stream: Stream) => {
  // Slower than the consumer's patience, and no slower: the abort below lands at 50 ms, so this is
  // late by any measure. It is a plain sleep rather than a wait on `closed` because the point of the
  // example is the reply that arrives *after* the stream is gone - and ten seconds of it would only
  // be ten seconds of a process with nothing left to do.
  await new Promise<void>((resolve) => setTimeout(resolve, 200));
  await stream.reply('too late');
});

void dialer.serve();
void acceptor.serve();

const stream = dialer.open('long job', { end: true });
setTimeout(() => controller.abort(new Error('the user navigated away')), 50);
try {
  await stream.result(options);
} catch (error) {
  console.log('abandoned:', (error as Error).message, 'state:', stream.state);
}

await dialer.close({ drainMs: 100 });
```

## `StreamSendOptions` (TypeScript)

`stream.send()`'s options object, re-exported from the package root under this name; inside the
module it is `SendOptions`.

It carries one field, and the field is the whole of muxws's "end" story: **`end` is a flag on a frame,
never a frame of its own** (WSM-FRM-014). `send(payload, { end: true })` is one frame that both
delivers and half-closes, which is why it is one call rather than a `send` followed by an `end`.

Python has no counterpart because Python takes a keyword argument: `await stream.send(payload,
end=True)`. The asymmetry is deliberate — a TypeScript positional boolean reads as `send(x, true)` at
the call site, which says nothing.

### Signature

```ts
export interface SendOptions {
  end?: boolean;
}

export type { SendOptions as StreamSendOptions };
```

### Parameters

| Name | Type | Default | What it does |
|---|---|---|---|
| `end` | `boolean` | `false` | Half-closes this side in the same frame that carries the payload. After it, this stream may not send again; the remote may still send until it ends too. |

### Return

Nothing — it is a type. It exists at compile time only and is absent from the emitted JavaScript.

### Raises

Raises: nothing itself. `send()` with `{ end: true }` on a stream that already ended raises
`StreamClosed`; see `stream.send()`.

### Example

```ts
import { JsonCodec, Peer, type StreamSendOptions, memoryPair } from 'muxws';

const [dialerSide, acceptorSide] = memoryPair();

const acceptor = new Peer(acceptorSide, { codec: new JsonCodec(), isDialer: false });
acceptor.onStream(async (payload, stream) => {
  const received: unknown[] = [payload];
  for await (const item of stream) received.push(item);
  console.log('the acceptor saw', received.length, 'payloads and the stream ended');
});
void acceptor.serve().catch(() => undefined);

const dialer = new Peer(dialerSide, { codec: new JsonCodec(), isDialer: true });
void dialer.serve().catch(() => undefined);

const stream = dialer.open({ chunk: 1 });
const last: StreamSendOptions = { end: true };
await stream.send({ chunk: 2 });
await stream.send({ chunk: 3 }, last);

await new Promise((resolve) => setTimeout(resolve, 50));
await dialer.close();
await acceptor.close();
```

## `stream.__aiter__()` (Python)

What makes `async for item in stream` work: it claims the stream for the iterate shape and yields
every payload the remote sends, ending when the remote ends the stream.

Leaving the loop early - `break`, `return`, an exception - is not observed by Python here, so a
consumer that stops early should `cancel()` the stream itself. Cancelling the *task* that is
iterating is observed: the stream is reset with `CANCELLED` before the cancellation propagates.

### Signature

```python
def __aiter__(self) -> AsyncIterator[Any]:
```

### Parameters

None — it is the async-iterable protocol method.

### Return

`AsyncIterator[Any]` — yields each payload in order, then stops when the remote ends the stream.

### Raises

- `StreamAlreadyConsumed` — the stream was already awaited, or is already being iterated. Two
  iterators would split the payloads between them.
- `RemoteError`, `StreamRefused`, `StreamTimeout`, `StreamReset` — raised **out of the loop** when
  the remote resets the stream mid-iteration.
- `ConnectionLost` — the socket died mid-iteration.
- `asyncio.CancelledError` — the iterating task was cancelled; the stream is reset with `CANCELLED`
  first.

### Example

```python
import asyncio

from muxws import Peer, RemoteError, Stream
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair


async def main() -> None:
    dialer_socket, acceptor_socket = memory_pair()
    dialer = Peer(dialer_socket, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(acceptor_socket, codec=JsonCodec(), is_dialer=False)

    async def handler(payload: object, stream: Stream) -> None:
        for index in range(3):
            await stream.send({"chunk": index})
        if payload == "fail at the end":
            raise RuntimeError("the source went away")
        await stream.end()

    acceptor.on_stream(handler)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    async for chunk in dialer.open("stream me", end=True):
        print(chunk)

    try:
        async for chunk in dialer.open("fail at the end", end=True):
            print(chunk)
    except RemoteError as exc:
        print("the iteration raised:", exc.payload)

    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


asyncio.run(main())
```

## `stream[Symbol.asyncIterator]()` (TypeScript)

What makes `for await (const item of stream)` work: it claims the stream for the iterate shape and
yields every payload.

Unlike Python's, this one **does** observe a consumer that walks away: a `break`, a `return` or a
throw out of the loop body finalises the generator, and the stream is reset with `CANCELLED` on the
way out. A loop that exited because the stream was reset does not send a second one.

### Signature

```ts
[Symbol.asyncIterator](): AsyncIterator<T>;
```

### Parameters

None — it is the async-iterable protocol method.

### Return

`AsyncIterator<T>` — yields each payload in order, then completes when the remote ends the stream.

### Raises

Throws `StreamAlreadyConsumed` synchronously when the stream was already awaited or is already being
iterated. The iteration itself rejects with a `StreamReset` subclass or `ConnectionLost` when the
stream is reset or the socket dies mid-loop.

### Example

```ts
import { JsonCodec, memoryPair, Peer, RemoteError, type Stream } from 'muxws';

const [dialerSocket, acceptorSocket] = memoryPair();
const dialer = new Peer(dialerSocket, { codec: new JsonCodec(), isDialer: true });
const acceptor = new Peer(acceptorSocket, { codec: new JsonCodec(), isDialer: false });

acceptor.onStream(async (payload: unknown, stream: Stream) => {
  for (const index of [0, 1, 2]) await stream.send({ chunk: index });
  if (payload === 'fail at the end') throw new Error('the source went away');
  await stream.end();
});

void dialer.serve();
void acceptor.serve();

for await (const chunk of dialer.open('stream me', { end: true })) console.log(chunk);

try {
  for await (const chunk of dialer.open('fail at the end', { end: true })) console.log(chunk);
} catch (error) {
  if (error instanceof RemoteError) console.log('the iteration threw:', error.payload);
}

await dialer.close({ drainMs: 100 });
```

## `StreamState` (Python)

The five states of a stream, tracked per stream per peer.

`IDLE` → `OPEN` when the stream is opened; `OPEN` → `HALF_CLOSED_LOCAL` when this side sends `end`,
or → `HALF_CLOSED_REMOTE` when the remote does; either half-closed state → `CLOSED` when the other
side ends too. Any state → `CLOSED` on a reset or socket death.

### Signature

```python
class StreamState(str, Enum):
    IDLE = "idle"
    OPEN = "open"
    HALF_CLOSED_LOCAL = "half_closed_local"
    HALF_CLOSED_REMOTE = "half_closed_remote"
    CLOSED = "closed"
```

### Parameters

None — it is an enum. `StreamState("open")` looks a member up by value, as with any `str` enum.

### Return

`StreamState` — a `str` subclass, so `stream.state == "open"` is true as well as
`stream.state is StreamState.OPEN`.

### Raises

- `ValueError` — from `StreamState(value)` when `value` is not one of the five.

### Example

```python
import asyncio

from muxws import StreamState


async def main() -> None:
    print([state.value for state in StreamState])
    print(StreamState("half_closed_local") is StreamState.HALF_CLOSED_LOCAL)
    try:
        StreamState("nearly_closed")
    except ValueError as exc:
        print(str(exc)[:34])


asyncio.run(main())
```

## `StreamState` (TypeScript)

### Signature

```ts
export enum StreamState {
  IDLE = 'idle',
  OPEN = 'open',
  HALF_CLOSED_LOCAL = 'half_closed_local',
  HALF_CLOSED_REMOTE = 'half_closed_remote',
  CLOSED = 'closed',
}
```

### Parameters

None — it is a string enum.

### Return

`StreamState` — the member. Because it is a string enum, `stream.state === 'open'` compares equal to
`stream.state === StreamState.OPEN`.

### Raises

Raises: nothing. There is no lookup-by-value that can fail: an unknown key is `undefined`.

### Example

```ts
import { StreamState } from 'muxws';

console.log(Object.values(StreamState));
console.log(StreamState.HALF_CLOSED_LOCAL === 'half_closed_local');
console.log(StreamState.CLOSED);
```

## See also

- [`Peer`](./peer.md) — `open()`, `request()` and the `on_stream` handler that hands you a stream.
- [Errors](./errors.md) — `StreamClosed`, `StreamReset`, `RemoteError`, `StreamRefused`,
  `StreamTimeout`, `ConnectionLost`, `StreamAlreadyConsumed` and the nine reset codes.
- [Types](./types.md) — `Frame` and the envelope fields a `Stream` is built from.
