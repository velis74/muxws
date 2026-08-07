# muxws M2 - Python peer core over an in-memory transport

## 1. Goal

At the end of M2 two Python `Peer` objects wired to each other in memory hold a complete muxws
conversation: they allocate stream ids with the right parity, open streams, send and receive payloads,
half-close, close, reset and cancel them, dispatch every incoming stream to one handler, and raise the
right exception at the right call site for every illegal move. `peer.open()` is synchronous and
returns a `Stream` that is simultaneously awaitable and async-iterable. Every cell of the stream state
table has a test, including the illegal ones, and `conformance/invalid/*.json` runs for the first
time. No WebSocket exists anywhere in this milestone.

## 2. Prerequisites

**M0** (layout, ruff/pytest config, colocated `*_test.py`, `asyncio_mode = "auto"`) and **M1**, which
left behind:

- `muxws/frames.py` - frozen `Frame` dataclass, the `ABSENT` payload sentinel, `to_mapping`,
  `from_mapping`, validation raising `ProtocolError` on a missing `type` or on `payload` + `fragment`
  together.
- `muxws/errors.py` - `ResetCode` (nine members; `5` is retired and undefined) and the full
  hierarchy: `MuxwsError`, `ProtocolError`, `ConnectionClosed`, `ConnectionGoingAway`,
  `StreamAlreadyConsumed`, `StreamClosed`, `CodecError` (`CodecNotRegistered`, `CodecMismatch`),
  `StreamReset` (`RemoteError`, `StreamTimeout`, `StreamRefused`, `ConnectionLost`). There is no
  `StreamLimit`.
- `muxws/codecs/` - the `Codec` protocol, `register_codec`, `get_codec`, `registered_codecs`, and
  `JsonCodec` registered by the library as `"json"`.
- `muxws/fragment.py` - `encoded_length`, `split_frame`, `Assembler`, pure and already tested.
- `conformance/frames/v1-frames.json` and the eight `conformance/invalid/*.json` files, written and
  schema-validated but never executed.

## 3. Files to create or modify

```
muxws/peer.py                    muxws/peer_test.py
muxws/stream.py                  muxws/stream_test.py
muxws/transports/__init__.py     # SocketAdapter protocol
muxws/transports/memory.py       muxws/transports/memory_test.py
muxws/conformance_test.py        # replays conformance/invalid/*.json against a memory pair
muxws/__init__.py                # + Peer, Stream, SocketAdapter, StreamHandler re-exports
```

## 4. Normative rules in force

### 4.1 Id allocation and parity (§5.1)

- **WSM-SID-001** Stream ids MUST be allocated by the library. A caller MUST NOT be able to supply
  one, anywhere in the API.
- **WSM-SID-002** The dialer MUST allocate odd ids (1, 3, 5, ...); the acceptor MUST allocate even ids
  (2, 4, 6, ...).
- **WSM-SID-003** Id `0` is reserved for connection-level frames, which in practice omit the field.
- **WSM-SID-004** Opens MUST be monotonic per peer and ids MUST NOT be reused, even after a stream
  closes. `data` and `reset` MAY name any previously-opened id in any order.
- **WSM-SID-005** An `open` naming an id with the wrong parity, or an id not greater than the highest
  id that peer has previously opened, is a **connection-level** protocol error.
- **WSM-SID-006** The id MUST be allocated and the `open` frame enqueued in one synchronous step, with
  no suspension point between them, so that wire order is allocation order by construction.
- **WSM-SID-007** On exhaustion at 2^31-1, the exhausting peer MUST send `goaway` with `last_stream`
  set to the highest id it has processed, MUST stop opening new streams, MUST let in-flight streams
  drain, and MUST then close.
- **WSM-SID-008** `stream.id` MUST be readable on the line following `open()`, with nothing awaited in
  between.

### 4.2 Retention and late frames (§5.2)

- **WSM-STM-001** A peer MUST retain exactly **`highest_open_seen` per parity plus the map of live
  streams**. It MUST NOT keep per-closed-stream bookkeeping (that is a per-connection memory leak).
- **WSM-STM-002** Any stream-level frame naming an id that is not currently live but does not exceed
  that peer's high-water mark MUST be **silently ignored** - no reset, no connection error, at most a
  counter.
- **WSM-STM-003** Any stream-level frame other than a valid `open` naming an id **above** that peer's
  high-water mark MUST be a **connection-level** protocol error.

### 4.3 State machine (§5.3)

| State | Entered by |
|---|---|
| `idle` | id allocated, nothing sent |
| `open` | `open` sent or received without `end` |
| `half_closed_local` | this peer sent `end: true` |
| `half_closed_remote` | this peer received `end: true` |
| `closed` | both ends sent `end`, or either sent `reset`, or the connection died |

Legend: `→ S` transition to state S; `ILL-S` stream-level protocol error (reset that stream,
connection survives); `ILL-C` connection-level protocol error (`goaway(PROTOCOL_ERROR)`, close
socket); `IGN` silently ignore; `RAISE` local API error at the call site, nothing sent - which error
is WSM-ERR-009 for `data`/`end` on a stream that is no longer open, and WSM-API-004 for `open`; `n/r`
not reachable.

| State \ Event | send `open` | recv `open` | send `data` | send `end` | recv `data` | recv `data(end)` | send `reset` | recv `reset` | socket death |
|---|---|---|---|---|---|---|---|---|---|
| `idle` | → `open` (→ `half_closed_local` if `end`) | → `open` (→ `half_closed_remote` if `end`) | n/r | n/r | ILL-C | ILL-C | n/r | ILL-C | → `closed` |
| `open` | RAISE | ILL-C | → `open` | → `half_closed_local` | → `open` | → `half_closed_remote` | → `closed` | → `closed` | → `closed` |
| `half_closed_local` | RAISE | ILL-C | RAISE | RAISE | → `half_closed_local` | → `closed` | → `closed` | → `closed` | → `closed` |
| `half_closed_remote` | RAISE | ILL-C | → `half_closed_remote` | → `closed` | ILL-S | ILL-S | → `closed` | → `closed` | → `closed` |
| `closed` | RAISE | ILL-C | RAISE | RAISE | IGN | IGN | NOOP | IGN | NOOP |

- **WSM-STM-010** Every row of this table, including every illegal cell, MUST have a test.
- **WSM-STM-011** `idle` is not externally observable: it exists only inside the indivisible
  allocate-and-enqueue step of WSM-SID-006. Cells marked `n/r` MUST NOT be reachable through the
  public API.
- **WSM-STM-012** A stream that is `half_closed_local` on one peer MUST be `half_closed_remote` on the
  other. Both ends half-closed means `closed`.
- **WSM-STM-013** The unary shape is `open(end=true)` → `half_closed_local`, one inbound
  `data(end=true)` → `closed`.
- **WSM-STM-014** On socket death every live stream MUST transition to `closed` and be failed locally
  with a synthesised `reset(CONNECTION_CLOSED)` (reset code 9 - see the table in M1's §4.4, which is
  the same table) **before** `on_close` fires.
- **WSM-STM-015** A `reset` for a stream that is already closed or was never opened MUST be ignored
  under WSM-STM-002.
- **WSM-STM-020** A frame illegal *for a stream* MUST reset **that stream** and leave the connection
  alone. Stream-level cases: `data` after receiving `end`; a non-fragment frame mid-reassembly
  (WSM-FRG-033); a frame the receiver declines as over-size (WSM-FRG-031); a payload over
  `max_payload_bytes` (WSM-FRG-032); an `open` beyond the receiver's own concurrency limit
  (WSM-STM-036). *(Only the first case is reachable in M2; the size and concurrency cases arrive with
  the receive-side caps in M5a.)*
- **WSM-STM-021** The code for a stream-level violation MUST be `PROTOCOL_ERROR`, except that size
  violations use `PAYLOAD_TOO_LARGE` and concurrency-limit rejection uses `REFUSED` (WSM-STM-036).
- **WSM-STM-022** *Retired.* It required concurrency-limit rejection to use `STREAM_LIMIT` rather than
  `REFUSED`, so that an opener would back off rather than retry at once. The distinction went with the
  announced quota: the limit is now the receiver's alone and is not advertised, so `REFUSED` - nothing
  ran, retry is the application's judgement - is the whole of what the opener can be told. The id is
  not reused, and reset code 5 is retired with it.
- **WSM-STM-023** A frame illegal *for the connection* MUST end the connection with
  `goaway(PROTOCOL_ERROR)` followed by a socket close. Connection-level cases: a message the codec
  cannot decode; a missing `type`; a wrong-parity stream id; an `open` whose id is not greater than
  that peer's highest previous open; a stream-level frame naming an id above the high-water mark.
- **WSM-STM-024** The discriminator, when a new case arises: if the peers can still agree about the
  state of every *other* stream, the connection is kept.

### 4.4 Incoming stream dispatch (§5.5)

- **WSM-STM-030** A peer MUST have exactly one incoming-stream handler, registered with
  `on_stream(handler)`. Registering a second MUST replace the first and MUST log. There MUST NOT be a
  path table, method map, or per-action registration in muxws.
- **WSM-STM-031** The handler MUST be invoked as `(payload, stream)` and **only once the opening
  payload is fully reassembled**. A fragmented `open` MUST NOT reach the application in pieces.
- **WSM-STM-032** The reassembled opening payload MUST also be available as `stream.payload`.
- **WSM-STM-033** With no registered handler, an incoming `open` MUST be answered with
  `reset(REFUSED)` (nothing ran, so the opener may safely retry elsewhere).
- **WSM-STM-034** A registered handler that raises MUST produce `reset(APPLICATION_ERROR)`, **always**,
  regardless of whether it had already sent anything (`REFUSED` promises the operation definitively did
  not happen - or a handler that debits an account and then raises invites a retried debit).
- **WSM-STM-035** A handler that returns without having ended its stream MUST end it implicitly.
- **WSM-ERR-006** A handler that raises MUST produce `reset(APPLICATION_ERROR)` with a `reason` and
  an optional structured `payload`. The default serializer produces
  `{"type": "ValueError", "message": str(exc)}`. The peer MUST accept an `error_serializer` hook of
  signature `(exc) -> Any | None` - the value it returns becomes the reset's `payload`, and `None`
  means "send no payload". The documentation MUST state plainly that a public-facing deployment should
  redact it.
- **WSM-ERR-008** `error_serializer` MUST be supplied **per peer** - an argument to `connect()` and to
  the acceptor's peer construction (`accept()` / `serve()`) - and MUST NOT be a module-level default.
  One process may hold a browser-facing peer that redacts and an internal peer that does not, and a
  process-wide setting forces the wrong answer for one of them. *(M2 takes it on the `Peer`
  constructor; M3/M4 carry it through `connect()` / `accept()`.)*
- **WSM-AUT-004** muxws MUST NOT look at the opening payload for routing: no path matching, no method
  dispatch, no handler table.
- **WSM-FRM-002** A receiver MUST ignore unknown *frame types*, logging once, and MUST NOT treat them
  as any kind of error.
- **WSM-AUT-002** muxws MUST NOT interpret per-stream `headers`. They exist for the application, and
  MUST NOT be used for re-authentication by the library.
- **WSM-INV-002** There MUST be one symmetric `Peer` type per language - or server push needs a
  second, parallel mechanism with its own correlation and cancellation story.
- **WSM-INV-006** Late frames below the high-water mark MUST be ignored and frames above it MUST kill
  the connection (WSM-STM-002/003, above) - or "ignore late frames" degenerates into "ignore
  everything" and a genuine id-space disagreement goes undetected.
- **WSM-INV-008** A handler that raises MUST always produce `APPLICATION_ERROR`, never `REFUSED`
  (WSM-STM-034, above) - or a handler that debits an account and then raises invites the client to
  retry the debit.

### 4.5 Call shapes and the awaitable handle (§9.1-9.3)

- **WSM-API-001** `peer.open()` MUST be **synchronous** and MUST return a `Stream`.
- **WSM-API-002** `Stream` MUST be simultaneously awaitable (resolving with the remote's **first**
  payload) and async-iterable (yielding every reassembled payload until the remote ends).
- **WSM-API-003** `open()` MUST take zero mandatory arguments; `payload` defaults to `null`/`None`.
- **WSM-API-004** `open()` MUST raise synchronously at the call site in exactly two cases:
  `ConnectionGoingAway` after a received `goaway`, and `ConnectionLost` while the peer is between
  sockets. It MUST NOT queue. There MUST NOT be a `StreamLimit` exception and `open()` MUST NOT fail
  for concurrency: the concurrency limit is the receiver's (WSM-STM-036) and surfaces asynchronously
  as `StreamRefused` on the pending await. *(In M2 only the `ConnectionLost` case exists; `goaway`
  arrives in M4.)*
- **WSM-API-018** `open()` MUST NOT take a `timeout` argument in either language. `open()` returns
  immediately, so there is nothing for a deadline on it to bound; deadlines live on the awaits,
  `stream.result(timeout=)` and `peer.request(timeout=)`.
- **WSM-API-005** `notify()` MUST be async and MUST return nothing (`None` / `void`). It MUST NOT
  return a `Stream` or any awaitable handle.
- **WSM-API-006** `request()` MUST be `open(payload, end=True)` awaited **to the stream's end**, plus a
  check that raises if the remote sent more than one payload.
- **WSM-API-007** `await stream` MUST resolve on the **first** payload and MUST NOT police a second one
  - that check belongs to `request()` alone.
- **WSM-API-008** `connect()`, `accept()`, `serve()`, `notify()`, `request()`, `ping()`, `close()` and
  every `Stream` send method MUST be async. Everything else MUST NOT be.
- **WSM-API-010** `Stream.__await__` MUST delegate to a **memoized** future - one per stream, created
  lazily on the first await, resolved with the stream's first payload or rejected with the stream's
  `StreamReset`.
- **WSM-API-011** Awaiting an already-awaited stream MUST NOT be an error and MUST return the same
  value again.
- **WSM-API-012** `result(timeout=...)` MUST be the same future with a deadline wrapped around the
  wait, never a second source of the value.
- **WSM-API-013** Iteration MUST read payloads off the stream's own queue and MUST NOT touch that
  future. The consumption claim MUST be recorded on the stream, not on the future.
- **WSM-API-014** The first of the two shapes to be used claims the stream; the other MUST raise
  `StreamAlreadyConsumed`, with an error naming both uses. Two `async for` loops over one stream is the
  same error.
- **WSM-API-009** `peer.id` MUST be a short random prefix minted **once per process** plus a monotonic
  **per-connection** counter, rendered `<prefix>-<counter>` (e.g. `a3f-17`). An id MUST NOT be reused
  within a process and MUST NOT be duplicated within a process; across processes only the prefix may
  coincide. Reuse is the failure being prevented - two connections under one name read as one
  connection in a log - so a scheme that recycles ids of closed connections MUST NOT be used.
- **WSM-API-017** In Python, `peer.open(...)` on a line by itself MUST emit no `RuntimeWarning`.
- **WSM-API-021** The socket adapter protocol (`send_text`/`sendText`, `send_bytes`/`sendBytes`,
  `receive`, `close`, plus the handshake hook) MUST be the **only** place transport-specific code
  lives. Text and binary sends MUST be separate methods, never one polymorphic `send`.

### 4.6 Timeouts and cancellation (§8.3)

- **WSM-ERR-010** `request()` MUST have **no default timeout**. A stream lives until it ends, is reset,
  or the connection dies.
- **WSM-ERR-011** When a timeout is given and expires, the peer MUST send `reset(TIMEOUT)` (so the
  remote stops working) and MUST raise `StreamTimeout` locally.
- **WSM-ERR-009** `send()`, `end()` and `reply()` on a stream that is no longer open MUST raise, and
  the type MUST distinguish the three cases: `StreamClosed` when the stream closed **normally** (both
  ends ended), that stream's own `StreamReset` subclass when it was reset, and `ConnectionLost` when
  the socket died (WSM-RCN-041). `StreamClosed` MUST NOT be a `StreamReset` subclass and MUST NOT be
  a `ProtocolError`: a normal close racing a last `send()` is an expected outcome, not a failure and
  not a caller bug. Test: `stream_test.py::test_send_after_normal_close_raises_stream_closed`.
- **WSM-ERR-012** `stream.cancel()` MUST send `reset(CANCELLED)` and close the stream locally
  **immediately**, without waiting for acknowledgement. Payloads still in flight from the remote MUST
  be discarded.
- **WSM-ERR-013** On the remote side of a `reset(CANCELLED)`, the handler task MUST be cancelled: in
  Python by `task.cancel()` (the handler observes `asyncio.CancelledError` at its next `await`); in
  TypeScript by aborting `stream.signal`, with any subsequent `await stream.send(...)` rejecting with
  `StreamReset`. *(The Python half is in force here; the TypeScript half is implemented in M3, which
  reproduces this rule again.)*
- **WSM-ERR-014** Local `asyncio.CancelledError` propagating out of `await stream.result()` or an
  `async for` MUST cause the peer to send `reset(CANCELLED)` and re-raise `CancelledError`. It MUST NOT
  be swallowed.
- **WSM-ERR-015** An incoming `reset(APPLICATION_ERROR)` on a stream this peer is consuming MUST raise
  `RemoteError` out of the pending `await` or the `async for`.
- **WSM-ERR-003** All muxws errors MUST be raised from the awaiting call site and MUST NOT be swallowed
  into a callback.
- **WSM-RCN-041** At socket death, before `on_close` fires: a pending `await stream` /
  `await stream.result()` rejects with `ConnectionLost`, resolving the memoized future once so a second
  await gets the same error rather than hanging; an `async for` raises `ConnectionLost` out of the loop
  at the next iteration and MUST NOT terminate normally; an in-flight `request()` raises
  `ConnectionLost`, never a partial value and never a hang; `send()`, `end()` and `reply()` raise
  `ConnectionLost`; `cancel()` and `reset()` are no-ops; `stream.closed` is set.
- **WSM-INV-009** The awaited future MUST be memoized and the consumption claim MUST live on the
  stream - or a stream's payloads get split silently between an `await` and an `async for`, and neither
  consumer looks wrong locally.

## 5. Decisions this brief takes

Three of the four decisions this brief used to carry are now normative rules and are reproduced in §4
instead: `open()` has no `timeout` (WSM-API-018, deadlines live on `result()` and `request()`),
sending on a normally closed stream raises `StreamClosed` rather than `StreamReset(NO_ERROR)`
(WSM-ERR-009), and `peer.id` is a per-process prefix plus a per-connection counter (WSM-API-009).
None of the three is open any more; implement the rule, not a variant of it.

- **D1 - the per-connection counter is peer state, minted where the peer is.** `peer.id` is
  `f"{_PROCESS_PREFIX}-{next(_counter)}"`, where `_PROCESS_PREFIX` is three lowercase hex characters
  drawn once at import from `random.getrandbits(12)` with `# noqa: S311` and a comment that a log
  correlation id is not security-sensitive, and `_counter` is a module-level `itertools.count()` that
  is never rewound. Nothing recycles the id of a closed connection (WSM-API-009).
- **D2 - the memory transport is `muxws.transports.memory.memory_pair()`**, returning two
  `SocketAdapter`s wired to each other by two `asyncio.Queue`s, plus `drop()` on either side to
  simulate socket death. It is shipped in the package (not the test tree) because M5b's reconnect tests
  and M6's sequence runner both need it.

## 6. Implementation notes and skeletons

### `muxws/transports/__init__.py`

```python
class SocketAdapter(Protocol):
    """The only place transport-specific code lives (WSM-API-021)."""

    async def send_text(self, text: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def receive(self) -> str | bytes: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...
```

### `muxws/peer.py`

```python
class Peer:
    def __init__(self, socket: SocketAdapter, *, codec: Codec, is_dialer: bool,
                 error_serializer: Callable[[BaseException], Any] | None = None) -> None: ...

    # synchronous, and with no timeout parameter - WSM-API-001, WSM-API-008, WSM-API-018
    def open(self, payload: Any = None, *, headers: dict | None = None, end: bool = False) -> Stream: ...

    async def notify(self, payload: Any = None, *, headers: dict | None = None) -> None: ...
    async def request(self, payload: Any = None, *, headers: dict | None = None,
                      timeout: float | None = None) -> Any: ...

    def on_stream(self, handler: StreamHandler) -> StreamHandler: ...   # decorator or plain call
    def on_close(self, handler: Callable[[CloseReason], None]) -> None: ...
    def on_frame(self, handler: Callable[[str, Frame, int], None]) -> None: ...

    async def serve(self) -> None: ...        # the read loop; returns when the socket closes

    id: str                                   # "<prefix>-<counter>", e.g. a3f-17 - WSM-API-009, D1
    tags: dict[str, Any]                      # plain dict, created here, dies with the socket
    streams: Mapping[int, Stream]             # live streams, read-only
    is_open: bool
```

Allocation and enqueue in one synchronous step (WSM-SID-006/WSM-INV-005):

```python
def open(self, payload=None, *, headers=None, end=False) -> Stream:
    self._raise_if_unopenable()               # ConnectionLost / ConnectionGoingAway, no await
    stream_id = self._next_id                 # no await anywhere between these two lines
    self._next_id += 2
    stream = Stream(self, stream_id, ...)
    self._streams[stream_id] = stream
    self._writer.enqueue(Frame("open", stream=stream_id, payload=payload, headers=headers, end=end))
    return stream
```

The writer is a task draining an outbound queue; `enqueue` is synchronous and must stay so. Do not
`await socket.send_text(...)` inside `open()` - that is the suspension point WSM-SID-006 forbids, and
two concurrent `open()` calls would then put a non-monotonic id sequence on the wire.

### `muxws/stream.py`

```python
class Stream:
    id: int
    headers: dict           # from the open frame; empty for locally opened streams
    payload: Any            # opening payload, already reassembled (WSM-STM-032)
    trailers: dict | None
    closed: asyncio.Event

    async def send(self, payload: Any, *, end: bool = False) -> None: ...
    async def end(self, payload: Any = None, *, trailers: dict | None = None) -> None: ...
    async def reply(self, payload: Any, *, trailers: dict | None = None) -> None: ...
    async def result(self, timeout: float | None = None) -> Any: ...
    async def cancel(self, reason: str | None = None) -> None: ...
    async def reset(self, code: ResetCode, reason: str | None = None) -> None: ...
    def __await__(self): ...                  # memoized future - WSM-API-010
    def __aiter__(self): ...                  # own queue - WSM-API-013
```

The consumption claim is one attribute on the stream (`self._claim: str | None` holding `"await"` or
`"iterate"`), checked by both entry points, raising `StreamAlreadyConsumed` naming both uses
(WSM-API-014). The memoized future is created lazily on first await and resolved exactly once - a
second `await` returns the same value (WSM-API-011), and `result(timeout=...)` wraps
`asyncio.wait_for` around **that same future** rather than reading the queue (WSM-API-012).

### Implementation notes that matter

1. **`peer.open()` on a line by itself must emit no `RuntimeWarning`** (WSM-API-017). `Stream` is not a
   coroutine and must not be one; the awaitable-ness comes from `__await__`, which is never called if
   nobody awaits, so nothing is "never awaited".
2. **Python 3.10 is the floor**: `asyncio.timeout()` is 3.11+. Use `asyncio.wait_for`.
3. **Handler tasks are tracked.** Each dispatched handler runs in its own `asyncio.Task` held on the
   stream, so an incoming `reset(CANCELLED)` can `task.cancel()` it (WSM-ERR-013). Keep a strong
   reference or the garbage collector may cancel it for you.
4. **`ILL-C` means the connection dies**: emit `goaway(PROTOCOL_ERROR)` with a `reason`, then close the
   socket, then fail every live stream with `ConnectionLost` in that order.
5. **High-water marks are two integers**, one per parity, and the live-stream map. Nothing else may be
   retained per stream after close (WSM-STM-001).
6. **Lint**: `ARG` means an unused handler argument is `_payload` / `_stream`; `B008` will fire if you
   ever put a mutable default in a signature - use `None` and normalise inside; `PT` wants
   `pytest.raises(...)` and parametrized cases; `S311` fires on `random` in D1 and the correct
   resolution is `# noqa: S311` with the comment, never `secrets`.
7. **`CancelledError` is not an application error.** WSM-ERR-014 requires re-raising it after sending
   `reset(CANCELLED)`; a bare `except Exception` around the handler will not catch it in 3.10+, which
   is correct - do not widen it to `BaseException`.

## 7. Tests to write

All against `memory_pair()`; the dialer allocates odd ids, the acceptor even.

| # | Test | Asserts |
|---|---|---|
| 1 | `stream_test.py::test_every_state_table_cell` | Parametrized over all 45 cells of §4.3, including `n/r` cells asserted unreachable through the public API (WSM-STM-010/011). |
| 2 | `peer_test.py::test_parity_and_monotonicity` | Dialer ids 1,3,5; acceptor 2,4,6; never reused after close (WSM-SID-002/004). |
| 3 | `peer_test.py::test_concurrent_opens_produce_increasing_ids_on_the_wire` | 50 `open()` calls from separate tasks put strictly increasing ids on the wire (WSM-SID-006). |
| 4 | `stream_test.py::test_id_readable_immediately_after_open` | `stream.id` on the next line, nothing awaited between (WSM-SID-008). |
| 5 | `peer_test.py::test_frame_for_closed_id_is_ignored` | `data` for a closed-but-below-high-water id: no reset out, connection alive (WSM-STM-002). |
| 6 | `peer_test.py::test_frame_above_high_water_mark_kills_connection` | `goaway(PROTOCOL_ERROR)` then close (WSM-STM-003). |
| 7 | `peer_test.py::test_wrong_parity_and_non_monotonic_open_kill_connection` | Both are ILL-C (WSM-SID-005). |
| 8 | `peer_test.py::test_data_after_end_resets_only_that_stream` | ILL-S: `reset(PROTOCOL_ERROR)` on that stream, other streams unaffected, connection alive (WSM-STM-020/021). |
| 9 | `peer_test.py::test_unknown_frame_type_is_ignored` | An unknown `type` is dropped, logged once, connection alive (WSM-FRM-002). |
| 10 | `peer_test.py::test_no_handler_refuses_with_refused` | Incoming `open` with no `on_stream` gets `reset(REFUSED)` (WSM-STM-033). |
| 11 | `peer_test.py::test_handler_raising_produces_application_error` | Even after the handler already sent a payload, the reset code is `APPLICATION_ERROR`, never `REFUSED`; `reason` and the serialized payload are present (WSM-STM-034, WSM-ERR-006). |
| 12 | `peer_test.py::test_error_serializer_hook_replaces_the_default` | A custom `error_serializer` supplies the reset `payload`. |
| 13 | `peer_test.py::test_handler_returning_ends_stream_implicitly` | Handler returns without `end()`; the opener sees `data(end=true)` (WSM-STM-035). |
| 14 | `peer_test.py::test_second_on_stream_replaces_and_logs` | Only the second handler runs; a log record is emitted (WSM-STM-030). |
| 15 | `peer_test.py::test_fragmented_open_reaches_handler_whole` | A two-fragment `open` invokes the handler once, with the reassembled payload, also visible as `stream.payload` (WSM-STM-031/032). |
| 16 | `stream_test.py::test_await_twice_returns_same_value` | The memoized future (WSM-API-010/011). |
| 17 | `stream_test.py::test_await_then_iterate_raises_and_first_consumer_got_everything` | `StreamAlreadyConsumed` naming both uses; the first consumer's payloads are intact. Two `async for` loops are the same error (WSM-API-014). |
| 18 | `stream_test.py::test_open_resolves_first_payload_while_request_raises` | Against a remote sending two payloads then ending: `await peer.open(p)` resolves with the first and does not raise; `await peer.request(p)` raises (WSM-API-006/007). |
| 19 | `stream_test.py::test_unconsumed_open_emits_no_runtime_warning` | `peer.open(p)` on a line by itself, under `warnings.catch_warnings(record=True)` (WSM-API-017). |
| 20 | `stream_test.py::test_result_timeout_uses_the_same_future` | `result(timeout=...)` returns the same value a prior `await` returned, and raises `StreamTimeout` plus emits `reset(TIMEOUT)` on expiry (WSM-API-012, WSM-ERR-011). |
| 21 | `stream_test.py::test_open_takes_no_timeout_argument` | `inspect.signature(Peer.open)` has no `timeout` parameter, and `peer.open(p, timeout=1)` is a `TypeError` - the deadline belongs on `result()` / `request()` (WSM-API-018). |
| 22 | `stream_test.py::test_send_after_normal_close_raises_stream_closed` **(spec)** | `StreamClosed` after both ends ended - and it is neither a `StreamReset` nor a `ProtocolError`; a stream that was reset instead re-raises its own `StreamReset` subclass, so a cancelled stream raises `StreamReset(CANCELLED)` (WSM-ERR-009). |
| 22a | `peer_test.py::test_peer_id_is_prefix_plus_monotonic_counter` | Ids match `^[0-9a-f]{3}-\d+$`; a hundred peers in one process are all distinct, the counter never rewinds, and closing a peer does not free its id for reuse (WSM-API-009). |
| 23 | `stream_test.py::test_cancel_is_immediate_and_discards_in_flight` | `cancel()` closes locally without waiting; payloads arriving after it are discarded (WSM-ERR-012). |
| 24 | `stream_test.py::test_remote_cancel_cancels_the_handler_task` | The remote handler observes `CancelledError` at its next `await` (WSM-ERR-013). |
| 25 | `stream_test.py::test_local_cancellation_sends_reset_and_reraises` | `CancelledError` out of `await stream.result()` sends `reset(CANCELLED)` and propagates (WSM-ERR-014). |
| 26 | `stream_test.py::test_remote_application_error_raises_remote_error` | `RemoteError` with `.payload` out of both the `await` and the `async for` (WSM-ERR-015). |
| 27 | `peer_test.py::test_socket_death_fails_every_shape` | N streams open in every shape at once - pending await, iteration, in-flight `request`, blocked `send` - each raising `ConnectionLost`; `cancel`/`reset` are no-ops; `closed` set; `on_close` fires after all of it. **The test must fail by hang detection** (a `wait_for` around the whole assertion block), not by an error nobody raised (WSM-STM-014, WSM-RCN-041). |
| 28 | `peer_test.py::test_notify_returns_none_and_leaves_no_handle` | `notify()` returns `None`; nothing awaitable is produced (WSM-API-005). |
| 29 | `peer_test.py::test_no_bookkeeping_survives_a_closed_stream` | After 1000 opened-and-closed streams, the peer holds only two high-water ints and an empty live map (WSM-STM-001). |
| 30 | `conformance_test.py::test_invalid_fixtures` | Parametrized over all eight `conformance/invalid/*.json`: the expected frames go out and `connection_survives` matches (WSM-TST-003). The over-cap and undecodable cases use a codec double, since size enforcement itself lands in M5a. |
| 31 | `transports/memory_test.py::test_pair_delivers_and_drop_kills_both_directions` | The transport itself, so a failure in it never gets misread as a peer bug. |

## 8. Done when

- [ ] `ruff check .` and `ruff format --check .` pass.
- [ ] `pytest --cov=muxws` passes with `peer.py` and `stream.py` above 95 % line coverage.
- [ ] All 45 state-table cells appear as parametrized test ids, and removing any transition from the
      implementation makes exactly one of them fail.
- [ ] The whole suite finishes in under 30 s with no test relying on wall-clock sleeps longer than
      50 ms (deadlines are injected, not waited out).
- [ ] `conformance/invalid/*.json` runs green from Python.
- [ ] No test opens a socket, imports `starlette`, `websockets` or `ws`, or reads an environment
      variable.

## 9. Out of scope

`ping`/`pong`, `goaway` drain and `close()` (all M4 - in M2 `open()` raises only `ConnectionLost`).
The receiver's concurrency limit and its `reset(REFUSED)` (WSM-STM-036/037, M5a): M2 imposes no limit
on how many streams the remote may open, and `open()` never fails for concurrency in any milestone.
Real transports, `MUXWS_CODEC`, the subprotocol assertion and the whole TypeScript port (M3).
Fragmentation wired into the send path, the one-unsent-fragment rule, the round-robin writer and the
receive-side caps (M5a); `PeerRegistry`, `on_frame` beyond the plain hook, and reconnect (M5b). There
is no `settings` frame to exchange anywhere in this project (WSM-CON-031). No
`conformance/sequences/` fixture is executed here: M4 adds the first one, M5a adds the round-robin
one, and M6 completes and replays the corpus.
