"""`Peer`: one end of one WebSocket, symmetric by construction (§5, §9, WSM-INV-002)."""

from __future__ import annotations

import asyncio
import itertools
import logging
import random

from collections.abc import Callable, Mapping
from typing import Any, TYPE_CHECKING

from muxws.codecs import Codec
from muxws.errors import (
    ConnectionClosed,
    ConnectionGoingAway,
    ConnectionLost,
    exception_for_reset,
    ProtocolError,
    RemoteError,
    ResetCode,
    StreamReset,
    StreamTimeout,
)
from muxws.fragment import encoded_length, MAX_FRAME_BYTES
from muxws.frames import ABSENT, Frame
from muxws.lifecycle import GoawayState, MAX_STREAM_ID, new_nonce, PingRegistry
from muxws.observability import CloseReason, log_frame
from muxws.stream import Stream, StreamState
from muxws.transports import SocketAdapter
from muxws.writer import CONNECTION_LANE, LaneEncodingError, Writer

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.reconnect import ConnectionLoop

logger = logging.getLogger("muxws.frames")

#: Three lowercase hex characters, drawn once per process. A log correlation id is not
#: security-sensitive, so `random` is right here and `secrets` would be cargo cult.
_PROCESS_PREFIX = f"{random.getrandbits(12):03x}"  # noqa: S311
#: Never rewound, and never consulted for reuse: two connections under one name read as one
#: connection in a log, which is the failure WSM-API-009 exists to prevent.
_CONNECTION_COUNTER = itertools.count()

StreamHandler = Callable[[Any, Stream], Any]
ErrorSerializer = Callable[[BaseException], Any]


def _loop_time() -> float:
    """The heartbeat's clock. One function, so an injected clock moves both halves together."""
    return asyncio.get_running_loop().time()


def default_error_serializer(exc: BaseException) -> Any:
    """WSM-ERR-006's default. A public-facing deployment should replace it with a redacting one."""
    return {"type": type(exc).__name__, "message": str(exc)}


class Peer:
    """One symmetric peer type per language: server push is a client request with the roles swapped."""

    def __init__(
        self,
        socket: SocketAdapter,
        *,
        codec: Codec,
        is_dialer: bool,
        error_serializer: ErrorSerializer | None = None,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        max_payload_bytes: int = 67_108_864,
        max_concurrent_streams: int = 100,
    ) -> None:
        self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
        #: An ordinary dict with ordinary dict semantics. muxws never reads it (WSM-REG-001/002).
        self.tags: dict[str, Any] = {}

        self._socket = socket
        self._codec = codec
        self._is_dialer = is_dialer
        self._error_serializer = error_serializer or default_error_serializer
        self._max_frame_bytes = _checked_frame_cap(max_frame_bytes, codec)
        #: The largest reassembled payload this peer accepts. **Local** (WSM-FRG-035): never
        #: announced, and a sender learns of it only from the reset it provokes.
        self._max_payload_bytes = max_payload_bytes
        #: How many streams the **remote** may have open here at once. Also local, also unannounced,
        #: and never checked by the sender (WSM-STM-036).
        self._max_concurrent_streams = max_concurrent_streams

        # The dialer allocates odd ids, the acceptor even ones (WSM-SID-002).
        self._next_id = 1 if is_dialer else 2
        self._streams: dict[int, Stream] = {}
        #: Exactly two integers plus the live-stream map; nothing per closed stream (WSM-STM-001).
        self._highest_local_open = 0
        self._highest_remote_open = 0
        self._ignored_late_frames = 0

        self._handler: StreamHandler | None = None
        self._close_handlers: list[Callable[[Any], None]] = []
        self._reconnect_handlers: list[Callable[[int, Any], None]] = []
        #: True while the reconnect helper intends to dial again. `CloseReason.will_retry` reads it,
        #: and it is false whenever `max_attempts` is exhausted or `close()` was deliberate. Read
        #: only: what the helper *does* next is decided by `should_retry`, never by this field.
        self._will_retry = False
        #: Latched by the first `will_retry=False` close, so there is never a second (WSM-RCN-044).
        self._final_close_reported = False
        self._frame_handlers: list[Callable[[str, Frame, int], None]] = []

        self._writer = Writer(codec, max_frame_bytes=self._max_frame_bytes)
        self._pings = PingRegistry()
        self._ping_started: dict[str, float] = {}
        self._goaway = GoawayState()
        self._writer_task: asyncio.Task[None] | None = None
        self._is_open = True
        #: Socket-open is not established. WSM-RCN-043 asks for `is_open` to be false for the whole
        #: window between a socket loss and the next *established* connection, and when a hello is
        #: configured the hello ack is the whole of the difference (WSM-RCN-004). Default **true**,
        #: so an acceptor - which has no hello and no helper - is established the moment its socket
        #: is (WSM-CON-030); only the reconnect helper ever writes it.
        self._established = True
        self._death: ConnectionClosed | None = None
        #: Held so the garbage collector cannot cancel the orderly shutdown WSM-SID-007 requires.
        self._exhaustion_task: asyncio.Task[None] | None = None

        #: One clock for both halves of the heartbeat: the stamp and the idle check read the same
        #: function, so an injected clock cannot move one without the other (WSM-RCN-010).
        self._clock: Callable[[], float] = _loop_time
        #: Stamped by `_report_frame` in **both** directions, because idle means idle: a busy socket
        #: must not pay for a ping every interval (WSM-RCN-010, §6).
        self._last_activity: float = 0.0
        #: The dialer's reconnect helper, held here so `close()` can stop it - a deliberate close
        #: MUST never dial again (WSM-RCN-040/044) - and so the GC cannot collect the supervisor.
        self._connection_loop: ConnectionLoop | None = None

    # ------------------------------------------------------------------ properties

    @property
    def is_open(self) -> bool:
        """Socket-open **and** established (WSM-RCN-043).

        Both halves, because a socket that is up but whose hello has not been acknowledged is not a
        connection an application may send on: frames put on it would precede the hello (WSM-RCN-023)
        and reach an acceptor that has not yet been told who is speaking.
        """
        return self._is_open and self._established

    @property
    def _has_a_socket(self) -> bool:
        """Whether a frame put on the writer now can still reach a wire.

        Not the same question as `is_open`, and the difference is why this exists. `is_open` answers
        "may the application start something here", which the hello window makes false (WSM-RCN-043).
        This answers "is there a socket underneath", which that window does not make false - the
        hello itself travels on it. `Stream.reset()` needs the second question: a stream the acceptor
        pushed during the hello window that is reset by its handler must put the `reset` on the wire,
        or the remote is left holding a stream this side has already closed (WSM-STM-021).
        """
        return self._is_open

    @property
    def last_activity(self) -> float:
        """When a frame last crossed this socket, in either direction, on `self._clock`."""
        return self._last_activity

    @property
    def streams(self) -> Mapping[int, Stream]:
        """Live streams, read-only."""
        return dict(self._streams)

    @property
    def is_dialer(self) -> bool:
        return self._is_dialer

    # ------------------------------------------------------------------ registration

    def on_stream(self, handler: StreamHandler) -> StreamHandler:
        """Register the one incoming-stream handler. A second replaces the first and logs."""
        if self._handler is not None:
            logger.warning("muxws conn=%s replacing the on_stream handler (WSM-STM-030)", self.id)
        self._handler = handler
        return handler

    def on_close(self, handler: Callable[[Any], None]) -> Callable[[Any], None]:
        self._close_handlers.append(handler)
        return handler

    def on_reconnect(self, handler: Callable[[int, Any], None]) -> Callable[[int, Any], None]:
        """Fires once per **re-established** connection (WSM-RCN-030).

        It guarantees exactly two things and nothing more: a live socket, and an identity the
        acceptor has already accepted on it. No stream survives a reconnect, nothing is replayed, and
        the new socket's id space starts empty (WSM-RCN-031/032).
        """
        self._reconnect_handlers.append(handler)
        return handler

    def _fire_reconnect(self, attempt: int) -> None:
        # Isolated, because this runs on the supervisor's own task: an application handler that
        # raised would unwind into the reconnect loop and stop it for good, and the next socket loss
        # would never be dialled out of (WSM-RCN-011/030). A library whose reconnect loop can be
        # killed by an application's logging call is not a reconnect loop.
        for handler in self._reconnect_handlers:
            try:
                handler(attempt, self)
            except Exception:  # noqa: BLE001 - one bad handler must not cost the others their turn
                logger.exception("muxws conn=%s an on_reconnect handler raised", self.id)

    def on_frame(self, handler: Callable[[str, Frame, int], None]) -> Callable[[str, Frame, int], None]:
        """`(direction, frame, byte_length)`, before encode and after decode (WSM-OBS-003)."""
        self._frame_handlers.append(handler)
        return handler

    # ------------------------------------------------------------------ opening

    def open(self, payload: Any = None, *, headers: dict[str, Any] | None = None, end: bool = False) -> Stream:
        """Open a stream. **Synchronous**, and it never queues (WSM-API-001/004).

        The guard and the allocation are separate methods because the reconnect helper's hello is the
        one stream that goes out while `is_open` is still false - it is what *makes* the connection
        established (WSM-RCN-004/043) - and it takes `_allocate_and_enqueue` directly. The guard is
        not parameterised for it: an `open(..., ignore_the_guard=True)` on the public method is one
        misread argument away from an application frame preceding the hello (WSM-RCN-023).
        """
        self._raise_if_unopenable()
        return self._allocate_and_enqueue(payload, headers=headers, end=end)

    def _allocate_and_enqueue(self, payload: Any, *, headers: dict[str, Any] | None, end: bool) -> Stream:
        """Allocate an id and enqueue the `open` - one indivisible step (WSM-SID-006).

        There is no suspension point anywhere between the allocation and the enqueue, so wire order
        is allocation order by construction. Two concurrent `open()` calls that could interleave here
        would put a non-monotonic id sequence on the wire - a protocol error this peer would be
        committing against itself (WSM-INV-005).
        """
        if self._exhausted():
            raise ConnectionGoingAway(
                f"stream ids are exhausted at {MAX_STREAM_ID}; this connection can open no more (WSM-SID-007)"
            )
        stream_id = self._next_id
        self._next_id += 2
        self._highest_local_open = stream_id
        if self._exhausted():
            self._begin_exhaustion_shutdown()
        stream = Stream(self, stream_id, headers=headers, payload=payload, local=True)
        stream.state = StreamState.HALF_CLOSED_LOCAL if end else StreamState.OPEN
        self._streams[stream_id] = stream
        self._enqueue(Frame("open", stream=stream_id, payload=payload, headers=headers, end=end))
        return stream

    def _raise_if_unopenable(self) -> None:
        """WSM-API-004: exactly two synchronous raises, and never one for concurrency."""
        # `is_open`, not `_is_open`: a socket whose hello has not been acknowledged is not yet a
        # connection, and a frame opened on it would precede the hello (WSM-RCN-023/043).
        if not self.is_open:
            raise ConnectionLost("the peer is between sockets; nothing is buffered for the next one")
        if self._goaway.received:
            raise ConnectionGoingAway(
                f"the remote sent goaway (code {self._goaway.received_code}); no new stream can be "
                f"opened on this connection. Dial again to get one that can."
            )
        if self._goaway.sent:
            raise ConnectionGoingAway("this peer sent goaway and opens no further streams (WSM-CON-021)")

    async def notify(self, payload: Any = None, *, headers: dict[str, Any] | None = None) -> None:
        """One-shot push. Returns nothing and produces no awaitable handle (WSM-API-005)."""
        stream = self.open(payload, headers=headers, end=True)
        stream._claim = "notify"
        return None

    async def request(
        self,
        payload: Any = None,
        *,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """`open(payload, end=True)` awaited to the stream's end (WSM-API-006).

        Unlike `await stream`, this polices a second payload: a unary call that quietly discarded
        extra values would hide a handler bug rather than report it (WSM-API-007).
        """
        stream = self.open(payload, headers=headers, end=True)
        return await self._collect_unary(stream, timeout)

    async def _collect_unary(self, stream: Stream, timeout: float | None) -> Any:
        async def collect() -> Any:
            payloads: list[Any] = []
            async for item in stream:
                payloads.append(item)
                if len(payloads) > 1:
                    await stream.reset(ResetCode.PROTOCOL_ERROR, "request() received more than one payload")
                    raise ProtocolError(
                        f"request() on stream {stream.id} received more than one payload; use "
                        f"peer.open() and iterate if the remote streams (WSM-API-006)"
                    )
            if not payloads:
                raise ProtocolError(f"request() on stream {stream.id} ended without a payload")
            return payloads[0]

        if timeout is None:
            return await collect()

        # Shielded, so the deadline cancels the *wait* and not the collector. Cancelling the
        # collector first would make its own cancellation handler send reset(CANCELLED), and the
        # remote would be told the caller changed its mind rather than that a deadline expired
        # (WSM-ERR-011). The reset goes out with TIMEOUT before the collector is torn down.
        collector = asyncio.ensure_future(collect())
        try:
            return await asyncio.wait_for(asyncio.shield(collector), timeout)
        except asyncio.TimeoutError:
            await stream.reset(ResetCode.TIMEOUT, f"deadline of {timeout}s expired")
            collector.cancel()
            await asyncio.gather(collector, return_exceptions=True)
            raise StreamTimeout(f"stream {stream.id} did not answer within {timeout}s", stream_id=stream.id) from None

    # ------------------------------------------------------------------ the writer

    def _enqueue(self, frame: Frame) -> None:
        """Synchronous by contract: `open()` must not suspend between allocating and enqueuing."""
        self._writer.enqueue(frame)

    async def _write_loop(self) -> None:
        while True:
            try:
                frame = await self._writer.next_frame()
            except LaneEncodingError as failure:
                self._fail_lane(failure)
                continue
            if frame is None:
                return
            try:
                encoded = self._codec.encode(frame)
                self._report_frame("tx", frame, encoded)
                if self._codec.binary:
                    await self._socket.send_bytes(encoded)  # type: ignore[arg-type]
                else:
                    await self._socket.send_text(encoded)  # type: ignore[arg-type]
                # Only now: fragment n+1 is sliced once fragment n has reached the socket, never
                # before (WSM-FRG-018).
                self._writer.advance(frame.stream if frame.stream is not None else CONNECTION_LANE)
            except ConnectionClosed:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # A codec that cannot encode this frame - a bytes payload under JSON, say - used to
                # take the writer task down with it. Nothing then drained the queue, every later
                # send sat in it forever, and the peer went on reporting itself open.
                logger.exception("muxws conn=%s could not send a %s frame", self.id, frame.type)
                self._fail_unsendable(frame, exc)

    def _fail_lane(self, failure: LaneEncodingError) -> None:
        """One lane's frame could not be encoded. Fail its stream; keep the connection working."""
        logger.exception(
            "muxws conn=%s could not encode a frame on stream %s", self.id, failure.lane, exc_info=failure.cause
        )
        stream = self._streams.get(failure.lane)
        if stream is None:
            self._die(ConnectionClosed(str(failure), code=1011))
            return
        self._enqueue(Frame("reset", stream=stream.id, code=int(ResetCode.INTERNAL_ERROR), reason=str(failure.cause)))
        stream._fail(
            exception_for_reset(ResetCode.INTERNAL_ERROR, str(failure.cause), stream_id=stream.id),
            notify_remote=False,
        )

    def _fail_unsendable(self, frame: Frame, exc: BaseException) -> None:
        """One frame could not be encoded. Fail its stream and keep the connection working."""
        stream = self._streams.get(frame.stream) if frame.stream is not None else None
        if stream is None:
            self._die(ConnectionClosed(f"could not encode a {frame.type} frame: {exc}", code=1011))
            return
        self._enqueue(Frame("reset", stream=stream.id, code=int(ResetCode.INTERNAL_ERROR), reason=str(exc)))
        stream._fail(
            exception_for_reset(ResetCode.INTERNAL_ERROR, str(exc), stream_id=stream.id),
            notify_remote=False,
        )

    def _report_frame(self, direction: str, frame: Frame, encoded: str | bytes) -> None:
        # Both directions, and every frame type: a socket carrying data one way only is not idle, and
        # a heartbeat that pinged it anyway would burn an interval's worth of frames for nothing
        # (WSM-RCN-010).
        self._last_activity = self._clock()
        length = encoded_length(encoded)
        # Isolated for the same reason as `_fire_reconnect`: this runs inside the read loop and
        # inside the write loop, so an observer that raised would kill the connection it was only
        # meant to be watching (WSM-OBS-003).
        for handler in self._frame_handlers:
            try:
                handler(direction, frame, length)
            except Exception:  # noqa: BLE001 - an observer is not allowed to break what it observes
                logger.exception("muxws conn=%s an on_frame handler raised", self.id)
        log_frame(self.id, direction, frame, length)

    # ------------------------------------------------------------------ the read loop

    async def serve(self) -> None:
        """Run the read loop until the socket closes."""
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._write_loop())
        try:
            while True:
                try:
                    message = await self._socket.receive()
                except ConnectionClosed as exc:
                    self._die(exc)
                    return
                try:
                    frame = self._codec.decode(message)
                except ProtocolError as exc:
                    await self._fail_connection(f"undecodable message: {exc}")
                    return
                self._report_frame("rx", frame, message)
                if not await self._within_frame_cap(frame, message):
                    continue
                try:
                    keep_going = await self._dispatch(frame)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    # A peer whose read loop died while still reporting `is_open` is the worst
                    # possible state: every pending await hangs, `on_close` never fires, and
                    # `open()` keeps succeeding into a queue nobody drains.
                    logger.exception("muxws conn=%s read loop failed on a %s frame", self.id, frame.type)
                    self._die(ConnectionClosed(f"read loop failed: {exc}", code=1011, reason=str(exc)))
                    return
                if not keep_going:
                    return
        finally:
            await self._stop_writer()

    async def _within_frame_cap(self, frame: Frame, message: str | bytes) -> bool:
        """WSM-FRG-031: measure the **whole encoded message**, never the `fragment` field alone.

        A receiver must accept anything up to `MAX_FRAME_BYTES` (WSM-FRG-004), so the check only
        bites above the constant - or, in a test, above the lowered construction cap.
        """
        size = encoded_length(message)
        if size <= self._max_frame_bytes:
            return True
        if frame.stream is None:
            await self._fail_connection(f"a connection-level frame of {size} bytes exceeds the cap")
            return False

        stream = self._streams.get(frame.stream)
        if stream is not None:
            await self._reset_stream(
                stream,
                ResetCode.PAYLOAD_TOO_LARGE,
                f"an encoded message of {size} bytes exceeds this receiver's cap (WSM-FRG-031)",
            )
        else:
            self._enqueue(
                Frame(
                    "reset",
                    stream=frame.stream,
                    code=int(ResetCode.PAYLOAD_TOO_LARGE),
                    reason=f"an encoded message of {size} bytes exceeds this receiver's cap",
                )
            )
        return False

    async def _dispatch(self, frame: Frame) -> bool:
        """Route one decoded frame. Returns False when the connection must end."""
        if frame.type == "open":
            return await self._on_open(frame)
        if frame.type in ("data", "reset"):
            return await self._on_stream_frame(frame)
        if frame.type == "ping":
            # Echoed verbatim and at once, with no application involvement whatever (WSM-CON-010).
            self._enqueue(Frame("pong", nonce=frame.nonce))
            return True
        if frame.type == "pong":
            self._on_pong(frame)
            return True
        if frame.type == "goaway":
            return await self._on_goaway(frame)
        # WSM-FRM-002: unknown types are ignored, logged once, and are never an error.
        logger.info("muxws conn=%s ignoring unknown frame type %r (WSM-FRM-002)", self.id, frame.type)
        return True

    # ------------------------------------------------------------------ liveness

    async def ping(self, timeout: float = 5.0) -> float:
        """Round-trip time in **seconds**.

        A `ping` frame, not a WebSocket control frame: browsers do not expose those to JavaScript, so
        a liveness mechanism built on them cannot work on half the peers that exist (WSM-CON-011).
        """
        # `_has_a_socket`, not `is_open`: a ping asks whether there is a wire to put a frame on, and
        # the hello window makes `is_open` false while the socket is perfectly alive (WSM-RCN-043).
        # The heartbeat is the caller that matters and it only runs on an established connection, so
        # the behaviour is unchanged - what goes away is a public call succeeding on a peer that
        # reports `is_open is False`.
        if not self._has_a_socket:
            raise ConnectionLost("cannot ping a peer that is between sockets")

        nonce = new_nonce()
        waiting = self._pings.open(nonce)
        self._ping_started[nonce] = asyncio.get_running_loop().time()
        self._enqueue(Frame("ping", nonce=nonce))
        try:
            return await asyncio.wait_for(waiting, timeout)
        except asyncio.TimeoutError:
            self._pings.give_up(nonce)
            self._ping_started.pop(nonce, None)
            raise ConnectionClosed(f"no pong within {timeout}s", code=1006) from None

    def _on_pong(self, frame: Frame) -> None:
        """Settle the ping this nonce belongs to; a nonce nobody is waiting for is dropped."""
        if frame.nonce is None:
            return
        elapsed = asyncio.get_running_loop().time() - self._ping_started.get(frame.nonce, 0.0)
        if not self._pings.settle(frame.nonce, elapsed):
            logger.debug("muxws conn=%s pong for an unknown nonce, ignored", self.id)
        self._ping_started.pop(frame.nonce, None)

    # ------------------------------------------------------------------ shutdown

    async def close(self, code: ResetCode = ResetCode.NO_ERROR, reason: str | None = None, drain: float = 10.0) -> None:
        """`goaway`, drain, then close - in that order (WSM-CON-025)."""
        # Both of these come **before** the `is_open` guard, and that ordering is the whole of
        # WSM-RCN-040/044 for a dialer. The gap between two sockets is precisely the window in which
        # `is_open` is already false and the helper is asleep in its backoff: a close that returned
        # early there would stop nothing, the driver would dial again behind the caller, and the
        # application would be handed back a live connection it had already given up on - on the same
        # `Peer`, so it would never think to close it a second time.
        self._will_retry = False
        if self._connection_loop is not None:
            await self._connection_loop.stop()
        if not self._is_open:
            return
        self._send_goaway(code, reason)
        await self._drain(drain)
        await self._socket.close(1000, reason or "")
        self._die(ConnectionClosed(reason or "closed", code=1000, reason=reason or "", was_clean=True))

    def _send_goaway(self, code: ResetCode, reason: str | None) -> None:
        """`last_stream` is the highest id **the remote** opened that we have dispatched.

        Their parity, not ours. Getting it backwards makes every drain reset everything, which looks
        like a race rather than like an arithmetic mistake.
        """
        if self._goaway.sent:
            return
        self._goaway.sent = True
        self._goaway.sent_code = int(code)
        self._enqueue(Frame("goaway", code=int(code), reason=reason, last_stream=self._highest_remote_open))

    async def _on_goaway(self, frame: Frame) -> bool:
        """The remote is stopping. Refuse what it never processed; let the rest finish."""
        self._goaway.received = True
        self._goaway.received_code = frame.code
        self._goaway.received_reason = frame.reason
        self._goaway.remote_last_stream = frame.last_stream

        # Streams above the cut-off were never processed, so they are safe to retry elsewhere - and
        # nothing goes out for them: the remote has already stopped reading (WSM-CON-023).
        for stream in list(self._streams.values()):
            if stream.local and not self._goaway.survives_drain(stream.id):
                stream._fail(
                    exception_for_reset(
                        ResetCode.REFUSED,
                        f"the remote went away before processing stream {stream.id}",
                        stream_id=stream.id,
                    ),
                    notify_remote=False,
                )
        return True

    async def _drain(self, timeout: float) -> None:
        """Let streams at or below the cut-off finish, then close regardless (WSM-CON-024).

        A deadline, not a poll loop: a peer that waited for quiet would never close against a remote
        that keeps one stream open.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while self._streams and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0)
        # Whatever is still live at the deadline takes the socket-death path: it fails locally and
        # nothing is sent for it, because the socket is about to be gone.
        await self._drain_outbound()

    def _exhausted(self) -> bool:
        return self._next_id > MAX_STREAM_ID

    def _begin_exhaustion_shutdown(self) -> None:
        """WSM-SID-007: running out of ids is an orderly shutdown, not an error.

        The rule asks for four things - send `goaway`, stop opening, let in-flight streams drain,
        then close - and only the second of them is something `open()` can do by returning. The other
        three are scheduled rather than awaited, because `open()` is specified to return a `Stream`
        without suspending (WSM-API-001): a call that blocked here to drain would be a different
        method. The stream just allocated is in flight and gets its drain window like any other.
        """
        if self._goaway.sent or self._exhaustion_task is not None:
            return
        self._exhaustion_task = asyncio.create_task(
            self.close(ResetCode.NO_ERROR, f"stream ids exhausted at {MAX_STREAM_ID} (WSM-SID-007)")
        )

    # ------------------------------------------------------------------ inbound opens

    async def _on_open(self, frame: Frame) -> bool:
        stream_id = frame.stream
        if stream_id is None:
            await self._fail_connection("open frame with no stream id")
            return False

        existing = self._streams.get(stream_id)
        if existing is not None and existing._opening and frame.fragment is not None:
            return await self._continue_open(existing, frame)

        remote_parity = 0 if self._is_dialer else 1
        if stream_id % 2 != remote_parity:
            await self._fail_connection(f"open on stream {stream_id}: wrong parity for the remote (WSM-SID-005)")
            return False
        if stream_id <= self._highest_remote_open:
            await self._fail_connection(
                f"open on stream {stream_id} is not greater than the remote's highest previous open "
                f"{self._highest_remote_open} (WSM-SID-005)"
            )
            return False

        self._highest_remote_open = stream_id

        if self._remote_stream_count() >= self._max_concurrent_streams:
            # Refused **without invoking the handler**: nothing ran, so the opener may safely take it
            # elsewhere (WSM-STM-036). There is no STREAM_LIMIT code and no announced quota - the
            # sender is told nothing in advance and learns only from this reset.
            self._enqueue(
                Frame(
                    "reset",
                    stream=stream_id,
                    code=int(ResetCode.REFUSED),
                    reason=f"this receiver already holds {self._max_concurrent_streams} of your streams",
                )
            )
            return True

        stream = Stream(self, stream_id, headers=frame.headers, local=False)
        stream.state = StreamState.HALF_CLOSED_REMOTE if frame.end else StreamState.OPEN
        self._streams[stream_id] = stream

        if frame.fragment is not None:
            stream._opening = True
            # A fragmented *open* is the same memory exposure as a fragmented *data*, and was the one
            # path into this peer that no cap watched.
            if not await self._within_payload_cap(stream, frame):
                return True
            payload = stream._assembler.feed(frame, self._codec)
            if payload is ABSENT:
                return True
            stream._opening = False
            stream.payload = payload
        else:
            stream.payload = frame.payload if frame.payload is not ABSENT else None

        self._start_handler(stream)
        return True

    async def _continue_open(self, stream: Stream, frame: Frame) -> bool:
        """A later fragment of an opening payload: not a second open (WSM-STM-031)."""
        if not await self._within_payload_cap(stream, frame):
            return True
        payload = stream._assembler.feed(frame, self._codec)
        if payload is ABSENT:
            return True
        stream._opening = False
        stream.payload = payload
        if frame.end:
            stream.state = StreamState.HALF_CLOSED_REMOTE
        self._start_handler(stream)
        return True

    def _start_handler(self, stream: Stream) -> None:
        """Dispatch to the one handler, or refuse when there is none (WSM-STM-033)."""
        if self._goaway.sent:
            # We have said we are stopping; nothing new runs here. REFUSED promises it did not run,
            # so the opener may safely take it elsewhere (WSM-CON-021).
            self._enqueue(
                Frame("reset", stream=stream.id, code=int(ResetCode.REFUSED), reason="this peer is going away")
            )
            stream._fail(
                exception_for_reset(ResetCode.REFUSED, "peer is going away", stream_id=stream.id),
                notify_remote=False,
            )
            return
        if self._handler is None:
            self._enqueue(Frame("reset", stream=stream.id, code=int(ResetCode.REFUSED), reason="no on_stream handler"))
            stream._fail(exception_for_reset(ResetCode.REFUSED, "no handler", stream_id=stream.id), notify_remote=False)
            return
        # Held on the stream so an incoming reset(CANCELLED) can cancel it (WSM-ERR-013).
        stream.handler_task = asyncio.create_task(self._run_handler(stream))

    async def _run_handler(self, stream: Stream) -> None:
        try:
            result = self._handler(stream.payload, stream)  # type: ignore[misc]
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            # Not an application error. WSM-ERR-014 keeps it propagating rather than swallowing it.
            raise
        except Exception as exc:  # noqa: BLE001 - every handler failure becomes one reset
            await self._reset_for_handler_error(stream, exc)
            return
        # WSM-STM-035: a handler that returns without ending its stream ends it implicitly.
        if stream.state in (StreamState.OPEN, StreamState.HALF_CLOSED_REMOTE):
            try:
                await stream.end()
            except (StreamReset, ConnectionLost):
                pass

    async def _reset_for_handler_error(self, stream: Stream, exc: BaseException) -> None:
        """WSM-STM-034/WSM-INV-008: **always** APPLICATION_ERROR, never REFUSED.

        REFUSED promises the operation definitively did not happen. A handler that debits an account
        and then raises would, under REFUSED, be inviting the client to retry the debit.
        """
        try:
            payload = self._error_serializer(exc)
        except Exception:  # noqa: BLE001
            # WSM-STM-034 is unconditional. A serializer that raises must not swallow the reset with
            # it, or the handler's failure reaches the opener as silence and the caller waits forever.
            logger.exception("muxws conn=%s error_serializer raised; sending the reset without a payload", self.id)
            payload = None
        if stream.state is not StreamState.CLOSED:
            self._enqueue(
                Frame(
                    "reset",
                    stream=stream.id,
                    code=int(ResetCode.APPLICATION_ERROR),
                    reason=str(exc),
                    payload=payload if payload is not None else ABSENT,
                )
            )
        stream._fail(RemoteError(str(exc), stream_id=stream.id, payload=payload), notify_remote=False)

    # ------------------------------------------------------------------ inbound stream frames

    async def _on_stream_frame(self, frame: Frame) -> bool:
        stream_id = frame.stream
        if stream_id is None:
            await self._fail_connection(f"{frame.type} frame with no stream id")
            return False

        stream = self._streams.get(stream_id)
        if stream is None:
            high_water = self._highest_local_open if self._is_our_parity(stream_id) else self._highest_remote_open
            if stream_id > high_water:
                # A genuine disagreement about the id space, not a late frame (WSM-STM-003).
                await self._fail_connection(
                    f"{frame.type} on stream {stream_id}, above the high-water mark {high_water} (WSM-STM-003)"
                )
                return False
            # Below the mark: expected during a normal race, silently ignored (WSM-STM-002).
            self._ignored_late_frames += 1
            return True

        if frame.type == "reset":
            stream._fail(
                exception_for_reset(
                    frame.code or 0,
                    frame.reason,
                    stream_id=stream_id,
                    payload=frame.payload if frame.payload is not ABSENT else None,
                ),
                notify_remote=False,
            )
            return True
        return await self._on_data(stream, frame)

    def _remote_stream_count(self) -> int:
        """Only the streams the **remote** opened (WSM-STM-037).

        The limit bounds work the other side can impose. Counting our own would be the announced
        quota rebuilt by hand, against a number this peer cannot know (WSM-INV-007).
        """
        return sum(1 for stream in self._streams.values() if not stream.local)

    def _is_our_parity(self, stream_id: int) -> bool:
        return stream_id % 2 == (1 if self._is_dialer else 0)

    async def _on_data(self, stream: Stream, frame: Frame) -> bool:
        if stream.state in (StreamState.HALF_CLOSED_REMOTE, StreamState.CLOSED):
            # data after the remote ended: illegal for this stream, harmless for the connection.
            await self._reset_stream(stream, ResetCode.PROTOCOL_ERROR, "data after end (WSM-STM-020)")
            return True

        if stream._assembler.in_progress and frame.fragment is None:
            await self._reset_stream(
                stream, ResetCode.PROTOCOL_ERROR, "non-fragment frame mid-reassembly (WSM-FRG-033)"
            )
            return True

        if frame.fragment is not None:
            if not await self._within_payload_cap(stream, frame):
                return True
            payload = stream._assembler.feed(frame, self._codec)
            if payload is ABSENT:
                return True
            stream._accept_payload(payload)
        elif frame.payload is not ABSENT:
            stream._accept_payload(frame.payload)

        if frame.end:
            stream._remote_end(frame.trailers)
        return True

    async def _within_payload_cap(self, stream: Stream, frame: Frame) -> bool:
        """WSM-FRG-032/WSM-INV-017: reject **on the crossing fragment**, not after reassembly.

        Bounding memory is the limit's whole purpose. A receiver that assembles the payload in order
        to measure it has already spent everything the limit existed to protect, and the partial
        buffer is dropped in the same step for the same reason (M5a decision 1).
        """
        incoming = encoded_length(frame.fragment) if frame.fragment is not None else 0
        if stream._assembler.byte_length + incoming <= self._max_payload_bytes:
            return True

        stream._assembler.reset()
        await self._reset_stream(
            stream,
            ResetCode.PAYLOAD_TOO_LARGE,
            f"a payload crossed this receiver's {self._max_payload_bytes}-byte limit (WSM-FRG-032)",
        )
        return False

    async def _reset_stream(self, stream: Stream, code: ResetCode, reason: str) -> None:
        self._enqueue(Frame("reset", stream=stream.id, code=int(code), reason=reason))
        stream._fail(exception_for_reset(code, reason, stream_id=stream.id), notify_remote=False)

    # ------------------------------------------------------------------ ending

    async def _fail_connection(self, reason: str) -> None:
        """ILL-C: goaway(PROTOCOL_ERROR), close the socket, then fail every live stream."""
        logger.warning("muxws conn=%s connection-level protocol error: %s", self.id, reason)
        self._enqueue(
            Frame(
                "goaway",
                code=int(ResetCode.PROTOCOL_ERROR),
                reason=reason,
                last_stream=self._highest_remote_open,
            )
        )
        await self._drain_outbound()
        await self._socket.close(1002, reason)
        self._die(ConnectionClosed(reason, code=1002, reason=reason, was_clean=False))

    async def _drain_outbound(self) -> None:
        """Let the writer flush what is already queued, so `goaway` actually reaches the wire."""
        for _ in range(200):
            if len(self._writer) == 0:
                break
            await asyncio.sleep(0)

    async def _stop_writer(self) -> None:
        if self._writer_task is not None and not self._writer_task.done():
            self._writer.stop()
            try:
                await asyncio.wait_for(self._writer_task, 1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()

    def _die(self, cause: ConnectionClosed, *, notify: bool = True) -> None:
        """Socket death: every live stream fails with ConnectionLost **before** on_close fires.

        `notify=False` is the one case where a peer dies without a report: the **first** connection
        that never established. `connect()` raising is the report (WSM-RCN-006), and there is nobody
        who could have received the callback - the caller never got the peer back, so it never
        attached a handler. Marking it dead is still not optional: leaving `is_open` to a race
        between the read loop and the cancel is how a peer that failed its hello reads as alive.
        """
        if not self._is_open:
            return
        self._is_open = False
        self._death = cause

        self._pings.fail_all(cause)
        # `ping()` prunes its own start time on a pong and on its own deadline, but neither of those
        # happens when the socket dies underneath an outstanding ping - which is the ordinary case
        # for a heartbeat-driven peer, once per reconnect, for the life of the process.
        self._ping_started.clear()
        # Nothing is held for a next socket (WSM-RCN-042); M5b calls exactly this one method.
        self._writer.discard_all()
        for stream in list(self._streams.values()):
            stream._fail(
                ConnectionLost(f"connection closed: {cause.reason or cause}", stream_id=stream.id),
                notify_remote=False,
            )
        self._streams.clear()

        if notify:
            self._notify_close(
                CloseReason(
                    code=cause.code, reason=cause.reason, was_clean=cause.was_clean, will_retry=self._will_retry
                )
            )

    def _notify_close(self, reason: CloseReason) -> None:
        """Fire `on_close` once with an already-built reason.

        Separate from `_die` because exhausting `max_attempts` is not a socket loss - the socket is
        long gone - and yet it MUST fire `on_close` once with `will_retry` false (WSM-RCN-044). Two
        callers and no more: `_die` for every socket loss, and the helper for that one withdrawal.

        **At most one `will_retry=False` close per peer, ever**, whichever gets here first. When the
        attempt cap is spent by failed *hellos* rather than by refused dials, the last loss already
        reported `will_retry=False` - the helper knew, before it adopted that socket, that it was the
        last one - and the helper's own withdrawal would then say the same thing twice. An
        application that treats it as "give up now" would run its teardown twice (WSM-RCN-044).
        """
        if not reason.will_retry:
            if self._final_close_reported:
                return
            self._final_close_reported = True
        # Isolated: `on_close` is where an application tears down, and a handler that raised would
        # take the rest of the fan-out with it - and, on the helper's path, the supervisor too.
        for handler in self._close_handlers:
            try:
                handler(reason)
            except Exception:  # noqa: BLE001 - one bad handler must not cost the others their turn
                logger.exception("muxws conn=%s an on_close handler raised", self.id)

    def _adopt_socket(self, socket: SocketAdapter) -> None:
        """Take a freshly established socket, for a `Peer` that survived a reconnect.

        `Peer` survives; `Stream` objects do not (WSM-RCN-032). The id space starts empty and the
        high-water marks reset.

        `tags` is deliberately **not** cleared here, and this method is the only place it could be.
        WSM-RCN-033 is a statement about the *acceptor*, where a reconnect is a whole new `Peer` and
        there is nothing to carry forward; only a dialer ever adopts a socket, and a dialer's tags are
        indexed by nothing. WSM-INV-014 states the rule without that qualification, which is why this
        is written down rather than assumed.

        The connection id advances, so a log shows the reconnect as a new `conn=` rather than as one
        continuous connection (WSM-API-009).
        """
        self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
        self._socket = socket
        self._streams.clear()
        self._next_id = 1 if self._is_dialer else 2
        self._highest_local_open = 0
        self._highest_remote_open = 0
        # A new socket is a new connection, so the "at most one" of WSM-RCN-044 is per connection and
        # not per `Peer` object. Left unreset, a bare `Peer` handed a fresh socket after a
        # `will_retry=False` close reports its next loss to nobody.
        self._final_close_reported = False
        self._goaway = GoawayState()
        self._writer = Writer(self._codec, max_frame_bytes=self._max_frame_bytes)
        self._writer_task = None
        # The id space starts empty, so the shutdown the *previous* socket started when it ran out
        # of ids is over. A stale task reference here would make `_begin_exhaustion_shutdown()` a
        # no-op on this socket, and the connection that exhausted its ids would never go away
        # (WSM-SID-007).
        self._exhaustion_task = None
        self._is_open = True
        self._death = None

    async def _close_socket_locally(self, code: int = 1000, reason: str = "") -> None:
        """Close the underlying socket, and nothing else.

        WSM-RCN-011 asks for two things when the heartbeat declares a socket dead - declared dead
        **and closed locally** - and `_die()` only does the first. Without the second the read loop
        stays parked inside `socket.receive()` on a socket the remote will never write to again,
        `serve()` never returns, and the supervisor waits for a loss it is never told about: no
        backoff, no re-dial (WSM-RCN-011).

        `code` defaults to 1000 and is never 1006: 1006 is reserved for "the connection dropped
        without a close frame" and a peer may not send it - the `websockets` adapter hands it
        straight to `connection.close(code=...)`, which rejects it, and the exception below would
        swallow the failure and leave the socket open. What the peer *reports* for the death is 1006;
        what it *sends* to end the socket is not.
        """
        try:
            await self._socket.close(code, reason)
        except Exception:  # noqa: BLE001 - a socket that cannot be closed is already gone
            logger.debug("muxws conn=%s closing a locally-declared-dead socket failed", self.id)

    def _forget(self, stream: Stream) -> None:
        """Drop a closed stream. Nothing is retained per closed stream (WSM-STM-001)."""
        self._streams.pop(stream.id, None)

    def __repr__(self) -> str:
        role = "dialer" if self._is_dialer else "acceptor"
        return f"<Peer {self.id} {role} streams={len(self._streams)}>"


def _checked_frame_cap(cap: int, codec: Codec) -> int:
    """WSM-FRG-034: a cap too small to hold an envelope plus one unit is a configuration error.

    Discovered here, at construction, rather than later as an infinite split loop. The cap is a
    protocol constant (WSM-FRG-004); the only way a smaller number reaches a peer is the test-only
    construction argument of WSM-FRG-005, which the conformance runner uses.
    """
    from muxws.fragment import split_frame

    if cap == MAX_FRAME_BYTES:
        return cap
    probe = Frame("data", stream=1, payload={"a": "aaaaaaaa"})
    try:
        split_frame(probe, cap, codec)
    except ProtocolError as exc:
        raise ProtocolError(
            f"max_frame_bytes={cap} cannot hold an envelope plus one indivisible unit of payload: {exc} (WSM-FRG-034)"
        ) from None
    return cap
