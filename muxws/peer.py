"""`Peer`: one end of one WebSocket. Both ends use the same type (§5, §9, WSM-INV-002)."""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import random

from collections.abc import Callable, Mapping
from types import MappingProxyType
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
#: security-sensitive, so it uses `random`.
_PROCESS_PREFIX = f"{random.getrandbits(12):03x}"  # noqa: S311
#: Monotonic and never reused, so no two connections in one process share an id (WSM-API-009).
_CONNECTION_COUNTER = itertools.count()
#: Seconds a connection-level protocol error waits for queued frames, `goaway` among them, before it
#: closes the socket. Equal to `close()`'s default drain.
_FAILURE_DRAIN = 10.0

#: The longest close reason a WebSocket close frame carries (RFC 6455 5.5: 125 bytes less the code).
_MAX_CLOSE_REASON_BYTES = 123

StreamHandler = Callable[[Any, Stream], Any]
ErrorSerializer = Callable[[BaseException], Any]
#: `(headers, payload)`. Mutates `headers` in place; the return value is ignored (WSM-API-027).
OpenInterceptor = Callable[[dict[str, Any], Any], None]
#: `(headers, stream)`. Mutates `headers` in place; the return value is ignored (WSM-API-028).
ReplyHeadersInterceptor = Callable[[dict[str, Any], Stream], None]
#: `(headers, stream)`, with `headers` read-only. The return value is ignored (WSM-API-029).
IncomingHeadersInterceptor = Callable[[Mapping[str, Any], Stream], None]


def _loop_time() -> float:
    """The heartbeat's clock. The activity stamp and the idle check both read this function."""
    return asyncio.get_running_loop().time()


def default_error_serializer(exc: BaseException) -> Any:
    """WSM-ERR-006's default. A public-facing deployment should replace it with a redacting one."""
    return {"type": type(exc).__name__, "message": str(exc)}


class Peer:
    """The peer type for both ends of a connection. A server push is a stream opened by the acceptor."""

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
        #: The largest reassembled payload this peer accepts. Local and never announced (WSM-FRG-035);
        #: a sender learns of it only from the reset it receives.
        self._max_payload_bytes = max_payload_bytes
        #: How many streams the remote may have open on this peer at once. Local, never announced, and
        #: not checked by the sender (WSM-STM-036).
        self._max_concurrent_streams = max_concurrent_streams

        # The dialer allocates odd ids, the acceptor even ones (WSM-SID-002).
        self._next_id = 1 if is_dialer else 2
        self._streams: dict[int, Stream] = {}
        #: Two integers plus the live-stream map; no state is kept per closed stream (WSM-STM-001).
        self._highest_local_open = 0
        self._highest_remote_open = 0
        self._ignored_late_frames = 0

        self._handler: StreamHandler | None = None
        self._close_handlers: list[Callable[[Any], None]] = []
        self._reconnect_handlers: list[Callable[[int, Any], None]] = []
        #: True while the reconnect helper intends to dial again. `CloseReason.will_retry` reads it;
        #: it is false when `max_attempts` is exhausted or after `close()`. The helper decides whether
        #: to dial with `should_retry`, not with this field.
        self._will_retry = False
        #: Set by the first `will_retry=False` close; a second one is suppressed (WSM-RCN-044).
        self._final_close_reported = False
        self._frame_handlers: list[Callable[[str, Frame, int], None]] = []
        #: Keyed by one token per registration: a function registered twice is two registrations, and
        #: each unregister function removes only its own. `_adopt_socket` does not clear them; they
        #: belong to the `Peer`, not to its connection (WSM-API-026).
        self._open_interceptors: dict[object, OpenInterceptor] = {}
        self._reply_interceptors: dict[object, ReplyHeadersInterceptor] = {}
        self._incoming_interceptors: dict[object, IncomingHeadersInterceptor] = {}

        self._writer = Writer(codec, max_frame_bytes=self._max_frame_bytes)
        self._pings = PingRegistry()
        self._ping_started: dict[str, float] = {}
        self._goaway = GoawayState()
        self._writer_task: asyncio.Task[None] | None = None
        self._is_open = True
        #: Whether the connection is established. `is_open` is false from a socket loss until the next
        #: established connection (WSM-RCN-043); with a hello configured, a socket is established
        #: once the hello is acknowledged (WSM-RCN-004). Defaults to true, so an acceptor, which has
        #: no hello and no reconnect helper, is established when its socket opens (WSM-CON-030). Only
        #: the reconnect helper writes it.
        self._established = True
        self._death: ConnectionClosed | None = None
        #: A strong reference to the WSM-SID-007 shutdown task, so the garbage collector does not
        #: collect it before it finishes.
        self._exhaustion_task: asyncio.Task[None] | None = None

        #: The heartbeat clock. The activity stamp and the idle check both read it (WSM-RCN-010).
        self._clock: Callable[[], float] = _loop_time
        #: Stamped by `_report_frame` for every frame in either direction, so a socket carrying
        #: traffic is not pinged (WSM-RCN-010, §6).
        self._last_activity: float = 0.0
        #: The dialer's reconnect helper. `close()` stops it, because a peer closed by `close()` MUST
        #: NOT dial again (WSM-RCN-040/044); the reference also keeps the supervisor task alive.
        self._connection_loop: ConnectionLoop | None = None

    # ------------------------------------------------------------------ properties

    @property
    def is_open(self) -> bool:
        """True when the socket is open and the connection is established (WSM-RCN-043).

        While the hello is unacknowledged the socket is open but `is_open` is false: a frame the
        application sent then would precede the hello on the wire (WSM-RCN-023).
        """
        return self._is_open and self._established

    @property
    def _has_a_socket(self) -> bool:
        """Whether a socket is attached, so a frame put on the writer now can reach the wire.

        Differs from `is_open` during the hello window: there `is_open` is false (WSM-RCN-043) while
        the socket is open and carries the hello. `Stream.reset()` uses this property, so a reset of a
        stream the acceptor pushed during the hello window is sent to the remote (WSM-STM-021).
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
        """True for the dialer, which allocates odd stream ids; false for the acceptor (even ids).

        Id parity is the only difference between the two ends. Both may open streams, and streams
        behave identically on both sides (WSM-INV-002).
        """
        return self._is_dialer

    # ------------------------------------------------------------------ registration

    def on_stream(self, handler: StreamHandler) -> StreamHandler:
        """Register the incoming-stream handler. A second registration replaces the first and logs a warning."""
        if self._handler is not None:
            logger.warning("muxws conn=%s replacing the on_stream handler (WSM-STM-030)", self.id)
        self._handler = handler
        return handler

    def on_close(self, handler: Callable[[Any], None]) -> Callable[[Any], None]:
        """Register a handler called on every socket loss, not only the last one (WSM-RCN-040).

        The handler receives a `CloseReason`. Its `will_retry` is true when the reconnect helper will
        dial again, and false when the attempt cap is spent or `close()` was called. Returns the
        handler, so it works as a decorator. An exception raised by a handler is logged and the
        remaining handlers still run.
        """
        self._close_handlers.append(handler)
        return handler

    def on_reconnect(self, handler: Callable[[int, Any], None]) -> Callable[[int, Any], None]:
        """Register a handler called once per re-established connection, as `(attempt, peer)` (WSM-RCN-030).

        When it runs, the socket is open and the acceptor has acknowledged the hello on it. No stream
        survives a reconnect, nothing is replayed, and the new socket's id space starts empty
        (WSM-RCN-031/032).
        """
        self._reconnect_handlers.append(handler)
        return handler

    def _fire_reconnect(self, attempt: int) -> None:
        # Runs on the supervisor task. An exception from a handler is logged and not propagated, or
        # it would end the reconnect loop and no later socket loss would be redialled
        # (WSM-RCN-011/030).
        for handler in self._reconnect_handlers:
            try:
                handler(attempt, self)
            except Exception:  # noqa: BLE001 - a raising handler does not stop the remaining handlers
                logger.exception("muxws conn=%s an on_reconnect handler raised", self.id)

    def on_frame(self, handler: Callable[[str, Frame, int], None]) -> Callable[[str, Frame, int], None]:
        """`(direction, frame, byte_length)`, before encode and after decode (WSM-OBS-003)."""
        self._frame_handlers.append(handler)
        return handler

    def intercept_open(self, interceptor: OpenInterceptor) -> Callable[[], None]:
        """Register an interceptor for the headers of every stream this peer opens (WSM-API-026/027).

        `open()`, `notify()` and `request()` call it synchronously as `(headers, payload)` before an id
        is allocated; it adds, replaces or deletes keys by mutating `headers`. One that raises fails
        the open with that exception. Returns a function that removes this registration.
        """
        return _register(self._open_interceptors, interceptor)

    def intercept_reply_headers(self, interceptor: ReplyHeadersInterceptor) -> Callable[[], None]:
        """Register an interceptor for the leading headers this peer answers with (WSM-API-026/028).

        Called once per stream the remote opened, as `(headers, stream)`, by the call that sends this
        peer's first frame on it: `send_headers()`, `send()`, `end()`, `reply()`, or the implicit end
        of a handler that returns. It adds, replaces or deletes keys by mutating `headers`. One that
        raises fails that call with that exception. Returns a function that removes this registration.
        """
        return _register(self._reply_interceptors, interceptor)

    def intercept_incoming_headers(self, interceptor: IncomingHeadersInterceptor) -> Callable[[], None]:
        """Register an interceptor for the leading headers the remote sends (WSM-API-026/029).

        Called once per stream as `(headers, stream)` when the remote's first frame on it arrives: the
        `open` of a stream the remote opened, the first `data` of a stream this peer opened. One that
        raises is logged and changes nothing. Returns a function that removes this registration.
        """
        return _register(self._incoming_interceptors, interceptor)

    # ------------------------------------------------------------------ opening

    def open(self, payload: Any = None, *, headers: dict[str, Any] | None = None, end: bool = False) -> Stream:
        """Open a stream. Synchronous; between sockets it raises instead of queuing the open (WSM-API-001/004).

        The guard and the allocation are separate methods. The reconnect helper's hello is sent while
        `is_open` is still false, because its acknowledgement establishes the connection
        (WSM-RCN-004/043), so the helper calls `_allocate_and_enqueue` directly. The guard takes no
        parameters, so no call through this method can bypass it and precede the hello (WSM-RCN-023).
        """
        self._raise_if_unopenable()
        if self._open_interceptors:
            headers = _intercepted_headers(self._open_interceptors, headers, payload, "open")
        return self._allocate_and_enqueue(payload, headers=headers, end=end)

    def _intercepted_reply_headers(self, headers: dict[str, Any] | None, stream: Stream) -> dict[str, Any] | None:
        """The leading headers of this peer's answer on `stream`, as the reply interceptors leave them.

        Called by the stream before its first frame is marked as sent, so after an interceptor raises
        the stream can still send its first frame (WSM-API-028).
        """
        if not self._reply_interceptors:
            return headers
        return _intercepted_headers(self._reply_interceptors, headers, stream, "reply headers")

    def _allocate_and_enqueue(self, payload: Any, *, headers: dict[str, Any] | None, end: bool) -> Stream:
        """Allocate an id and enqueue the `open` with no suspension point between them (WSM-SID-006).

        Wire order therefore equals allocation order. If two `open()` calls could interleave here, ids
        could go on the wire out of order, which the remote treats as a protocol error (WSM-INV-005).
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
        """Raise `ConnectionLost` or `ConnectionGoingAway` when no stream can be opened (WSM-API-004).

        No exception is raised for the number of open streams.
        """
        # `is_open`, not `_is_open`: until the hello is acknowledged the connection is not established,
        # and a frame opened now would precede the hello (WSM-RCN-023/043).
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

        Unlike `await stream`, a second payload is an error: the stream is reset with
        `PROTOCOL_ERROR` and `ProtocolError` is raised (WSM-API-007).
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

        # Shielded, so the deadline cancels the wait and not the collector. A cancelled collector
        # sends reset(CANCELLED); on a deadline the remote must receive reset(TIMEOUT) (WSM-ERR-011),
        # so the TIMEOUT reset is enqueued before the collector is cancelled.
        collector = asyncio.ensure_future(collect())
        try:
            return await asyncio.wait_for(asyncio.shield(collector), timeout)
        except asyncio.TimeoutError:
            await stream.reset(ResetCode.TIMEOUT, f"deadline of {timeout}s expired")
            collector.cancel()
            await asyncio.gather(collector, return_exceptions=True)
            raise StreamTimeout(f"stream {stream.id} did not answer within {timeout}s", stream_id=stream.id) from None
        except asyncio.CancelledError:
            # The caller was cancelled. The shield keeps the collector running, so it is cancelled
            # here; its cancellation handler sends reset(CANCELLED), as without a timeout (WSM-ERR-011).
            collector.cancel()
            await asyncio.gather(collector, return_exceptions=True)
            raise

    # ------------------------------------------------------------------ the writer

    def _enqueue(self, frame: Frame) -> None:
        """Synchronous by contract: `open()` must not suspend between allocating and enqueuing."""
        self._writer.enqueue(frame)

    def _enqueue_reset(self, stream_id: int, code: ResetCode, reason: str | None) -> None:
        """Enqueue a stream's `reset` in place of its queued frames (WSM-FRM-014, `Writer.discard`)."""
        self._writer.discard(stream_id)
        self._writer.enqueue(Frame("reset", stream=stream_id, code=int(code), reason=reason))

    async def _write_loop(self) -> None:
        while True:
            try:
                frame = await self._writer.next_frame()
            except LaneEncodingError as failure:
                await self._fail_lane(failure)
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
                # Fragment n+1 is sliced after fragment n has reached the socket (WSM-FRG-018).
                self._writer.advance(frame.stream if frame.stream is not None else CONNECTION_LANE)
                # Yield one event-loop turn per frame (WSM-INV-004). `next_frame()` returns without
                # suspending when there is work, and `send_text` on a socket with buffer space (uvicorn
                # on loopback, `MemorySocket`) also completes without suspending. Without this yield
                # the loop sends every queued frame and fragment in one run of this task, no other
                # task can enqueue a frame in between, and frames of different streams do not
                # interleave unless the socket applies backpressure.
                await asyncio.sleep(0)
            except ConnectionClosed:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # A frame the codec cannot encode (for example a bytes payload under JSON) fails its
                # stream only. If the exception ended this task, nothing would drain the queue while
                # the peer still reported itself open.
                logger.exception("muxws conn=%s could not send a %s frame", self.id, frame.type)
                await self._fail_unsendable(frame, exc)

    async def _fail_lane(self, failure: LaneEncodingError) -> None:
        """One lane's frame could not be encoded. Fail its stream; keep the connection working.

        With no live stream on the lane, the peer is marked dead and the socket is closed with 1011.
        """
        logger.exception(
            "muxws conn=%s could not encode a frame on stream %s", self.id, failure.lane, exc_info=failure.cause
        )
        stream = self._streams.get(failure.lane)
        if stream is None:
            await self._die_and_close(ConnectionClosed(str(failure), code=1011, reason=str(failure)))
            return
        self._enqueue(Frame("reset", stream=stream.id, code=int(ResetCode.INTERNAL_ERROR), reason=str(failure.cause)))
        stream._fail(
            exception_for_reset(ResetCode.INTERNAL_ERROR, str(failure.cause), stream_id=stream.id),
            notify_remote=False,
        )

    async def _fail_unsendable(self, frame: Frame, exc: BaseException) -> None:
        """One frame could not be encoded or sent. Fail its stream and keep the connection working.

        With no live stream for the frame, the peer is marked dead and the socket is closed with 1011.
        """
        stream = self._streams.get(frame.stream) if frame.stream is not None else None
        if stream is None:
            reason = f"could not encode a {frame.type} frame: {exc}"
            await self._die_and_close(ConnectionClosed(reason, code=1011, reason=reason))
            return
        self._enqueue(Frame("reset", stream=stream.id, code=int(ResetCode.INTERNAL_ERROR), reason=str(exc)))
        stream._fail(
            exception_for_reset(ResetCode.INTERNAL_ERROR, str(exc), stream_id=stream.id),
            notify_remote=False,
        )

    def _report_frame(self, direction: str, frame: Frame, encoded: str | bytes) -> None:
        # Every frame type in both directions counts as activity: a socket carrying traffic in one
        # direction only is not idle and is not pinged (WSM-RCN-010).
        self._last_activity = self._clock()
        length = encoded_length(encoded)
        # This runs inside the read loop and the write loop. An exception from a handler is logged
        # and not propagated, so it cannot end the connection (WSM-OBS-003).
        for handler in self._frame_handlers:
            try:
                handler(direction, frame, length)
            except Exception:  # noqa: BLE001 - a raising frame handler must not end the connection
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
                    # The peer is marked dead and the socket closed here. A read loop that ended while
                    # `is_open` stayed true would leave every pending await hanging, `on_close` never
                    # called, and `open()` enqueuing frames that nothing reads; a socket left open
                    # would leave the remote waiting on a peer that reads nothing.
                    logger.exception("muxws conn=%s read loop failed on a %s frame", self.id, frame.type)
                    await self._fail_internal(f"read loop failed: {exc}", str(exc))
                    return
                if not keep_going:
                    return
        except Exception as exc:  # noqa: BLE001
            # An exception outside `_dispatch` (a socket adapter or codec raising something other than
            # `ConnectionClosed` or `ProtocolError`) ends the connection the same way.
            logger.exception("muxws conn=%s read loop failed", self.id)
            await self._fail_internal(f"read loop failed: {exc}", str(exc))
        finally:
            await self._stop_writer()

    async def _within_frame_cap(self, frame: Frame, message: str | bytes) -> bool:
        """Check the size of the whole encoded message, not of the `fragment` field (WSM-FRG-031).

        A receiver must accept anything up to `MAX_FRAME_BYTES` (WSM-FRG-004), so only messages above
        the constant, or above a lower cap passed at construction in tests, are rejected.
        """
        size = encoded_length(message)
        if size <= self._max_frame_bytes:
            return True
        if frame.stream is None:
            await self._fail_connection(f"a connection-level frame of {size} bytes exceeds the cap")
            return False

        stream = self._streams.get(frame.stream)
        continues_open = stream is not None and stream._opening and frame.fragment is not None
        if frame.type == "open" and not continues_open:
            # The id of an over-cap `open` is validated and counts toward the high-water mark, so a later
            # `data` or `reset` on it is ignored (WSM-STM-002) and is not above the mark (WSM-STM-003).
            if not await self._admit_remote_open(frame.stream):
                return False
        elif stream is None and frame.type in ("data", "reset"):
            # No live stream to reset: the id is classified by the high-water mark (WSM-STM-002/003).
            await self._on_stream_frame(frame)
            return False
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
            # Answered immediately with the same nonce; the application is not involved (WSM-CON-010).
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

        Sends a muxws `ping` frame, not a WebSocket control frame: browsers do not expose WebSocket
        ping/pong to JavaScript (WSM-CON-011). Raises `ConnectionLost` when no socket is attached and
        `ConnectionClosed` (code 1006) when no pong arrives within `timeout` seconds.
        """
        # `_has_a_socket`, not `is_open`: `is_open` is false during the hello window while the socket
        # is open (WSM-RCN-043).
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
        """Send `goaway`, drain for up to `drain` seconds, then close the socket (WSM-CON-025).

        The drain waits for live streams to end and for the write loop to send every queued frame,
        including every remaining fragment of a fragmented payload; one deadline bounds both. An
        exception from the socket adapter's `close()` is logged, and the peer is marked closed.
        """
        # The reconnect helper is stopped before the `_is_open` check. Between two sockets `_is_open`
        # is false while the helper waits in its backoff; returning early there would leave the
        # helper running, and it would dial again after `close()` returned (WSM-RCN-040/044).
        self._will_retry = False
        if self._connection_loop is not None:
            await self._connection_loop.stop()
        if not self._is_open:
            return
        self._send_goaway(code, reason)
        await self._drain(drain)
        await self._close_socket(1000, reason or "")
        self._die(ConnectionClosed(reason or "closed", code=1000, reason=reason or "", was_clean=True))

    def _send_goaway(self, code: ResetCode, reason: str | None) -> None:
        """Enqueue `goaway` once. `last_stream` is the highest id the remote opened on this peer.

        The id has the remote's parity. With this peer's own highest id instead, the remote would
        treat its streams as unprocessed and fail them all.
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

        # Local streams above `last_stream` were not processed by the remote and are safe to retry
        # elsewhere. They fail locally with REFUSED and no reset is sent for them (WSM-CON-023).
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
        """Wait until no stream is live and every queued frame is sent, or `timeout` seconds have passed (WSM-CON-024).

        The wait ends at the deadline even if the remote keeps a stream open or the socket does not
        accept the queued frames.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self._streams and loop.time() < deadline:
            # Polls every 5 ms. A `sleep(0)` poll would keep the CPU busy for the whole drain window
            # and delay the application tasks that end the remaining streams (WSM-CON-024).
            await asyncio.sleep(0.005)
        # Streams still live at the deadline fail locally with `ConnectionLost` when the socket
        # closes; no reset is sent for them.
        await self._drain_outbound(deadline)

    def _exhausted(self) -> bool:
        return self._next_id > MAX_STREAM_ID

    def _begin_exhaustion_shutdown(self) -> None:
        """Schedule an orderly shutdown when stream ids are exhausted (WSM-SID-007).

        WSM-SID-007 requires sending `goaway`, opening no further streams, letting in-flight streams
        drain, and closing. `open()` returns without suspending (WSM-API-001), so `close()` runs as a
        separate task. The stream just allocated is in flight and drains like any other.
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

        if not await self._admit_remote_open(stream_id):
            return False

        if self._remote_stream_count() >= self._max_concurrent_streams:
            # Refused without invoking the handler, so the opener may retry it elsewhere
            # (WSM-STM-036). The limit is not announced; the sender learns of it from this reset.
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
        self._intercept_incoming(stream, stream.headers)

        if frame.fragment is not None:
            stream._opening = True
            # A fragmented `open` is subject to the same payload cap as a fragmented `data`.
            if not await self._within_payload_cap(stream, frame):
                return True
            payload = await self._reassemble(stream, frame)
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
        payload = await self._reassemble(stream, frame)
        if payload is ABSENT:
            return True
        stream._opening = False
        stream.payload = payload
        if frame.end:
            stream.state = StreamState.HALF_CLOSED_REMOTE
        self._start_handler(stream)
        return True

    async def _admit_remote_open(self, stream_id: int) -> bool:
        """Validate the id of an `open` that does not continue a live opening (WSM-SID-005).

        Records the id as the remote's highest open. Returns False after failing the connection.
        """
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
        return True

    def _start_handler(self, stream: Stream) -> None:
        """Dispatch to the one handler, or refuse when there is none (WSM-STM-033)."""
        if self._goaway.sent:
            # This peer sent goaway and starts no new handler. REFUSED tells the opener the stream
            # was not processed, so it may retry it elsewhere (WSM-CON-021).
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
            # Cancellation is propagated, not reported as an application error (WSM-ERR-014).
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
            except Exception as exc:  # noqa: BLE001 - a reply interceptor's failure is the handler's (WSM-API-028)
                await self._reset_for_handler_error(stream, exc)

    async def _reset_for_handler_error(self, stream: Stream, exc: BaseException) -> None:
        """Reset the stream with APPLICATION_ERROR, never REFUSED (WSM-STM-034/WSM-INV-008).

        REFUSED states that the operation did not happen. A handler that raises may already have had
        side effects, so the opener must not be told it is safe to retry.
        """
        try:
            payload = self._error_serializer(exc)
        except Exception:  # noqa: BLE001
            # The reset is sent even when the serializer raises (WSM-STM-034); without it the opener
            # would wait indefinitely.
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
                # An id above the high-water mark is a connection-level error, not a late frame
                # (WSM-STM-003).
                await self._fail_connection(
                    f"{frame.type} on stream {stream_id}, above the high-water mark {high_water} (WSM-STM-003)"
                )
                return False
            # At or below the mark: a late frame for a closed stream, ignored (WSM-STM-002).
            self._ignored_late_frames += 1
            return True

        if frame.type == "reset":
            # The stream is closed in both directions; its queued frames are not sent (WSM-FRM-014).
            self._writer.discard(stream_id)
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
        """The number of live streams the remote opened (WSM-STM-037).

        The concurrency limit bounds streams the remote opens on this peer; streams this peer opened
        are not counted (WSM-INV-007).
        """
        return sum(1 for stream in self._streams.values() if not stream.local)

    def _is_our_parity(self, stream_id: int) -> bool:
        return stream_id % 2 == (1 if self._is_dialer else 0)

    async def _on_data(self, stream: Stream, frame: Frame) -> bool:
        if stream.state in (StreamState.HALF_CLOSED_REMOTE, StreamState.CLOSED):
            # `data` after the remote ended: a stream-level error; the connection stays open.
            await self._reset_stream(stream, ResetCode.PROTOCOL_ERROR, "data after end (WSM-STM-020)")
            return True

        # WSM-FRM-016, checked before any payload is delivered: headers may appear only on the
        # remote's first frame on the stream. A violation resets the stream; the connection stays
        # open, as for `data` after end above.
        answer = stream.local and not stream._received_a_frame
        if not stream._note_remote_frame(frame.headers):
            await self._reset_stream(stream, ResetCode.PROTOCOL_ERROR, "headers after the first frame (WSM-FRM-016)")
            return True
        if answer:
            self._intercept_incoming(stream, stream.reply_headers)

        if stream._assembler.in_progress and frame.fragment is None:
            await self._reset_stream(
                stream, ResetCode.PROTOCOL_ERROR, "non-fragment frame mid-reassembly (WSM-FRG-033)"
            )
            return True

        if frame.fragment is not None:
            if not await self._within_payload_cap(stream, frame):
                return True
            payload = await self._reassemble(stream, frame)
            if payload is ABSENT:
                return True
            stream._accept_payload(payload)
        elif frame.payload is not ABSENT:
            stream._accept_payload(frame.payload)

        if frame.end:
            stream._remote_end(frame.trailers)
        return True

    def _intercept_incoming(self, stream: Stream, headers: Mapping[str, Any]) -> None:
        """Hand the remote's leading headers to every incoming interceptor (WSM-API-029).

        Runs inside the read loop. An exception from an interceptor is logged; the frame, the stream
        and the connection are processed as if that interceptor were not registered.
        """
        if not self._incoming_interceptors:
            return
        view = MappingProxyType(headers)
        for interceptor in list(self._incoming_interceptors.values()):
            try:
                result = interceptor(view, stream)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:  # noqa: BLE001 - a raising interceptor must not end the read loop
                # A CancelledError raised by this synchronous call comes from the interceptor: the read
                # loop's own cancellation is delivered at an await, not inside this call.
                logger.exception("muxws conn=%s stream=%s an incoming headers interceptor raised", self.id, stream.id)
                continue
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                logger.error(
                    "muxws conn=%s stream=%s an incoming headers interceptor returned an awaitable; it is not "
                    "awaited (WSM-API-029)",
                    self.id,
                    stream.id,
                )

    async def _within_payload_cap(self, stream: Stream, frame: Frame) -> bool:
        """Reject the fragment that crosses `max_payload_bytes`, before reassembly (WSM-FRG-032/WSM-INV-017).

        The limit bounds memory, so the payload is measured as fragments arrive and the partial buffer
        is dropped when the limit is crossed.
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

    async def _reassemble(self, stream: Stream, frame: Frame) -> Any:
        """Add one fragment to the stream's payload. Returns the payload, or `ABSENT` while more are expected.

        Fragments that mix text and binary values, or a joined payload the codec cannot decode, reset
        this stream with PROTOCOL_ERROR and return `ABSENT`. Each message decoded on its own, so the
        peers still agree about every other stream and the connection stays open (WSM-STM-020/024).
        """
        try:
            return stream._assembler.feed(frame, self._codec)
        except ProtocolError as exc:
            await self._reset_stream(stream, ResetCode.PROTOCOL_ERROR, f"undecodable reassembled payload: {exc}")
            return ABSENT

    async def _reset_stream(self, stream: Stream, code: ResetCode, reason: str) -> None:
        self._enqueue_reset(stream.id, code, reason)
        stream._fail(exception_for_reset(code, reason, stream_id=stream.id), notify_remote=False)

    # ------------------------------------------------------------------ ending

    async def _fail_connection(self, reason: str) -> None:
        """ILL-C: goaway(PROTOCOL_ERROR), close the socket, then fail every live stream."""
        logger.warning("muxws conn=%s connection-level protocol error: %s", self.id, reason)
        # Marked as sent before the drain, so no stream is opened while the queued frames are sent
        # (WSM-CON-021).
        self._goaway.sent = True
        self._goaway.sent_code = int(ResetCode.PROTOCOL_ERROR)
        self._enqueue(
            Frame(
                "goaway",
                code=int(ResetCode.PROTOCOL_ERROR),
                reason=reason,
                last_stream=self._highest_remote_open,
            )
        )
        await self._drain_outbound(asyncio.get_running_loop().time() + _FAILURE_DRAIN)
        await self._close_socket(1002, _close_frame_reason(reason))
        self._die(ConnectionClosed(reason, code=1002, reason=reason, was_clean=False))

    async def _fail_internal(self, message: str, reason: str) -> None:
        """A failure inside this peer: goaway(INTERNAL_ERROR), then mark the peer dead and close the socket (1011)."""
        if self._is_open:
            # Marked as sent before the drain, so no stream is opened while the queued frames are sent
            # (WSM-CON-021).
            self._goaway.sent = True
            self._goaway.sent_code = int(ResetCode.INTERNAL_ERROR)
            self._enqueue(
                Frame(
                    "goaway", code=int(ResetCode.INTERNAL_ERROR), reason=reason, last_stream=self._highest_remote_open
                )
            )
            await self._drain_outbound(asyncio.get_running_loop().time() + _FAILURE_DRAIN)
        await self._die_and_close(ConnectionClosed(message, code=1011, reason=reason))

    async def _die_and_close(self, cause: ConnectionClosed) -> None:
        """Mark the peer dead with `cause`, then close the socket with 1011.

        The peer is marked dead first, so `on_close` reports `cause` and not the closure the read loop
        observes. The close reason is fixed and short: a WebSocket close reason is limited to 123 bytes.
        """
        self._die(cause)
        await self._close_socket_locally(1011, "internal error")

    async def _drain_outbound(self, deadline: float) -> None:
        """Wait until the write loop has sent every queued frame, the write loop has ended, or `deadline` passes.

        `deadline` is in event-loop time. Every remaining fragment of a fragmented payload counts as
        queued, including while the write loop awaits the socket's send.
        """
        task = self._writer_task
        if task is not None and task.done():
            return
        # Without a write loop yet, `serve()` may be scheduled and not started; the wait is for the
        # writer alone.
        idle = asyncio.ensure_future(self._writer.wait_idle())
        waited: set[asyncio.Future[Any]] = {idle} if task is None else {idle, task}
        try:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            await asyncio.wait(waited, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
        finally:
            idle.cancel()

    async def _close_socket(self, code: int, reason: str) -> None:
        """Close the socket. An exception from the adapter is logged; the caller marks the peer dead regardless."""
        try:
            await self._socket.close(code, reason)
        except Exception:  # noqa: BLE001 - a socket whose close fails is treated as closed
            logger.warning("muxws conn=%s closing the socket failed", self.id, exc_info=True)

    async def _stop_writer(self) -> None:
        if self._writer_task is not None and not self._writer_task.done():
            self._writer.stop()
            try:
                await asyncio.wait_for(self._writer_task, 1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()

    def _die(self, cause: ConnectionClosed, *, notify: bool = True) -> None:
        """Socket death: every live stream fails with `ConnectionLost`, then `on_close` handlers run.

        `notify=False` is used only for a first connection that was never established: `connect()`
        raises instead (WSM-RCN-006), and the caller never received the peer, so no handler is
        registered. The peer is still marked dead, so `is_open` is false regardless of how the read
        loop and the cancellation are ordered.
        """
        if not self._is_open:
            return
        self._is_open = False
        self._death = cause

        self._pings.fail_all(cause)
        # `ping()` removes its start time on a pong or on its deadline. Neither happens when the socket
        # dies while a ping is outstanding, so the entries are cleared here.
        self._ping_started.clear()
        # Nothing is held for a next socket (WSM-RCN-042).
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
        """Call the `on_close` handlers with `reason`.

        Separate from `_die` because exhausting `max_attempts` is not a socket loss, and it MUST still
        call `on_close` once with `will_retry` false (WSM-RCN-044). Callers: `_die` for every socket
        loss, and the reconnect helper when it gives up.

        At most one `will_retry=False` close is reported per connection; a second one is suppressed.
        When the attempt cap is spent by failed hellos, the last socket loss already reported
        `will_retry=False`, and the helper's report is suppressed (WSM-RCN-044).
        """
        if not reason.will_retry:
            if self._final_close_reported:
                return
            self._final_close_reported = True
        # An exception from a handler is logged; the remaining handlers still run, and on the
        # reconnect helper's path the supervisor keeps running.
        for handler in self._close_handlers:
            try:
                handler(reason)
            except Exception:  # noqa: BLE001 - a raising handler does not stop the remaining handlers
                logger.exception("muxws conn=%s an on_close handler raised", self.id)

    def _adopt_socket(self, socket: SocketAdapter) -> None:
        """Take a freshly established socket, for a `Peer` that survived a reconnect.

        `Peer` survives; `Stream` objects do not (WSM-RCN-032). The id space starts empty and the
        high-water marks reset.

        `tags` is not cleared. WSM-RCN-033 applies to the acceptor, where a reconnect creates a new
        `Peer`; only a dialer adopts a socket. WSM-INV-014 states the rule without that qualification.

        The connection id advances, so the reconnect appears in logs under a new `conn=` (WSM-API-009).
        """
        self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
        self._socket = socket
        self._streams.clear()
        self._next_id = 1 if self._is_dialer else 2
        self._highest_local_open = 0
        self._highest_remote_open = 0
        # WSM-RCN-044's "at most one" applies per connection, not per `Peer` object; without this
        # reset, a `Peer` given a new socket after a `will_retry=False` close would not report its
        # next loss.
        self._final_close_reported = False
        self._goaway = GoawayState()
        self._writer = Writer(self._codec, max_frame_bytes=self._max_frame_bytes)
        self._writer_task = None
        # The id space starts empty. A task reference left from the previous socket would make
        # `_begin_exhaustion_shutdown()` a no-op on this socket, and a connection that exhausts its ids
        # would not shut down (WSM-SID-007).
        self._exhaustion_task = None
        self._is_open = True
        self._death = None

    async def _close_socket_locally(self, code: int = 1000, reason: str = "") -> None:
        """Close the underlying socket, and nothing else.

        When the heartbeat declares a socket dead, WSM-RCN-011 requires the peer to mark it dead and
        close it locally; `_die()` does only the first. Without the local close, the read loop stays in
        `socket.receive()`, `serve()` does not return, and the reconnect helper does not redial.

        `code` defaults to 1000 and must not be 1006, which a peer may not send. The `websockets`
        adapter passes it to `connection.close(code=...)`, which rejects 1006; the exception handler
        below would hide that failure and the socket would stay open. The peer reports the death with
        code 1006 but closes the socket with 1000.
        """
        try:
            await self._socket.close(code, reason)
        except Exception:  # noqa: BLE001 - a close failure means the socket is already gone
            logger.debug("muxws conn=%s closing a locally-declared-dead socket failed", self.id)

    def _forget(self, stream: Stream) -> None:
        """Remove a closed stream from the live map. No state is kept per closed stream (WSM-STM-001)."""
        self._streams.pop(stream.id, None)

    def __repr__(self) -> str:
        role = "dialer" if self._is_dialer else "acceptor"
        return f"<Peer {self.id} {role} streams={len(self._streams)}>"


def _close_frame_reason(reason: str) -> str:
    """`reason` cut to the 123 UTF-8 bytes a WebSocket close frame carries, at a character boundary.

    The `websockets` adapter raises on a longer reason and leaves the socket open. The `goaway` frame
    and `on_close` carry the full text.
    """
    return reason.encode("utf-8", "replace")[:_MAX_CLOSE_REASON_BYTES].decode("utf-8", "ignore")


def _intercepted_headers(
    interceptors: dict[object, Any], headers: dict[str, Any] | None, argument: Any, kind: str
) -> dict[str, Any] | None:
    """Run outgoing interceptors over a copy of the caller's headers (WSM-API-027/028).

    Runs before anything is enqueued, so an interceptor that raises leaves nothing on the wire. The
    result is copied again, so an interceptor that keeps a reference to the mapping cannot change the
    frame after it returns. Returns `None` when the caller passed no headers and the interceptors
    added none, so the frame carries no `headers` field.
    """
    working: dict[str, Any] = dict(headers) if headers is not None else {}
    for interceptor in list(interceptors.values()):
        result = interceptor(working, argument)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError(
                f"{kind} interceptor {interceptor!r} returned an awaitable; interceptors are synchronous "
                f"and nothing awaits them (WSM-API-027)"
            )
    foreign = [key for key in working if not isinstance(key, str)]
    if foreign:
        raise TypeError(f"{kind} interceptors left non-string header keys {foreign!r} (WSM-API-027)")
    if not working and headers is None:
        return None
    return dict(working)


def _register(registry: dict[object, Any], entry: Any) -> Callable[[], None]:
    """Add `entry` under a fresh token and return the function that removes exactly that entry."""
    token = object()
    registry[token] = entry

    def unregister() -> None:
        registry.pop(token, None)

    return unregister


def _checked_frame_cap(cap: int, codec: Codec) -> int:
    """Raise `ProtocolError` when `cap` cannot hold an envelope plus one unit of payload (WSM-FRG-034).

    Checked at construction. The cap is a protocol constant (WSM-FRG-004); a smaller value comes only
    from the test-only construction argument of WSM-FRG-005, which the conformance runner uses.
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
