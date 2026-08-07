"""`Peer`: one end of one WebSocket, symmetric by construction (§5, §9, WSM-INV-002)."""

from __future__ import annotations

import asyncio
import itertools
import logging
import random

from collections.abc import Callable, Mapping
from typing import Any

from muxws.codecs import Codec
from muxws.errors import (
    ConnectionClosed,
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
from muxws.observability import CloseReason
from muxws.stream import Stream, StreamState
from muxws.transports import SocketAdapter

logger = logging.getLogger("muxws.frames")

#: Three lowercase hex characters, drawn once per process. A log correlation id is not
#: security-sensitive, so `random` is right here and `secrets` would be cargo cult.
_PROCESS_PREFIX = f"{random.getrandbits(12):03x}"  # noqa: S311
#: Never rewound, and never consulted for reuse: two connections under one name read as one
#: connection in a log, which is the failure WSM-API-009 exists to prevent.
_CONNECTION_COUNTER = itertools.count()

StreamHandler = Callable[[Any, Stream], Any]
ErrorSerializer = Callable[[BaseException], Any]


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
    ) -> None:
        self.id = f"{_PROCESS_PREFIX}-{next(_CONNECTION_COUNTER)}"
        #: An ordinary dict with ordinary dict semantics. muxws never reads it (WSM-REG-001/002).
        self.tags: dict[str, Any] = {}

        self._socket = socket
        self._codec = codec
        self._is_dialer = is_dialer
        self._error_serializer = error_serializer or default_error_serializer
        self._max_frame_bytes = max_frame_bytes

        # The dialer allocates odd ids, the acceptor even ones (WSM-SID-002).
        self._next_id = 1 if is_dialer else 2
        self._streams: dict[int, Stream] = {}
        #: Exactly two integers plus the live-stream map; nothing per closed stream (WSM-STM-001).
        self._highest_local_open = 0
        self._highest_remote_open = 0
        self._ignored_late_frames = 0

        self._handler: StreamHandler | None = None
        self._close_handlers: list[Callable[[Any], None]] = []
        self._frame_handlers: list[Callable[[str, Frame, int], None]] = []

        self._outbound: asyncio.Queue[Frame | None] = asyncio.Queue()
        self._writer_task: asyncio.Task[None] | None = None
        self._is_open = True
        self._death: ConnectionClosed | None = None

    # ------------------------------------------------------------------ properties

    @property
    def is_open(self) -> bool:
        return self._is_open

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

    def on_frame(self, handler: Callable[[str, Frame, int], None]) -> Callable[[str, Frame, int], None]:
        """`(direction, frame, byte_length)`, before encode and after decode (WSM-OBS-003)."""
        self._frame_handlers.append(handler)
        return handler

    # ------------------------------------------------------------------ opening

    def open(self, payload: Any = None, *, headers: dict[str, Any] | None = None, end: bool = False) -> Stream:
        """Open a stream. **Synchronous**, and it never queues (WSM-API-001/004).

        Allocation and enqueue are one indivisible step with no suspension point between them, so
        wire order is allocation order by construction (WSM-SID-006). Two concurrent `open()` calls
        that could interleave here would put a non-monotonic id sequence on the wire - a protocol
        error this peer would be committing against itself (WSM-INV-005).
        """
        self._raise_if_unopenable()
        stream_id = self._next_id
        self._next_id += 2
        self._highest_local_open = stream_id
        stream = Stream(self, stream_id, headers=headers, payload=payload, local=True)
        stream.state = StreamState.HALF_CLOSED_LOCAL if end else StreamState.OPEN
        self._streams[stream_id] = stream
        self._enqueue(Frame("open", stream=stream_id, payload=payload, headers=headers, end=end))
        return stream

    def _raise_if_unopenable(self) -> None:
        """WSM-API-004: exactly two synchronous raises, and never one for concurrency."""
        if not self._is_open:
            raise ConnectionLost("the peer is between sockets; nothing is buffered for the next one")

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
        self._outbound.put_nowait(frame)

    async def _write_loop(self) -> None:
        while True:
            frame = await self._outbound.get()
            if frame is None:
                return
            try:
                encoded = self._codec.encode(frame)
                self._report_frame("tx", frame, encoded)
                if self._codec.binary:
                    await self._socket.send_bytes(encoded)  # type: ignore[arg-type]
                else:
                    await self._socket.send_text(encoded)  # type: ignore[arg-type]
            except ConnectionClosed:
                return
            except asyncio.CancelledError:
                raise

    def _report_frame(self, direction: str, frame: Frame, encoded: str | bytes) -> None:
        length = encoded_length(encoded)
        for handler in self._frame_handlers:
            handler(direction, frame, length)
        if logger.isEnabledFor(logging.DEBUG):
            # Never the payload's contents: application data routinely holds secrets (WSM-OBS-002).
            logger.debug(
                "muxws conn=%s dir=%s type=%-6s stream=%s end=%d bytes=%d",
                self.id,
                direction,
                frame.type,
                frame.stream if frame.stream is not None else "-",
                int(frame.end),
                length,
            )

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
                if not await self._dispatch(frame):
                    return
        finally:
            await self._stop_writer()

    async def _dispatch(self, frame: Frame) -> bool:
        """Route one decoded frame. Returns False when the connection must end."""
        if frame.type == "open":
            return await self._on_open(frame)
        if frame.type in ("data", "reset"):
            return await self._on_stream_frame(frame)
        if frame.type in ("ping", "pong", "goaway"):
            # Connection-level frames arrive in M4; tolerating them now costs nothing.
            return True
        # WSM-FRM-002: unknown types are ignored, logged once, and are never an error.
        logger.info("muxws conn=%s ignoring unknown frame type %r (WSM-FRM-002)", self.id, frame.type)
        return True

    # ------------------------------------------------------------------ inbound opens

    async def _on_open(self, frame: Frame) -> bool:
        stream_id = frame.stream
        if stream_id is None:
            await self._fail_connection("open frame with no stream id")
            return False

        existing = self._streams.get(stream_id)
        if existing is not None and existing._assembler.in_progress and frame.fragment is not None:
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
        stream = Stream(self, stream_id, headers=frame.headers, local=False)
        stream.state = StreamState.HALF_CLOSED_REMOTE if frame.end else StreamState.OPEN
        self._streams[stream_id] = stream

        if frame.fragment is not None:
            payload = stream._assembler.feed(frame, self._codec)
            if payload is ABSENT:
                return True
            stream.payload = payload
        else:
            stream.payload = frame.payload if frame.payload is not ABSENT else None

        self._start_handler(stream)
        return True

    async def _continue_open(self, stream: Stream, frame: Frame) -> bool:
        """A later fragment of an opening payload: not a second open (WSM-STM-031)."""
        payload = stream._assembler.feed(frame, self._codec)
        if payload is ABSENT:
            return True
        stream.payload = payload
        if frame.end:
            stream.state = StreamState.HALF_CLOSED_REMOTE
        self._start_handler(stream)
        return True

    def _start_handler(self, stream: Stream) -> None:
        """Dispatch to the one handler, or refuse when there is none (WSM-STM-033)."""
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
        payload = self._error_serializer(exc)
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
                    ResetCode(frame.code or 0),
                    frame.reason,
                    stream_id=stream_id,
                    payload=frame.payload if frame.payload is not ABSENT else None,
                ),
                notify_remote=False,
            )
            return True
        return await self._on_data(stream, frame)

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
            payload = stream._assembler.feed(frame, self._codec)
            if payload is ABSENT:
                return True
            stream._accept_payload(payload)
        elif frame.payload is not ABSENT:
            stream._accept_payload(frame.payload)

        if frame.end:
            stream._remote_end(frame.trailers)
        return True

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
        for _ in range(100):
            if self._outbound.empty():
                break
            await asyncio.sleep(0)

    async def _stop_writer(self) -> None:
        if self._writer_task is not None and not self._writer_task.done():
            await self._outbound.put(None)
            try:
                await asyncio.wait_for(self._writer_task, 1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()

    def _die(self, cause: ConnectionClosed) -> None:
        """Socket death: every live stream fails with ConnectionLost **before** on_close fires."""
        if not self._is_open:
            return
        self._is_open = False
        self._death = cause

        for stream in list(self._streams.values()):
            stream._fail(
                ConnectionLost(f"connection closed: {cause.reason or cause}", stream_id=stream.id),
                notify_remote=False,
            )
        self._streams.clear()

        reason = CloseReason(code=cause.code, reason=cause.reason, was_clean=cause.was_clean, will_retry=False)
        for handler in self._close_handlers:
            handler(reason)

    def _forget(self, stream: Stream) -> None:
        """Drop a closed stream. Nothing is retained per closed stream (WSM-STM-001)."""
        self._streams.pop(stream.id, None)

    def __repr__(self) -> str:
        role = "dialer" if self._is_dialer else "acceptor"
        return f"<Peer {self.id} {role} streams={len(self._streams)}>"
