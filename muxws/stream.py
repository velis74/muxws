"""`Stream`: one independently addressed, independently cancellable, bidirectional exchange (§5, §9)."""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, TYPE_CHECKING

from muxws.errors import (
    exception_for_reset,
    ProtocolError,
    ResetCode,
    StreamAlreadyConsumed,
    StreamClosed,
    StreamReset,
    StreamTimeout,
)
from muxws.fragment import Assembler
from muxws.frames import ABSENT, Frame

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.peer import Peer


class StreamState(str, Enum):
    """The five states of §5.3, tracked per stream per peer."""

    IDLE = "idle"
    OPEN = "open"
    HALF_CLOSED_LOCAL = "half_closed_local"
    HALF_CLOSED_REMOTE = "half_closed_remote"
    CLOSED = "closed"


#: Sentinel pushed into the iteration queue to end an `async for` cleanly.
_END = object()


def _sendable_reset_code(code: ResetCode | int) -> ResetCode:
    """Return `code` as a `ResetCode`, or raise `ProtocolError` for a code that must not be sent.

    `CONNECTION_CLOSED` is synthesised locally when the socket dies and MUST NEVER appear on the wire
    (§8.1). Codes outside the enum are not defined by this protocol generation.
    """
    if code == ResetCode.CONNECTION_CLOSED:
        raise ProtocolError(
            "CONNECTION_CLOSED is synthesised locally when the socket dies and must never be sent; "
            "use cancel() or another reset code (§8.1)"
        )
    try:
        return ResetCode(code)
    except ValueError:
        raise ProtocolError(f"{code!r} is not a reset code this generation defines (§8.1)") from None


class Stream:
    """Simultaneously awaitable and async-iterable; the first use claims it (WSM-API-002/014)."""

    def __init__(
        self,
        peer: Peer,
        stream_id: int,
        *,
        headers: dict[str, Any] | None = None,
        payload: Any = ABSENT,
        local: bool,
    ) -> None:
        self._peer = peer
        self.id = stream_id
        #: The `open` frame's headers, as the opener sent them; the same on both peers.
        self.headers: dict[str, Any] = headers or {}
        #: The answering side's leading headers, empty until it sends any (WSM-API-025). The same on
        #: both peers: the opener reads the headers it received, the answering peer the headers it sent.
        self.reply_headers: dict[str, Any] = {}
        self.payload: Any = payload
        self.trailers: dict[str, Any] | None = None
        self.closed = asyncio.Event()
        #: Set when `reply_headers` can no longer change: on the answering side's first frame on the
        #: stream, or when the stream closes without an answer (a reset or a dead socket). The second
        #: case keeps a wait on this event from hanging (WSM-INV-011).
        self.reply_headers_arrived = asyncio.Event()

        self.state = StreamState.IDLE
        #: True when this peer opened the stream, false when the remote did (WSM-STM-037 counts only
        #: the remote's).
        self.local = local
        #: True for a stream opened with `oneway`: neither peer sends a frame on it after the `open`
        #: (WSM-FRM-020).
        self.oneway = False

        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._future: asyncio.Future[Any] | None = None
        self._claim: str | None = None
        #: How the stream ended: None while live, "normal" when both ends ended, else the
        #: `StreamReset` that ended it (WSM-ERR-009).
        self._close_cause: str | StreamReset | None = None
        self._assembler = Assembler()
        #: True between the first fragment of a remote opening payload and the frame that completes
        #: it. Kept separately from the assembler state, so a wrong-parity `open` on this id is not
        #: taken for a continuation of a reassembly in progress.
        self._opening = False
        #: The handler task, so an incoming reset(CANCELLED) can cancel it (WSM-ERR-013).
        self.handler_task: asyncio.Task[None] | None = None
        #: Each side may send leading headers only on its first frame (WSM-FRM-016). At construction
        #: one side's first frame has already happened: the `open` this peer enqueued for a local
        #: stream, or the `open` it read for a remote one.
        self._sent_a_frame = local
        self._received_a_frame = not local

    # ------------------------------------------------------------------ sending

    async def send(self, payload: Any, *, end: bool = False, headers: dict[str, Any] | None = None) -> None:
        """Send one payload. Raises per WSM-ERR-009 on a stream that is no longer open."""
        self._raise_if_not_sendable()
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            raise StreamClosed(f"stream {self.id} already sent end; it cannot send again")
        leading = self._leading_headers(headers)
        self._peer._enqueue(Frame("data", stream=self.id, payload=payload, headers=leading, end=end))
        if end:
            self._local_end()

    async def send_headers(self, headers: dict[str, Any]) -> None:
        """Announce this side's leading headers with no payload at all (WSM-API-024).

        The headers travel on a `data` frame with no payload, so an answering peer can send them
        before it has a payload. This is this side's first frame, so a later `send(headers=)` on the
        same stream raises.
        """
        self._raise_if_not_sendable()
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            raise StreamClosed(f"stream {self.id} already sent end; it cannot send headers after it")
        self._peer._enqueue(Frame("data", stream=self.id, headers=self._leading_headers(headers)))

    async def end(
        self,
        payload: Any = ABSENT,
        *,
        trailers: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        """End this side of the stream, optionally with a last payload and trailers."""
        self._raise_if_not_sendable()
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            raise StreamClosed(f"stream {self.id} already sent end; it cannot end twice")
        leading = self._leading_headers(headers)
        self._peer._enqueue(
            Frame("data", stream=self.id, payload=payload, headers=leading, end=True, trailers=trailers)
        )
        self._local_end()

    async def reply(
        self,
        payload: Any,
        *,
        trailers: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        """`send` plus `end`, which is what a unary handler wants."""
        await self.end(payload, trailers=trailers, headers=headers)

    def _leading_headers(self, headers: dict[str, Any] | None) -> dict[str, Any] | None:
        """WSM-FRM-016, sending side: headers may appear only on this side's first frame on the stream.

        Called for every frame this side sends, with or without headers, because the rule concerns
        which frame is first. Headers passed after the first frame raise `ProtocolError` rather than
        being dropped (WSM-API-024).
        """
        if headers is not None and self._sent_a_frame:
            raise ProtocolError(
                f"stream {self.id} was opened by this peer, so its first frame was the open; "
                f"pass headers to open() instead (WSM-FRM-016)"
                if self.local
                else f"stream {self.id} has already sent its first frame; headers ride that one or none (WSM-FRM-016)"
            )
        if not self._sent_a_frame and not self.local:
            # Runs before the first frame is marked as sent, so an interceptor that raises leaves the
            # stream able to send it (WSM-API-028).
            headers = self._peer._intercepted_reply_headers(headers, self)
        self._sent_a_frame = True
        # This peer is answering, so the headers it sends are its `reply_headers`, as they are on the
        # opener's side.
        if not self.local:
            self._accept_reply_headers(headers)
        return headers

    def _raise_if_not_sendable(self) -> None:
        """Raise the stream's reset, or `StreamClosed` after a normal close (WSM-ERR-009).

        Nothing is sent on a `oneway` stream, so every send raises `StreamClosed` (WSM-STM-040).
        """
        if self._close_cause is None and self.oneway:
            raise StreamClosed(f"stream {self.id} is oneway; nothing can be sent on it")
        if self._close_cause is None:
            return
        if isinstance(self._close_cause, StreamReset):
            # A new instance per raise: re-raising the stored one appends to its traceback on every
            # call, so a loop that keeps sending would grow it without bound.
            raise self._close_cause.clone()
        raise StreamClosed(f"stream {self.id} closed normally; nothing more can be sent on it")

    # ------------------------------------------------------------------ resetting

    async def cancel(self, reason: str | None = None) -> None:
        """`reset(CANCELLED)`, closing locally at once without waiting for acknowledgement."""
        await self.reset(ResetCode.CANCELLED, reason)

    async def reset(self, code: ResetCode, reason: str | None = None) -> None:
        """Terminate the stream in both directions.

        A no-op once the stream is closed, including after socket death (WSM-RCN-041). Raises
        `ProtocolError` for `CONNECTION_CLOSED` or a code outside `ResetCode`.
        """
        code = _sendable_reset_code(code)
        if self.state is StreamState.CLOSED:
            return
        # `_has_a_socket`, not `is_open`: `is_open` is false during the hello window while the socket
        # is open (WSM-RCN-043), and the reset of a stream the acceptor pushed in that window must be
        # sent.
        if self._peer._has_a_socket and not self.oneway:
            self._peer._enqueue_reset(self.id, code, reason)
        self._fail(exception_for_reset(code, reason, stream_id=self.id), notify_remote=False)

    # ------------------------------------------------------------------ receiving

    def _note_remote_frame(self, headers: dict[str, Any] | None) -> bool:
        """WSM-FRM-016, receiving side. Returns False when headers arrive after the remote's first frame.

        Called for every stream-level frame the remote sends, with or without headers, so the first
        frame is recognised even when it carries none; `reply_headers_arrived` is set on that frame.
        """
        first = not self._received_a_frame
        self._received_a_frame = True
        if not first:
            return headers is None
        # On a stream this peer opened, the remote's first frame is its answer and carries the reply
        # headers. On a stream the remote opened, its first frame was the `open`, whose headers were
        # stored in `headers` at construction.
        if self.local:
            self._accept_reply_headers(headers)
        return True

    def _accept_reply_headers(self, headers: dict[str, Any] | None) -> None:
        """Store `headers` as `reply_headers` (when given) and set `reply_headers_arrived`."""
        if headers is not None:
            self.reply_headers = headers
        self.reply_headers_arrived.set()

    def _accept_payload(self, payload: Any) -> None:
        """Deliver one reassembled payload to whichever shape is consuming this stream."""
        if self._future is not None and not self._future.done():
            self._future.set_result(payload)
        self._queue.put_nowait(payload)

    def _remote_end(self, trailers: dict[str, Any] | None) -> None:
        if trailers is not None:
            self.trailers = trailers
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            self._close_normally()
        elif self.state is not StreamState.CLOSED:
            self.state = StreamState.HALF_CLOSED_REMOTE
            self._queue.put_nowait(_END)
            self._settle_future_if_empty()

    def _local_end(self) -> None:
        if self.state is StreamState.HALF_CLOSED_REMOTE:
            self._close_normally()
        else:
            self.state = StreamState.HALF_CLOSED_LOCAL

    def _close_normally(self) -> None:
        self.state = StreamState.CLOSED
        self._close_cause = "normal"
        self._queue.put_nowait(_END)
        self._settle_future_if_empty()
        self._mark_closed()
        self._peer._forget(self)

    def _mark_closed(self) -> None:
        """Set `closed` and `reply_headers_arrived`, on every close path.

        A stream that closes before it was answered receives no reply headers, so a wait for them ends
        here (WSM-API-025).
        """
        self.reply_headers_arrived.set()
        self.closed.set()

    def _settle_future_if_empty(self) -> None:
        """Fail a pending await with `ProtocolError` when the stream ends without a payload."""
        self._fail_future(ProtocolError(f"stream {self.id} ended without producing a payload"))

    def _fail_future(self, error: BaseException) -> None:
        """Resolve the memoized future with an error, and mark the exception as retrieved.

        The awaiter that created the future may have been cancelled, and asyncio would then log the
        exception as never retrieved. This corresponds to the TypeScript requirement in WSM-API-016.
        """
        if self._future is None or self._future.done():
            return
        self._future.set_exception(error)
        self._future.add_done_callback(lambda future: future.exception())

    def _fail(self, error: StreamReset, *, notify_remote: bool) -> None:
        """Close the stream because it was reset, locally or remotely, or because the socket died."""
        _ = notify_remote
        if self.state is StreamState.CLOSED:
            return
        self.state = StreamState.CLOSED
        self._close_cause = error
        self._assembler.reset()
        self._fail_future(error)
        self._queue.put_nowait(error)
        self._mark_closed()
        if self.handler_task is not None and not self.handler_task.done():
            self.handler_task.cancel()
        self._peer._forget(self)

    # ------------------------------------------------------------------ consuming

    def _claim_for(self, use: str) -> None:
        if self._claim is not None and self._claim != use:
            raise StreamAlreadyConsumed(
                f"stream {self.id} was already consumed by {self._claim!r} and cannot also be "
                f"consumed by {use!r}; a stream has one consumer (WSM-API-014)"
            )
        if self._claim == "iterate" and use == "iterate":
            raise StreamAlreadyConsumed(
                f"stream {self.id} is already being iterated; two 'iterate' consumers would split "
                f"its payloads between them (WSM-API-014)"
            )
        self._claim = use

    def _memoized_future(self) -> asyncio.Future[Any]:
        """One future per stream, created lazily and resolved exactly once (WSM-API-010/011)."""
        if self._future is None:
            self._future = asyncio.get_running_loop().create_future()
            self._resolve_from_current_state()
        return self._future

    def _resolve_from_current_state(self) -> None:
        """Settle a newly created future from what the stream has already received.

        The first await may come after a payload, a reset, or an end without a payload has arrived.
        Each of these settles the future immediately, so the await does not hang (WSM-INV-011).
        """
        if self._future is None or self._future.done():
            return
        if isinstance(self._close_cause, StreamReset):
            self._fail_future(self._close_cause)
            return
        if self._queue.empty():
            return
        pending = self._queue._queue[0]  # noqa: SLF001 - peek, without consuming
        if isinstance(pending, StreamReset):
            self._fail_future(pending)
        elif pending is _END:
            self._fail_future(ProtocolError(f"stream {self.id} ended without producing a payload"))
        else:
            self._future.set_result(pending)

    def __await__(self):
        self._claim_for("await")
        return self._wait(None).__await__()

    async def result(self, timeout: float | None = None) -> Any:
        """Await the stream's first payload, with an optional deadline in seconds (WSM-API-012).

        Uses the same memoized future as `await stream`, so a second read returns the first payload,
        not the next one. On expiry the stream is reset with TIMEOUT and `StreamTimeout` is raised.
        """
        self._claim_for("await")
        return await self._wait(timeout)

    async def _wait(self, timeout: float | None) -> Any:
        """The wait shared by `await stream` and `result()`.

        Shielded, so cancelling one awaiter does not cancel the memoized future, and a later await gets
        the same answer (WSM-API-011). When the awaiter is cancelled, the stream is reset with
        CANCELLED and the `CancelledError` propagates (WSM-ERR-014).
        """

        future = self._memoized_future()
        try:
            if timeout is None:
                return await asyncio.shield(future)
            return await asyncio.wait_for(asyncio.shield(future), timeout)
        except asyncio.TimeoutError:
            await self.reset(ResetCode.TIMEOUT, f"deadline of {timeout}s expired")
            raise StreamTimeout(f"stream {self.id} did not answer within {timeout}s", stream_id=self.id) from None
        except asyncio.CancelledError:
            await self.reset(ResetCode.CANCELLED, "consumer cancelled")
            raise

    def __aiter__(self) -> AsyncIterator[Any]:
        self._claim_for("iterate")
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                await self.reset(ResetCode.CANCELLED, "consumer cancelled")
                raise
            if item is _END:
                return
            if isinstance(item, StreamReset):
                raise item
            yield item

    def __repr__(self) -> str:
        return f"<Stream {self.id} {self.state.value}>"
