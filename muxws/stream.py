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
        self.headers: dict[str, Any] = headers or {}
        self.payload: Any = payload
        self.trailers: dict[str, Any] | None = None
        self.closed = asyncio.Event()

        self.state = StreamState.IDLE
        #: True when this peer opened the stream, false when the remote did (WSM-STM-037 counts only
        #: the remote's).
        self.local = local

        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._future: asyncio.Future[Any] | None = None
        self._claim: str | None = None
        #: How the stream ended: None while live, "normal" when both ends ended, else the
        #: `StreamReset` that killed it (WSM-ERR-009).
        self._close_cause: str | StreamReset | None = None
        self._assembler = Assembler()
        #: The handler task, so an incoming reset(CANCELLED) can cancel it (WSM-ERR-013).
        self.handler_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ sending

    async def send(self, payload: Any, *, end: bool = False) -> None:
        """Send one payload. Raises per WSM-ERR-009 on a stream that is no longer open."""
        self._raise_if_not_sendable()
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            raise StreamClosed(f"stream {self.id} already sent end; it cannot send again")
        self._peer._enqueue(Frame("data", stream=self.id, payload=payload, end=end))
        if end:
            self._local_end()

    async def end(self, payload: Any = ABSENT, *, trailers: dict[str, Any] | None = None) -> None:
        """End this side of the stream, optionally with a last payload and trailers."""
        self._raise_if_not_sendable()
        if self.state is StreamState.HALF_CLOSED_LOCAL:
            raise StreamClosed(f"stream {self.id} already sent end; it cannot end twice")
        self._peer._enqueue(Frame("data", stream=self.id, payload=payload, end=True, trailers=trailers))
        self._local_end()

    async def reply(self, payload: Any, *, trailers: dict[str, Any] | None = None) -> None:
        """`send` plus `end`, which is what a unary handler wants."""
        await self.end(payload, trailers=trailers)

    def _raise_if_not_sendable(self) -> None:
        """WSM-ERR-009: three different outcomes, three different classes."""
        if self._close_cause is None:
            return
        if isinstance(self._close_cause, StreamReset):
            raise self._close_cause
        raise StreamClosed(f"stream {self.id} closed normally; nothing more can be sent on it")

    # ------------------------------------------------------------------ resetting

    async def cancel(self, reason: str | None = None) -> None:
        """`reset(CANCELLED)`, closing locally at once without waiting for acknowledgement."""
        await self.reset(ResetCode.CANCELLED, reason)

    async def reset(self, code: ResetCode, reason: str | None = None) -> None:
        """Terminate the stream in both directions.

        A no-op once the stream is closed - including after socket death, where there is nothing to
        send it on (WSM-RCN-041).
        """
        if self.state is StreamState.CLOSED:
            return
        if self._peer.is_open:
            self._peer._enqueue(Frame("reset", stream=self.id, code=int(code), reason=reason))
        self._fail(exception_for_reset(code, reason, stream_id=self.id), notify_remote=False)

    # ------------------------------------------------------------------ receiving

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
        self.closed.set()
        self._peer._forget(self)

    def _settle_future_if_empty(self) -> None:
        """A stream that ends without ever producing a payload must not leave an await hanging."""
        if self._future is not None and not self._future.done():
            self._future.set_exception(ProtocolError(f"stream {self.id} ended without producing a payload"))

    def _fail(self, error: StreamReset, *, notify_remote: bool) -> None:
        """Close the stream because it was reset, locally or remotely, or because the socket died."""
        _ = notify_remote
        if self.state is StreamState.CLOSED:
            return
        self.state = StreamState.CLOSED
        self._close_cause = error
        self._assembler.reset()
        if self._future is not None and not self._future.done():
            self._future.set_exception(error)
        self._queue.put_nowait(error)
        self.closed.set()
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
        """Hand a freshly created future whatever answer the stream already has.

        The first await may arrive long after the answer did - a payload, a reset, or an end with no
        payload at all. Every one of those must settle the future at once; leaving it pending is the
        spinner that never stops which WSM-INV-011 names.
        """
        if self._future is None or self._future.done():
            return
        if isinstance(self._close_cause, StreamReset):
            self._future.set_exception(self._close_cause)
            return
        if self._queue.empty():
            return
        pending = self._queue._queue[0]  # noqa: SLF001 - peek, without consuming
        if isinstance(pending, StreamReset):
            self._future.set_exception(pending)
        elif pending is _END:
            self._future.set_exception(ProtocolError(f"stream {self.id} ended without producing a payload"))
        else:
            self._future.set_result(pending)

    def __await__(self):
        self._claim_for("await")
        return self._memoized_future().__await__()

    async def result(self, timeout: float | None = None) -> Any:
        """The same future with a deadline wrapped around the wait (WSM-API-012).

        Never a second source of the value: `await stream` and `await stream.result()` resolve from
        one place, so a second read returns the first read's value rather than the next payload.
        """
        self._claim_for("await")
        future = self._memoized_future()
        if timeout is None:
            return await future
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout)
        except asyncio.TimeoutError:
            await self.reset(ResetCode.TIMEOUT, f"deadline of {timeout}s expired")
            raise StreamTimeout(f"stream {self.id} did not answer within {timeout}s", stream_id=self.id) from None

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
