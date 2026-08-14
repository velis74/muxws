"""Reset codes and the muxws exception hierarchy (WSM-ERR-002..009, §8.1-8.2)."""

from __future__ import annotations

from enum import IntEnum
from typing import Any


class ResetCode(IntEnum):
    """Numeric on the wire, named in both APIs; shared by `reset` and `goaway`.

    There are **nine** members. `5` is retired (it was `STREAM_LIMIT`, WSM-STM-022): the number MUST
    NOT be reused and MUST NOT appear on the wire, so no name maps to it.
    """

    NO_ERROR = 0
    CANCELLED = 1
    APPLICATION_ERROR = 2
    PROTOCOL_ERROR = 3
    REFUSED = 4
    # 5 is retired - see the class docstring.
    TIMEOUT = 6
    PAYLOAD_TOO_LARGE = 7
    INTERNAL_ERROR = 8
    CONNECTION_CLOSED = 9


class MuxwsError(Exception):
    """Root of every error this library raises."""


class ProtocolError(MuxwsError):
    """This peer or the remote violated the specification."""


class ConnectionClosed(MuxwsError):
    """The socket died. Raised by `serve()` and peer-level calls, never by a stream (WSM-ERR-002)."""

    def __init__(self, message: str = "", *, code: int = 1006, reason: str = "", was_clean: bool = False) -> None:
        super().__init__(message or reason or "connection closed")
        self.code = code
        self.reason = reason
        self.was_clean = was_clean


class ConnectionGoingAway(MuxwsError):
    """`open()` was called after a `goaway` arrived. Raised synchronously at the call site."""


class StreamAlreadyConsumed(MuxwsError):
    """A stream was awaited and iterated, or iterated twice (WSM-API-014)."""


class StreamClosed(MuxwsError):
    """`send()`/`end()`/`reply()` on a stream that closed **normally**.

    Deliberately neither a `StreamReset` nor a `ProtocolError` (WSM-ERR-009): a normal close racing a
    last `send()` is an expected outcome, not a failure and not a caller bug.
    """


class CodecError(MuxwsError):
    """Configuration failure. Outside `StreamReset`: not a stream failure, not retryable."""

    def __init__(self, message: str, *, configured: str | None = None, available: list[str] | None = None) -> None:
        super().__init__(message)
        self.configured = configured
        self.available = available if available is not None else []


class CodecNotRegistered(CodecError):
    """The configured codec name was never registered. Raised before any socket is opened."""


class CodecMismatch(CodecError):
    """The acceptor's codec differs from ours; the WebSocket handshake was rejected."""


class TransportUrlError(MuxwsError, ValueError):
    """A transport was given an address it cannot open (WSM-ERR-016). Never raised directly.

    The concrete class is the transport's - `muxws.transports.unix.UnixUrlError` for a `ws+unix:` URL
    naming no socket file, the `websockets` adapter's for a `ws:` URL that library cannot parse - and
    each lives in its own transport's module rather than here. The adapter seam is public
    (WSM-API-021) and a third-party adapter cannot add a class to this file, so the convention has to
    be one it can follow too. Only the base is shared, because `except TransportUrlError` must be
    writable by an application that has not imported the transport whose URL was wrong.

    A `ValueError` as well as a `MuxwsError`: a caller who never heard of this library is already
    catching `ValueError` around a URL it typed, and an application that funnels every muxws failure
    through one handler must not have a bad address leak through it.

    Errors the `SocketAdapter` contract itself mandates stay in the shared vocabulary above:
    `ConnectionClosed` is required of every adapter, so it is owned by the seam. A URL grammar, a
    missing package or a missing kernel feature is owned by exactly one transport.
    """


class TransportUnsupportedError(MuxwsError, RuntimeError):
    """This runtime cannot provide the transport at all (WSM-ERR-016). Never raised directly.

    No `AF_UNIX` in the interpreter, an optional dependency that was never installed: conditions that
    are local, permanent, and fixed by changing the environment rather than the address. A separate
    base from `TransportUrlError` for that reason - that one means *retype the URL*, this one means
    *the URL is fine, change where or how you are running*.

    A `RuntimeError` rather than a `ValueError` says so to a caller who never heard of muxws, and a
    `MuxwsError` so that an application-wide handler still catches it. A subclass raised for a
    dependency that is not installed MUST name the install that fixes it - `pip install
    muxws[websockets]` - because "no module named X" is what the interpreter already said. As with
    `TransportUrlError`, the concrete subclasses belong to their transports' modules and are not
    exported from the package root.
    """


class StreamReset(MuxwsError):
    """A stream ended early. Carries the reset code, its reason and the stream id."""

    #: A `ResetCode` for every code this generation defines, and a bare `int` for one it does not -
    #: a peer of another generation, or an implementation using the retired 5, still reset the
    #: stream, and refusing to represent that would be refusing to hear it.
    code: ResetCode | int = ResetCode.NO_ERROR

    def __init__(
        self,
        reason: str | None = None,
        *,
        code: ResetCode | int | None = None,
        stream_id: int | None = None,
    ) -> None:
        if code is not None:
            self.code = _as_reset_code(code)
        label = self.code.name if isinstance(self.code, ResetCode) else f"reset code {self.code}"
        super().__init__(reason if reason is not None else label)
        self.reason = reason
        self.stream_id = stream_id

    def clone(self) -> StreamReset:
        """A fresh instance of the same failure, carrying no traceback of its own.

        `send()` on a reset stream raises the stream's stored failure. Raising the stored *instance*
        appends a frame to its traceback every time, so a caller that keeps trying grows an object
        that is never released.
        """
        fresh = self.__class__.__new__(self.__class__)
        fresh.__dict__.update(self.__dict__)
        Exception.__init__(fresh, *self.args)
        return fresh


class RemoteError(StreamReset):
    """The remote handler raised. Carries the serialized error object, if the remote sent one."""

    code = ResetCode.APPLICATION_ERROR

    def __init__(self, reason: str | None = None, *, stream_id: int | None = None, payload: Any = None) -> None:
        super().__init__(reason, stream_id=stream_id)
        self.payload = payload


class StreamTimeout(StreamReset):
    """A local deadline expired; the remote was told to stop working."""

    code = ResetCode.TIMEOUT


class StreamRefused(StreamReset):
    """Not accepted and definitively not processed. Retry - elsewhere, or after a delay."""

    code = ResetCode.REFUSED


class ConnectionLost(StreamReset):
    """Synthesised locally when the socket dies. MUST NEVER appear on the wire (reset code 9)."""

    code = ResetCode.CONNECTION_CLOSED


#: Maps a reset code arriving on the wire to the exception class that represents it locally.
_RESET_CODE_EXCEPTIONS: dict[ResetCode, type[StreamReset]] = {
    ResetCode.APPLICATION_ERROR: RemoteError,
    ResetCode.TIMEOUT: StreamTimeout,
    ResetCode.REFUSED: StreamRefused,
    ResetCode.CONNECTION_CLOSED: ConnectionLost,
}


def _as_reset_code(code: ResetCode | int) -> ResetCode | int:
    """A defined code as its enum member, an undefined one as the raw integer."""
    try:
        return ResetCode(code)
    except ValueError:
        return int(code)


def exception_for_reset(
    code: ResetCode | int,
    reason: str | None = None,
    *,
    stream_id: int | None = None,
    payload: Any = None,
) -> StreamReset:
    """Build the `StreamReset` subclass that represents `code`, falling back to `StreamReset` itself.

    `payload` is the structured error object a `reset(APPLICATION_ERROR)` may carry (WSM-ERR-006); it
    is meaningful only for `RemoteError` and ignored for every other code.
    """
    code = _as_reset_code(code)
    cls = _RESET_CODE_EXCEPTIONS.get(code) if isinstance(code, ResetCode) else None
    if cls is None:
        return StreamReset(reason, code=code, stream_id=stream_id)
    if cls is RemoteError:
        return RemoteError(reason, stream_id=stream_id, payload=payload)
    return cls(reason, stream_id=stream_id)
