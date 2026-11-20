"""`Exchange`: what each side sent on one finished stream, how the stream ended and how long it took (WSM-OBS-004)."""

from __future__ import annotations

import base64
import json
import time

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, TYPE_CHECKING

from muxws.errors import StreamReset

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.stream import Stream

#: Called once per finished stream with its `Exchange`. The return value is ignored; a coroutine is not awaited.
ExchangeHandler = Callable[["Exchange"], Any]


@dataclass(frozen=True, slots=True)
class ExchangeSide:
    """What one side of a stream sent: leading headers, payloads, trailers and whether it ended."""

    #: The side's leading headers: the `open` frame's for the opener, the reply headers for the answerer.
    headers: dict[str, Any] | None
    #: The side's logical payloads in order, cut to the handler's `max_payloads`.
    payloads: tuple[Any, ...]
    #: How many payloads the side sent in total, including those not in `payloads`.
    payload_count: int
    #: The trailers of the side's end frame.
    trailers: dict[str, Any] | None
    #: Whether the side sent `end`.
    ended: bool

    def _as_dict(self, trim_payload_size: int | None) -> dict[str, Any]:
        payloads = [_plain(payload) for payload in self.payloads]
        if trim_payload_size is not None:
            payloads = [_trimmed(payload, trim_payload_size) for payload in payloads]
        return {
            "headers": _plain(self.headers),
            "payloads": payloads,
            "payload_count": self.payload_count,
            "trailers": _plain(self.trailers),
            "ended": self.ended,
        }


@dataclass(frozen=True, slots=True)
class Exchange:
    """One finished stream, as the reporting peer saw it. Durations are in seconds."""

    #: `Peer.id` of the connection the stream lived on.
    conn: str
    #: The stream id.
    stream: int
    opened_by: Literal["local", "remote"]
    oneway: bool
    #: What the side that opened the stream sent.
    request: ExchangeSide
    #: What the other side sent.
    response: ExchangeSide
    outcome: Literal["ok", "reset", "lost"]
    #: Which peer sent or caused the reset; None unless `outcome` is "reset".
    reset_by: Literal["local", "remote"] | None
    #: The reset code; None unless `outcome` is "reset".
    code: int | None
    #: The reset reason; None unless `outcome` is "reset".
    reason: str | None
    #: Wall-clock time (UTC) at which the stream object was created.
    started_at: datetime
    #: Seconds from creation to the answering side's first `data` frame (a reset does not count);
    #: None when it sent none.
    first_response: float | None
    #: Seconds from creation to the end of the stream.
    duration: float

    def as_dict(self, trim_payload_size: int | None = None) -> dict[str, Any]:
        """A new JSON-ready dict of the exchange; binary values become base64 strings.

        With `trim_payload_size`, a payload whose compact JSON exceeds that many UTF-8 bytes is
        replaced by a string of its first `trim_payload_size` bytes, cut back to a character boundary,
        followed by `…(trimmed N bytes)…`. Headers and trailers are never trimmed. Raises `ValueError`
        for a negative or non-integer limit.
        """
        if trim_payload_size is not None and (not _is_int(trim_payload_size) or trim_payload_size < 0):
            raise ValueError(f"trim_payload_size must be an integer >= 0 or None, not {trim_payload_size!r}")
        at = self.started_at.astimezone(timezone.utc)
        return {
            "ts": f"{at:%Y-%m-%dT%H:%M:%S}.{at.microsecond // 1000:03d}Z",
            "conn": self.conn,
            "stream": self.stream,
            "opened_by": self.opened_by,
            "oneway": self.oneway,
            "request": self.request._as_dict(trim_payload_size),
            "response": self.response._as_dict(trim_payload_size),
            "outcome": self.outcome,
            "reset_by": self.reset_by,
            "code": self.code,
            "reason": self.reason,
            "first_response_ms": _milliseconds(self.first_response),
            "duration_ms": _milliseconds(self.duration),
        }


def checked_max_payloads(max_payloads: int | None) -> int | None:
    """Return `max_payloads`, or raise `ValueError` when it is neither None nor an integer >= 0."""
    if max_payloads is not None and (not _is_int(max_payloads) or max_payloads < 0):
        raise ValueError(f"max_payloads must be an integer >= 0 or None, not {max_payloads!r}")
    return max_payloads


class SideRecord:
    """The live record of one side of a stream. Payloads are kept by reference, up to `limit`."""

    __slots__ = ("count", "ended", "headers", "limit", "payloads", "trailers")

    def __init__(self, limit: int | None) -> None:
        self.limit = limit
        self.headers: dict[str, Any] | None = None
        self.payloads: list[Any] = []
        self.count = 0
        self.trailers: dict[str, Any] | None = None
        self.ended = False

    def add(self, payload: Any) -> None:
        self.count += 1
        if self.limit is None or len(self.payloads) < self.limit:
            self.payloads.append(payload)

    def frozen(self, limit: int | None) -> ExchangeSide:
        payloads = self.payloads if limit is None else self.payloads[:limit]
        return ExchangeSide(self.headers, tuple(payloads), self.count, self.trailers, self.ended)


class ExchangeRecorder:
    """The live record of one stream, from its creation until it leaves the peer's stream table."""

    __slots__ = ("conn", "first_response", "request", "reset_by", "response", "started", "started_at")

    def __init__(self, conn: str, limit: int | None) -> None:
        self.conn = conn
        self.started_at = datetime.now(timezone.utc)
        self.started = time.monotonic()
        self.request = SideRecord(limit)
        self.response = SideRecord(limit)
        self.first_response: float | None = None
        #: "local" or "remote" for a reset, None for a socket loss; read only when the stream was reset.
        self.reset_by: str | None = "local"

    def side(self, opener: bool) -> SideRecord:
        """The opener's side (`request`) when `opener` is true, else the answerer's (`response`)."""
        return self.request if opener else self.response

    def answered(self) -> None:
        """Stamp the answering side's first `data` frame."""
        if self.first_response is None:
            self.first_response = time.monotonic() - self.started

    def exchange(self, stream: Stream, limit: int | None, ended_at: float) -> Exchange:
        """The `Exchange` of `stream`, closed at monotonic time `ended_at`, with payloads cut to `limit`."""
        cause = stream._close_cause
        outcome: Literal["ok", "reset", "lost"] = "ok"
        reset_by = code = reason = None
        if isinstance(cause, StreamReset):
            if self.reset_by is None:
                outcome = "lost"
            else:
                outcome = "reset"
                reset_by = self.reset_by
                code = int(cause.code)
                reason = cause.reason
        return Exchange(
            conn=self.conn,
            stream=stream.id,
            opened_by="local" if stream.local else "remote",
            oneway=stream.oneway,
            request=self.request.frozen(limit),
            response=self.response.frozen(limit),
            outcome=outcome,
            reset_by=reset_by,  # type: ignore[arg-type]
            code=code,
            reason=reason,
            started_at=self.started_at,
            first_response=self.first_response,
            duration=ended_at - self.started,
        )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _milliseconds(seconds: float | None) -> float | None:
    return None if seconds is None else round(seconds * 1000, 3)


def _plain(value: Any) -> Any:
    """A copy of `value` with every binary value as a base64 string and every tuple as a list."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Mapping):
        return {_plain(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _trimmed(payload: Any, limit: int) -> Any:
    """`payload`, or its compact JSON cut to `limit` UTF-8 bytes with a marker when longer."""
    try:
        text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)
    except (TypeError, ValueError):  # non-string keys json cannot write, or a circular reference
        text = str(payload)
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return payload
    kept = data[:limit].decode("utf-8", "ignore")
    return f"{kept}…(trimmed {len(data) - len(kept.encode('utf-8'))} bytes)…"
