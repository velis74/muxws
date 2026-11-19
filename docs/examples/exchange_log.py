"""JSONL exchange log: one JSON line per muxws stream, with the request and the response paired."""

from __future__ import annotations

import base64
import json
import logging
import math
import re
import time

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from muxws import ABSENT, Assembler, Codec, Frame, Peer

_log = logging.getLogger(__name__)
_DUMP_KWARGS: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False}
_SKIPPED = object()
_SURROGATE = re.compile("[\ud800-\udfff]")


def _json_default(value: Any) -> Any:
    """Bytes become base64 text; any other value JSON cannot hold becomes its `str()`."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _json_key(key: Any) -> str:
    """A map key as text: bytes as base64, any other non-string as its `str()`."""
    if isinstance(key, str):
        return key
    if isinstance(key, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(key)).decode("ascii")
    return str(key)


def _finite(value: Any) -> Any:
    """`value` with every NaN and infinity replaced by None and every map key a string."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {_json_key(k): _finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    return value


def _dumps(value: Any) -> str:
    """Strict JSON text: non-finite numbers become null, lone surrogates `\\uXXXX` escapes."""
    try:
        text = json.dumps(value, default=_json_default, allow_nan=False, **_DUMP_KWARGS)
    except (TypeError, ValueError):  # a non-finite number, or a map key json.dumps refuses
        text = json.dumps(_finite(value), default=_json_default, allow_nan=False, **_DUMP_KWARGS)
    return _SURROGATE.sub(lambda match: f"\\u{ord(match.group()):04x}", text)


def trim_text(text: str, limit: int | None) -> str | None:
    """`text` cut to `limit` UTF-8 bytes on a character boundary plus a marker; None when it fits."""
    encoded = text.encode("utf-8")
    if limit is None or len(encoded) <= limit:
        return None
    cut = limit
    while cut > 0 and encoded[cut] & 0xC0 == 0x80:  # a UTF-8 continuation byte
        cut -= 1
    return f"{encoded[:cut].decode('utf-8')}…(trimmed {len(encoded) - cut} bytes)…"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class _Side:
    """What one side of a stream sent."""

    headers: dict[str, Any] | None = None
    payloads: list[str] = field(default_factory=list)  # JSON text of each kept payload
    trailers: dict[str, Any] | None = None
    dropped: int = 0
    bytes: int = 0
    ended: bool = False
    assembler: Assembler = field(default_factory=Assembler)

    def record(self) -> str:
        """JSON text of this side, with the kept payload texts spliced in."""
        return (
            f'{{"headers":{_dumps(self.headers)},"payloads":[{",".join(self.payloads)}],'
            f'"trailers":{_dumps(self.trailers)},"dropped":{self.dropped},"bytes":{self.bytes}}}'
        )


@dataclass
class _Exchange:
    """One stream in flight."""

    conn: str
    stream: int
    opened_by: str
    oneway: bool
    ts: str
    started: float
    request: _Side = field(default_factory=_Side)
    response: _Side = field(default_factory=_Side)
    first_response: float | None = None
    included: bool | None = None  # None until the opening payload is complete


@dataclass
class _Connection:
    """The streams of one attached peer, and the highest stream id each side has opened."""

    exchanges: dict[int, _Exchange] = field(default_factory=dict)
    highest: dict[str, int] = field(default_factory=lambda: {"local": 0, "remote": 0})


class ExchangeLog:
    """Writes one JSON line per finished stream of every attached peer."""

    def __init__(
        self,
        codec: Codec,
        *,
        write: Callable[[str], None] | None = None,
        logger: str = "app.muxws.exchanges",
        trim_payload_size: int | None = 4096,
        max_messages: int | None = 100,
        redact: Callable[[Any, str], Any] | None = None,
        include: Callable[[dict[str, Any] | None, Any], bool] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._codec = codec
        self._write = write if write is not None else logging.getLogger(logger).info
        self._trim = trim_payload_size
        self._max = max_messages
        self._redact = redact
        self._include = include
        self._clock = clock if clock is not None else time.monotonic

    def attach(self, peer: Peer) -> None:
        """Log the streams of `peer`, on this connection and on every reconnected one."""
        connection = _Connection()

        def on_frame(direction: str, frame: Frame, byte_length: int) -> None:
            if frame.stream is None:
                return
            try:
                self._frame(peer, connection, direction, frame, byte_length)
            except Exception:  # noqa: BLE001 - logging must not affect the connection
                connection.exchanges.pop(frame.stream, None)
                _log.exception("exchange log dropped stream %s of conn=%s", frame.stream, peer.id)

        def on_close(_reason: Any) -> None:
            lost = list(connection.exchanges.values())
            connection.exchanges.clear()
            connection.highest = {"local": 0, "remote": 0}  # a reconnected socket starts a new id space
            for exchange in lost:
                try:
                    self._finish(connection, exchange, "lost", None, None)
                except Exception:  # noqa: BLE001 - one failed record must not stop the others
                    _log.exception("exchange log dropped stream %s of conn=%s", exchange.stream, peer.id)

        peer.on_frame(on_frame)
        peer.on_close(on_close)

    def _frame(self, peer: Peer, connection: _Connection, direction: str, frame: Frame, byte_length: int) -> None:
        now = self._clock()
        sender = "local" if direction == "tx" else "remote"
        exchange = connection.exchanges.get(frame.stream)  # type: ignore[arg-type]
        if exchange is None:
            # Only an open with an id above the highest seen starts a stream; any other frame is from a finished one.
            if frame.type != "open" or frame.stream <= connection.highest[sender]:  # type: ignore[operator]
                return
            connection.highest[sender] = frame.stream  # type: ignore[assignment]
            exchange = _Exchange(peer.id, frame.stream, sender, frame.oneway, _utc_now(), now)  # type: ignore[arg-type]
            connection.exchanges[exchange.stream] = exchange

        side_name = "request" if sender == exchange.opened_by else "response"
        side = exchange.request if side_name == "request" else exchange.response
        side.bytes += byte_length
        if side_name == "response" and exchange.first_response is None:
            exchange.first_response = now
        if frame.headers is not None and side.headers is None:
            side.headers = dict(frame.headers)

        if frame.type == "reset":
            self._finish(connection, exchange, "reset", frame.code, frame.reason, now)
            return
        if frame.type not in ("open", "data"):
            return

        undecided = exchange.included is None and side_name == "request"
        has_room = self._max is None or len(side.payloads) < self._max
        needs_payload = (exchange.included is not False and has_room) or (undecided and self._include is not None)
        if frame.fragment is None:
            payload = frame.payload
        elif needs_payload:
            payload = side.assembler.feed(frame, self._codec)
        else:
            payload = ABSENT if frame.more else _SKIPPED  # complete, but neither kept nor decoded

        if undecided and not frame.more:
            opening = None if payload is ABSENT or payload is _SKIPPED else payload
            exchange.included = self._include is None or bool(self._include(exchange.request.headers, opening))
        if payload is not ABSENT and exchange.included is not False:
            if has_room:
                side.payloads.append(self._loggable(payload, side_name))
            else:
                side.dropped += 1

        if frame.trailers is not None:
            side.trailers = dict(frame.trailers)
        if frame.end:
            side.ended = True
        if exchange.oneway and side_name == "request" and not frame.more:
            self._finish(connection, exchange, "ok", None, None, now)
        elif exchange.request.ended and exchange.response.ended:
            self._finish(connection, exchange, "ok", None, None, now)

    def _loggable(self, payload: Any, side: str) -> str:
        """JSON text a payload is logged as: redacted, then trimmed when it is too long."""
        if self._redact is not None:
            payload = self._redact(payload, side)
        text = _dumps(payload)
        trimmed = trim_text(text, self._trim)
        return text if trimmed is None else _dumps(trimmed)

    def _finish(
        self,
        connection: _Connection,
        exchange: _Exchange,
        outcome: str,
        code: int | None,
        reason: str | None,
        now: float | None = None,
    ) -> None:
        connection.exchanges.pop(exchange.stream, None)
        if exchange.included is None and self._include is not None:
            # The stream ended before its opening payload was complete.
            exchange.included = bool(self._include(exchange.request.headers, None))
        if exchange.included is False:
            return
        now = self._clock() if now is None else now
        first = exchange.first_response
        fields = (
            ("ts", _dumps(exchange.ts)),
            ("conn", _dumps(exchange.conn)),
            ("stream", _dumps(exchange.stream)),
            ("opened_by", _dumps(exchange.opened_by)),
            ("oneway", _dumps(exchange.oneway)),
            ("request", exchange.request.record()),
            ("response", exchange.response.record()),
            ("outcome", _dumps(outcome)),
            ("code", _dumps(code)),
            ("reason", _dumps(reason)),
            ("first_response_ms", _dumps(None if first is None else round((first - exchange.started) * 1000, 3))),
            ("duration_ms", _dumps(round((now - exchange.started) * 1000, 3))),
        )
        self._write("{" + ",".join(f'"{key}":{value}' for key, value in fields) + "}")
