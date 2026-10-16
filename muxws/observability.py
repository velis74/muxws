"""Observability types shared by the peer (§12).

`CloseReason` is what `on_close` hands its handler: one type per language, used for every socket loss
(WSM-RCN-045). `log_frame` is the one-line `muxws.frames` logger.
"""

from __future__ import annotations

import logging

from dataclasses import dataclass

from muxws.frames import Frame


@dataclass(frozen=True, slots=True)
class CloseReason:
    """Why a socket ended. The same four fields in both languages (WSM-RCN-045)."""

    code: int
    reason: str
    was_clean: bool
    #: True when the reconnect helper will dial again. False when `max_attempts` is exhausted, after
    #: `close()`, and always for a peer without the reconnect helper.

    will_retry: bool = False


logger = logging.getLogger("muxws.frames")


def log_frame(connection_id: str, direction: str, frame: Frame, byte_length: int) -> None:
    """One line per frame at DEBUG, under the `muxws.frames` logger (WSM-OBS-001).

    `conn=` is `peer.id`, a per-process prefix plus a per-connection counter (WSM-API-009): lines
    with the same `conn=` belong to the same connection, and a reconnect has a new `conn=`.

    Payload contents are never logged, at any level, because application data may contain secrets
    (WSM-OBS-002).
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    parts = [f"muxws conn={connection_id} dir={direction} type={frame.type:<6}"]
    if frame.stream is not None:
        parts.append(f"stream={frame.stream}")
    if frame.type in ("open", "data"):
        parts.append(f"end={int(frame.end)}")
    parts.append(f"bytes={byte_length}")
    if frame.fragment is not None:
        parts.append(f"frag={'more' if frame.more else 'last'}")
    if frame.headers:
        parts.append(f"headers={len(frame.headers)}")
    if frame.code is not None:
        parts.append(f"code={frame.code}")
    if frame.last_stream is not None:
        parts.append(f"last={frame.last_stream}")
    if frame.reason:
        parts.append(f"reason={frame.reason!r}")
    logger.debug(" ".join(parts))
