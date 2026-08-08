"""Observability types shared by the peer (§12).

M5a fills this module out with the one-line `muxws.frames` logger and `on_frame` dispatch; M5b gives
`CloseReason` its `will_retry` field. It exists already because `on_close` needs a type to hand its
handler, and that type is one per language used for every socket loss (WSM-RCN-045).
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
    #: False only when `max_attempts` is exhausted or `close()` was called deliberately. Until the
    #: reconnect helper lands in M5b there is nothing that retries, so it is always False here.
    will_retry: bool = False


logger = logging.getLogger("muxws.frames")


def log_frame(connection_id: str, direction: str, frame: Frame, byte_length: int) -> None:
    """One line per frame at DEBUG, under the `muxws.frames` logger (WSM-OBS-001).

    `conn=` is `peer.id` - a per-process prefix plus a per-connection counter (WSM-API-009) - so two
    lines carrying the same `conn=` are always the same connection, and a reconnect shows as a new
    one rather than as a continuation.

    The payload's **contents** never appear, at any level. Application data routinely holds secrets,
    and a frame line is emitted for every frame (WSM-OBS-002).
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
