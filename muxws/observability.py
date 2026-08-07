"""Observability types shared by the peer (§12).

M5a fills this module out with the one-line `muxws.frames` logger and `on_frame` dispatch; M5b gives
`CloseReason` its `will_retry` field. It exists already because `on_close` needs a type to hand its
handler, and that type is one per language used for every socket loss (WSM-RCN-045).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CloseReason:
    """Why a socket ended. The same four fields in both languages (WSM-RCN-045)."""

    code: int
    reason: str
    was_clean: bool
    #: False only when `max_attempts` is exhausted or `close()` was called deliberately. Until the
    #: reconnect helper lands in M5b there is nothing that retries, so it is always False here.
    will_retry: bool = False
