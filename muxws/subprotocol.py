"""The `muxws.v1.<codec>` subprotocol assertion (§2.3, WSM-CDC-020..029).

This is an **assertion, not a negotiation**. There is no fallback encoding, no list of acceptable
alternatives, no per-connection multi-codec support and no runtime codec branching anywhere in the
peer (WSM-CDC-023).
"""

from __future__ import annotations

import logging
import re

from muxws.errors import CodecMismatch

logger = logging.getLogger("muxws.codec")

#: The generation prefix. The version component of this name is the **only** version on the wire
#: (WSM-CON-009); a change requiring the remote to act on a new frame type bumps it, and a v1
#: acceptor then rejects the offer at the handshake (WSM-CDC-025).
PREFIX = "muxws.v1."


def offer(codec_name: str, extra: list[str] | None = None) -> list[str]:
    """The subprotocol list a dialer offers: the muxws entry **first** (WSM-CDC-020).

    An application may append its own entries - a bearer token is the common case - and the acceptor
    ignores every one of them (WSM-CDC-021).
    """
    return [f"{PREFIX}{codec_name}", *(extra or [])]


#: Any generation, so an offer from a future peer can be named as such rather than reported as an
#: absent muxws entry. WSM-CDC-025 makes rejecting it the acceptor's job; saying *why* is this one's.
_ANY_GENERATION = re.compile(r"^muxws\.v(\d+)\.")


def generation_of(entry: str) -> int | None:
    """The generation integer in a muxws subprotocol name, or None if it is not one."""
    match = _ANY_GENERATION.match(entry)
    return int(match.group(1)) if match else None


def find_offer(offered: list[str]) -> str | None:
    """The single offered entry carrying the muxws prefix, or None.

    Every other value is left entirely alone: it belongs to the application's authentication, and
    interpreting it here would be muxws deciding something that is not its business (WSM-CDC-021).
    """
    for entry in offered:
        if entry.startswith(PREFIX):
            return entry
    return None


def select(offered: list[str], configured: str) -> str | None:
    """The value an acceptor selects, or None to refuse the handshake (WSM-CDC-022).

    Refusal is the answer for a mismatched codec, for a different generation, and for an offer with
    no muxws entry at all. The acceptor logs the failure itself (WSM-CDC-029): this is the half of
    the diagnostic readable where a response body is readable, and the dialer's half (WSM-CDC-024)
    is not complete on its own.
    """
    wanted = f"{PREFIX}{configured}"
    entry = find_offer(offered)
    if entry == wanted:
        return wanted

    logger.error(
        "muxws refusing the upgrade: the dialer offered %s, this acceptor is configured for %r. "
        "Set MUXWS_CODEC here or VITE_MUXWS_CODEC / MUXWS_CODEC there so both ends agree; muxws "
        "never negotiates a fallback (WSM-CDC-022/023).",
        _describe(offered, entry),
        configured,
    )
    return None


def _describe(offered: list[str], entry: str | None) -> str:
    if entry is not None:
        return f"{entry!r}"
    other = next((value for value in offered if generation_of(value) is not None), None)
    if other is not None:
        return (
            f"{other!r}, which is generation {generation_of(other)} and not 1 - a frame type the "
            f"remote must act on requires a new generation, and a v1 acceptor rejects it here "
            f"(WSM-CDC-025)"
        )
    return f"no {PREFIX}* subprotocol at all (offered {offered!r})"


def mismatch_error(configured: str) -> CodecMismatch:
    """The error a dialer composes **for itself** when its handshake is refused (WSM-CDC-024).

    A browser cannot read a rejection body, so the diagnostic cannot come from the server. It names
    both environment variables, because the reader does not yet know which end is wrong.
    """
    return CodecMismatch(
        f"the acceptor refused the muxws handshake for codec {configured!r}. Both ends must be "
        f"configured for the same codec: VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the "
        f"server. muxws asserts the codec at the handshake and never falls back (WSM-CDC-022/024).",
        configured=configured,
    )
