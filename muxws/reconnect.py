"""The reconnect helper: backoff, heartbeat and hello replay (§7).

**Dialer only.** An acceptor cannot dial and MUST NOT have one.

The helper's entire persistent state is an attempt counter (WSM-RCN-001). Everything else - the
delay, the jitter, whether to give up - is computed from it, which is what makes the schedule a pure
function that can be tested without a clock (WSM-RCN-003).
"""

from __future__ import annotations

import random

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Reconnect:
    """Backoff options. Seconds as floats, because this is the Python port (§9.3)."""

    initial_delay: float = 0.25
    factor: float = 2.0
    max_delay: float = 30.0
    jitter: float = 0.3
    #: `None` means unlimited. Exhausting it fires `on_close` once with `will_retry` false and never
    #: dials again (WSM-RCN-044).
    max_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.initial_delay <= 0:
            raise ValueError("initial_delay must be greater than zero")
        if self.factor < 1:
            raise ValueError("factor must be at least 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter is a fraction between 0 and 1")


#: The draw a schedule needs. Injected so the schedule can be tested as the pure function
#: WSM-RCN-003 says it is, rather than sampled and hoped about.
RandomDraw = Callable[[], float]


def _uniform() -> float:
    """A draw in [-1, 1).

    `random`, deliberately, with `secrets` ruled out by WSM-RCN-005: reconnect jitter exists to
    disperse a thundering herd, not to resist an adversary, and a cryptographic source here would be
    cargo cult. (The ping nonce in M4 is the opposite case and does use `secrets`.)
    """
    return random.uniform(-1.0, 1.0)  # noqa: S311 - reconnect jitter is not security-sensitive


def backoff_delay(attempts: int, options: Reconnect, draw: RandomDraw = _uniform) -> float:
    """The delay before retry number `attempts`, jittered.

        delay = min(initial_delay * factor ** attempts, max_delay)
        delay = delay * (1 + uniform(-jitter, +jitter))

    Jitter is applied to **every** computed delay, including the capped ones (WSM-RCN-002). Without
    it, N peers whose sockets died at the same instant retry at the same instant, and a server coming
    back up is knocked over by the reconnection rather than by the load.
    """
    if attempts < 0:
        raise ValueError("attempts cannot be negative")
    base = min(options.initial_delay * options.factor**attempts, options.max_delay)
    return max(0.0, base * (1.0 + options.jitter * draw()))


def unjittered_delay(attempts: int, options: Reconnect) -> float:
    """The schedule before jitter and after the cap - the half that is exactly predictable."""
    return min(options.initial_delay * options.factor**attempts, options.max_delay)


def should_retry(attempts: int, options: Reconnect) -> bool:
    """False once `max_attempts` is exhausted (WSM-RCN-044)."""
    return options.max_attempts is None or attempts < options.max_attempts


class AttemptCounter:
    """The helper's entire persistent state (WSM-RCN-001).

    It resets **only when the connection is established**, where established means both of: the
    socket is open with the subprotocol accepted (WSM-CON-030), and the hello has been acknowledged
    (WSM-RCN-004). Resetting on socket-open is the single most common way this gets written wrong,
    and it silently converts exponential backoff into a fixed-interval hammer against a server that
    accepts sockets while its backend is down (WSM-INV-012).
    """

    __slots__ = ("_attempts",)

    def __init__(self) -> None:
        self._attempts = 0

    @property
    def value(self) -> int:
        return self._attempts

    def failed(self) -> int:
        self._attempts += 1
        return self._attempts

    def established(self) -> None:
        """Called at exactly one point: after the subprotocol AND after the hello acknowledgement."""
        self._attempts = 0


@dataclass(frozen=True, slots=True)
class Hello:
    """The opening payload replayed on every connection this peer ever makes (WSM-RCN-020).

    Captured **once**, at `connect()`, and never re-read, recomputed or supplied as a callback. The
    encoded form is held rather than the object, so "byte-identical on every replay" (WSM-RCN-027) is
    true by construction rather than by hoping the application did not mutate its own dict.

    It is an ordinary `open(payload, headers=..., end=True)` and nothing marks it on the wire
    (WSM-RCN-021). The acknowledgement is the acceptor's handler returning, which ends the stream
    implicitly (WSM-RCN-022/WSM-STM-035) - no application code is required to send one.

    A credential MUST NOT be carried here: authentication is a handshake concern (WSM-RCN-025,
    WSM-AUT-001).
    """

    payload: Any = None
    headers: dict[str, Any] | None = None
    timeout: float = 10.0

    @property
    def configured(self) -> bool:
        """A peer given no hello sends none, and is established as soon as the socket is
        (WSM-RCN-024)."""
        return self.payload is not None or self.headers is not None
