"""Connection liveness and orderly shutdown (§6).

There is no handshake after the WebSocket upgrade. A connection is established when the socket is
open with the subprotocol accepted (WSM-CON-030). This module provides liveness checks that do not
depend on WebSocket control frames, which browsers do not expose, and the state for a shutdown that
lets in-flight streams finish.

There is no `settings` frame (WSM-CON-031), so no settings state or acknowledgement is tracked.
"""

from __future__ import annotations

import asyncio
import secrets

from dataclasses import dataclass, field

#: The largest stream id the protocol allows. A peer that allocates the last id of its parity shuts
#: the connection down in an orderly way instead of wrapping (WSM-SID-007).
MAX_STREAM_ID = 2**31 - 1


def new_nonce() -> str:
    """A ping nonce.

    16 hex characters from `secrets`, so the nonce is not guessable. The reconnect jitter in
    `reconnect.py` uses `random`.
    """
    return secrets.token_hex(8)


class PingRegistry:
    """Outstanding pings, keyed by nonce.

    A `pong` may arrive after its ping's deadline has expired. Matching by order would then assign it
    to the next ping and report a wrong round-trip time. A pong with an unknown nonce is dropped.
    """

    __slots__ = ("_pending",)

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[float]] = {}

    def __len__(self) -> int:
        return len(self._pending)

    def open(self, nonce: str) -> asyncio.Future[float]:
        """Start waiting for the pong carrying `nonce`; the future resolves with the round trip.

        Keyed by nonce, so a pong settles only the ping that carried the same nonce.
        """
        future: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        self._pending[nonce] = future
        return future

    def settle(self, nonce: str, elapsed: float) -> bool:
        """Resolve the ping this nonce belongs to. False when nothing was waiting for it."""
        future = self._pending.pop(nonce, None)
        if future is None or future.done():
            return False
        future.set_result(elapsed)
        return True

    def give_up(self, nonce: str) -> None:
        self._pending.pop(nonce, None)

    def fail_all(self, error: BaseException) -> None:
        """Socket death: fail every outstanding ping with `error` and clear the registry."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()


@dataclass(slots=True)
class GoawayState:
    """The `goaway` state of one connection, per direction.

    After sending `goaway` this peer refuses new streams (WSM-CON-021). After receiving one, `open()`
    raises (WSM-CON-022), and this peer's streams above the remote's `last_stream` were not processed
    and are safe to retry elsewhere (WSM-CON-023).
    """

    sent: bool = False
    received: bool = False
    #: The `last_stream` of the received `goaway`: the highest id this peer opened that the remote
    #: processed and will complete. It has this peer's parity.
    remote_last_stream: int | None = None
    sent_code: int | None = None
    received_code: int | None = None
    received_reason: str | None = None
    draining: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def is_going_away(self) -> bool:
        return self.sent or self.received

    def survives_drain(self, stream_id: int) -> bool:
        """True when a stream this peer opened is at or below the remote's `last_stream`.

        True for every stream when no `goaway` has been received.
        """

        if self.remote_last_stream is None:
            return True
        return stream_id <= self.remote_last_stream
