"""Connection liveness and orderly shutdown (§6).

There is **no post-socket handshake**. A connection is established the moment the socket is open with
the subprotocol accepted (WSM-CON-030), and this module adds nothing a peer must send or wait for
before anything else. What it adds is a defined *end*: liveness that does not depend on WebSocket
control frames a browser cannot see, and a shutdown that lets in-flight work finish.

There is no `settings` frame anywhere in this project (WSM-CON-031), so there is no value object
here holding an announced/effective pair and no ack bookkeeping. If you are looking for one, it was
deleted from the protocol.
"""

from __future__ import annotations

import asyncio
import secrets

from dataclasses import dataclass, field

#: The largest stream id the protocol allows. The next allocation past it is impossible, so the
#: exhausting peer shuts the connection down in an orderly way rather than wrapping (WSM-SID-007).
MAX_STREAM_ID = 2**31 - 1


def new_nonce() -> str:
    """A ping nonce.

    `secrets` rather than `random`: this one is cheap and there is no reason to make a nonce
    guessable. (The reconnect jitter in M5b is the opposite case and deliberately uses `random`.)
    """
    return secrets.token_hex(8)


class PingRegistry:
    """Outstanding pings, keyed by **nonce rather than by order**.

    Order would be wrong: a `pong` may arrive after its ping's deadline has already expired and been
    given up on, and matching by position would then credit it to the next ping and report a
    round-trip time that never happened. An unknown nonce is simply dropped (it is a late echo, not
    an error).
    """

    __slots__ = ("_pending",)

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[float]] = {}

    def __len__(self) -> int:
        return len(self._pending)

    def open(self, nonce: str) -> asyncio.Future[float]:
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
        """Socket death: nobody is going to answer, so nobody should keep waiting."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()


@dataclass(slots=True)
class GoawayState:
    """What each peer knows about the other's intention to stop.

    The two directions are tracked separately because they mean different things. Having *sent* one
    means this peer refuses new work (WSM-CON-021); having *received* one means `open()` raises here
    (WSM-CON-022) and that streams above the remote's `last_stream` were never processed and are safe
    to retry elsewhere (WSM-CON-023).
    """

    sent: bool = False
    received: bool = False
    #: The highest id **the remote opened** that it promises to still complete. Their parity, not
    #: ours - getting that backwards silently resets everything on every drain.
    remote_last_stream: int | None = None
    sent_code: int | None = None
    received_code: int | None = None
    received_reason: str | None = None
    draining: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def is_going_away(self) -> bool:
        return self.sent or self.received

    def survives_drain(self, stream_id: int) -> bool:
        """True when a stream of ours is at or below what the remote promised to finish.

        With no `last_stream` known - a `goaway` we sent rather than received - every live stream is
        allowed its drain window; the remote has told us nothing that would cut it short.
        """
        if self.remote_last_stream is None:
            return True
        return stream_id <= self.remote_last_stream
