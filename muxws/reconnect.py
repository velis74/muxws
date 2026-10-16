"""The reconnect helper: backoff, heartbeat and hello replay (§7).

**Dialer only.** An acceptor cannot dial and MUST NOT have one.

The helper's only persistent state is an attempt counter (WSM-RCN-001). The delay, the jitter and
whether to give up are computed from it, so the schedule is a pure function that can be tested
without a clock (WSM-RCN-003).
"""

from __future__ import annotations

import asyncio
import logging
import random

from collections import deque
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from muxws.errors import ConnectionClosed, StreamReset, StreamTimeout
from muxws.observability import CloseReason

#: How many delays `ConnectionLoop.delays` keeps. Bounded, because with `max_attempts=None` against a
#: server that stays down an unbounded list would grow by one float per retry.
MAX_RECORDED_DELAYS = 100

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.peer import Peer
    from muxws.transports import SocketAdapter

logger = logging.getLogger("muxws.reconnect")


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


#: The random draw the schedule uses. Injectable, so tests can evaluate the schedule as a pure
#: function (WSM-RCN-003).
RandomDraw = Callable[[], float]


def _uniform() -> float:
    """A draw in [-1, 1).

    Uses `random`; WSM-RCN-005 rules out `secrets`. Jitter spreads reconnects of many peers over
    time and has no security function. The ping nonce in `lifecycle.py` uses `secrets`.
    """
    return random.uniform(-1.0, 1.0)  # noqa: S311 - reconnect jitter is not security-sensitive


def backoff_delay(attempts: int, options: Reconnect, draw: RandomDraw = _uniform) -> float:
    """The delay before retry number `attempts`, jittered.

        delay = min(initial_delay * factor ** attempts, max_delay)
        delay = delay * (1 + uniform(-jitter, +jitter))

    Jitter is applied to every computed delay, including capped ones (WSM-RCN-002), so peers whose
    sockets died at the same instant do not retry at the same instant. The result is never negative.
    """
    if attempts < 0:
        raise ValueError("attempts cannot be negative")
    base = unjittered_delay(attempts, options)
    return max(0.0, base * (1.0 + options.jitter * draw()))


def unjittered_delay(attempts: int, options: Reconnect) -> float:
    """The delay before jitter and after the cap.

    `factor ** attempts` exceeds the float range after about a thousand attempts at the default
    factor. `initial_delay > 0` and `factor >= 1`, so a growth that overflows is past any cap, and
    the result is `max_delay`.
    """
    try:
        grown = options.initial_delay * options.factor**attempts
    except OverflowError:
        return options.max_delay
    return min(grown, options.max_delay)


def should_retry(attempts: int, options: Reconnect) -> bool:
    """False once `max_attempts` is exhausted (WSM-RCN-044)."""
    return options.max_attempts is None or attempts < options.max_attempts


class AttemptCounter:
    """The helper's entire persistent state (WSM-RCN-001).

    It resets only when the connection is established: the socket is open with the subprotocol
    accepted (WSM-CON-030) and the hello has been acknowledged (WSM-RCN-004). If it reset on
    socket-open, a server that accepts sockets while its backend is down would be redialled at a
    fixed interval instead of with exponential backoff (WSM-INV-012).
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
        """Reset the counter. Called only from `ConnectionLoop._adopt` (WSM-INV-012)."""
        self._attempts = 0


@dataclass(frozen=True, slots=True)
class Hello:
    """The opening payload replayed on every connection this peer ever makes (WSM-RCN-020).

    Captured once, at `connect()`, as a deep copy of `payload` and `headers`; it is not re-read,
    recomputed or supplied as a callback. Later changes to the application's objects do not affect
    the replay, so every replay is byte-identical (WSM-RCN-027).

    It is an ordinary `open(payload, headers=..., end=True)` and nothing marks it on the wire
    (WSM-RCN-021). The acknowledgement is the acceptor's handler returning, which ends the stream
    implicitly (WSM-RCN-022/WSM-STM-035); no application code sends it.

    A credential MUST NOT be carried here: authentication is a handshake concern (WSM-RCN-025,
    WSM-AUT-001).
    """

    payload: Any = None
    headers: dict[str, Any] | None = None
    timeout: float = 10.0
    #: The copies taken at construction. Only these reach the wire; `payload` and `headers` are not
    #: read again.
    _captured_payload: Any = field(init=False, repr=False, compare=False, default=None)
    _captured_headers: dict[str, Any] | None = field(init=False, repr=False, compare=False, default=None)

    def __post_init__(self) -> None:
        # Deep copies, so later mutation of the application's objects does not change the replay
        # (WSM-RCN-027). The dataclass is frozen, hence `object.__setattr__`.
        object.__setattr__(self, "_captured_payload", deepcopy(self.payload))
        object.__setattr__(self, "_captured_headers", deepcopy(self.headers))

    @property
    def configured(self) -> bool:
        """True when a payload or headers were given. A peer with no hello sends none and is
        established when the socket opens (WSM-RCN-024)."""
        return self.payload is not None or self.headers is not None

    @property
    def payload_for_wire(self) -> Any:
        """A deep copy of the captured payload, new on every read.

        A copy handed to the codec or to frame handlers on one connection is not the object the next
        connection sends (WSM-RCN-020/027).
        """
        return deepcopy(self._captured_payload)

    @property
    def headers_for_wire(self) -> dict[str, Any] | None:
        """A deep copy of the captured headers, new on every read (WSM-RCN-027)."""
        return deepcopy(self._captured_headers)


class Heartbeat:
    """A `ping` every `interval` on an **otherwise idle** socket (WSM-RCN-010).

    `peer.last_activity` is stamped by every frame in either direction, so a connection carrying
    traffic is not pinged. The loop compares the idle time against `interval` instead of pinging on a
    fixed schedule (§6).

    A dead socket is detected within `interval + timeout` (WSM-RCN-011): the ping goes out at most one
    interval after the last frame, and `peer.ping()` gives up after `timeout`. Detection does not
    depend on TCP timeouts. The heartbeat uses muxws `ping` frames because browsers do not expose
    WebSocket ping/pong (WSM-CON-011).
    """

    def __init__(
        self,
        peer: Peer,
        *,
        interval: float,
        timeout: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._peer = peer
        self._interval = interval
        self._timeout = timeout
        self._sleep = sleep

    async def run(self) -> None:
        peer = self._peer
        if self._interval <= 0:
            # An interval of zero or less disables the heartbeat, for deployments with their own
            # liveness signal.
            return

        # The socket has just been established; that counts as activity. Without this stamp an unused
        # peer would be pinged as soon as the heartbeat starts.
        peer._last_activity = peer._clock()

        while True:
            idle = peer._clock() - peer.last_activity
            if idle < self._interval:
                await self._sleep(self._interval - idle)
                continue
            try:
                await peer.ping(timeout=self._timeout)
            except ConnectionClosed:
                await self._declare_dead()
                return
            except Exception:  # noqa: BLE001 - the peer is already between sockets
                return

    async def _declare_dead(self) -> None:
        """No pong arrived within the deadline: mark the socket dead and close it locally.

        `_die` runs first, so the reported reason is the missing pong and not the local close. The
        local close (WSM-RCN-011) ends the read loop's `receive()`, so `serve()` returns and the
        supervisor proceeds to backoff and redial, the same path as any other socket loss.

        The peer reports code 1006; `_close_socket_locally` sends 1000, because 1006 may not be sent.
        """
        peer = self._peer
        # `reason=` is set as well as the message, because `CloseReason.reason` is read from it and
        # distinguishes a missing pong from other 1006 closes (WSM-RCN-045).
        text = f"no pong within {self._timeout}s"
        peer._die(ConnectionClosed(text, code=1006, reason=text))
        await peer._close_socket_locally(reason="no pong")


#: How the loop gets a socket. A closure, not a URL: the helper knows nothing about transports.
Dial = Callable[[], Awaitable["SocketAdapter"]]


class ConnectionLoop:
    """The reconnect driver. **Dialer only** - an acceptor cannot dial (§3).

    The only state kept between connections is the attempt counter (WSM-RCN-001). It resets only
    when a connection is established: socket open and hello acknowledged (WSM-RCN-004/WSM-INV-012).
    """

    def __init__(
        self,
        peer: Peer,
        dial: Dial,
        *,
        options: Reconnect,
        hello: Hello,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        draw: RandomDraw = _uniform,
    ) -> None:
        if not peer.is_dialer:
            raise ValueError(
                "an acceptor has no reconnect helper: it cannot dial, and a peer that tried to would "
                "be dialling the client that dialled it (§3)"
            )
        self._peer = peer
        self._dial = dial
        self._options = options
        self._hello = hello
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        #: One injected sleep for both the backoff and the heartbeat, so a test that fakes time fakes
        #: it for both.
        self._sleep = sleep
        self._draw = draw

        self._counter = AttemptCounter()
        self._delays: deque[float] = deque(maxlen=MAX_RECORDED_DELAYS)
        self._reconnections = 0
        self._stopped = False
        self._supervisor: asyncio.Task[None] | None = None
        self._serving: asyncio.Task[None] | None = None
        self._heartbeat: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ what tests read

    @property
    def attempts(self) -> int:
        return self._counter.value

    @property
    def delays(self) -> list[float]:
        """The most recent `MAX_RECORDED_DELAYS` delays actually waited, in order.

        WSM-RCN-004 requires this sequence to grow until a connection is established. Bounded by
        `MAX_RECORDED_DELAYS`.
        """
        return list(self._delays)

    @property
    def reconnections(self) -> int:
        return self._reconnections

    # ------------------------------------------------------------------ the first connection

    async def establish(self) -> None:
        """The hello half of the **first** connection; the socket is already on the peer.

        Raises the underlying error regardless of `reconnect` (WSM-RCN-006/WSM-INV-018). `connect()`
        propagates it and does not return a peer that retries in the background, so a wrong URL, an
        unreachable host or a codec mismatch is reported at the call site.
        """
        # The first connection is also not established until the hello is acknowledged, and the peer
        # may not be sent on before then (WSM-RCN-043).
        self._peer._established = not self._hello.configured
        self._serving = asyncio.create_task(self._peer.serve())
        try:
            await self._perform_hello()
        except BaseException:
            # Nothing was established, so `on_close` is not called: the exception raised below reports
            # the failure (WSM-RCN-006), and the caller never received the peer. The peer is marked
            # dead here, so `is_open` is false regardless of how the read loop and the cancellation
            # are ordered.
            never = "the first connection was never established"
            self._peer._die(ConnectionClosed(never, code=1006, reason=never), notify=False)
            await self._abandon_socket()
            raise
        # The counter is not reset here: `establish()` runs once, before any attempt can have failed,
        # so the counter is still at zero. `_adopt()` is the only reset site (WSM-RCN-004/WSM-INV-012).
        self._peer._established = True
        self._peer._will_retry = should_retry(0, self._options)

    def start(self) -> None:
        """Start the supervisor. Called only after `establish()` returned."""
        if self._supervisor is None and not self._stopped:
            self._supervisor = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        """Stop the helper: no further dial, and a pending backoff sleep is cancelled (WSM-RCN-040)."""
        self._stopped = True
        self._peer._will_retry = False
        current = asyncio.current_task()
        await _cancel(self._heartbeat, current)
        await _cancel(self._supervisor, current)
        self._heartbeat = None

    # ------------------------------------------------------------------ the supervisor

    async def _supervise(self) -> None:
        """Serve until the socket is lost, then back off, dial, send the hello and establish, in that order."""
        self._start_heartbeat()
        while True:
            await self._until_the_socket_dies()
            # After `stop()`, which `peer.close()` calls, the helper does not dial again.
            if self._stopped:
                return
            # The dial decision uses `should_retry` on the helper's own counter, not `peer._will_retry`,
            # which only `CloseReason` reads.
            if not should_retry(self._counter.value, self._options):
                # Reached only with `max_attempts=0`. `establish()` has already set `will_retry` false,
                # so the socket loss reported WSM-RCN-044's single `will_retry=False` close and
                # `_give_up()`'s report is suppressed. `ts/reconnect.ts` has the same branch.
                self._give_up()
                return
            if await self._redial():
                continue
            if not self._stopped:
                self._give_up()
            return

    async def _until_the_socket_dies(self) -> None:
        """Wait for `serve()` to return, which marks the socket loss, then stop the heartbeat.

        By then `_die` has already called the `on_close` handlers.
        """
        task = self._serving
        if task is not None:
            try:
                await task
            except Exception as exc:  # noqa: BLE001 - however serve() ended, the socket is gone
                logger.debug("muxws conn=%s serve() ended with %r", self._peer.id, exc)
        await self._stop_heartbeat()

    async def _redial(self) -> bool:
        """Back off and dial until an attempt is established. Returns False when it gave up or was stopped."""
        while not self._stopped and should_retry(self._counter.value, self._options):
            delay = backoff_delay(self._counter.value, self._options, self._draw)
            self._delays.append(delay)
            await self._sleep(delay)
            if self._stopped:
                return False
            try:
                socket = await self._dial()
            except Exception as exc:  # noqa: BLE001 - every dial failure counts as one failed attempt
                # `on_close` is not called: a dial that produced no socket is not a socket loss
                # (WSM-RCN-040).
                logger.debug("muxws conn=%s dial failed: %r", self._peer.id, exc)
                self._counter.failed()
                continue
            if await self._adopt(socket):
                return True
        return False

    async def _adopt(self, socket: SocketAdapter) -> bool:
        """Adopt a dialled socket, serve it and send the hello. True when the connection is established."""
        peer = self._peer
        # Set before the socket is adopted, because the socket may die during the hello and `_die`
        # reads this field. For an attempt that is not yet established, `will_retry` is whether one
        # more attempt is left (WSM-RCN-040/045).
        peer._will_retry = should_retry(self._counter.value + 1, self._options)
        # Until the hello is acknowledged `is_open` stays false, so no application frame precedes the
        # hello on this socket (WSM-RCN-023/043).
        peer._established = not self._hello.configured
        peer._adopt_socket(socket)
        self._serving = asyncio.create_task(peer.serve())
        try:
            await self._perform_hello()
        except asyncio.CancelledError:
            # `stop()` cancelled the attempt during the hello. The socket is closed, because nothing
            # will serve it (WSM-RCN-040).
            await self._abandon_socket()
            raise
        except Exception as exc:  # noqa: BLE001 - a reset hello and a timed-out hello are handled alike
            await self._fail_attempt(exc)
            return False

        # The only place the counter resets: the socket is open with the subprotocol accepted
        # (WSM-CON-030) and the hello is acknowledged (WSM-RCN-004). A reset when `dial()` returned
        # would redial a server that accepts sockets while its backend is down at a fixed interval
        # instead of with exponential backoff (WSM-INV-012).
        self._counter.established()
        peer._established = True
        peer._will_retry = should_retry(0, self._options)
        self._reconnections += 1
        # Called after the hello is acknowledged (WSM-RCN-023/030).
        peer._fire_reconnect(self._reconnections)
        self._start_heartbeat()
        return True

    async def _fail_attempt(self, cause: BaseException) -> None:
        """WSM-RCN-026: a failed hello is a failed **attempt**, not a connection.

        The socket is closed, the loss is reported, the counter is incremented, and `on_reconnect` is
        not called, because the acceptor did not acknowledge the hello.

        `_die` is a no-op when the socket died during the hello and the read loop already reported
        it; when the hello was reset or timed out on an open socket, `_die` reports the loss.
        `_will_retry` was set before the socket was adopted, so both cases produce the same
        `CloseReason`.
        """
        peer = self._peer
        text = f"the hello did not complete: {cause}"
        peer._die(ConnectionClosed(text, code=1006, reason=text))
        await self._abandon_socket()
        self._counter.failed()

    def _give_up(self) -> None:
        """`max_attempts` is exhausted: call `on_close` with `will_retry=False` and stop (WSM-RCN-044).

        When the attempts ran out on failed dials, the last socket loss reported `will_retry=True`,
        and this report tells the application that no reconnection follows.

        When they ran out on failed hellos, the last socket loss already reported `will_retry=False`,
        and `_notify_close` suppresses this report, so the application receives one
        `will_retry=False` close.
        """
        peer = self._peer
        peer._will_retry = False
        peer._notify_close(
            CloseReason(
                code=1006,
                reason=f"gave up reconnecting after {self._options.max_attempts} attempts",
                was_clean=False,
                will_retry=False,
            )
        )

    # ------------------------------------------------------------------ the hello

    async def _perform_hello(self) -> None:
        """An ordinary `open`, awaited to its acknowledgement (WSM-RCN-021/022).

        Nothing marks it on the wire: the acceptor's `on_stream` handler receives it as any other
        stream, and the acknowledgement is that handler returning, which ends the stream implicitly
        (WSM-STM-035).

        It is sent before any application frame on this socket and before `on_reconnect`
        (WSM-RCN-023).

        Raises the underlying error, not a `ConnectionClosed` wrapping it (WSM-RCN-006): the stream's
        reset when the hello is reset, `StreamTimeout` when it is not acknowledged within
        `hello.timeout` seconds.
        """
        hello = self._hello
        if not hello.configured:
            # A peer with no hello sends none and is established when the socket opens
            # (WSM-RCN-024/WSM-CON-030).
            return

        # `_allocate_and_enqueue` rather than `open()`: `open()` raises while `is_open` is false, and
        # `is_open` stays false until this hello is acknowledged (WSM-RCN-023/043). The allocation and
        # the enqueue still happen with no suspension point between them (WSM-SID-006).
        stream = self._peer._allocate_and_enqueue(hello.payload_for_wire, headers=hello.headers_for_wire, end=True)
        try:
            await asyncio.wait_for(stream.closed.wait(), hello.timeout)
        except asyncio.TimeoutError:
            raise StreamTimeout(
                f"the hello was not acknowledged within {hello.timeout}s", stream_id=stream.id
            ) from None
        if isinstance(stream._close_cause, StreamReset):
            # The stream's reset as received: `StreamRefused`, `ConnectionLost`, or another `StreamReset`.
            raise stream._close_cause

    # ------------------------------------------------------------------ tasks

    def _start_heartbeat(self) -> None:
        beat = Heartbeat(self._peer, interval=self._ping_interval, timeout=self._ping_timeout, sleep=self._sleep)
        self._heartbeat = asyncio.create_task(beat.run())

    async def _stop_heartbeat(self) -> None:
        await _cancel(self._heartbeat, asyncio.current_task())
        self._heartbeat = None

    async def _abandon_socket(self) -> None:
        """Give up this socket: close it locally, and let its read loop finish.

        Closed with code 1000: the socket may still be open (a timed-out hello means only that no
        answer arrived), so the close frame reaches the remote, and 1006 may not be sent
        (WSM-RCN-011). The adapter raises on 1006 and the socket would stay open.
        """
        await self._peer._close_socket_locally(reason="abandoned")
        await _cancel(self._serving, asyncio.current_task())
        self._serving = None


async def _cancel(task: asyncio.Task[None] | None, current: asyncio.Task[Any] | None) -> None:
    """Cancel a task and wait for it to finish, unless it is the current task (awaiting it would deadlock)."""

    if task is None or task is current:
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
