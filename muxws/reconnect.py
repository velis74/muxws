"""The reconnect helper: backoff, heartbeat and hello replay (§7).

**Dialer only.** An acceptor cannot dial and MUST NOT have one.

The helper's entire persistent state is an attempt counter (WSM-RCN-001). Everything else - the
delay, the jitter, whether to give up - is computed from it, which is what makes the schedule a pure
function that can be tested without a clock (WSM-RCN-003).
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

#: How many delays `ConnectionLoop.delays` keeps. It is instrumentation on an object that lives as
#: long as the application does, and with `max_attempts=None` against a permanently down server an
#: unbounded list is a leak that grows one float per retry, forever.
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
        """Called from exactly one place, `ConnectionLoop._adopt`, and only there.

        A second caller is the failure WSM-INV-012 describes: the two would mask each other, every
        test would stay green, and the delay sequence would silently stop growing.
        """
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
    #: The capture. Never read by an application, never re-read from `payload`: the copy taken here
    #: is the only thing that ever reaches the wire.
    _captured_payload: Any = field(init=False, repr=False, compare=False, default=None)
    _captured_headers: dict[str, Any] | None = field(init=False, repr=False, compare=False, default=None)

    def __post_init__(self) -> None:
        # Capture **by value**, at construction, so WSM-RCN-027's byte-identical replay is true by
        # construction rather than by hoping the application never mutates the dict it passed us. The
        # dataclass is frozen, hence `object.__setattr__`.
        object.__setattr__(self, "_captured_payload", deepcopy(self.payload))
        object.__setattr__(self, "_captured_headers", deepcopy(self.headers))

    @property
    def configured(self) -> bool:
        """A peer given no hello sends none, and is established as soon as the socket is
        (WSM-RCN-024)."""
        return self.payload is not None or self.headers is not None

    @property
    def payload_for_wire(self) -> Any:
        """A fresh copy per replay.

        Handing the same object to the codec twice would let anything that mutated it in between -
        the codec, a frame handler, the application holding the original - change the *next*
        connection's hello (WSM-RCN-020/027).
        """
        return deepcopy(self._captured_payload)

    @property
    def headers_for_wire(self) -> dict[str, Any] | None:
        """A **fresh** deep copy on every read, which is what makes WSM-RCN-027 true.

        The copy one connection handed to the codec must not be the object the next connection
        sends, or a replay could differ from the hello it is replaying.
        """
        return deepcopy(self._captured_headers)


class Heartbeat:
    """A `ping` every `interval` on an **otherwise idle** socket (WSM-RCN-010).

    Idle means idle: `peer.last_activity` is stamped by every frame in either direction, so a busy
    connection never pays for a ping. The timer is therefore read, not scheduled - a fixed schedule
    would burn a ping every interval on a socket that has been talking the whole time (§6).

    Detection of a dead socket is bounded by `interval + timeout` (WSM-RCN-011): the ping goes out at
    most one interval after the last frame, and `peer.ping()` gives up after `timeout`. Nothing here
    waits on anything resembling a TCP timeout, which is the entire reason the heartbeat exists -
    native WebSocket ping/pong cannot be used, because browsers do not expose it (WSM-CON-011).
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
            # A heartbeat is a bounded detection time, not a requirement. Disabling it is a choice a
            # deployment with its own liveness signal is allowed to make.
            return

        # The socket has just been established, and that is the most recent thing that happened on
        # it. Without this stamp an unused peer reads as infinitely idle and gets a ping the instant
        # the heartbeat starts, which is the opposite of what "idle for an interval" means.
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
            except Exception:  # noqa: BLE001 - the peer is already between sockets; nothing to do
                return

    async def _declare_dead(self) -> None:
        """No pong within the deadline: the socket is dead however alive the transport looks.

        `_die` first, so the reason the application is told is the swallowed pong rather than the
        local close that follows it. Then the socket is closed **locally** (WSM-RCN-011) - without
        that, the read loop stays parked on a `receive()` that will never return, the supervisor
        never learns the connection ended, and there would be no backoff at all. Closing it here is
        what makes a dead socket take *the same* path as a clean close: one code path, not two.

        1006 is what the peer *reports*; it is never what it *sends* - `_close_socket_locally`
        carries the reason that code may not go on the wire.
        """
        peer = self._peer
        # `reason=` as well as the message: it is what reaches `CloseReason.reason`, and an
        # application whose `on_close` logged an empty string would have no way to tell a swallowed
        # pong from any other 1006 (WSM-RCN-045).
        text = f"no pong within {self._timeout}s"
        peer._die(ConnectionClosed(text, code=1006, reason=text))
        await peer._close_socket_locally(reason="no pong")


#: How the loop gets a socket. A closure, not a URL: the helper knows nothing about transports.
Dial = Callable[[], Awaitable["SocketAdapter"]]


class ConnectionLoop:
    """The reconnect driver. **Dialer only** - an acceptor cannot dial (§3).

    Everything the loop remembers between connections is the attempt counter (WSM-RCN-001). The
    ordering below is the whole of the milestone, and the one point where the counter resets is the
    single thing most easily got wrong (WSM-RCN-004/WSM-INV-012).
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
        #: One injected sleep for both halves of the driver, so a test that fakes time fakes all of
        #: it; the heartbeat reading a real clock while the backoff read a fake one would be two
        #: different timelines in one connection.
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

        A growing sequence is what WSM-RCN-004 asserts. Bounded because this is instrumentation on a
        long-lived object: a peer retrying forever against a server that never comes back would
        otherwise accumulate one float per attempt for the life of the process.
        """
        return list(self._delays)

    @property
    def reconnections(self) -> int:
        return self._reconnections

    # ------------------------------------------------------------------ the first connection

    async def establish(self) -> None:
        """The hello half of the **first** connection; the socket is already on the peer.

        Raises whatever went wrong, whatever `reconnect` says (WSM-RCN-006/WSM-INV-018). `connect()`
        does not swallow it and does not hand back a peer that is retrying in the background: a typo
        in the URL, an unreachable host or a codec mismatch has to surface at the call site, or the
        application holds a peer that looks alive and retries forever against something that will
        never answer.
        """
        # Socket-open is not established, and it is not established for the first connection either:
        # until the hello is acknowledged this peer may not be sent on (WSM-RCN-043).
        self._peer._established = not self._hello.configured
        self._serving = asyncio.create_task(self._peer.serve())
        try:
            await self._perform_hello()
        except BaseException:
            # Nothing was established, so there is no loss to report and no `on_close` to fire: the
            # raise below **is** the report (WSM-RCN-006), and the caller never got the peer, so
            # there is nobody who could have registered a handler. The peer is still marked dead
            # here rather than left to the read loop and the cancel to settle between them - that
            # race is how a peer whose hello never completed goes on reporting `is_open` true.
            never = "the first connection was never established"
            self._peer._die(ConnectionClosed(never, code=1006, reason=never), notify=False)
            await self._abandon_socket()
            raise
        # The counter is not reset here. It is at its initial value already - `establish()` runs once,
        # before anything can have failed - and a second reset call site is exactly what WSM-INV-012
        # warns about: whichever of the two a later edit made load-bearing, the other would keep every
        # test green while backoff quietly flattened into a fixed-interval hammer. `_adopt()` is the
        # one place a connection becomes established (WSM-RCN-004).
        self._peer._established = True
        self._peer._will_retry = should_retry(0, self._options)

    def start(self) -> None:
        """Start the supervisor. Called only after `establish()` returned."""
        if self._supervisor is None and not self._stopped:
            self._supervisor = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        """Deliberate shutdown: no further dial, and the pending sleep is cancelled (WSM-RCN-040)."""
        self._stopped = True
        self._peer._will_retry = False
        current = asyncio.current_task()
        await _cancel(self._heartbeat, current)
        await _cancel(self._supervisor, current)
        self._heartbeat = None

    # ------------------------------------------------------------------ the supervisor

    async def _supervise(self) -> None:
        """Serve, lose, back off, dial, hello, establish - in that order and no other."""
        self._start_heartbeat()
        while True:
            await self._until_the_socket_dies()
            # A deliberate `stop()` - which `peer.close()` also calls - never dials again.
            if self._stopped:
                return
            # `should_retry`, and never `peer._will_retry`: the field is peer state anything can
            # write, so keying the dial decision on it would enforce the cap by a mutable attribute
            # rather than by the helper's own arithmetic. It is *read* by `CloseReason` and by
            # nothing else. Asking the helper here is also what makes `max_attempts=0` fire
            # WSM-RCN-044's single `will_retry=False` close rather than returning in silence.
            if not should_retry(self._counter.value, self._options):
                # Unreachable with effect, and kept anyway. This branch is only entered at
                # `max_attempts=0`, where `establish()` has already set `will_retry` false and the
                # loss itself latched WSM-RCN-044's single report - so deleting `_give_up()` here
                # leaves every test green. It stays because `ts/reconnect.ts` has the same branch and
                # a reader diffing the two ports should find them the same shape; a future change to
                # the latch could also make it load-bearing again without anyone noticing it had gone.
                self._give_up()
                return
            if await self._redial():
                continue
            if not self._stopped:
                self._give_up()
            return

    async def _until_the_socket_dies(self) -> None:
        """`serve()` returning **is** the socket loss; `_die` has already fired `on_close`."""
        task = self._serving
        if task is not None:
            try:
                await task
            except Exception as exc:  # noqa: BLE001 - however serve() ended, the socket is gone
                logger.debug("muxws conn=%s serve() ended with %r", self._peer.id, exc)
        await self._stop_heartbeat()

    async def _redial(self) -> bool:
        """Back off and dial until one attempt is *established*. False when it gave up."""
        while not self._stopped and should_retry(self._counter.value, self._options):
            delay = backoff_delay(self._counter.value, self._options, self._draw)
            self._delays.append(delay)
            await self._sleep(delay)
            if self._stopped:
                return False
            try:
                socket = await self._dial()
            except Exception as exc:  # noqa: BLE001 - every dial failure is the same failed attempt
                # No `on_close` here. A dial that never produced a socket is not a socket loss, and
                # firing one would report a connection ending that never began (WSM-RCN-040).
                logger.debug("muxws conn=%s dial failed: %r", self._peer.id, exc)
                self._counter.failed()
                continue
            if await self._adopt(socket):
                return True
        return False

    async def _adopt(self, socket: SocketAdapter) -> bool:
        """One attempt past the dial: adopt, serve, hello. True only if it is *established*."""
        peer = self._peer
        # Before the socket, because this socket may die during the hello and `_die` reads it: what
        # `CloseReason.will_retry` has to say is whether the helper intends to dial *again*, which
        # for an attempt that has not established yet is whether one more attempt is left
        # (WSM-RCN-040/045).
        peer._will_retry = should_retry(self._counter.value + 1, self._options)
        # A socket is not a connection. Until the hello is acknowledged `is_open` stays false, so an
        # application frame cannot precede the hello on this socket (WSM-RCN-023/043).
        peer._established = not self._hello.configured
        peer._adopt_socket(socket)
        self._serving = asyncio.create_task(peer.serve())
        try:
            await self._perform_hello()
        except Exception as exc:  # noqa: BLE001 - a reset hello and a timed-out one fail alike
            await self._fail_attempt(exc)
            return False

        # The one point at which the counter resets: the socket is open with the subprotocol
        # accepted (WSM-CON-030) **and** the hello is acknowledged (WSM-RCN-004). Resetting it when
        # `dial()` returned would turn exponential backoff into a fixed-interval hammer against a
        # server that accepts sockets while its backend is down (WSM-INV-012).
        self._counter.established()
        peer._established = True
        peer._will_retry = should_retry(0, self._options)
        self._reconnections += 1
        # After the socket and after the hello acknowledgement, never before (WSM-RCN-023/030).
        peer._fire_reconnect(self._reconnections)
        self._start_heartbeat()
        return True

    async def _fail_attempt(self, cause: BaseException) -> None:
        """WSM-RCN-026: a failed hello is a failed **attempt**, not a connection.

        The socket is closed, the loss is reported, the counter climbs and `on_reconnect` does not
        fire - the acceptor never acknowledged this peer, so there is no identity to announce.

        `_die` is a no-op when the socket died under the hello and the read loop reported it first;
        it is the whole of the report when the hello was reset or timed out on a socket that is still
        up. `_will_retry` was set for this attempt before the socket was adopted, so either order
        produces the same `CloseReason`.
        """
        peer = self._peer
        text = f"the hello did not complete: {cause}"
        peer._die(ConnectionClosed(text, code=1006, reason=text))
        await self._abandon_socket()
        self._counter.failed()

    def _give_up(self) -> None:
        """WSM-RCN-044: `max_attempts` exhausted fires `on_close` once and never dials again.

        When the cap ran out on *refused dials*, the last loss fired `on_close` promising a retry -
        one was genuinely coming - and this is the only thing that ever withdraws it. An application
        told a reconnection was on its way and never told otherwise waits forever for one nobody is
        attempting.

        When it ran out on failed *hellos*, that last loss already reported `will_retry=False`: the
        helper knew before it adopted that socket that it was the last attempt. `_notify_close` then
        suppresses this one, because at most one `will_retry=False` close reaches an application per
        peer, ever - a teardown that ran twice is what saying it twice costs.
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

        Nothing marks it on the wire and nothing interprets it: the acceptor's own `on_stream`
        handler sees it exactly as it sees any other stream, and the acknowledgement is that handler
        returning - which ends the stream implicitly (WSM-STM-035). No application code is required
        to send one.

        It goes out before any application frame on this socket and before `on_reconnect`
        (WSM-RCN-023), which is what it means for the identity to be one the acceptor has already
        accepted.

        What it raises is the **underlying** error, never a close wrapped round it: `connect()` has
        to report what actually went wrong (WSM-RCN-006), and a deadline dressed up as a
        `ConnectionClosed` tells a caller the socket died when nothing of the sort happened.
        """
        hello = self._hello
        if not hello.configured:
            # A peer given no hello sends none and is established as soon as the socket is
            # (WSM-RCN-024/WSM-CON-030): with the `settings` exchange gone there is nothing else to
            # wait for.
            return

        # `_allocate_and_enqueue` rather than `open()`, because `open()`'s guard is exactly what this
        # hello has to get past: `is_open` is false until the hello is acknowledged, which is what
        # keeps an application frame from preceding it (WSM-RCN-023/043). The body is indivisible,
        # so this is still one allocation and one enqueue with nothing between them (WSM-SID-006).
        stream = self._peer._allocate_and_enqueue(hello.payload_for_wire, headers=hello.headers_for_wire, end=True)
        try:
            await asyncio.wait_for(stream.closed.wait(), hello.timeout)
        except asyncio.TimeoutError:
            raise StreamTimeout(
                f"the hello was not acknowledged within {hello.timeout}s", stream_id=stream.id
            ) from None
        if isinstance(stream._close_cause, StreamReset):
            # The reset itself - a `StreamRefused`, a `ConnectionLost`, whatever the acceptor sent.
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

        Locally, and with 1000: the socket is still up - a hello that timed out proves only that
        nobody answered it - so this close reaches a real remote, and 1006 is a code a peer may not
        send (WSM-RCN-011). Sending it raises inside the adapter, the socket stays open, and the
        failed attempt leaks it.
        """
        await self._peer._close_socket_locally(reason="abandoned")
        await _cancel(self._serving, asyncio.current_task())
        self._serving = None


async def _cancel(task: asyncio.Task[None] | None, current: asyncio.Task[Any] | None) -> None:
    """Cancel and collect a task, unless it is the one asking - which would deadlock on itself."""
    if task is None or task is current:
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
