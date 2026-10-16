"""The reconnect helper's schedule, counter and hello (§7)."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import pathlib
import re

from collections.abc import Callable
from typing import Any

import pytest
import websockets

import muxws

from muxws.conftest import DialableServer
from muxws.errors import ConnectionLost, ResetCode, StreamRefused, StreamTimeout
from muxws.frames import Frame
from muxws.observability import CloseReason
from muxws.peer import Peer
from muxws.reconnect import (
    AttemptCounter,
    backoff_delay,
    ConnectionLoop,
    Heartbeat,
    Hello,
    MAX_RECORDED_DELAYS,
    Reconnect,
    should_retry,
    unjittered_delay,
)
from muxws.registry import PeerRegistry
from muxws.stream import Stream
from muxws.transports.websockets_ import WebsocketsSocket


async def _until(condition: Callable[[], bool]) -> None:
    """Poll until `condition` holds; the driver's progress is observable only through its effects."""
    while not condition():
        await asyncio.sleep(0.005)


class _EnoughBeatsError(Exception):
    """Ends an injected sleep. A heartbeat runs until the socket dies, so the test stops it with this."""


# --------------------------------------------------------------------------- the schedule


def test_schedule_before_jitter_and_cap():
    """WSM-RCN-003 **(spec)**: the delay is a pure function of (attempts, options).

    The defaults double from 0.25 s and stop at 30 s. The test uses no clock, no sleep and no random
    sampling, because the delay is computed separately from waiting.
    """
    options = Reconnect()
    schedule = [unjittered_delay(attempt, options) for attempt in range(12)]
    assert schedule == [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0, 30.0, 30.0]


def test_the_schedule_stays_at_the_cap_however_many_attempts_failed():
    """WSM-RCN-003: past the float range of `factor ** attempts` the delay is still the cap.

    At the default factor that is attempt 1024.
    """
    options = Reconnect()
    for attempts in (1023, 1024, 10_000):
        assert unjittered_delay(attempts, options) == 30.0
        assert backoff_delay(attempts, options, lambda: 1.0) == pytest.approx(30.0 * 1.3)
    assert unjittered_delay(10_000, Reconnect(factor=1.5, max_delay=60.0)) == 60.0


def test_jitter_is_applied_to_every_delay_including_the_capped_ones():
    """WSM-RCN-002: jitter applies to capped delays as well, so peers at the cap do not retry together."""
    options = Reconnect()
    for attempt in (0, 3, 9):
        base = unjittered_delay(attempt, options)
        assert backoff_delay(attempt, options, draw=lambda: 1.0) == base * 1.3
        assert backoff_delay(attempt, options, draw=lambda: -1.0) == base * 0.7
        assert backoff_delay(attempt, options, draw=lambda: 0.0) == base


def test_jitter_disperses_n_simultaneous_reconnects():
    """WSM-RCN-002 **(spec)**: N peers that lose their sockets at the same instant retry at different instants.

    Without jitter, all peers reconnect to a restarting server at the same time.
    """
    options = Reconnect()
    draws = iter([-1.0 + 2.0 * index / 199 for index in range(200)])
    delays = [backoff_delay(0, options, draw=lambda: next(draws)) for _ in range(200)]

    assert len(set(delays)) > 190, "the delays must actually differ"
    assert min(delays) >= 0.25 * 0.7
    assert max(delays) <= 0.25 * 1.3
    spread = max(delays) - min(delays)
    assert spread > 0.25 * 0.5, f"the window is barely used: {spread}"


def test_a_delay_is_never_negative():
    """A jitter of 1.0 with the worst draw lands exactly on zero, never below it."""
    options = Reconnect(jitter=1.0)
    assert backoff_delay(0, options, draw=lambda: -1.0) == 0.0


def test_the_growth_factor_and_cap_are_configurable():
    options = Reconnect(initial_delay=1.0, factor=3.0, max_delay=10.0)
    assert [unjittered_delay(attempt, options) for attempt in range(5)] == [1.0, 3.0, 9.0, 10.0, 10.0]


def test_a_factor_of_one_is_a_fixed_interval():
    options = Reconnect(factor=1.0)
    assert unjittered_delay(0, options) == unjittered_delay(9, options) == 0.25


def test_nonsense_options_are_refused_at_construction():
    with pytest.raises(ValueError, match="initial_delay"):
        Reconnect(initial_delay=0)
    with pytest.raises(ValueError, match="factor"):
        Reconnect(factor=0.5)
    with pytest.raises(ValueError, match="jitter"):
        Reconnect(jitter=1.5)
    # A negative attempt count is a caller error; `factor ** -1` would return a delay shorter than
    # `initial_delay`.
    with pytest.raises(ValueError, match="negative"):
        backoff_delay(-1, Reconnect())


# --------------------------------------------------------------------------- the attempt counter


def test_the_counter_is_the_helpers_entire_persistent_state():
    """WSM-RCN-001: the attempt counter is the helper's state; the delay is computed from it."""
    counter = AttemptCounter()
    assert counter.value == 0
    assert [counter.failed() for _ in range(3)] == [1, 2, 3]
    counter.established()
    assert counter.value == 0
    assert list(vars(AttemptCounter).get("__slots__", ())) == ["_attempts"]


def test_counter_resets_only_on_established():
    """WSM-RCN-004/WSM-INV-012: socket-open alone does not reset the counter.

    If it did, a server that accepts sockets while its backend is down would be redialled at a fixed
    interval instead of with exponential backoff.
    """
    options = Reconnect()
    counter = AttemptCounter()

    # A server that accepts and then drops before the hello completes: the counter keeps increasing,
    # so the delays keep growing (WSM-RCN-004's named test, as a schedule).
    delays = []
    for _ in range(5):
        delays.append(unjittered_delay(counter.value, options))
        counter.failed()  # socket opened, hello never acknowledged - still a failed attempt

    assert delays == [0.25, 0.5, 1.0, 2.0, 4.0]
    assert delays == sorted(delays), "a growing delay sequence is the whole assertion"

    counter.established()
    assert unjittered_delay(counter.value, options) == 0.25


def test_max_attempts_exhausted_stops_the_helper():
    """WSM-RCN-044: after the cap is reached, the helper does not dial again."""
    unlimited = Reconnect()
    assert should_retry(10_000, unlimited) is True

    bounded = Reconnect(max_attempts=3)
    assert [should_retry(attempt, bounded) for attempt in range(5)] == [True, True, True, False, False]


# --------------------------------------------------------------------------- the hello


def test_a_peer_given_no_hello_sends_none():
    """WSM-RCN-024: a peer with no hello is established at socket-open (WSM-CON-030)."""
    assert Hello().configured is False
    assert Hello(payload={"tab": "abc"}).configured is True
    assert Hello(headers={"trace": "x"}).configured is True


def test_the_hello_is_captured_once_and_is_frozen():
    """WSM-RCN-020: the hello is captured once; it is not re-read, recomputed, or supplied as a callback."""
    import dataclasses

    import pytest

    hello = Hello(payload={"tab": "abc"})
    assert dataclasses.is_dataclass(hello)
    with pytest.raises(dataclasses.FrozenInstanceError):
        hello.payload = {"tab": "changed"}  # type: ignore[misc]

    signature = inspect.signature(Hello)
    assert not any(
        callable(default := parameter.default) and default is not None for parameter in signature.parameters.values()
    ), "a hello is a value, not a callback"


def test_the_hello_deadline_defaults_to_ten_seconds():
    assert Hello().timeout == 10.0


# --------------------------------------------------------------------------- the source itself


def test_jitter_uses_random_and_never_secrets():
    """WSM-RCN-005: `secrets` MUST NOT be used here, and the noqa comment states the reason."""
    from muxws import reconnect as module

    source = inspect.getsource(module)
    assert "import secrets" not in source
    assert "secrets." not in source
    assert "noqa: S311" in source
    assert "not security-sensitive" in source


async def test_acceptor_has_no_reconnect_helper(dialable_server: DialableServer):
    """§3/§7: an acceptor cannot dial; reconnecting would dial the client that dialled it.

    The helper refuses an acceptor at construction, not at the first reconnect, so the error is
    raised even for connections that never drop.
    """
    from muxws import reconnect as module
    from muxws.transports.memory import memory_pair

    _, acceptor_side = memory_pair()
    acceptor = Peer(acceptor_side, codec=dialable_server.codec, is_dialer=False)

    with pytest.raises(ValueError, match="acceptor"):
        ConnectionLoop(acceptor, dialable_server.dial, options=Reconnect(), hello=Hello())
    assert "Dialer only" in inspect.getdoc(module), "and the module says so where a reader will look"


# --------------------------------------------------------------------------- the driver, wired up


async def test_counter_does_not_reset_when_hello_never_completes(dialable_server: DialableServer):
    """WSM-RCN-004/WSM-INV-012 **(spec)**: a server that accepts and drops before the hello.

    Two attempts get a socket and lose it with the hello outstanding. Neither is established
    (WSM-RCN-004), so the counter increases and the delays grow. A helper that reset the counter when
    `dial()` returned would produce `[0.01, 0.01, 0.01]` here, a fixed retry interval (WSM-INV-012).
    `on_reconnect` fires once, for the attempt that was acknowledged (WSM-RCN-026/030).
    """
    reconnected: list[int] = []
    established = asyncio.Event()

    socket = await dialable_server.dial()
    peer = Peer(socket, codec=dialable_server.codec, is_dialer=True)
    peer.on_reconnect(lambda attempt, _peer: (reconnected.append(attempt), established.set()))
    loop = ConnectionLoop(
        peer,
        dialable_server.dial,
        options=Reconnect(initial_delay=0.01, jitter=0.0),
        hello=Hello(payload={"tab": "abc"}),
        ping_interval=0.0,
        draw=lambda: 0.0,
    )
    await loop.establish()
    loop.start()
    try:
        assert loop.attempts == 0
        assert dialable_server.received == [{"tab": "abc"}], "the hello goes out on the first socket too"

        dropped: list[Any] = []

        async def drop_the_first_two(payload: Any, stream: Stream) -> None:
            _ = stream
            dropped.append(payload)
            if len(dropped) <= 2:
                await dialable_server.drop()

        dialable_server.handler = drop_the_first_two
        await dialable_server.drop()
        await asyncio.wait_for(established.wait(), 5.0)

        assert loop.delays == [0.01, 0.02, 0.04], "every attempt that failed made the next one wait longer"
        assert loop.attempts == 0, "and the counter resets at exactly one point: an acknowledged hello"
        assert reconnected == [1]
        assert loop.reconnections == 1
        assert peer.is_open is True
        assert dialable_server.dials == 4
    finally:
        await loop.stop()


async def test_a_dial_that_never_produced_a_socket_is_not_a_socket_loss(dialable_server: DialableServer):
    """WSM-RCN-040: `on_close` fires per socket loss; a refused dial had no socket and fires nothing."""
    closes: list[Any] = []
    established = asyncio.Event()

    socket = await dialable_server.dial()
    peer = Peer(socket, codec=dialable_server.codec, is_dialer=True)
    peer.on_close(closes.append)
    peer.on_reconnect(lambda _attempt, _peer: established.set())
    loop = ConnectionLoop(
        peer,
        dialable_server.dial,
        options=Reconnect(initial_delay=0.01, jitter=0.0),
        hello=Hello(),
        ping_interval=0.0,
    )
    await loop.establish()
    loop.start()
    try:
        dialable_server.refuse_next(3)
        await dialable_server.drop()
        await asyncio.wait_for(established.wait(), 5.0)

        assert dialable_server.refusals == 3
        assert len(closes) == 1, "one socket died, so on_close fired once - not once per refused dial"
        assert closes[0].will_retry is True
        assert loop.attempts == 0
    finally:
        await loop.stop()


async def test_dead_socket_takes_the_same_backoff_path_as_a_clean_close(dialable_server: DialableServer):
    """WSM-RCN-011: a heartbeat timeout and a dropped socket take the same code path.

    The same peer loses its socket twice, once because the server dropped it and once because no pong
    arrived. Both losses produce the same result: one `on_close` each, one delay each drawn from a
    counter that was reset in between, one `on_reconnect` each. A separate code path for the missing
    pong could skip the backoff or use a different counter.
    """
    closes: list[Any] = []
    established = asyncio.Event()

    peer, loop = await dialable_server.driver(ping_interval=0.02, ping_timeout=0.05)
    peer.on_close(closes.append)
    peer.on_reconnect(lambda _attempt, _peer: established.set())
    await loop.establish()
    loop.start()
    try:
        # A clean loss: the server drops the socket and the read loop reports it.
        await dialable_server.drop()
        await asyncio.wait_for(established.wait(), 5.0)
        after_the_drop = {"closes": len(closes), "delays": loop.delays, "reconnections": loop.reconnections}

        # An unresponsive socket: the server stops reading without closing, so only the heartbeat
        # detects it.
        established.clear()
        await dialable_server.go_silent()
        await asyncio.wait_for(established.wait(), 5.0)

        assert after_the_drop == {"closes": 1, "delays": [0.01], "reconnections": 1}
        assert len(closes) == 2, "one loss, one on_close - a dead socket is not a special case"
        assert [close.will_retry for close in closes] == [True, True]
        assert [close.code for close in closes] == [1006, 1006]
        assert loop.delays == [0.01, 0.01], "the second loss backed off from a counter that had reset"
        assert loop.reconnections == 2
        assert loop.attempts == 0
        assert peer.is_open is True
        assert closes[1].reason == "no pong within 0.05s", "and it says which of the two it was"
    finally:
        await loop.stop()


async def test_swallowed_pong_is_detected_within_interval_plus_timeout(dialable_server: DialableServer):
    """WSM-RCN-011 **(spec)**: detection time is bounded by `ping_interval + ping_timeout`.

    The server stops reading without closing. The socket stays open and every frame the dialer sends
    is discarded; a transport-level timeout takes minutes or never fires. The test relies on none:
    the detection budget is the two configured values, which add up to 0.1 s. The muxws `ping` frame
    exists because browsers do not expose WebSocket-level pings (WSM-CON-011).
    """
    interval, timeout = 0.05, 0.05
    closes: list[Any] = []
    dead = asyncio.Event()

    peer, loop = await dialable_server.driver(ping_interval=interval, ping_timeout=timeout)

    def record(reason: Any) -> None:
        closes.append(reason)
        dead.set()

    peer.on_close(record)
    await loop.establish()
    loop.start()
    try:
        started = asyncio.get_running_loop().time()
        await dialable_server.go_silent()
        await asyncio.wait_for(dead.wait(), 5.0)
        elapsed = asyncio.get_running_loop().time() - started

        assert elapsed <= interval + timeout + 0.15, f"detection took {elapsed:.3f}s, not the promised bound"
        assert closes[0].reason == f"no pong within {timeout}s"
        assert peer.is_open is False
        assert [frame.type for frame in dialable_server.frames(0)][-1] == "ping", "the detector is the ping"
    finally:
        await loop.stop()


async def test_heartbeat_timer_resets_on_any_traffic(pair):
    """WSM-RCN-010/§6: a ping is sent only after `interval` without traffic; a busy socket gets none.

    The test uses an injected clock and an injected sleep, so no wall time passes and the result does
    not depend on machine speed. Every sleep the heartbeat requests is followed by a frame crossing
    the socket, and that frame goes through `_report_frame`, the code path that updates
    `last_activity` in both directions.

    The second half is the control: it fails for a heartbeat that never runs or that ignores
    `interval`.

    Each run is bounded: a heartbeat that never sleeps pings in a loop, and an unbounded test would
    hang instead of failing.
    """
    peer = pair.dialer
    now = [0.0]
    peer._clock = lambda: now[0]
    beats = 0

    async def busy_sleep(delay: float) -> None:
        nonlocal beats
        beats += 1
        now[0] += delay
        # Alternating directions: if only one direction updated the timestamp, a socket receiving
        # traffic would look idle and get a ping every interval.
        peer._report_frame("rx" if beats % 2 else "tx", Frame("data", stream=1, payload={"row": beats}), "12345678")
        if beats >= 25:
            raise _EnoughBeatsError
        await asyncio.sleep(0)

    await _run_heartbeat(peer, busy_sleep)
    await pair.settle()

    assert beats == 25, "the heartbeat slept every round rather than pinging"
    assert pair.frames_of_type("dialer", "ping") == [], "a busy socket must not pay for a ping"

    silences = 0

    async def silent_sleep(delay: float) -> None:
        nonlocal silences
        silences += 1
        now[0] += delay
        if silences >= 2:
            raise _EnoughBeatsError
        await asyncio.sleep(0)

    await _run_heartbeat(peer, silent_sleep)
    await pair.settle()

    assert len(pair.frames_of_type("dialer", "ping")) == 1, "and an idle one gets exactly one ping"


async def _run_heartbeat(peer: Peer, sleep: Any) -> None:
    """Run a heartbeat until its injected sleep stops it; fail if the heartbeat never sleeps."""
    beat = Heartbeat(peer, interval=1.0, timeout=1.0, sleep=sleep)
    try:
        await asyncio.wait_for(beat.run(), 5.0)
    except _EnoughBeatsError:
        return
    except asyncio.TimeoutError:  # pragma: no cover - reached only when the test fails
        pytest.fail("the heartbeat never asked to sleep: it pinged in a loop instead of waiting out the interval")
    pytest.fail("the heartbeat returned on its own; only its injected sleep may end it")


async def test_max_attempts_exhausted_fires_on_close_once_and_never_dials_again(dialable_server: DialableServer):
    """WSM-RCN-044: exhausting the cap fires `on_close` once with `will_retry` false, and then no more.

    The socket loss already fired `on_close` with `will_retry` true. Exhausting the cap fires it once
    more with `will_retry` false; without that call, the application waits for a reconnection that
    is not attempted.
    """
    closes: list[Any] = []
    given_up = asyncio.Event()

    peer, loop = await dialable_server.driver(options=Reconnect(initial_delay=0.001, jitter=0.0, max_attempts=2))

    def record(reason: Any) -> None:
        closes.append(reason)
        if not reason.will_retry:
            given_up.set()

    peer.on_close(record)
    await loop.establish()
    loop.start()
    try:
        dialable_server.refuse_next(10)
        await dialable_server.drop()
        await asyncio.wait_for(given_up.wait(), 5.0)
        dials = dialable_server.dials

        assert [close.will_retry for close in closes] == [True, False]
        assert len(closes) == 2, "the loss, then the withdrawal - and nothing else"
        assert dials == 3, "the first connection plus exactly max_attempts dials after the loss"
        assert loop.delays == [0.001, 0.002], "and it backed off between them"
        assert loop.attempts == 2
        assert peer.is_open is False

        await asyncio.sleep(0.05)
        assert dialable_server.dials == dials, "never dials again"
        assert len(closes) == 2, "and never reports again"
    finally:
        await loop.stop()


async def test_max_attempts_of_zero_gives_up_at_the_first_loss_without_dialling(dialable_server: DialableServer):
    """WSM-RCN-044: with a cap of zero the peer never retries and still reports `will_retry` false.

    The dial decision is `should_retry(counter, options)` only. `peer._will_retry` is writable by any
    code holding the peer; deciding on it would let an assignment disable the rule, and a cap of zero
    would return without the `will_retry=False` close.
    """
    closes: list[Any] = []
    given_up = asyncio.Event()

    peer, loop = await dialable_server.driver(options=Reconnect(initial_delay=0.01, jitter=0.0, max_attempts=0))
    peer.on_close(lambda reason: (closes.append(reason), None if reason.will_retry else given_up.set()))
    await loop.establish()
    loop.start()
    try:
        dials = dialable_server.dials
        await dialable_server.drop()
        await asyncio.wait_for(given_up.wait(), 5.0)
        await asyncio.sleep(0.05)

        assert [close.will_retry for close in closes] == [False], "exactly one, and it promises nothing"
        assert dialable_server.dials == dials, "a cap of zero never dials"
        assert loop.delays == [], "and never waits out a backoff it is not going to use"
        assert peer.is_open is False
    finally:
        await loop.stop()


async def test_the_dial_decision_is_the_helpers_arithmetic_not_the_peers_field(dialable_server: DialableServer):
    """WSM-RCN-044/WSM-RCN-001: `should_retry` decides; `_will_retry` only reports.

    `_will_retry` is peer state, writable by any code holding the peer; the tests in
    `socket_death_test.py` write it, and an application can. If the helper used it as its stop
    condition, one assignment would stop the reconnect loop without any report. The attempt counter
    is the helper's entire persistent state (WSM-RCN-001), and the cap is checked against it.
    """
    again = asyncio.Event()
    peer, loop = await dialable_server.driver()
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    loop.start()
    try:
        peer._will_retry = False
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)

        assert loop.reconnections == 1, "the helper's own arithmetic still said one attempt was left"
        assert peer.is_open is True
    finally:
        await loop.stop()


async def test_the_cap_exhausted_by_failed_hellos_reports_giving_up_once(dialable_server: DialableServer):
    """WSM-RCN-044: at most one `will_retry=False` close per peer.

    When the cap runs out on refused dials, the last loss reported `will_retry` true, and the helper
    fires the `will_retry=False` close. When it runs out on failed hellos, the helper set
    `_will_retry` false before adopting the last socket, so that loss already reported
    `will_retry=False` and the helper fires no second one. An application that tears down on
    `will_retry=False` would otherwise tear down twice.
    """
    closes: list[Any] = []
    given_up = asyncio.Event()

    peer, loop = await dialable_server.driver(
        options=Reconnect(initial_delay=0.001, jitter=0.0, max_attempts=2),
        hello=Hello(payload={"tab": "abc"}, timeout=0.05),
    )
    peer.on_close(lambda reason: (closes.append(reason), None if reason.will_retry else given_up.set()))
    await loop.establish()
    loop.start()
    try:
        dialable_server.on_hello = "hang"
        await dialable_server.drop()
        await asyncio.wait_for(given_up.wait(), 5.0)
        await asyncio.sleep(0.05)

        assert [close.will_retry for close in closes] == [True, True, False]
        assert sum(1 for close in closes if not close.will_retry) == 1, "and it is withdrawn once, not twice"
    finally:
        await loop.stop()


async def test_a_throwing_application_handler_does_not_stop_the_driver(dialable_server: DialableServer, caplog):
    """An exception from an `on_reconnect` or `on_close` handler stops neither the driver nor other handlers.

    `on_reconnect` fires from inside the reconnect loop on the supervisor's task. A propagated
    exception would end the supervisor, and the peer would not dial again. The exception is logged,
    and the other handlers for the same event still run.
    """
    reconnects: list[int] = []
    closes: list[Any] = []
    again = asyncio.Event()

    def explode_on_reconnect(_attempt: int, _peer: Peer) -> None:
        raise RuntimeError("the application's own bookkeeping is broken")

    def explode_on_close(_reason: Any) -> None:
        raise RuntimeError("and so is its teardown")

    peer, loop = await dialable_server.driver()
    peer.on_reconnect(explode_on_reconnect)
    peer.on_reconnect(lambda attempt, _peer: (reconnects.append(attempt), again.set()))
    peer.on_close(explode_on_close)
    peer.on_close(closes.append)
    await loop.establish()
    loop.start()
    try:
        with caplog.at_level("ERROR"):
            for _ in range(2):
                again.clear()
                await dialable_server.drop()
                await asyncio.wait_for(again.wait(), 5.0)

        assert reconnects == [1, 2], "the driver kept dialling, and the second handler kept running"
        assert len(closes) == 2
        assert loop.reconnections == 2
        logged = [record.getMessage() for record in caplog.records]
        assert any("on_reconnect handler raised" in message for message in logged), "swallowed silently"
        assert any("on_close handler raised" in message for message in logged)
    finally:
        await loop.stop()


async def test_the_recorded_delays_do_not_grow_without_bound(dialable_server: DialableServer):
    """Related to WSM-RCN-001: `delays` is bounded to the most recent `MAX_RECORDED_DELAYS` entries.

    `delays` is instrumentation on an object that outlives its connections. With no attempt cap and a
    server that does not come back, an unbounded list would grow by one float per retry for the life
    of the process.
    """
    again = asyncio.Event()
    peer, loop = await dialable_server.driver(options=Reconnect(initial_delay=0.001, jitter=0.0))
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    loop.start()
    try:
        dialable_server.refuse_next(1)
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)
        assert loop.delays == [0.001, 0.002], "the driver's own appends land here"

        for index in range(MAX_RECORDED_DELAYS + 10):
            loop._delays.append(float(index))

        assert len(loop.delays) == MAX_RECORDED_DELAYS
        assert loop.delays[0] == 10.0, "the oldest goes, not the newest"
        assert loop.delays[-1] == float(MAX_RECORDED_DELAYS + 9)
    finally:
        await loop.stop()


# --------------------------------------------------------------------------- the hello, on the wire


async def test_three_drops_replay_byte_identical_hellos(dialable_server: DialableServer):
    """WSM-RCN-027 **(spec)**: three drops, three identical hellos, no application involvement.

    The comparison is on the encoded messages, not on decoded objects: a reordered key or a tuple
    decoded as a list compares equal after decoding but is a different hello on the wire.

    `on_reconnect` fires after each acknowledgement, never before; in the interleaved order below,
    every `reconnect:n` is immediately preceded by a `hello`.

    The last third checks WSM-INV-013: the helper replays the hello, so a peer whose application
    registered no `on_reconnect` handler is still found in the server's registry. If the application
    had to replay the hello, omitting it would leave an open socket with no subscriptions and no
    error.
    """
    order: list[str] = []
    registry = PeerRegistry()

    async def register_the_hello(payload: Any, stream: Stream) -> None:
        order.append("hello")
        acceptor = stream._peer
        acceptor.tags["tab"] = payload["tab"]
        registry.register(acceptor)

    dialable_server.handler = register_the_hello
    reconnected: list[int] = []
    again = asyncio.Event()

    peer, loop = await dialable_server.driver(
        hello=Hello(payload={"tab": "abc", "rooms": ["lobby", "news"]}, headers={"trace": "t"})
    )
    peer.on_reconnect(
        lambda attempt, _peer: (reconnected.append(attempt), order.append(f"reconnect:{attempt}"), again.set())
    )
    await loop.establish()
    loop.start()
    try:
        for _ in range(3):
            again.clear()
            await dialable_server.drop()
            await asyncio.wait_for(again.wait(), 5.0)

        hellos = [dialable_server.raw(index)[0] for index in range(4)]
        assert len(set(hellos)) == 1, f"four connections, one hello, byte for byte: {hellos}"
        assert dialable_server.frames(3)[0].payload == {"tab": "abc", "rooms": ["lobby", "news"]}
        assert reconnected == [1, 2, 3]
        assert order == ["hello", "hello", "reconnect:1", "hello", "reconnect:2", "hello", "reconnect:3"]
    finally:
        await loop.stop()

    silent = DialableServer()
    silent.handler = register_the_hello
    try:
        quiet, quiet_loop = await silent.driver(hello=Hello(payload={"tab": "quiet"}))
        await quiet_loop.establish()
        quiet_loop.start()
        try:
            assert quiet._reconnect_handlers == [], "this application asked to be told nothing"
            await silent.drop()
            await asyncio.wait_for(_until(lambda: len(silent.received) == 2), 5.0)
            # The previous peer's index entries are removed when its close hook runs, one or two
            # event-loop turns after the new connection is up (WSM-REG-016).
            await asyncio.wait_for(_until(lambda: registry.peers_for(tab="quiet") == [silent.acceptors[-1]]), 5.0)

            assert silent.raw(1)[0] == silent.raw(0)[0], "replayed without being asked"
            assert registry.peers_for(tab="quiet") == [silent.acceptors[-1]], "and findable again"
        finally:
            await quiet_loop.stop()
    finally:
        await silent.aclose()


async def test_reset_hello_and_timed_out_hello_both_back_off(dialable_server: DialableServer):
    """WSM-RCN-026 **(spec)**: a failed hello is a failed *attempt*, not a connection.

    Both ways a hello fails, reset and no answer, take the same path: the socket is closed, the
    counter increases, the next attempt waits longer, and `on_reconnect` does not fire because the
    acceptor never acknowledged this peer.
    """
    answered: list[Any] = []

    async def answer(payload: Any, stream: Stream) -> None:
        answered.append(payload)
        if len(answered) == 2:
            await stream.reset(ResetCode.REFUSED, "the backend is down")
        elif len(answered) == 3:
            # Not acknowledged; the deadline ends this attempt.
            await stream.closed.wait()

    dialable_server.handler = answer
    closes: list[Any] = []
    reconnected: list[int] = []
    again = asyncio.Event()

    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}, timeout=0.05))
    peer.on_close(closes.append)
    peer.on_reconnect(lambda attempt, _peer: (reconnected.append(attempt), again.set()))
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)

        assert reconnected == [1], "neither failed hello fired on_reconnect"
        assert loop.delays == [0.01, 0.02, 0.04], "each failed hello made the next attempt wait longer"
        assert loop.attempts == 0, "and the acknowledged one, and only it, reset the counter"
        assert dialable_server.sockets[1].is_closed, "the reset hello's socket was closed"
        assert dialable_server.sockets[2].is_closed, "and so was the one that never answered"
        assert [close.will_retry for close in closes] == [True, True, True]
        assert len(answered) == 4
    finally:
        await loop.stop()


async def test_hello_is_an_ordinary_open_reaching_on_stream(dialable_server: DialableServer):
    """WSM-RCN-021/022: the acceptor's own handler receives the hello, and nothing marks it on the wire.

    The hello is compared byte by byte with an application `open()` carrying the same payload and
    headers on the same socket: the two messages differ only in the stream id. A flag, a reserved
    header, a separate frame type or an extra envelope key would appear as a difference.

    The acknowledgement is the handler returning (WSM-RCN-022/WSM-STM-035); the handler below sends
    nothing.
    """
    seen: list[tuple[Any, Stream]] = []

    async def record(payload: Any, stream: Stream) -> None:
        seen.append((payload, stream))

    dialable_server.handler = record
    payload = {"tab": "abc"}
    headers = {"trace": "t"}

    peer, loop = await dialable_server.driver(hello=Hello(payload=payload, headers=headers))
    await loop.establish()
    loop.start()
    try:
        ordinary = peer.open(payload, headers=headers, end=True)
        await asyncio.wait_for(_until(lambda: len(dialable_server.raw(0)) >= 2), 5.0)

        hello_message, ordinary_message = dialable_server.raw(0)[0], dialable_server.raw(0)[1]
        assert hello_message.replace('"stream":1', f'"stream":{ordinary.id}') == ordinary_message
        assert dialable_server.frames(0)[0].type == "open", "an open, like any other"

        hello_payload, hello_stream = seen[0]
        assert hello_payload == payload
        assert isinstance(hello_stream, Stream), "delivered to on_stream like any other stream"
        assert hello_stream.headers == headers
    finally:
        await loop.stop()


async def test_hello_precedes_every_application_frame_on_the_socket(dialable_server: DialableServer):
    """WSM-RCN-023: the hello precedes every application frame and `on_reconnect`.

    The earliest point an application can act on a new socket is inside `on_reconnect`, so the test
    sends from there. The hello is already on the wire and acknowledged at that point, so the
    acceptor knows the peer's identity before any application frame arrives.
    """
    wire_at_reconnect: list[list[str | bytes]] = []
    pushes: list[asyncio.Task[None]] = []
    again = asyncio.Event()

    def push(attempt: int, reconnected: Peer) -> None:
        wire_at_reconnect.append(list(dialable_server.sockets[-1].sent))
        pushes.append(asyncio.create_task(reconnected.notify({"app": attempt})))
        again.set()

    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}))
    peer.on_reconnect(push)
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)
        await asyncio.gather(*pushes)
        await asyncio.wait_for(_until(lambda: len(dialable_server.received) == 3), 5.0)

        already_sent = [dialable_server.codec.decode(message) for message in wire_at_reconnect[0]]
        assert [frame.type for frame in already_sent] == ["open"], "the hello, and only the hello"
        assert already_sent[0].payload == {"tab": "abc"}
        assert [frame.payload for frame in dialable_server.frames(1)] == [{"tab": "abc"}, {"app": 1}]
        assert dialable_server.received == [{"tab": "abc"}, {"tab": "abc"}, {"app": 1}]
    finally:
        await loop.stop()


async def test_the_peer_is_not_open_during_the_hello_window(dialable_server: DialableServer):
    """WSM-RCN-043/023: `is_open` is false until the hello is *acknowledged*, not until socket-open.

    Between socket-open and the acknowledged hello, a peer that set `is_open` at socket-open would
    accept `open()`, `notify()` and `request()` and send their frames ahead of the hello, to an
    acceptor that does not yet know the peer's identity (WSM-RCN-023). The hello is sent through the
    allocate-and-enqueue body, not `open()`, so the `is_open` guard does not block it.
    """
    in_the_window = asyncio.Event()
    release = asyncio.Event()
    again = asyncio.Event()

    async def hold_the_second_hello(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        if len(dialable_server.received) > 1:
            in_the_window.set()
            await release.wait()

    dialable_server.handler = hold_the_second_hello
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}, timeout=5.0))
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(in_the_window.wait(), 5.0)

        assert peer.is_open is False, "a socket whose hello is outstanding is not an open connection"
        with pytest.raises(ConnectionLost):
            peer.open({"app": "too early"})
        with pytest.raises(ConnectionLost):
            await peer.notify({"app": "too early"})
        with pytest.raises(ConnectionLost):
            await peer.request({"app": "too early"})
        assert [frame.payload for frame in dialable_server.frames(1)] == [{"tab": "abc"}], "the hello, alone"

        release.set()
        await asyncio.wait_for(again.wait(), 5.0)
        assert peer.is_open is True, "and the acknowledgement is what ends the window"
    finally:
        release.set()
        await loop.stop()


async def test_a_reset_hello_raises_the_reset_itself(dialable_server: DialableServer):
    """WSM-RCN-006: `connect()` raises the underlying error, not `ConnectionClosed` wrapping it.

    A refused hello and a dead socket are different faults; with `ConnectionClosed` for both, the
    caller cannot distinguish them.
    """
    dialable_server.on_hello = "reset"
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}))

    with pytest.raises(StreamRefused, match="refused"):
        await loop.establish()
    assert peer.is_open is False


async def test_no_hello_means_established_at_subprotocol_accept(dialable_server: DialableServer):
    """WSM-RCN-024/WSM-CON-030: a peer given no hello sends none, and is established at once.

    There is no `settings` exchange, so for such a peer "established" is socket-open with the
    subprotocol accepted. The wire snapshot is taken inside `on_reconnect`, before any application
    traffic.
    """
    snapshots: list[dict[str, Any]] = []
    again = asyncio.Event()

    peer, loop = await dialable_server.driver(hello=Hello())

    def record(attempt: int, reconnected: Peer) -> None:
        snapshots.append(
            {
                "attempt": attempt,
                "wire": list(dialable_server.sockets[-1].sent),
                "attempts": loop.attempts,
                "is_open": reconnected.is_open,
            }
        )
        again.set()

    peer.on_reconnect(record)
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)

        assert snapshots == [{"attempt": 1, "wire": [], "attempts": 0, "is_open": True}]
        assert dialable_server.received == [], "no hello was configured, so none was ever sent"
    finally:
        await loop.stop()


async def test_mutating_the_hello_object_after_connect_does_not_change_the_wire(dialable_server: DialableServer):
    """WSM-RCN-020: the hello is captured once, by value, and not re-read.

    The test mutates the three objects through which the application can reach the hello after
    `connect()`: the payload dict it passed, a container nested inside it, and the copy a previous
    replay passed to the codec. None of the mutations changes what the next connection sends.
    """
    payload = {"tab": "abc", "rooms": ["lobby"]}
    headers = {"trace": "t"}
    hello = Hello(payload=payload, headers=headers)
    again = asyncio.Event()

    peer, loop = await dialable_server.driver(hello=hello)
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    loop.start()
    try:
        payload["tab"] = "hijacked"
        payload["rooms"].append("private")
        headers["trace"] = "hijacked"
        hello.payload_for_wire["tab"] = "hijacked"
        hello.headers_for_wire["trace"] = "hijacked"

        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)

        assert dialable_server.raw(1)[0] == dialable_server.raw(0)[0]
        assert dialable_server.received == [{"tab": "abc", "rooms": ["lobby"]}] * 2
        assert dialable_server.frames(1)[0].headers == {"trace": "t"}
    finally:
        await loop.stop()


# --------------------------------------------------------------------------- the first attempt


async def test_first_attempt_failure_raises_with_unlimited_retries_configured(dialable_server: DialableServer):
    """WSM-RCN-006/WSM-INV-018 **(spec)**: the first attempt raises, whatever `reconnect` says.

    Both ways a first attempt fails raise instead of retrying: no answer to the dial, and an answer
    that never acknowledges the hello. Reconnection applies only to connections that were established
    and then lost; no option changes this. A peer retrying in the background would report a wrong URL
    as no error.

    The test uses port 1, not an ephemeral port: a port number containing "400" would be reported as
    a refused upgrade and a codec mismatch.
    """
    started = asyncio.get_running_loop().time()
    with pytest.raises(OSError, match=".") as refused:
        await asyncio.wait_for(muxws.connect("ws://127.0.0.1:1/", reconnect=Reconnect()), 5.0)

    assert not isinstance(refused.value, asyncio.TimeoutError), "it must raise, not retry until the deadline"
    assert asyncio.get_running_loop().time() - started < 2.0, "and raise the underlying error at once"
    assert "retry_initial" not in inspect.signature(muxws.connect).parameters, "no option changes this"

    # The dial succeeded but the connection was not established: the same rule applies.
    dialable_server.on_hello = "hang"
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}, timeout=0.05))
    closes: list[Any] = []
    peer.on_close(closes.append)

    # The underlying error, not `ConnectionClosed` (WSM-RCN-006: "with the underlying error"). An
    # unanswered hello and a dead socket are different faults.
    with pytest.raises(StreamTimeout, match="not acknowledged"):
        await loop.establish()

    assert dialable_server.sockets[0].is_closed, "the socket it gave up on is closed, not left open"
    assert closes == [], "nothing was established, so nothing was lost and nothing is reported"
    # The peer is closed when `connect()` raises (WSM-RCN-006). Otherwise `is_open` would depend on
    # a race between the read loop seeing the abandoned socket and the cancel that tears it down. The
    # close cause is asserted as well as the flag: it must be the helper's, not whatever the
    # transport reported.
    assert peer.is_open is False, "a first connection that never established is dead, not pending"
    assert peer._death is not None
    assert peer._death.reason == "the first connection was never established", "the helper marked it, not the race"
    await asyncio.sleep(0.05)
    assert dialable_server.dials == 1, "and nothing dialled again behind the caller"


async def test_close_reason_shape_is_identical_in_both_languages(dialable_server: DialableServer):
    """WSM-RCN-045: exactly four fields, the same four, one type per language, for every loss.

    The TypeScript field list is read from its source, so a field added to one implementation and not
    the other fails this test.
    """
    python_fields = [field.name for field in dataclasses.fields(CloseReason)]
    assert python_fields == ["code", "reason", "was_clean", "will_retry"]

    source = (pathlib.Path(__file__).resolve().parents[1] / "ts" / "observability.ts").read_text(encoding="utf-8")
    declaration = re.search(r"export interface CloseReason \{(.*?)\n\}", source, re.DOTALL)
    assert declaration is not None, "the TypeScript port must declare the interface"
    assert re.findall(r"readonly (\w+):", declaration.group(1)) == [_camel(name) for name in python_fields]

    # One type for every socket loss, not one per reason: the drop and the cap exhaustion that follows.
    closes: list[Any] = []
    given_up = asyncio.Event()
    peer, loop = await dialable_server.driver(options=Reconnect(initial_delay=0.001, jitter=0.0, max_attempts=1))
    peer.on_close(lambda reason: (closes.append(reason), None if reason.will_retry else given_up.set()))
    await loop.establish()
    loop.start()
    try:
        dialable_server.refuse_next(5)
        await dialable_server.drop()
        await asyncio.wait_for(given_up.wait(), 5.0)

        assert {type(close) for close in closes} == {CloseReason}
        assert [close.will_retry for close in closes] == [True, False]
    finally:
        await loop.stop()


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(word.capitalize() for word in rest)


# --------------------------------------------------------------------------- connect() and the reconnect driver


async def test_connect_wires_the_driver_to_a_real_socket():
    """`connect()` constructs the reconnect driver.

    The test uses the public entry point against a real `websockets` server: the server kills the
    connection, the peer dials again, replays the hello captured at `connect()` (WSM-RCN-020), and
    fires `on_reconnect` after the acknowledgement, not before (WSM-RCN-030).
    """
    hellos: list[Any] = []
    server_side: list[Any] = []

    async def handle(connection: Any) -> None:
        server_side.append(connection)
        peer = await muxws.accept(WebsocketsSocket(connection))
        peer.on_stream(lambda payload, _stream: hellos.append(payload))
        await peer.serve()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        peer = await muxws.connect(
            url,
            hello={"tab": "abc"},
            reconnect=Reconnect(initial_delay=0.01, max_delay=0.05),
            ping_interval=0.0,
        )
        reconnected: list[int] = []
        again = asyncio.Event()
        peer.on_reconnect(lambda attempt, _peer: (reconnected.append(attempt), again.set()))
        try:
            assert hellos == [{"tab": "abc"}], "the hello goes out on the first connection too"

            await server_side[0].close()
            await asyncio.wait_for(again.wait(), 5.0)

            assert reconnected == [1]
            assert hellos == [{"tab": "abc"}, {"tab": "abc"}], "replayed verbatim on the new socket"
            assert peer.is_open is True

            await peer.notify({"after": "the reconnect"})
            await asyncio.sleep(0.1)
            assert hellos[-1] == {"after": "the reconnect"}, "and the new socket carries application traffic"
        finally:
            await peer.close()

    # `close()` reaches the driver only through the reference `connect()` gives the peer, and only
    # the driver can stop a scheduled dial. Both are asserted: a peer without the reference
    # reconnects correctly but keeps dialling after the application closes it.
    assert peer._connection_loop is not None, "connect() must hold the driver it built"
    assert peer._connection_loop._stopped is True, "and close() must have stopped it"


async def test_connect_registers_its_handlers_before_the_hello_goes_out():
    """WSM-STM-033/WSM-RCN-023: the acceptor may answer the hello with a stream of its own.

    An acceptor that pushes as soon as it receives the hello (a session backlog, a subscription
    confirmation) pushes before `connect()` returns. An `on_stream` registered on the returned peer is
    too late, and the push is answered with `reset(REFUSED, "no on_stream handler")`. The handler is
    therefore a `connect()` parameter, registered before `establish()` runs.
    """
    pushed: list[Any] = []
    connections: list[Any] = []

    async def handle(connection: Any) -> None:
        connections.append(connection)
        acceptor = await muxws.accept(WebsocketsSocket(connection))

        async def push_at_once(payload: Any, _stream: Stream) -> None:
            # Before returning, so the push precedes the acknowledgement on the wire and arrives
            # while the dialer is inside `establish()`.
            await acceptor.notify({"backlog": payload["tab"]})

        acceptor.on_stream(push_at_once)
        await acceptor.serve()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        closes: list[Any] = []
        reconnects: list[int] = []
        peer = await muxws.connect(
            url,
            hello={"tab": "abc"},
            reconnect=Reconnect(initial_delay=0.01, jitter=0.0),
            ping_interval=0.0,
            on_stream=lambda payload, _stream: pushed.append(payload),
            on_close=closes.append,
            on_reconnect=lambda attempt, _peer: reconnects.append(attempt),
        )
        try:
            await asyncio.wait_for(_until(lambda: pushed == [{"backlog": "abc"}]), 5.0)

            await connections[0].close()
            await asyncio.wait_for(_until(lambda: reconnects == [1]), 5.0)
            await asyncio.wait_for(_until(lambda: len(pushed) == 2), 5.0)

            assert pushed == [{"backlog": "abc"}] * 2, "and again on the socket the helper dialled"
            assert [close.will_retry for close in closes] == [True]
        finally:
            await peer.close()


async def test_connect_and_accept_register_their_interceptors_before_any_frame():
    """WSM-API-026: the interceptor arguments are registered before the hello and before `serve()`.

    The acceptor pushes a stream as soon as it receives the hello, so the dialer's incoming
    interceptor sees that push only if `connect()` registered it before `establish()` ran.
    """
    pushed: list[dict[str, Any]] = []
    answered: list[dict[str, Any]] = []
    acceptor_saw: list[dict[str, Any]] = []

    async def handle(connection: Any) -> None:
        acceptor = await muxws.accept(
            WebsocketsSocket(connection),
            open_interceptors=[lambda headers, _payload: headers.update(pushed_by="acceptor")],
            reply_headers_interceptors=[lambda headers, _stream: headers.update(served_by="acceptor")],
            incoming_headers_interceptors=[lambda headers, _stream: acceptor_saw.append(dict(headers))],
        )

        async def handler(payload: Any, stream: Stream) -> None:
            if isinstance(payload, dict) and "tab" in payload:
                await acceptor.notify({"backlog": payload["tab"]})
            await stream.reply("ok")

        acceptor.on_stream(handler)
        await acceptor.serve()

    def record(headers: Any, stream: Stream) -> None:
        (answered if stream.local else pushed).append(dict(headers))

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        peer = await muxws.connect(
            url,
            hello={"tab": "abc"},
            ping_interval=0.0,
            on_stream=lambda _payload, _stream: None,
            open_interceptors=[lambda headers, _payload: headers.update(conn="tab-abc")],
            incoming_headers_interceptors=[record],
        )
        try:
            await asyncio.wait_for(_until(lambda: pushed == [{"pushed_by": "acceptor"}]), 5.0)
            assert await asyncio.wait_for(peer.request("q"), 5.0) == "ok"
            assert acceptor_saw[0] == {}, "the hello passes no open interceptor"
            assert acceptor_saw[-1] == {"conn": "tab-abc"}
            assert answered[-1] == {"served_by": "acceptor"}
        finally:
            await peer.close()


async def test_a_swallowed_pong_on_a_real_socket_re_dials():
    """WSM-RCN-011: the heartbeat closes the socket with a code a peer is allowed to send.

    `MemorySocket.close` discards the code, so the in-memory transport does not detect this. A real
    `websockets` connection validates it; 1006 is reserved for "the connection dropped without a
    close frame", so sending it raises. If that exception is swallowed, the socket stays open, the
    read loop stays blocked in `receive()`, `serve()` does not return, and the supervisor is never
    notified of the loss: no backoff, no re-dial, and the peer reports itself closed.

    The test uses a real server that answers nothing, with the heartbeat enabled. The detection
    budget is `ping_interval + ping_timeout`; no TCP timeout is involved.
    """
    connections: list[Any] = []
    stop = asyncio.Event()

    async def swallow_everything(connection: Any) -> None:
        # No muxws peer on this side: `Peer.serve()` echoes a `ping` verbatim (WSM-CON-010), and the
        # heartbeat does not time out against a server that answers.
        connections.append(connection)
        await stop.wait()

    async with websockets.serve(
        swallow_everything, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol
    ) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        closes: list[Any] = []
        peer = await muxws.connect(
            url,
            reconnect=Reconnect(initial_delay=0.01, jitter=0.0),
            ping_interval=0.05,
            ping_timeout=0.05,
        )
        peer.on_close(closes.append)
        try:
            await asyncio.wait_for(_until(lambda: len(connections) >= 2), 5.0)

            assert closes, "the swallowed pong must be reported as a socket loss (WSM-RCN-040)"
            assert closes[0].reason == "no pong within 0.05s"
            assert closes[0].will_retry is True
            assert peer._connection_loop is not None
            assert peer._connection_loop.delays[:1] == [0.01], "and it backed off before dialling again"
        finally:
            stop.set()
            await peer.close()


async def test_an_abandoned_socket_is_closed_with_a_code_a_peer_may_send():
    """WSM-RCN-026: the close made by a failed attempt also uses a code a peer is allowed to send.

    The driver closes a socket itself in two places: the heartbeat (above) and a failed attempt,
    which is also on the path `connect()` takes. A failed attempt cancels the read loop, so a close
    the adapter refused would leave the TCP connection open, one per unanswered hello, for as long
    as the helper retries.

    The test asserts the close code the server received from a socket that closed.
    `MemorySocket.close` discards the code and both ends stop regardless, so the in-memory transport
    does not detect this.
    """
    server_side: list[Any] = []

    async def never_acknowledge(connection: Any) -> None:
        server_side.append(connection)
        acceptor = await muxws.accept(WebsocketsSocket(connection))
        # A handler that never returns never ends its stream, so the hello is never acknowledged
        # (WSM-RCN-022) and the attempt fails on its deadline.
        acceptor.on_stream(lambda _payload, _stream: asyncio.Event().wait())
        await acceptor.serve()

    async with websockets.serve(
        never_acknowledge, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol
    ) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        with pytest.raises(StreamTimeout):
            await muxws.connect(url, hello={"tab": "abc"}, hello_timeout=0.05, ping_interval=0.0)

        assert len(server_side) == 1, "the dial itself succeeded; it is the hello that did not"
        await asyncio.wait_for(_until(lambda: server_side[0].close_code is not None), 2.0)
        assert server_side[0].close_code == 1000, "1006 is a code a peer may not send, so it never arrives"


async def test_a_deliberate_close_stops_the_driver_dialling(dialable_server: DialableServer):
    """WSM-RCN-040/044: `close()` stops the reconnect loop, so no dial follows it."""
    socket = await dialable_server.dial()
    peer = Peer(socket, codec=dialable_server.codec, is_dialer=True)
    loop = ConnectionLoop(
        peer,
        dialable_server.dial,
        options=Reconnect(initial_delay=0.01, jitter=0.0),
        hello=Hello(),
        ping_interval=0.0,
    )
    await loop.establish()
    loop.start()
    peer._connection_loop = loop

    await peer.close()
    dials = dialable_server.dials
    await asyncio.sleep(0.1)
    assert dialable_server.dials == dials, "a deliberate close never dials again"


async def test_a_deliberate_close_between_sockets_stops_the_driver(dialable_server: DialableServer):
    """WSM-RCN-040/044: `close()` between two sockets also stops the reconnect loop.

    Between two sockets `is_open` is already false and the helper is sleeping in a backoff that only
    the helper can cancel. A `close()` that returned early because the peer is not open would let the
    driver dial again and re-establish a connection on the same `Peer` object the application closed.
    """
    peer, loop = await dialable_server.driver(options=Reconnect(initial_delay=0.02, jitter=0.0))
    peer._connection_loop = loop
    await loop.establish()
    loop.start()

    dialable_server.refuse_next(200)
    await dialable_server.drop()
    await asyncio.wait_for(_until(lambda: dialable_server.dials >= 3), 5.0)
    assert peer.is_open is False, "the gap is where this test has to happen"

    await peer.close()
    dials = dialable_server.dials
    # Longer than several of the following backoffs, so the absence of a dial shows that the driver
    # stopped.
    await asyncio.sleep(0.3)

    assert dialable_server.dials == dials, "a close in the gap never dials again either"
    assert loop._stopped is True


async def test_a_stream_reset_inside_the_hello_window_still_reaches_the_wire(dialable_server: DialableServer):
    """WSM-STM-021 and WSM-RCN-043: a `reset` is sent during the hello window.

    `is_open` is false during the hello window (WSM-RCN-043), so the application cannot start
    anything new. The socket is open, and a stream the acceptor pushed during the window that this
    side terminates is still reset on the wire. A dropped `reset` would leave the remote holding a
    stream this peer has closed until the connection ends.
    """
    pushed = asyncio.Event()
    release = asyncio.Event()

    async def push_then_hold_the_hello(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        if len(dialable_server.received) > 1:
            # Opened while the dialer's hello is unacknowledged, so the dialer answers it inside the
            # window.
            dialable_server.acceptors[-1].open({"push": "during the hello"})
            await release.wait()

    async def refuse_it(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reset(ResetCode.CANCELLED, "not now")
        pushed.set()

    dialable_server.handler = push_then_hold_the_hello
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}, timeout=5.0))
    peer.on_stream(refuse_it)
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(pushed.wait(), 5.0)
        # Bounded, and the timeout is suppressed so the assertion below reports the wire contents.
        # `_until` alone would hang when the reset is dropped.
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(_until(lambda: any(f.type == "reset" for f in dialable_server.frames(1))), 2.0)

        resets = [frame for frame in dialable_server.frames(1) if frame.type == "reset"]
        assert [frame.code for frame in resets] == [int(ResetCode.CANCELLED)], (
            "the reset never reached the wire; the remote still believes that stream is live"
        )
    finally:
        release.set()
        await loop.stop()


async def test_every_reconnect_presents_the_same_credential_at_a_fresh_upgrade():
    """WSM-AUT-003, library part: re-authentication happens only by re-dialling.

    The other part of the rule applies to the deploying application and is not testable here; muxws
    does not interpret credentials. The dial callable closes over the headers it was given, so every
    attempt presents the same credential in a new HTTP upgrade, and there is no in-band
    re-authentication path.

    The headers are asserted at the HTTP upgrade, where credentials are sent; frames do not show
    whether a header was sent.
    """
    seen: list[str | None] = []

    async def handler(connection: Any) -> None:
        seen.append(connection.request.headers.get("Authorization"))
        await connection.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as server:
        port = server.sockets[0].getsockname()[1]
        dial = muxws.api._websocket_dialer(
            f"ws://127.0.0.1:{port}",
            muxws.resolve_codec(),
            headers={"Authorization": "Bearer the-one-token"},
            subprotocols=None,
        )
        sockets = [await dial() for _ in range(3)]
        await _until(lambda: len(seen) == 3)
        for socket in sockets:
            await socket.close()

    assert seen == ["Bearer the-one-token"] * 3, (
        f"a reconnect presented a different credential, or none: {seen}. The dial closure is the only "
        f"place a credential enters, and every attempt must go through it (WSM-AUT-003)"
    )


async def test_header_interceptors_survive_a_reconnect_and_leave_the_hello_alone(dialable_server: DialableServer):
    """WSM-API-026/027: registrations belong to the `Peer`, and open interceptors do not run on the hello.

    The hellos are compared as raw messages, as in WSM-RCN-027's test: an interceptor applied to the
    hello would make the replay differ from the first one.
    """
    answered: list[dict[str, Any]] = []

    async def answer(_payload: Any, stream: Stream) -> None:
        await stream.send_headers({"served-by": len(dialable_server.acceptors)})

    dialable_server.handler = answer
    again = asyncio.Event()
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}))
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    peer.intercept_open(lambda headers, _payload: headers.update(conn="tab-abc"))
    peer.intercept_incoming_headers(lambda headers, _stream: answered.append(dict(headers)))
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)
        stream = peer.open({"after": "reconnect"}, end=True)
        await stream.closed.wait()

        assert stream.headers == {"conn": "tab-abc"}
        assert dialable_server.frames(1)[1].headers == {"conn": "tab-abc"}
        assert dialable_server.raw(1)[0] == dialable_server.raw(0)[0], "the hello replays byte for byte"
        assert dialable_server.frames(1)[0].headers is None
        assert answered[-1] == {"served-by": 2}
    finally:
        await loop.stop()


async def test_close_during_a_dial_never_reconnects(dialable_server: DialableServer):
    """WSM-RCN-040: a dial still in flight when `close()` runs does not bring the connection back."""
    held = asyncio.Event()
    dialling = asyncio.Event()

    async def slow_dial() -> Any:
        dialling.set()
        await held.wait()
        return await dialable_server.dial()

    peer, loop = await dialable_server.driver()
    loop._dial = slow_dial
    peer._connection_loop = loop
    reconnects: list[int] = []
    peer.on_reconnect(lambda attempt, _peer: reconnects.append(attempt))
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(dialling.wait(), 5.0)
        await peer.close(drain=0.1)
        held.set()
        await asyncio.sleep(0.05)

        assert not peer.is_open
        assert reconnects == [], "a connection closed by the application is not re-established"
        assert len(dialable_server.sockets) == 1, "the dial was cancelled before it produced a socket"
    finally:
        await loop.stop()


async def test_stop_during_the_hello_establishes_nothing(dialable_server: DialableServer):
    """WSM-RCN-040: a hello still outstanding when `stop()` runs leaves no connection behind."""
    answer = asyncio.Event()
    hellos: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        hellos.append(payload)
        if len(hellos) > 1:
            await answer.wait()
        await stream.reply("ack")

    dialable_server.handler = handler
    peer, loop = await dialable_server.driver(hello=Hello(payload={"session": "abc"}))
    reconnects: list[int] = []
    peer.on_reconnect(lambda attempt, _peer: reconnects.append(attempt))
    await loop.establish()
    loop.start()
    try:
        await dialable_server.drop()
        await asyncio.wait_for(_until(lambda: len(hellos) == 2), 5.0)
        await loop.stop()
        answer.set()
        await asyncio.sleep(0.05)

        assert not peer.is_open
        assert reconnects == [], "a stopped helper does not report a reconnection"
        assert dialable_server.sockets[1].is_closed, "the half-established socket is closed"
    finally:
        await loop.stop()
