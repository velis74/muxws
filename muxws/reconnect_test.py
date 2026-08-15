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
    """Poll until `condition` holds. The driver's progress is observable only through what it did."""
    while not condition():
        await asyncio.sleep(0.005)


class _EnoughBeatsError(Exception):
    """Ends an injected sleep. A heartbeat runs until the socket dies, so a test has to stop it."""


# --------------------------------------------------------------------------- the schedule


def test_schedule_before_jitter_and_cap():
    """WSM-RCN-003 **(spec)**: a pure function of (attempts, options), tested as one.

    The defaults double from 0.25 s and stop at 30 s. No clock, no sleeping, no sampling - the whole
    point of computing the delay separately from waiting it out.
    """
    options = Reconnect()
    schedule = [unjittered_delay(attempt, options) for attempt in range(12)]
    assert schedule == [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0, 30.0, 30.0]


def test_jitter_is_applied_to_every_delay_including_the_capped_ones():
    """WSM-RCN-002: including the capped ones - which is where a herd would otherwise re-form."""
    options = Reconnect()
    for attempt in (0, 3, 9):
        base = unjittered_delay(attempt, options)
        assert backoff_delay(attempt, options, draw=lambda: 1.0) == base * 1.3
        assert backoff_delay(attempt, options, draw=lambda: -1.0) == base * 0.7
        assert backoff_delay(attempt, options, draw=lambda: 0.0) == base


def test_jitter_disperses_n_simultaneous_reconnects():
    """WSM-RCN-002 **(spec)**: N peers dying at the same instant retry at different instants.

    Without this, a server coming back up is knocked over by the reconnection rather than the load.
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
    # A negative attempt count is not a small delay, it is a caller that lost track of its counter -
    # `factor ** -1` would quietly hand it back a delay shorter than `initial_delay`.
    with pytest.raises(ValueError, match="negative"):
        backoff_delay(-1, Reconnect())


# --------------------------------------------------------------------------- the attempt counter


def test_the_counter_is_the_helpers_entire_persistent_state():
    """WSM-RCN-001: everything else is computed from it."""
    counter = AttemptCounter()
    assert counter.value == 0
    assert [counter.failed() for _ in range(3)] == [1, 2, 3]
    counter.established()
    assert counter.value == 0
    assert list(vars(AttemptCounter).get("__slots__", ())) == ["_attempts"]


def test_counter_resets_only_on_established():
    """WSM-RCN-004/WSM-INV-012: socket-open alone must not reset it.

    A server that accepts sockets while its backend is down turns exponential backoff into a
    fixed-interval hammer if this is got wrong, and nothing about the peer looks broken.
    """
    options = Reconnect()
    counter = AttemptCounter()

    # A server that accepts and then drops before the hello completes: the counter keeps climbing,
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
    """WSM-RCN-044: after which it never dials again."""
    unlimited = Reconnect()
    assert should_retry(10_000, unlimited) is True

    bounded = Reconnect(max_attempts=3)
    assert [should_retry(attempt, bounded) for attempt in range(5)] == [True, True, True, False, False]


# --------------------------------------------------------------------------- the hello


def test_a_peer_given_no_hello_sends_none():
    """WSM-RCN-024: and is established as soon as the socket is (WSM-CON-030)."""
    assert Hello().configured is False
    assert Hello(payload={"tab": "abc"}).configured is True
    assert Hello(headers={"trace": "x"}).configured is True


def test_the_hello_is_captured_once_and_is_frozen():
    """WSM-RCN-020: never re-read, recomputed, or supplied as a callback."""
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
    """WSM-RCN-005: `secrets` MUST NOT be used here, and the noqa carries the reason."""
    from muxws import reconnect as module

    source = inspect.getsource(module)
    assert "import secrets" not in source
    assert "secrets." not in source
    assert "noqa: S311" in source
    assert "not security-sensitive" in source


async def test_acceptor_has_no_reconnect_helper(dialable_server: DialableServer):
    """§3/§7: an acceptor cannot dial, and one that tried would dial the client that dialled it.

    Refused at construction rather than at the first reconnect: a helper that only failed when the
    socket died would look correct for the whole life of every connection that never dropped.
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

    Two attempts get a socket and lose it with the hello outstanding. Neither is *established*
    (WSM-RCN-004), so the counter climbs and the delays grow - a helper that reset the counter when
    `dial()` returned would produce `[0.01, 0.01, 0.01]` here and hammer the server at a fixed
    interval forever (WSM-INV-012). `on_reconnect` fires once, for the attempt that was acknowledged
    (WSM-RCN-026/030).
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
    """WSM-RCN-040: `on_close` fires per **socket loss**; a refused dial lost nothing."""
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
    """WSM-RCN-011: one code path for a dead socket, never two.

    The same peer loses its socket twice - once because the server dropped it, once because nobody
    answered a ping - and the two losses are indistinguishable downstream: one `on_close` each, one
    delay each drawn from a counter that reset in between, one `on_reconnect` each. A second code
    path for the swallowed pong is how a dead socket ends up not backing off at all, or backing off
    from a counter the clean close never touched.
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

        # A dead one: the server stops reading without closing, so only the heartbeat can notice.
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
    """WSM-RCN-011 **(spec)**: bounded by `ping_interval + ping_timeout`, and by nothing else.

    The server stops reading without closing. The socket is up, the operating system is content, and
    every frame the dialer sends is swallowed - a transport-level timeout would take minutes to
    notice, if it ever did. Nothing here waits on one: the whole detection budget is two configured
    numbers adding up to a tenth of a second, which is the entire reason a `ping` frame exists when
    the WebSocket layer already has one browsers will not expose (WSM-CON-011).
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
    """WSM-RCN-010/§6: idle means idle - a busy socket must not pay for a ping.

    Against an injected clock and an injected sleep, so no wall time passes and nothing depends on
    how fast the machine is. Every sleep the heartbeat asks for is answered by a frame crossing the
    socket, as a busy connection would answer it, and that frame goes through `_report_frame` - the
    real stamping path, in both directions - rather than by writing `last_activity` behind its back.

    The second half is the control. Without it, a heartbeat that never ran at all, or one whose
    `interval` was quietly ignored, would pass the first half perfectly.

    Each run is bounded, because the way this rule is broken is a heartbeat that never sleeps at all:
    it does not fail an assertion, it pings forever, and an unbounded test would hang rather than
    report anything.
    """
    peer = pair.dialer
    now = [0.0]
    peer._clock = lambda: now[0]
    beats = 0

    async def busy_sleep(delay: float) -> None:
        nonlocal beats
        beats += 1
        now[0] += delay
        # Alternating, because a stamp on one direction only would leave a socket that is being
        # talked *at* looking idle, and it would get a ping every interval for its trouble.
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
    """Run a heartbeat until its injected sleep stops it, and report a heartbeat that never sleeps."""
    beat = Heartbeat(peer, interval=1.0, timeout=1.0, sleep=sleep)
    try:
        await asyncio.wait_for(beat.run(), 5.0)
    except _EnoughBeatsError:
        return
    except asyncio.TimeoutError:  # pragma: no cover - the failure this test exists to report
        pytest.fail("the heartbeat never asked to sleep: it pinged in a loop instead of waiting out the interval")
    pytest.fail("the heartbeat returned on its own; only its injected sleep may end it")


async def test_max_attempts_exhausted_fires_on_close_once_and_never_dials_again(dialable_server: DialableServer):
    """WSM-RCN-044: once, with `will_retry` false, and then never again.

    The loss itself already fired `on_close` promising a retry, because one was coming. Exhausting
    the cap withdraws that promise exactly once: an application told a reconnection was on its way
    and never told otherwise waits forever for one nobody is attempting.
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
    """WSM-RCN-044: a cap of zero is a peer that never retries, and it must still say so.

    The dial decision is `should_retry(counter, options)` and nothing else. Keying it on
    `peer._will_retry` - a field on the peer that anything at all can write - would make the rule
    something a stray assignment turns off, and would make a cap of zero return in silence.
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
    """WSM-RCN-044/WSM-RCN-001: `should_retry` decides; `_will_retry` only *reports*.

    `_will_retry` is peer state, writable by anything holding the peer - the tests in
    `socket_death_test.py` write it, and so could an application. A helper that read it back as its
    own stop condition would let one stray assignment silently retire the reconnect loop, with
    nothing anywhere reporting that the peer had stopped trying. The attempt counter is the helper's
    entire persistent state (WSM-RCN-001), and the cap is read off it.
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
    """WSM-RCN-044: at most one `will_retry=False` close per peer, ever.

    When the cap runs out on *refused dials* the last loss promised a retry, and the helper's
    withdrawal is the only thing that ever says otherwise. When it runs out on failed *hellos* the
    last loss already knew - the helper set `_will_retry` false before adopting that socket, because
    it knew it was the last one - so the withdrawal would say the same thing a second time. An
    application whose `on_close` tears down on `will_retry=False` would tear down twice.
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
    """An application callback runs on the supervisor's own task; it must not be able to end it.

    `on_reconnect` fires from inside the reconnect loop. A handler that raised - an application's
    metrics call, a logger with a bad format string - would unwind straight into the supervisor,
    which would never dial again for the life of the peer, with nothing anywhere reporting why. The
    same argument makes one bad handler cost the others nothing: they are unrelated applications of
    the same event.
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
    """WSM-RCN-001-adjacent: `delays` is instrumentation on an object that outlives its connections.

    With no attempt cap against a server that never comes back, an unbounded list grows one float per
    retry for the life of the process. It keeps the most recent `MAX_RECORDED_DELAYS`, which is what
    every reader of it wants anyway.
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

    The comparison is on the **encoded messages**, not on decoded objects: a mutation the encoder
    smooths over - a key reordered, a tuple that came back a list - compares equal decoded and is
    still a different hello on the wire, which is where the acceptor reads it.

    `on_reconnect` fires after each acknowledgement and never before, which is what the interleaved
    order below records: every `reconnect:n` has a `hello` immediately in front of it.

    The last third is WSM-INV-013: the hello is replayed by the *helper*, so an application that
    registered no `on_reconnect` handler at all - one that does nothing on a reconnect because it
    was never told there was one - still ends up with a peer the server can find in its registry.
    An application that had to replay it itself would, on forgetting, hold a socket that looks
    healthy, is subscribed to nothing, and reports no error.
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
            # The old peer's index entries go when its close hook runs, which is a turn or two after
            # the new connection is up (WSM-REG-016).
            await asyncio.wait_for(_until(lambda: registry.peers_for(tab="quiet") == [silent.acceptors[-1]]), 5.0)

            assert silent.raw(1)[0] == silent.raw(0)[0], "replayed without being asked"
            assert registry.peers_for(tab="quiet") == [silent.acceptors[-1]], "and findable again"
        finally:
            await quiet_loop.stop()
    finally:
        await silent.aclose()


async def test_reset_hello_and_timed_out_hello_both_back_off(dialable_server: DialableServer):
    """WSM-RCN-026 **(spec)**: a failed hello is a failed *attempt*, not a connection.

    The two ways a hello fails - reset, and never answered - take one path, because a server whose
    backend is down answers one way today and the other tomorrow. Either way the socket is closed,
    the counter climbs, the next attempt waits longer, and `on_reconnect` does not fire: the acceptor
    never acknowledged this peer, so there is no identity to announce.
    """
    answered: list[Any] = []

    async def answer(payload: Any, stream: Stream) -> None:
        answered.append(payload)
        if len(answered) == 2:
            await stream.reset(ResetCode.REFUSED, "the backend is down")
        elif len(answered) == 3:
            # Never acknowledged. Only the deadline can end this attempt.
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
    """WSM-RCN-021/022: the acceptor's own handler sees it, and nothing marks it on the wire.

    The proof that nothing marks it is a byte comparison against an ordinary application `open()`
    carrying the same payload and headers on the same socket: the two messages differ in the stream
    id and in nothing else. A flag, a reserved header, a distinguished frame type or an extra
    envelope key would all show up here as a difference.

    The acknowledgement is the handler *returning* (WSM-RCN-022/WSM-STM-035). The handler below
    sends nothing at all, and that is enough.
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
    """WSM-RCN-023: before any application frame, and before `on_reconnect`.

    The earliest an application can act on a new socket is inside `on_reconnect`, so that is where
    this one pushes. Even then the hello is already on the wire and already acknowledged: that is
    what makes the identity one the acceptor has *already* accepted rather than one it is about to
    be told about, and it is why the ordering is a rule rather than a nicety.
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

    The window between a new socket and its acknowledged hello is the one place this can go wrong
    quietly: the socket is up, so a peer that flipped `is_open` at socket-open accepts `open()`,
    `notify()` and `request()`, and their frames go out **in front of** the hello - reaching an
    acceptor that has not yet been told who is speaking, which is the whole thing WSM-RCN-023 is
    about. The hello itself goes through the allocate-and-enqueue body rather than `open()`, so the
    guard closing over the window does not close over the hello too.
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
    """WSM-RCN-006: `connect()` raises the **underlying** error, not a close wrapped round it.

    An acceptor that refuses the hello and a socket that died are two different faults with two
    different remedies, and a caller that got `ConnectionClosed` for both cannot tell them apart.
    """
    dialable_server.on_hello = "reset"
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}))

    with pytest.raises(StreamRefused, match="refused"):
        await loop.establish()
    assert peer.is_open is False


async def test_no_hello_means_established_at_subprotocol_accept(dialable_server: DialableServer):
    """WSM-RCN-024/WSM-CON-030: a peer given no hello sends none, and is established at once.

    There is no `settings` exchange to wait for, so for such a peer "established" really is
    socket-open with the subprotocol accepted. The snapshot is taken *inside* `on_reconnect`,
    because a moment later the application's own traffic would make an empty wire unprovable.
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
    """WSM-RCN-020: captured once, by value, and never re-read.

    Three ways the application can reach the hello after `connect()` - the payload dict it passed,
    a container nested inside it, and the copy a previous replay handed the codec - and none of them
    changes what the next connection sends. "Byte-identical on every replay" has to be true by
    construction; hoping the application never mutates its own dict is not a mechanism.
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

    Two ways a first attempt fails, and both raise rather than retry: nothing answers the dial, and
    something answers it that never acknowledges the hello. Reconnection applies to connections that
    were established and then lost; a peer handed back retrying in the background turns a typo in the
    URL into silence, and there is deliberately no option that changes it.

    Port 1 is pinned rather than picked: an ephemeral port whose number happens to contain "400"
    would be mistaken for a refused upgrade and reported as a codec mismatch.
    """
    started = asyncio.get_running_loop().time()
    with pytest.raises(OSError, match=".") as refused:
        await asyncio.wait_for(muxws.connect("ws://127.0.0.1:1/", reconnect=Reconnect()), 5.0)

    assert not isinstance(refused.value, asyncio.TimeoutError), "it must raise, not retry until the deadline"
    assert asyncio.get_running_loop().time() - started < 2.0, "and raise the underlying error at once"
    assert "retry_initial" not in inspect.signature(muxws.connect).parameters, "no option changes this"

    # The dial succeeded and the connection still was not established: the same rule applies.
    dialable_server.on_hello = "hang"
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}, timeout=0.05))
    closes: list[Any] = []
    peer.on_close(closes.append)

    # The **underlying** error, not a close wrapped round it: WSM-RCN-006 says "with the underlying
    # error", and a caller told `ConnectionClosed` would read a deadline nobody answered as a socket
    # that died - two different faults with two different remedies.
    with pytest.raises(StreamTimeout, match="not acknowledged"):
        await loop.establish()

    assert dialable_server.sockets[0].is_closed, "the socket it gave up on is closed, not left open"
    assert closes == [], "nothing was established, so nothing was lost and nothing is reported"
    # Dead, and deterministically so: `connect()` raising is the report (WSM-RCN-006), but leaving
    # `is_open` to a race between the read loop noticing the abandoned socket and the cancel that
    # tears it down would hand a caller who caught the error a peer that still says it is open. The
    # *cause* is what tells the two apart - the helper's own, not whatever the transport happened to
    # report on the way out - which is why it is asserted rather than only the flag.
    assert peer.is_open is False, "a first connection that never established is dead, not pending"
    assert peer._death is not None
    assert peer._death.reason == "the first connection was never established", "the helper marked it, not the race"
    await asyncio.sleep(0.05)
    assert dialable_server.dials == 1, "and nothing dialled again behind the caller"


async def test_close_reason_shape_is_identical_in_both_languages(dialable_server: DialableServer):
    """WSM-RCN-045: exactly four fields, the same four, one type per language, for every loss.

    The TypeScript half is read out of its source rather than restated here: a field added to one
    port and not the other is precisely what this test is for, and a hand-copied list would drift
    along with it.
    """
    python_fields = [field.name for field in dataclasses.fields(CloseReason)]
    assert python_fields == ["code", "reason", "was_clean", "will_retry"]

    source = (pathlib.Path(__file__).resolve().parents[1] / "ts" / "observability.ts").read_text(encoding="utf-8")
    declaration = re.search(r"export interface CloseReason \{(.*?)\n\}", source, re.DOTALL)
    assert declaration is not None, "the TypeScript port must declare the interface"
    assert re.findall(r"readonly (\w+):", declaration.group(1)) == [_camel(name) for name in python_fields]

    # One type for every socket loss, not one per reason: the drop, and the exhaustion that follows.
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


# --------------------------------------------------------------------------- the seam itself


async def test_connect_wires_the_driver_to_a_real_socket():
    """`connect()` builds the driver, or nothing in the library reconnects at all.

    A `ConnectionLoop` with perfect unit tests that `connect()` never constructs reconnects nothing,
    so this goes through the real public entry point against a real `websockets` server:
    the connection is killed from the server side, the peer dials again on its own, replays the
    hello it captured at `connect()` (WSM-RCN-020) and fires `on_reconnect` after the acknowledgement
    and not before (WSM-RCN-030).
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

    # `close()` can only reach the driver through the reference `connect()` hands the peer, and only
    # the driver can stop a dial that is already scheduled. Both halves are asserted here because
    # neither shows up as a failing reconnection: a peer whose loop was never handed over reconnects
    # perfectly right up until the application closes it, and then goes on dialling.
    assert peer._connection_loop is not None, "connect() must hold the driver it built"
    assert peer._connection_loop._stopped is True, "and close() must have stopped it"


async def test_connect_registers_its_handlers_before_the_hello_goes_out():
    """WSM-STM-033/WSM-RCN-023: the acceptor may answer the hello with a stream of its own.

    An acceptor that pushes the instant it sees the hello - a session's backlog, a subscription
    confirmation - is pushing before `connect()` has returned, so an application that registers
    `on_stream` on the peer it gets back is one await too late and its own server's first push comes
    home as `reset(REFUSED, "no on_stream handler")`. The handler cannot be registered by the caller
    at all; it has to be a parameter, and `connect()` has to register it before `establish()` runs.
    """
    pushed: list[Any] = []
    connections: list[Any] = []

    async def handle(connection: Any) -> None:
        connections.append(connection)
        acceptor = await muxws.accept(WebsocketsSocket(connection))

        async def push_at_once(payload: Any, _stream: Stream) -> None:
            # Before returning, so the push is on the wire in front of the acknowledgement: the
            # dialer is still inside `establish()` when it arrives.
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


async def test_a_swallowed_pong_on_a_real_socket_re_dials():
    """WSM-RCN-011: the heartbeat's local close must be a code a peer is allowed to send.

    The in-memory rig cannot catch this: `MemorySocket.close` throws the code away. A real
    `websockets` connection validates it, and 1006 is reserved for "the connection dropped without a
    close frame", so sending it raises. An exception swallowed there leaves the socket open, the
    read loop parked inside `receive()`, `serve()` never returning, and the supervisor waiting for a
    loss it was never told about: no backoff, no re-dial, and a peer that reports itself closed
    forever.

    Hence a **real** server with the heartbeat **enabled**, answering nothing. Nothing here waits on
    a TCP timeout: the whole detection budget is `ping_interval + ping_timeout`.
    """
    connections: list[Any] = []
    stop = asyncio.Event()

    async def swallow_everything(connection: Any) -> None:
        # No muxws peer on this side, deliberately: `Peer.serve()` echoes a `ping` verbatim
        # (WSM-CON-010), and a server that answers is the one case the heartbeat cannot detect.
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
    """WSM-RCN-026: the *other* local close - the one a failed attempt makes - has the same rule.

    The heartbeat's close above is one of two places the driver ends a socket itself; this is the
    other, and it is on the path `connect()` itself takes. It has the same defect available to it and
    a different consequence: the failed attempt is not waiting on the read loop - it cancels it - so
    a close the adapter refused costs no re-dial, it leaks. Every hello that goes unanswered against
    a real server would leave a live TCP connection behind, one per attempt, for as long as the
    helper keeps trying.

    Hence the assertion on the **server's** view: the code it received, from a socket that actually
    closed. The memory rig cannot see either half - `MemorySocket.close` discards the code and both
    ends stop regardless.
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
    """WSM-RCN-040/044: `close()` reaches the loop, so nothing dials again behind the caller."""
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
    """WSM-RCN-040/044: and in the gap, where `close()` is the only thing that can.

    The window between two sockets is the one moment `close()` has nothing to close: `is_open` is
    already false, and the helper is asleep in a backoff nobody but the helper can cancel. A `close()`
    that took the peer's own state as the whole story and returned early here would stop nothing -
    the driver would dial on, re-establish, and hand the application back a live connection it had
    already given up. On the same `Peer` object, so it would never think to close it twice.
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
    # Longer than several of the backoffs still to come, so "it never dialled again" is a statement
    # about the driver having stopped rather than about the test having been quick.
    await asyncio.sleep(0.3)

    assert dialable_server.dials == dials, "a close in the gap never dials again either"
    assert loop._stopped is True


async def test_a_stream_reset_inside_the_hello_window_still_reaches_the_wire(dialable_server: DialableServer):
    """WSM-STM-021 against WSM-RCN-043: two different questions about one socket.

    Closing `is_open` over the hello window (WSM-RCN-043) is right for the application - nothing new
    may start there. It is wrong for a `reset` already owed to the remote: the socket is alive, the
    hello is travelling on it, and a stream the acceptor pushed inside the window that this side
    terminates must say so. A `reset` dropped here leaves the remote holding a stream this peer has
    already closed, with nothing to tell it otherwise until the connection ends.
    """
    pushed = asyncio.Event()
    release = asyncio.Event()

    async def push_then_hold_the_hello(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        if len(dialable_server.received) > 1:
            # Opened while our own hello is still unacknowledged, so the dialer answers it from
            # inside the window.
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
        # Bounded deliberately, and the timeout is swallowed so the assertion below reports what was
        # actually on the wire. `_until` alone would hang here when the reset is dropped, and a test
        # that hangs on the defect it exists to catch reads in CI as an anonymous timeout.
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
    """WSM-AUT-003, the library half: re-authentication is re-dialling, and nothing else.

    The rule's other half binds the deploying application and cannot be tested - muxws does not know
    what a credential is. What *is* testable is the claim the rule makes about this library: the dial
    callable closes over the headers it was given, so every attempt presents the same credential at a
    **fresh HTTP upgrade**, and there is no second, in-band path by which a reconnecting peer could
    re-authenticate.

    Asserted at the upgrade rather than through a peer, because that is where a credential travels: a
    test that watched frames could not tell a header that was sent from one that was dropped.
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
