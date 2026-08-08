"""The reconnect helper's schedule, counter and hello (§7)."""

from __future__ import annotations

import inspect

from muxws.reconnect import (
    AttemptCounter,
    backoff_delay,
    Hello,
    Reconnect,
    should_retry,
    unjittered_delay,
)

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
    import pytest

    with pytest.raises(ValueError, match="initial_delay"):
        Reconnect(initial_delay=0)
    with pytest.raises(ValueError, match="factor"):
        Reconnect(factor=0.5)
    with pytest.raises(ValueError, match="jitter"):
        Reconnect(jitter=1.5)


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


def test_the_helper_is_dialer_only():
    """§7: an acceptor cannot dial and MUST NOT have one."""
    from muxws import reconnect as module

    assert "Dialer only" in inspect.getdoc(module)
