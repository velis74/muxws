"""Random legal API sequences against a live pair, asserting the invariants that hold for all of them.

The rest of the suite enumerates: `stream_test.py` walks all forty-five cells of the state table one
at a time, `conformance/sequences/` scripts thirteen written-down exchanges. Neither reaches an
ordering nobody wrote down, and a sequence can be legal in every step and wrong as a whole.

So this drives *random* sequences of ordinary calls and asserts only what must be true of **any** of
them. It cannot say which rule broke, only that something did, so it stands beside the enumerated
tests rather than replacing them.

Seeds are fixed and listed, so a failure is reproducible: the seed that failed is in the assertion
message, and adding it to `SEEDS` turns a chance discovery into a permanent regression test.
"""

from __future__ import annotations

import asyncio
import random

from typing import Any

import pytest

from muxws.errors import ConnectionLost, MuxwsError, ResetCode
from muxws.frames import Frame
from muxws.stream import Stream, StreamState

#: Fixed, not drawn: a suite whose inputs change per run reports failures nobody can reproduce.
SEEDS = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89]

#: Long enough that a stream can be opened, written on, ended and raced against another; short enough
#: that ten of them run in the time an ordinary test takes.
STEPS = 40


class Recorder:
    """What actually crossed the wire, and what the application saw."""

    def __init__(self) -> None:
        self.sent: list[Frame] = []
        self.failures: list[BaseException] = []

    def frame(self, direction: str, frame: Frame, _length: int) -> None:
        if direction == "tx":
            self.sent.append(frame)


async def _drive(seed: int, pair: Any) -> Recorder:
    """One random sequence. Every call here is one an application is allowed to make."""
    rng = random.Random(seed)  # noqa: S311 - exploring orderings, not generating secrets
    recorder = Recorder()
    pair.dialer.on_frame(recorder.frame)

    async def handler(payload: Any, stream: Stream) -> None:
        # A handler that does something, so streams are not all trivially one-sided.
        if isinstance(payload, dict) and payload.get("reply"):
            await stream.reply({"echo": payload})

    pair.acceptor.on_stream(handler)
    pair.start()

    live: list[Stream] = []
    for _ in range(STEPS):
        choice = rng.randrange(6)
        try:
            if choice == 0 or not live:
                live.append(pair.dialer.open({"reply": rng.random() < 0.5, "n": rng.randrange(100)}))
            elif choice == 1:
                await rng.choice(live).send({"n": rng.randrange(100)})
            elif choice == 2:
                stream = rng.choice(live)
                await stream.end()
            elif choice == 3:
                stream = rng.choice(live)
                await stream.cancel("the caller changed its mind")
            elif choice == 4:
                stream = rng.choice(live)
                # Occasionally ask for the one code an application may not send. The library must
                # refuse it (§8.1) - `CONNECTION_CLOSED` means "the socket under this stream died",
                # which is both false when a caller says it and unfalsifiable by the remote. This is
                # the only path by which code 9 can reach the wire at all: socket death synthesises
                # it locally and never enqueues a frame, so without this call the invariant below is
                # asserting against a sequence that could not produce a violation.
                code = ResetCode.CONNECTION_CLOSED if rng.random() < 0.25 else ResetCode.APPLICATION_ERROR
                await stream.reset(code, "a handler said no")
            else:
                await pair.settle(rounds=2)
        except Exception as failure:  # noqa: BLE001 - what came out is the measurement
            # Deliberately everything. Calling at the wrong moment has several documented outcomes -
            # `StreamClosed` for a second `end()`, that stream's own `StreamReset`, `ConnectionLost`,
            # `ConnectionGoingAway` - and enumerating them here would make the first invariant assert
            # only what this line had already filtered for. Recording whatever escaped and asserting
            # afterwards that it was a muxws error is the same test with teeth.
            recorder.failures.append(failure)
        live = [stream for stream in live if stream.state is not StreamState.CLOSED]

    await pair.settle(rounds=40)
    return recorder


@pytest.mark.parametrize("seed", SEEDS)
async def test_a_random_legal_sequence_upholds_every_invariant(seed: int, make_pair):
    """Whatever the ordering, these five hold. Each names the failure it would have caught."""
    pair = make_pair()
    try:
        recorder = await asyncio.wait_for(_drive(seed, pair), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail(f"seed {seed}: the sequence hung. A call blocked forever rather than failing (WSM-INV-011)")

    # 1. Nothing escapes that is not a muxws error. An AttributeError or a KeyError out of an
    #    ordinary call is the library breaking, not the caller.
    stray = [f for f in recorder.failures if not isinstance(f, MuxwsError)]
    assert stray == [], f"seed {seed}: a call raised something that is not a muxws error: {stray!r}"

    # 2. Reset code 9 is synthesised locally when the socket dies and MUST NEVER appear on the wire.
    on_wire = [f for f in recorder.sent if f.type == "reset" and f.code == int(ResetCode.CONNECTION_CLOSED)]
    assert on_wire == [], f"seed {seed}: CONNECTION_CLOSED reached the wire (§8.1)"

    # 3. Opens are monotonic and this peer's parity, however the calls interleaved (WSM-SID-002/004).
    opens = [f.stream for f in recorder.sent if f.type == "open" and f.fragment is None]
    assert opens == sorted(set(opens)), f"seed {seed}: opens were not monotonic: {opens}"
    assert all(stream_id % 2 == 1 for stream_id in opens), f"seed {seed}: the dialer allocated an even id: {opens}"

    # 4. Nothing is retained per closed stream (WSM-STM-001). A sequence that opened and closed
    #    dozens of streams must leave the peer holding only what is still live.
    assert len(pair.dialer.streams) <= len(opens), f"seed {seed}: the peer kept more streams than it opened"
    assert all(s.state is not StreamState.CLOSED for s in pair.dialer.streams.values()), (
        f"seed {seed}: a closed stream is still in the live map"
    )

    # 5. The connection survives every legal sequence. A stream-level mistake is a stream-level
    #    failure; only a protocol error at connection level may end it, and nothing here commits one.
    assert pair.dialer.is_open, f"seed {seed}: an ordinary sequence of legal calls killed the connection"

    await pair.stop()


@pytest.mark.parametrize("seed", SEEDS)
async def test_a_random_sequence_then_socket_death_fails_everything_and_sends_nothing(seed: int, make_pair):
    """The same sequences, ended by the socket dying under them.

    Reset code `CONNECTION_CLOSED` is synthesised *locally* when a socket dies, so a sequence that
    never dies never produces one and the test above asserts its absence from the wire vacuously. Only
    a run in which the socket does die separates a library that never sends code 9 from one that
    would: removing the guard in `_sendable_reset_code` leaves the test above green and fails this one.

    What must hold after the socket goes, whatever the ordering that preceded it: every stream that
    was live ends up closed and failed with `ConnectionLost`, nothing new can be started, and no frame
    carrying code 9 was ever put on the wire (§8.1).
    """
    pair = make_pair()
    try:
        recorder = await asyncio.wait_for(_drive(seed, pair), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail(f"seed {seed}: the sequence hung before the socket was even dropped")

    live_before = [stream for stream in pair.dialer.streams.values() if stream.state is not StreamState.CLOSED]
    await pair.dialer_socket.drop()
    await pair.settle(rounds=40)

    on_wire = [f for f in recorder.sent if f.code == int(ResetCode.CONNECTION_CLOSED)]
    assert on_wire == [], f"seed {seed}: CONNECTION_CLOSED reached the wire; it is synthesised locally (§8.1)"

    assert pair.dialer.is_open is False, f"seed {seed}: the peer reports itself open after its socket died"
    assert pair.dialer.streams == {}, f"seed {seed}: streams survived the socket that carried them"

    for stream in live_before:
        assert stream.state is StreamState.CLOSED, f"seed {seed}: stream {stream.id} outlived its socket"
        with pytest.raises(ConnectionLost):
            await asyncio.wait_for(stream.result(), timeout=1.0)

    # Nothing is buffered for a socket that does not exist yet (WSM-RCN-042, WSM-INV-010).
    with pytest.raises(ConnectionLost):
        pair.dialer.open({"after": "death"})
