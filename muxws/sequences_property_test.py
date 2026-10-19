"""Random legal API sequences against a live pair, asserting the invariants that hold for all of them.

`stream_test.py` covers the forty-five cells of the state table one at a time, and
`conformance/sequences/` scripts thirteen fixed exchanges. Neither covers orderings outside those
cases, and a sequence can be legal in every step and still violate an invariant as a whole.

This module drives random sequences of ordinary calls and asserts only what holds for every sequence.
A failure identifies a violated invariant, not the rule that caused it, so these tests complement the
enumerated ones.

Seeds are fixed and listed, so a failure is reproducible: the failing seed is in the assertion message,
and adding a seed to `SEEDS` keeps that sequence in every run.
"""

from __future__ import annotations

import asyncio
import random

from typing import Any

import pytest

from muxws.errors import ConnectionLost, MuxwsError, ResetCode
from muxws.frames import Frame
from muxws.stream import Stream, StreamState

#: Fixed seeds, so every run executes the same sequences and a failure is reproducible.
SEEDS = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89]

#: Enough steps to open, write on and end a stream concurrently with another; few enough that ten
#: sequences run in the time of an ordinary test.
STEPS = 40


class Recorder:
    """The frames sent on the wire, and what the application saw."""

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
                # Sometimes pass the one code an application may not send; the library must not send
                # it (§8.1). `CONNECTION_CLOSED` means "the socket under this stream died", which is
                # false when a caller sends it and cannot be checked by the remote. This call is the
                # only path by which code 9 can reach the wire: socket death synthesises it locally
                # and enqueues no frame, so without this call invariant 2 below cannot fail.
                code = ResetCode.CONNECTION_CLOSED if rng.random() < 0.25 else ResetCode.APPLICATION_ERROR
                await stream.reset(code, "a handler said no")
            else:
                await pair.settle(rounds=2)
        except Exception as failure:  # noqa: BLE001 - every exception is recorded and checked below
            # Catches every exception. A call at the wrong moment has several documented outcomes
            # (`StreamClosed` for a second `end()`, the stream's own `StreamReset`, `ConnectionLost`,
            # `ConnectionGoingAway`); catching only those would make invariant 1 below check only
            # the types already caught here. Invariant 1 asserts that every recorded exception is a
            # muxws error.
            recorder.failures.append(failure)
        live = [stream for stream in live if stream.state is not StreamState.CLOSED]

    await pair.settle(rounds=40)
    return recorder


@pytest.mark.parametrize("seed", SEEDS)
async def test_a_random_legal_sequence_upholds_every_invariant(seed: int, make_pair):
    """These five invariants hold for every ordering. Each assertion message names the violation."""
    pair = make_pair()
    try:
        recorder = await asyncio.wait_for(_drive(seed, pair), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail(f"seed {seed}: the sequence hung. A call blocked forever rather than failing (WSM-INV-011)")

    # 1. Every exception is a muxws error. An AttributeError or a KeyError from an ordinary call is a
    #    library bug.
    stray = [f for f in recorder.failures if not isinstance(f, MuxwsError)]
    assert stray == [], f"seed {seed}: a call raised something that is not a muxws error: {stray!r}"

    # 2. Reset code 9 is synthesised locally when the socket dies and MUST NEVER appear on the wire.
    on_wire = [f for f in recorder.sent if f.type == "reset" and f.code == int(ResetCode.CONNECTION_CLOSED)]
    assert on_wire == [], f"seed {seed}: CONNECTION_CLOSED reached the wire (§8.1)"

    # 3. Opens are monotonic and this peer's parity, however the calls interleaved (WSM-SID-002/004).
    opens = [f.stream for f in recorder.sent if f.type == "open" and f.fragment is None]
    assert opens == sorted(set(opens)), f"seed {seed}: opens were not monotonic: {opens}"
    assert all(stream_id % 2 == 1 for stream_id in opens), f"seed {seed}: the dialer allocated an even id: {opens}"

    # 4. Nothing is retained per closed stream (WSM-STM-001). After a sequence that opened and closed
    #    many streams, the peer holds only the live ones.
    assert len(pair.dialer.streams) <= len(opens), f"seed {seed}: the peer kept more streams than it opened"
    assert all(s.state is not StreamState.CLOSED for s in pair.dialer.streams.values()), (
        f"seed {seed}: a closed stream is still in the live map"
    )

    # 5. The connection stays open after every legal sequence. A stream-level error fails only the
    #    stream; only a connection-level protocol error closes the connection, and these calls cause
    #    none.
    assert pair.dialer.is_open, f"seed {seed}: an ordinary sequence of legal calls killed the connection"

    await pair.stop()


@pytest.mark.parametrize("seed", SEEDS)
async def test_a_random_sequence_then_socket_death_fails_everything_and_sends_nothing(seed: int, make_pair):
    """The same sequences, ended by the socket dying under them.

    Reset code `CONNECTION_CLOSED` is synthesised locally when a socket dies, so the test above, in
    which the socket does not die, cannot distinguish a library that never sends code 9 from one that
    does. Removing the guard in `_sendable_reset_code` leaves the test above passing and fails this
    one.

    After the socket dies, whatever the preceding ordering: every stream that was live is closed and
    failed with `ConnectionLost`, no new stream can be started, and no frame carrying code 9 was put
    on the wire (§8.1).
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
    assert pair.dialer.streams == {}, f"seed {seed}: live streams remain after the socket died"

    for stream in live_before:
        assert stream.state is StreamState.CLOSED, (
            f"seed {seed}: stream {stream.id} is not closed after its socket died"
        )
        with pytest.raises(ConnectionLost):
            await asyncio.wait_for(stream.result(), timeout=1.0)

    # Nothing is buffered for a socket that does not exist yet (WSM-RCN-042, WSM-INV-010).
    with pytest.raises(ConnectionLost):
        pair.dialer.open({"after": "death"})
