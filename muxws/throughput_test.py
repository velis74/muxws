"""What the send path actually costs, measured rather than claimed.

The demo paces itself at four ticks a second per symbol so a reader can follow one row, and the first
person to run it reasonably read that as the library's speed. It is not: it is an
`asyncio.sleep(0.25)` in the demo's own generator.

These record the real ceiling. They assert a **floor far below** what the machine achieves, so they
document the number without becoming a benchmark that fails on a loaded CI runner - the point is that
the figure is measured on every run and printed when it regresses catastrophically, not that it is
pinned.
"""

from __future__ import annotations

import asyncio
import time

from collections.abc import Callable

from muxws.frames import Frame


async def _drain_until(done: Callable[[], bool], *, limit: float = 30.0) -> None:
    """Turn the loop until everything sent has arrived.

    A fixed number of `sleep(0)` turns is what the other tests use and it is wrong here: the count is
    the measurement, so a wait that runs out mid-flight reports a throughput figure for a run that
    never finished - which is exactly how the first version of this file under-reported by 60%.
    """
    deadline = time.perf_counter() + limit
    while not done() and time.perf_counter() < deadline:
        await asyncio.sleep(0)


async def test_how_many_small_frames_a_second_one_peer_pair_moves(pair):
    """One stream, small payloads, over the in-memory transport - the codec and peer cost alone."""
    received = 0

    def count(direction: str, _frame: Frame, _length: int) -> None:
        nonlocal received
        if direction == "rx":
            received += 1

    pair.acceptor.on_frame(count)
    pair.acceptor.on_stream(lambda _payload, _stream: None)

    stream = pair.dialer.open({"start": True})
    started = time.perf_counter()
    for index in range(5_000):
        await stream.send({"symbol": "ACME", "price": 101.25 + index % 7, "volume": index})
    await _drain_until(lambda: received >= 5_000)
    elapsed = time.perf_counter() - started

    rate = received / elapsed
    print(f"\n  {received} frames in {elapsed:.3f}s = {rate:,.0f} frames/second (one stream)")
    assert received > 4_000, f"only {received} of 5000 frames arrived"
    assert rate > 2_000, f"{rate:,.0f} frames/second is far below anything this path should manage"


async def test_the_rate_holds_up_across_many_concurrent_streams(pair):
    """Twenty streams interleaved, which is the demo's shape - the rotation is not the bottleneck."""
    received = 0

    def count(direction: str, _frame: Frame, _length: int) -> None:
        nonlocal received
        if direction == "rx":
            received += 1

    pair.acceptor.on_frame(count)
    pair.acceptor.on_stream(lambda _payload, _stream: None)

    streams = [pair.dialer.open({"topic": "ticks", "n": index}) for index in range(20)]
    started = time.perf_counter()
    for round_number in range(250):
        for stream in streams:
            await stream.send({"price": 100 + round_number, "n": stream.id})
    await _drain_until(lambda: received >= 5_000)
    elapsed = time.perf_counter() - started

    rate = received / elapsed
    print(f"\n  {received} frames in {elapsed:.3f}s = {rate:,.0f} frames/second (20 streams)")
    assert received > 4_000, f"only {received} of 5000 frames arrived"
    assert rate > 2_000, f"{rate:,.0f} frames/second across 20 streams is below what the rotation costs"
