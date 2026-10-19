"""Send-path throughput: frames per second through the codec and the peer, printed on every run.
Each assertion is a minimum far below typical throughput, so the figure is reported on every run and
a large slowdown fails the test, while a loaded CI runner still passes.
"""

from __future__ import annotations

import asyncio
import time

from collections.abc import Callable

from muxws.frames import Frame


async def _drain_until(done: Callable[[], bool], *, limit: float = 30.0) -> None:
    """Turn the loop until everything sent has arrived.

    The wait is on arrival, not on a fixed number of `sleep(0)` turns: the rate is computed from the
    arrival count, so stopping before all frames arrive would report an incomplete run.
    """
    deadline = time.perf_counter() + limit
    while not done() and time.perf_counter() < deadline:
        await asyncio.sleep(0)


async def test_small_frame_throughput_on_one_stream_is_above_the_minimum(pair):
    """One stream, small payloads, over the in-memory transport: codec and peer overhead only."""
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
    assert rate > 2_000, f"{rate:,.0f} frames/second is below the minimum of 2,000"


async def test_throughput_across_twenty_streams_is_above_the_minimum(pair):
    """Twenty interleaved streams, as in the demo; the round-robin rotation does not reduce the rate."""
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
    assert rate > 2_000, f"{rate:,.0f} frames/second across 20 streams is below the minimum"
