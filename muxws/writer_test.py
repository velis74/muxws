"""The round-robin writer and the one-unsent-fragment rule (§4.2)."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.fragment import encoded_length, split_frame
from muxws.frames import Frame
from muxws.writer import CONNECTION_LANE, StreamQueue, Writer


@pytest.fixture
def codec() -> JsonCodec:
    return JsonCodec()


@pytest.fixture
def writer(codec: JsonCodec) -> Writer:
    return Writer(codec, max_frame_bytes=256)


def _big(size: int = 4000) -> dict[str, Any]:
    return {"body": "x" * size}


async def _drain(writer: Writer, limit: int = 500) -> list[Frame]:
    """Take frames until the writer has nothing left, recording the order they came out in."""
    out: list[Frame] = []
    for _ in range(limit):
        frame = writer._rotate()
        if frame is None:
            break
        out.append(frame)
        writer.advance(frame.stream or CONNECTION_LANE)
    return out


async def test_at_most_one_unsent_fragment_per_stream(writer: Writer):
    """WSM-FRG-018: fragment n+1 is sliced only once fragment n has been handed over."""
    writer.enqueue(Frame("data", stream=1, payload=_big()))

    depths: list[int] = []
    for _ in range(60):
        frame = writer._rotate()
        if frame is None:
            break
        depths.append(writer.prepared_depth_of(1))
        writer.advance(1)

    assert len(depths) > 5, "the payload must actually fragment for this to mean anything"
    assert max(depths) <= 1, f"more than one fragment was queued at once: {depths}"


async def test_round_robin_selects_across_streams_not_fifo(writer: Writer):
    """WSM-FRG-019: three streams with work produce a strictly rotating send order."""
    for stream_id in (1, 3, 5):
        for index in range(3):
            writer.enqueue(Frame("data", stream=stream_id, payload={"n": index}))

    order = [frame.stream for frame in await _drain(writer)]
    assert order[:9] == [1, 3, 5, 1, 3, 5, 1, 3, 5], f"not a rotation: {order[:9]}"


async def test_a_small_frame_overtakes_a_fragmented_payload(writer: Writer):
    """WSM-INV-004, the reason all of this exists.

    A 200-byte update on one stream must not wait for a megabyte on another. Without the rotation it
    would go out last; with it, it goes out on the second turn.
    """
    writer.enqueue(Frame("data", stream=1, payload=_big(40_000)))
    writer.enqueue(Frame("data", stream=3, payload={"progress": 42}))

    order = [frame.stream for frame in await _drain(writer, limit=40)]
    position = order.index(3)
    assert position <= 2, f"the small frame waited {position} turns behind the big one"
    assert order.count(1) > 10, "the big payload must still be fragmenting when the small one lands"


async def test_fragments_of_one_payload_stay_contiguous_on_their_stream(writer: Writer, codec: JsonCodec):
    """WSM-FRG-017: interleaved with *other* streams, never with themselves."""
    writer.enqueue(Frame("data", stream=1, payload=_big()))
    writer.enqueue(Frame("data", stream=3, payload=_big()))

    frames = await _drain(writer)
    for stream_id in (1, 3):
        mine = [f for f in frames if f.stream == stream_id]
        assert [f.more for f in mine] == [True] * (len(mine) - 1) + [False]
        rejoined = "".join(str(f.fragment) for f in mine)
        assert codec.decode_payload(rejoined) == _big()


async def test_every_frame_out_is_under_the_cap(writer: Writer, codec: JsonCodec):
    writer.enqueue(Frame("data", stream=1, payload=_big(20_000)))
    for frame in await _drain(writer):
        assert encoded_length(codec.encode(frame)) <= 256


async def test_the_writer_cuts_where_the_splitter_cuts(writer: Writer, codec: JsonCodec):
    """One boundary computation, used two ways - so they cannot drift apart."""
    source = Frame("data", stream=1, payload=_big(3000), end=True)
    writer.enqueue(source)
    assert [f.fragment for f in await _drain(writer)] == [f.fragment for f in split_frame(source, 256, codec)]


async def test_connection_level_frames_take_their_turn(writer: Writer):
    """A `ping` neither jumps the rotation nor waits for a megabyte to finish."""
    writer.enqueue(Frame("data", stream=1, payload=_big(20_000)))
    writer.enqueue(Frame("ping", nonce="abc"))

    order = [frame.type for frame in await _drain(writer, limit=20)]
    assert order.index("ping") <= 2, f"the ping waited behind the export: {order[:5]}"


async def test_no_fifo_of_frames_exists_in_the_send_path():
    """WSM-FRG-019 spelled as a source check: the ordering decision is not made at enqueue time."""
    import inspect

    from muxws import writer as module

    source = inspect.getsource(module)
    # A single deque of *frames* spanning streams is exactly what the rule forbids. The per-stream
    # `_waiting` deque is fine and necessary - contiguity (WSM-FRG-017) requires it.
    assert "deque[Frame]" in source
    assert source.count("deque[Frame]") == 1, "there is more than one frame queue in the send path"


async def test_discard_all_leaves_nothing_for_a_next_socket(writer: Writer):
    """WSM-RCN-042/WSM-INV-010: connection loss calls this, and nothing may survive it."""
    writer.enqueue(Frame("data", stream=1, payload=_big()))
    writer.enqueue(Frame("data", stream=3, payload={"n": 1}))
    writer._rotate()

    writer.discard_all()
    assert len(writer) == 0
    assert writer.lanes == 0
    assert writer._rotate() is None


async def test_discarding_one_stream_leaves_the_others_alone(writer: Writer):
    writer.enqueue(Frame("data", stream=1, payload=_big()))
    writer.enqueue(Frame("data", stream=3, payload={"n": 1}))

    writer.discard(1)
    order = [frame.stream for frame in await _drain(writer)]
    assert set(order) == {3}


async def test_a_spent_lane_is_retired(writer: Writer):
    """WSM-STM-001: nothing is retained per closed stream, including an empty queue."""
    writer.enqueue(Frame("data", stream=1, payload={"n": 1}))
    await _drain(writer)
    assert writer.depth_of(1) == 0
    assert 1 not in writer._queues


async def test_next_frame_waits_and_wakes(writer: Writer):
    """The writer sleeps when there is nothing to send, and wakes on the next enqueue."""
    waiting = asyncio.create_task(writer.next_frame())
    await asyncio.sleep(0)
    assert not waiting.done()

    writer.enqueue(Frame("data", stream=1, payload={"n": 1}))
    frame = await asyncio.wait_for(waiting, timeout=1.0)
    assert frame is not None
    assert frame.stream == 1


async def test_a_stream_queue_holds_one_prepared_fragment(codec: JsonCodec):
    """The rule as a unit, without a writer around it."""
    queue = StreamQueue(1)
    queue.put(Frame("data", stream=1, payload=_big()))

    queue.prepare(256, codec)
    assert queue.prepared_depth == 1
    queue.prepare(256, codec)
    assert queue.prepared_depth == 1, "prepare() must be idempotent, not cumulative"

    assert queue.take() is not None
    assert queue.prepared_depth == 0
    assert queue.has_work, "the tail is still there even with nothing prepared"


async def test_the_write_loop_yields_so_the_rotation_has_something_to_rotate(make_pair):
    """WSM-INV-004 on a **fast** socket, which is the only link that can test it.

    The round-robin is only worth having if another stream can get a frame into the writer while a
    large payload is going out. Nothing in `_write_loop` is guaranteed to suspend: `next_frame()`
    returns without awaiting when there is work, and a socket whose buffer has room - uvicorn on
    loopback, and `MemorySocket` always - completes its send without yielding either. Without a
    deliberate turn per frame the loop drains a whole megabyte in one uninterrupted run, no other
    task runs, nothing else can enqueue, and the rotation has exactly one lane to choose from.

    On a link slow enough that backpressure supplies the missing suspension the guarantee holds
    either way, so a slow transport proves nothing here; every test transport and localhost are the
    fastest links there are, which is what makes this the place the rule is provable.
    """
    pair = make_pair()
    pair.acceptor.on_stream(lambda _payload, _stream: None)
    order: list[int | None] = []
    pair.dialer.on_frame(lambda direction, frame, _n: order.append(frame.stream) if direction == "tx" else None)
    pair.start()

    ticking = True

    async def keep_ticking() -> None:
        while ticking:
            try:
                pair.dialer.open({"tick": 1}, end=True)
            except Exception:  # noqa: BLE001 - the connection ending is how this task retires
                return
            await asyncio.sleep(0)

    ticker = asyncio.create_task(keep_ticking())
    try:
        await asyncio.sleep(0)
        export = pair.dialer.open({"export": "x" * 400_000}, end=True)
        for _ in range(400):
            await asyncio.sleep(0)
        ticking = False

        positions = [index for index, stream in enumerate(order) if stream == export.id]
        assert len(positions) > 3, f"the export must actually fragment for this to mean anything: {len(positions)}"
        between = [stream for stream in order[positions[0] : positions[-1]] if stream != export.id]
        assert between, (
            f"{len(positions)} export fragments reached the wire with nothing else between them: the "
            f"export monopolised the socket and WSM-INV-004 does not hold on this link"
        )
    finally:
        ticking = False
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
        await pair.stop()
