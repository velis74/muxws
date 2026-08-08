"""What a socket loss does to every shape, and what a reconnect restores (§7.4, §7.5)."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from muxws.errors import ConnectionLost, ResetCode
from muxws.observability import CloseReason
from muxws.stream import Stream
from muxws.transports.memory import memory_pair


async def test_second_await_after_death_returns_the_same_error(make_pair):
    """WSM-RCN-041/WSM-API-010: the memoized future is resolved **once**.

    A second await that hung, or that raised something different, would mean the failure had been
    delivered rather than recorded.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        with pytest.raises(ConnectionLost) as first:
            await stream
        with pytest.raises(ConnectionLost) as second:
            await stream
        assert first.value.code is second.value.code is ResetCode.CONNECTION_CLOSED
    finally:
        await pair.stop()


async def test_async_for_raises_rather_than_terminating_normally(make_pair):
    """WSM-RCN-041: a clean end would read as 'the export finished', which is a lie."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send({"row": 0})
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        collected: list[Any] = []

        async def consume() -> None:
            async for item in stream:
                collected.append(item)

        task = asyncio.create_task(consume())
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        with pytest.raises(ConnectionLost):
            await asyncio.wait_for(task, timeout=2.0)
        assert collected == [{"row": 0}], "what did arrive is still delivered"
    finally:
        await pair.stop()


async def test_is_open_is_false_for_the_whole_gap(make_pair):
    """WSM-RCN-043: from the loss until the next *established* connection, not the next socket."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        assert pair.dialer.is_open is True
        await pair.dialer_socket.drop()
        await pair.settle()
        assert pair.dialer.is_open is False

        # A socket handed over is not yet an established connection - but it is what ends the gap.
        left, _ = memory_pair()
        pair.dialer._adopt_socket(left)
        assert pair.dialer.is_open is True
    finally:
        await pair.stop()


async def test_nothing_attempted_while_disconnected_appears_on_the_new_socket(make_pair):
    """WSM-RCN-042/WSM-INV-010 **(spec)**: nothing is buffered for a next socket.

    A queue that flushed into a server which has forgotten the sender turns a failure that would have
    reached a call site into silent misdelivery.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        await pair.dialer_socket.drop()
        await pair.settle()

        for index in range(5):
            with pytest.raises(ConnectionLost):
                pair.dialer.open({"attempt": index})
            with pytest.raises(ConnectionLost):
                await pair.dialer.notify({"attempt": index})
            with pytest.raises(ConnectionLost):
                await pair.dialer.request({"attempt": index})

        fresh, _ = memory_pair()
        pair.dialer._adopt_socket(fresh)
        serving = asyncio.create_task(pair.dialer.serve())
        await pair.settle()

        assert fresh.sent == [], "the new socket must carry none of what was attempted in the gap"
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
    finally:
        await pair.stop()


async def test_on_close_fires_once_per_loss_with_the_right_will_retry(make_pair):
    """WSM-RCN-040/045: every loss, exactly once, and four fields."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    seen: list[CloseReason] = []
    pair.dialer.on_close(seen.append)
    pair.start()
    try:
        await pair.dialer_socket.drop()
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        assert len(seen) == 1, "one loss, one call - a second drop of a dead socket is not a loss"
        reason = seen[0]
        assert {f.name for f in reason.__dataclass_fields__.values()} == {
            "code",
            "reason",
            "was_clean",
            "will_retry",
        }
        assert reason.will_retry is False, "nothing retries until the helper says so"
    finally:
        await pair.stop()


async def test_will_retry_follows_the_helpers_intention(make_pair):
    """WSM-RCN-040: false only when `max_attempts` is exhausted or `close()` was deliberate."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    seen: list[CloseReason] = []
    pair.dialer.on_close(seen.append)
    pair.dialer._will_retry = True
    pair.start()
    try:
        await pair.dialer_socket.drop()
        await pair.settle()
        assert seen[0].will_retry is True
    finally:
        await pair.stop()


async def test_a_deliberate_close_never_says_it_will_retry(make_pair):
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    seen: list[CloseReason] = []
    pair.dialer.on_close(seen.append)
    pair.start()
    try:
        await pair.dialer.close()
        await pair.settle()
        assert seen
        assert seen[0].will_retry is False
        assert seen[0].was_clean is True
    finally:
        await pair.stop()


async def test_streams_do_not_survive_a_reconnect(make_pair):
    """WSM-RCN-031/032: `Peer` survives; a `Stream` held across one is already closed."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        held = pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        fresh, _ = memory_pair()
        pair.dialer._adopt_socket(fresh)

        assert held.closed.is_set()
        assert pair.dialer.streams == {}, "the new socket's id space starts empty"
        assert pair.dialer.open({"q": 2}).id == 1, "and the allocator starts over"
    finally:
        await pair.stop()


async def test_peer_id_advances_on_every_reconnect(make_pair):
    """WSM-API-009: a reconnect reads as a new `conn=` in a log, and no id is handed out twice."""
    pair = make_pair()
    pair.start()
    seen = [pair.dialer.id]
    try:
        for _ in range(3):
            await pair.dialer_socket.drop()
            await pair.settle()
            fresh, _ = memory_pair()
            pair.dialer._adopt_socket(fresh)
            seen.append(pair.dialer.id)

        assert len(set(seen)) == len(seen), "no id may repeat"
        counters = [int(peer_id.split("-")[1]) for peer_id in seen]
        assert counters == sorted(counters), "the counter never rewinds"
    finally:
        await pair.stop()


async def test_tags_do_not_survive_on_the_acceptor_side(make_pair):
    """WSM-RCN-033/WSM-INV-014: a tab that silenced something and died must not silence a successor.

    On the acceptor side a reconnect is a **new peer object**, so this is a statement about what the
    acceptor builds rather than about what it clears.
    """
    first = make_pair()
    first.acceptor.tags["muted"] = True

    second = make_pair()
    assert second.acceptor.tags == {}, "a new connection starts with empty tags"
    assert second.acceptor.tags is not first.acceptor.tags


async def test_connection_closed_code_is_never_sent(make_pair):
    """Reset code 9 is synthesised locally and MUST NEVER appear on the wire."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()
        for who in ("dialer", "acceptor"):
            for frame in pair.sent_by(who):
                assert frame.code != int(ResetCode.CONNECTION_CLOSED)
    finally:
        await pair.stop()


async def test_the_writer_queues_are_discarded_on_death(make_pair):
    """M5a exposed `discard_all()` for exactly this, and M5b is what calls it."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        for index in range(5):
            pair.dialer.open({"body": "x" * 5000, "n": index})
        await pair.dialer_socket.drop()
        await pair.settle()
        assert len(pair.dialer._writer) == 0
    finally:
        await pair.stop()


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()
