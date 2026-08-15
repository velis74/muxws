"""What a socket loss does to every shape, and what a reconnect restores (§7.4, §7.5)."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from muxws.errors import ConnectionClosed, ConnectionLost, ResetCode
from muxws.lifecycle import MAX_STREAM_ID
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


async def test_cancel_and_reset_are_noops_after_death(make_pair):
    """WSM-RCN-041: no-ops, not errors, and not sends.

    A stream is already closed by the time an application gets round to cancelling it, and the socket
    it would have been cancelled on is gone. Raising here would make orderly cleanup - a `finally`
    that cancels whatever it holds - into a second failure on top of the one that already happened,
    and sending would be writing to a socket this peer has declared dead.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()
        after_death = len(pair.dialer_socket.sent)

        await stream.cancel("changed my mind")
        await stream.reset(ResetCode.NO_ERROR)
        # Twice, because a no-op that only holds the first time is a no-op nobody can rely on.
        await stream.cancel()
        await stream.reset(ResetCode.INTERNAL_ERROR)

        assert len(pair.dialer_socket.sent) == after_death, "a no-op puts nothing on the wire"
        assert len(pair.dialer._writer) == 0, "and queues nothing for a socket that will never carry it"
        assert stream.closed.is_set()
        assert isinstance(stream._close_cause, ConnectionLost), "and none of them rewrote how it died"
    finally:
        await pair.stop()


async def test_is_open_false_for_the_whole_gap(make_pair):
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


async def test_on_close_fires_once_per_loss_with_correct_will_retry(make_pair):
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


async def test_streams_do_not_survive_reconnect(make_pair):
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


async def test_a_reconnect_clears_the_dead_sockets_exhaustion_shutdown(make_pair):
    """WSM-SID-007/WSM-RCN-031: the new socket's id space starts empty, so its shutdown does too.

    Running out of stream ids schedules an orderly shutdown and remembers the task so a second
    `open()` cannot schedule a second one. That memory belongs to the socket that ran out, not to the
    `Peer`: carried into the next connection it makes `_begin_exhaustion_shutdown()` a no-op there,
    and a connection that exhausts its ids never says goodbye and never closes - it simply refuses
    every `open()` from then on, looking healthy the whole time.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.dialer._next_id = MAX_STREAM_ID
        pair.dialer.open({"q": 1})
        first = pair.dialer._exhaustion_task
        assert first is not None

        await pair.dialer_socket.drop()
        await pair.settle()
        await asyncio.wait_for(first, 5.0)

        fresh, _ = memory_pair()
        pair.dialer._adopt_socket(fresh)
        assert pair.dialer._exhaustion_task is None, "the dead socket's shutdown must not carry forward"

        pair.dialer._next_id = MAX_STREAM_ID
        pair.dialer.open({"q": 2})
        assert pair.dialer._exhaustion_task is not None
        assert pair.dialer._exhaustion_task is not first, "and the new socket schedules its own"
    finally:
        # Nothing is serving `fresh`, so the second shutdown would sit out its whole drain window
        # waiting for a stream no read loop will ever end (WSM-CON-024). That it *started* is the
        # assertion; running it out is `test_id_exhaustion_sends_goaway_drains_and_closes`'s job.
        second = pair.dialer._exhaustion_task
        if second is not None:
            second.cancel()
            await asyncio.gather(second, return_exceptions=True)
        await pair.stop()


async def test_the_writer_queues_are_discarded_on_death(make_pair):
    """A dead socket's writer queues are discarded: nothing queued for it reaches the next one."""
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


async def test_a_new_socket_may_report_its_own_final_close(make_pair):
    """WSM-RCN-044 is per connection, not per `Peer` object.

    The latch that stops one loss being reported twice must not outlive the connection it was set on.
    A `Peer` handed a fresh socket after a `will_retry=False` close is a new connection, and its next
    loss is a loss nobody has heard about - left unreset, it is reported to nobody at all.
    """
    pair = make_pair()
    closes: list[CloseReason] = []
    pair.dialer.on_close(closes.append)
    pair.start()

    pair.dialer._will_retry = False
    await pair.dialer_socket.drop()
    await pair.settle()
    assert [reason.will_retry for reason in closes] == [False], "the first loss reports, once"

    replacement, _ = memory_pair()
    pair.dialer._adopt_socket(replacement)
    pair.dialer._will_retry = False
    pair.dialer._die(ConnectionClosed("the second socket died too", code=1006))

    assert [reason.will_retry for reason in closes] == [False, False], (
        "the second connection's loss was swallowed by a latch left over from the first"
    )
