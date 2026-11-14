"""`oneway` streams: `notify()` sends one, and the receiver sends no frame on it (WSM-FRM-020, WSM-STM-040/041)."""

from __future__ import annotations

import asyncio
import logging

from typing import Any

from muxws.errors import ResetCode, StreamClosed
from muxws.frames import Frame
from muxws.stream import Stream, StreamState


async def test_notify_leaves_no_live_stream_at_the_opener(make_pair):
    """WSM-STM-041: the stream is closed once its `open` is enqueued."""
    pair = make_pair()
    seen: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = stream
        seen.set_result(payload)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        await pair.dialer.notify({"event": "cache-invalidated"})
        assert dict(pair.dialer.streams) == {}
        assert await asyncio.wait_for(seen, 1.0) == {"event": "cache-invalidated"}
        [opened] = pair.frames_of_type("dialer", "open")
        assert (opened.end, opened.oneway) == (True, True)
    finally:
        await pair.stop()


async def test_a_oneway_handler_cannot_send_and_its_stream_closes_without_a_frame(make_pair):
    """WSM-STM-040: every send raises `StreamClosed`, and the return sends no implicit end."""
    pair = make_pair()
    outcome: dict[str, Any] = {}

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        outcome["state"] = stream.state
        try:
            await stream.send({"answer": 1})
        except StreamClosed as exc:
            outcome["raised"] = exc
        outcome["stream"] = stream

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        await pair.dialer.notify({"q": 1})
        await pair.settle(24)

        assert outcome["state"] is StreamState.HALF_CLOSED_REMOTE
        assert isinstance(outcome["raised"], StreamClosed)
        assert outcome["stream"].closed.is_set()
        assert dict(pair.acceptor.streams) == {}
        assert pair.frames_of_type("acceptor", "data") == []
        assert pair.frames_of_type("acceptor", "reset") == []
    finally:
        await pair.stop()


async def test_reset_on_a_oneway_stream_closes_it_locally_without_a_frame(make_pair):
    """WSM-STM-040: `reset()` sends nothing on a oneway stream."""
    pair = make_pair()
    closed = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reset(ResetCode.CANCELLED, "not interested")
        closed.set()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        await pair.dialer.notify({"q": 1})
        await asyncio.wait_for(closed.wait(), 1.0)
        await pair.settle()
        assert pair.frames_of_type("acceptor", "reset") == []
    finally:
        await pair.stop()


async def test_a_oneway_handler_that_raises_is_logged_and_not_answered(make_pair, caplog):
    """WSM-STM-040: no reset(APPLICATION_ERROR) for a oneway stream."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("the handler blew up")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with caplog.at_level(logging.ERROR, logger="muxws.frames"):
            await pair.dialer.notify({"q": 1})
            await pair.settle(24)
        assert pair.frames_of_type("acceptor", "reset") == []
        assert "handler of oneway stream" in caplog.text
        assert "the handler blew up" in caplog.text
    finally:
        await pair.stop()


async def test_a_oneway_open_without_a_handler_is_dropped_without_a_frame(make_pair, caplog):
    """WSM-STM-040 over WSM-STM-033: no reset(REFUSED) for a oneway stream."""
    pair = make_pair()
    pair.start()
    try:
        with caplog.at_level(logging.WARNING, logger="muxws.frames"):
            await pair.dialer.notify({"q": 1})
            await pair.settle(24)
        assert pair.frames_of_type("acceptor", "reset") == []
        assert dict(pair.acceptor.streams) == {}
        assert "no on_stream handler" in caplog.text
    finally:
        await pair.stop()


async def test_a_oneway_open_after_this_peer_sent_goaway_is_dropped(make_lone):
    """WSM-STM-040 over WSM-CON-021: no reset(REFUSED) and no handler after goaway."""
    lone = make_lone()
    called: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = stream
        called.append(payload)

    lone.peer.on_stream(handler)
    lone.start()
    closing = asyncio.create_task(lone.peer.close(drain=0.2))
    try:
        await lone.settle()
        assert lone.frames_of_type("goaway")
        lone.inject(Frame("open", stream=1, payload={"q": 1}, end=True, oneway=True))
        await lone.settle()
        assert called == []
        assert lone.frames_of_type("reset") == []
    finally:
        await asyncio.gather(closing, return_exceptions=True)
        await lone.stop()


async def test_a_oneway_open_without_end_is_taken_as_ending(make_lone):
    """WSM-FRM-020: the receiver treats a oneway open as carrying `end`."""
    lone = make_lone()
    states: list[StreamState] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        states.append(stream.state)

    lone.peer.on_stream(handler)
    lone.start()
    try:
        lone.inject(Frame("open", stream=1, payload={"q": 1}, oneway=True))
        await lone.settle()
        assert states == [StreamState.HALF_CLOSED_REMOTE]
        assert lone.frames_of_type("data") == []
        assert lone.frames_of_type("reset") == []
    finally:
        await lone.stop()


async def test_a_fragmented_notify_carries_oneway_on_its_first_fragment(make_pair):
    """WSM-FRM-020: `oneway` rides the first fragment; the handler gets the whole payload."""
    pair = make_pair(max_frame_bytes=96)
    seen: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = stream
        seen.set_result(payload)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        payload = {"body": "x" * 400}
        await pair.dialer.notify(payload)
        assert await asyncio.wait_for(seen, 1.0) == payload
        fragments = pair.frames_of_type("dialer", "open")
        assert len(fragments) > 1
        assert [fragment.oneway for fragment in fragments] == [True] + [False] * (len(fragments) - 1)
        assert fragments[-1].end
        await pair.settle()
        assert pair.frames_of_type("acceptor", "data") == []
    finally:
        await pair.stop()
