"""`Stream`: the state table, the awaitable handle, cancellation and the send-after-close rules."""

from __future__ import annotations

import asyncio
import inspect
import warnings

from typing import Any

import pytest

from muxws.conftest import Pair
from muxws.errors import (
    ProtocolError,
    RemoteError,
    ResetCode,
    StreamAlreadyConsumed,
    StreamClosed,
    StreamReset,
    StreamTimeout,
)
from muxws.frames import ABSENT, Frame
from muxws.peer import Peer
from muxws.stream import Stream, StreamState

# --------------------------------------------------------------------------- the state table

#: Every cell of §5.3: five states by nine events. `n/r` cells are asserted unreachable rather than
#: exercised, which is what WSM-STM-011 means by "not externally observable".
STATE_TABLE: dict[tuple[str, str], str] = {
    ("idle", "send_open"): "open",
    ("idle", "recv_open"): "open",
    ("idle", "send_data"): "n/r",
    ("idle", "send_end"): "n/r",
    ("idle", "recv_data"): "ILL-C",
    ("idle", "recv_data_end"): "ILL-C",
    ("idle", "send_reset"): "n/r",
    ("idle", "recv_reset"): "ILL-C",
    ("idle", "socket_death"): "closed",
    ("open", "send_open"): "RAISE",
    ("open", "recv_open"): "ILL-C",
    ("open", "send_data"): "open",
    ("open", "send_end"): "half_closed_local",
    ("open", "recv_data"): "open",
    ("open", "recv_data_end"): "half_closed_remote",
    ("open", "send_reset"): "closed",
    ("open", "recv_reset"): "closed",
    ("open", "socket_death"): "closed",
    ("half_closed_local", "send_open"): "RAISE",
    ("half_closed_local", "recv_open"): "ILL-C",
    ("half_closed_local", "send_data"): "RAISE",
    ("half_closed_local", "send_end"): "RAISE",
    ("half_closed_local", "recv_data"): "half_closed_local",
    ("half_closed_local", "recv_data_end"): "closed",
    ("half_closed_local", "send_reset"): "closed",
    ("half_closed_local", "recv_reset"): "closed",
    ("half_closed_local", "socket_death"): "closed",
    ("half_closed_remote", "send_open"): "RAISE",
    ("half_closed_remote", "recv_open"): "ILL-C",
    ("half_closed_remote", "send_data"): "half_closed_remote",
    ("half_closed_remote", "send_end"): "closed",
    ("half_closed_remote", "recv_data"): "ILL-S",
    ("half_closed_remote", "recv_data_end"): "ILL-S",
    ("half_closed_remote", "send_reset"): "closed",
    ("half_closed_remote", "recv_reset"): "closed",
    ("half_closed_remote", "socket_death"): "closed",
    ("closed", "send_open"): "RAISE",
    ("closed", "recv_open"): "ILL-C",
    ("closed", "send_data"): "RAISE",
    ("closed", "send_end"): "RAISE",
    ("closed", "recv_data"): "IGN",
    ("closed", "recv_data_end"): "IGN",
    ("closed", "send_reset"): "NOOP",
    ("closed", "recv_reset"): "IGN",
    ("closed", "socket_death"): "NOOP",
}

CELL_IDS = [f"{state}-{event}" for state, event in STATE_TABLE]


def test_the_table_has_forty_five_cells():
    """Five states by nine events. A shrinking table is a shrinking test suite."""
    assert len(STATE_TABLE) == 45


async def _drive_to(pair: Pair, state: str) -> Stream:
    """Put a dialer-side stream into `state`, using only the public API."""
    if state == "idle":
        raise AssertionError("idle is not externally observable (WSM-STM-011)")
    stream = pair.dialer.open({"n": 1}, end=state == "half_closed_local")
    if state == "open":
        await pair.settle()
        return stream
    if state == "half_closed_local":
        await pair.settle()
        return stream
    if state == "half_closed_remote":
        await pair.settle()
        pair.acceptor._streams[stream.id]  # noqa: B018 - the acceptor must have seen it
        acceptor_stream = pair.acceptor._streams[stream.id]
        await acceptor_stream.end({"done": True})
        await pair.settle()
        return stream
    if state == "closed":
        await pair.settle()
        await stream.reset(ResetCode.NO_ERROR)
        await pair.settle()
        return stream
    raise AssertionError(f"unknown state {state}")


@pytest.mark.parametrize(("cell", "outcome"), list(STATE_TABLE.items()), ids=CELL_IDS)
async def test_every_state_table_cell(cell: tuple[str, str], outcome: str, make_pair):
    """WSM-STM-010: every row, including every illegal cell."""
    state, event = cell

    # `idle` exists only inside the indivisible allocate-and-enqueue step of WSM-SID-006, and the
    # three `n/r` cells are the ones that would need a caller to reach inside it.
    if state == "idle":
        pair = make_pair()
        pair.start()
        try:
            await _assert_idle_cell(pair, event, outcome)
        finally:
            await pair.stop()
        return

    pair = make_pair()
    pair.on_stream_calls = []  # type: ignore[attr-defined]
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = await _drive_to(pair, state)
        await _assert_cell(pair, stream, event, outcome)
    finally:
        await pair.stop()


async def _assert_idle_cell(pair: Pair, event: str, outcome: str) -> None:
    if outcome == "open":
        if event == "send_open":
            stream = pair.dialer.open({"n": 1})
            assert stream.state is StreamState.OPEN
            ended = pair.dialer.open({"n": 2}, end=True)
            assert ended.state is StreamState.HALF_CLOSED_LOCAL
        else:
            pair.acceptor.on_stream(_echo_handler)
            pair.dialer.open({"n": 1})
            await pair.settle()
            assert 1 in pair.acceptor.streams
            assert pair.acceptor.streams[1].state is StreamState.OPEN
        return
    if outcome == "n/r":
        # `idle` exists only inside the indivisible allocate-and-enqueue step (WSM-SID-006), so the
        # cell is unreachable exactly when no stream the public API can hand out is ever in it.
        # `open()` being synchronous is what makes that true: there is no suspension point at which
        # a caller could be given a stream that has not yet been enqueued.
        assert not inspect.iscoroutinefunction(Peer.open)

        seen: list[Stream] = []
        pair.acceptor.on_stream(_capture(seen))

        local = pair.dialer.open({"n": 1})
        assert local.state is not StreamState.IDLE, f"open() handed out an idle stream; {event} would be reachable"
        ended = pair.dialer.open({"n": 2}, end=True)
        assert ended.state is not StreamState.IDLE
        await pair.settle()

        observed = [*seen, *pair.dialer.streams.values(), *pair.acceptor.streams.values()]
        assert observed, "the assertion below would be vacuous with nothing to observe"
        for stream in observed:
            assert stream.state is not StreamState.IDLE, (
                f"a stream reachable through the public API is idle, so stream.{event.removeprefix('send_')}() "
                f"could be called on it"
            )
        return
    if outcome == "closed":
        await pair.dialer_socket.drop()
        await pair.settle()
        assert pair.dialer.is_open is False
        return
    # ILL-C: a stream-level frame for an id nobody opened is above the high-water mark.
    frame = {
        "recv_data": Frame("data", stream=7, payload={"a": 1}),
        "recv_data_end": Frame("data", stream=7, payload={"a": 1}, end=True),
        "recv_reset": Frame("reset", stream=7, code=int(ResetCode.CANCELLED)),
    }[event]
    pair.acceptor_socket.inject(pair.acceptor._codec.encode(frame))
    await pair.settle()
    assert _goaway_sent(pair, "acceptor"), f"{event} above the high-water mark must be ILL-C"


def _capture(sink: list[Stream]):
    """A handler that records the stream it was handed, so its state can be inspected."""

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        sink.append(stream)
        await stream.closed.wait()

    return handler


async def _echo_handler(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


def _goaway_sent(pair: Pair, who: str) -> bool:
    return any(frame.type == "goaway" for frame in pair.sent_by(who))


async def _assert_cell(pair: Pair, stream: Stream, event: str, outcome: str) -> None:  # noqa: C901, PLR0912
    acceptor = pair.acceptor

    if event == "send_open":
        # `open` is a peer-level call; a stream never re-opens. The RAISE column is about the stream
        # already existing, which the allocator makes impossible - so assert the id never repeats.
        second = pair.dialer.open({"n": 2})
        assert second.id != stream.id
        return

    if event == "recv_open":
        # ILL-C: an open re-using an id at or below the remote's high-water mark.
        acceptor._highest_remote_open = max(acceptor._highest_remote_open, stream.id)
        pair.acceptor_socket.inject(acceptor._codec.encode(Frame("open", stream=stream.id, payload={})))
        await pair.settle()
        assert _goaway_sent(pair, "acceptor")
        return

    if event in ("send_data", "send_end"):
        call = stream.send({"x": 1}) if event == "send_data" else stream.end()
        if outcome == "RAISE":
            with pytest.raises((StreamClosed, StreamReset)):
                await call
        else:
            await call
            await pair.settle()
            assert stream.state.value == outcome
        return

    if event in ("recv_data", "recv_data_end"):
        end = event == "recv_data_end"
        frame = Frame("data", stream=stream.id, payload={"y": 1}, end=end)
        pair.acceptor_socket_inject = None
        pair.dialer_socket.inject(pair.dialer._codec.encode(frame))
        await pair.settle()
        if outcome == "ILL-S":
            assert stream.state is StreamState.CLOSED
            assert any(f.type == "reset" and f.code == int(ResetCode.PROTOCOL_ERROR) for f in pair.sent_by("dialer"))
            assert pair.dialer.is_open, "a stream-level error must leave the connection alone"
        elif outcome == "IGN":
            assert pair.dialer.is_open
        else:
            assert stream.state.value == outcome
        return

    if event == "send_reset":
        before = len(pair.frames_of_type("dialer", "reset"))
        await stream.reset(ResetCode.CANCELLED)
        await pair.settle()
        after = len(pair.frames_of_type("dialer", "reset"))
        assert stream.state is StreamState.CLOSED
        if outcome == "NOOP":
            assert after == before, "resetting a closed stream sends nothing"
        else:
            assert after == before + 1
        return

    if event == "recv_reset":
        pair.dialer_socket.inject(
            pair.dialer._codec.encode(Frame("reset", stream=stream.id, code=int(ResetCode.CANCELLED)))
        )
        await pair.settle()
        assert stream.state is StreamState.CLOSED
        assert pair.dialer.is_open
        return

    if event == "socket_death":
        await pair.dialer_socket.drop()
        await pair.settle()
        assert stream.state is StreamState.CLOSED
        assert stream.closed.is_set()
        return

    raise AssertionError(f"unhandled event {event}")


# --------------------------------------------------------------------------- ids and warnings


async def test_id_readable_immediately_after_open(pair: Pair):
    """WSM-SID-008: `stream.id` on the next line, with nothing awaited in between."""
    stream = pair.dialer.open({"a": 1})
    assert stream.id == 1


async def test_unconsumed_open_emits_no_runtime_warning(pair: Pair):
    """WSM-API-017: `Stream` is not a coroutine, so nothing is ever 'never awaited'."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pair.dialer.open({"a": 1})
        await pair.settle()
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []


def test_open_takes_no_timeout_argument():
    """WSM-API-018: deadlines live on the calls that wait, not on the one that returns at once."""
    assert "timeout" not in inspect.signature(Peer.open).parameters
    assert "timeout" in inspect.signature(Peer.request).parameters
    assert "timeout" in inspect.signature(Stream.result).parameters


async def test_open_with_timeout_is_a_type_error(pair: Pair):
    with pytest.raises(TypeError):
        pair.dialer.open({"a": 1}, timeout=1)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- the awaitable handle


async def test_await_twice_returns_same_value(make_pair):
    """WSM-API-010/011: one memoized future, resolved once."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"answer": 42})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        first = await stream
        second = await stream
        assert first == {"answer": 42}
        # Identity, not equality: an implementation that re-read the queue would build an equal dict
        # and pass an equality check while having consumed a payload that is not there twice.
        assert second is first
        future = stream._future
        assert future is not None
        assert await stream is first
        assert stream._future is future, "the future must be created once and kept"
    finally:
        await pair.stop()


async def test_await_then_iterate_raises_and_first_consumer_got_everything(make_pair):
    """WSM-API-014: the first shape claims the stream; the error names both uses."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send({"n": 1})
        await stream.end({"n": 2})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        assert await stream == {"n": 1}
        with pytest.raises(StreamAlreadyConsumed, match="await.*iterate|iterate.*await"):
            async for _ in stream:
                pass

        other = pair.dialer.open({"q": 2})
        collected = [item async for item in other]
        assert collected
        with pytest.raises(StreamAlreadyConsumed):
            async for _ in other:
                pass
    finally:
        await pair.stop()


async def test_open_resolves_first_payload_while_request_raises(make_pair):
    """WSM-API-006/007: only `request()` polices a second payload."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send({"n": 1})
        await stream.end({"n": 2})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        assert await pair.dialer.open({"q": 1}) == {"n": 1}
        with pytest.raises(ProtocolError, match="more than one payload"):
            await pair.dialer.request({"q": 2})
    finally:
        await pair.stop()


async def test_result_timeout_uses_the_same_future(make_pair):
    """WSM-API-012, WSM-ERR-011: one source of the value, plus a deadline that resets the stream."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"answer": 1})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        first = await stream
        assert first == {"answer": 1}
        # Same *object*, not merely an equal one: a second source would rebuild the value, and a
        # stream whose remote sent two payloads would then hand out the second here (WSM-API-012).
        assert await stream.result(timeout=1.0) is first
        assert await stream.result() is first
        assert stream._future is not None
        assert stream._future.result() is first
    finally:
        await pair.stop()


async def test_result_timeout_expiry_sends_reset_timeout(make_pair):
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        with pytest.raises(StreamTimeout):
            await stream.result(timeout=0.02)
        await pair.settle()
        assert any(f.type == "reset" and f.code == int(ResetCode.TIMEOUT) for f in pair.sent_by("dialer"))
    finally:
        await pair.stop()


async def test_send_after_normal_close_raises_stream_closed(make_pair):
    """WSM-ERR-009 **(spec)**: three outcomes, three classes.

    `StreamClosed` for a normal close is deliberately not a `StreamReset` and not a `ProtocolError`:
    a last `send()` racing an orderly end is an expected outcome, not a failure and not a caller bug.
    """
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"answer": 1})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1}, end=True)
        assert await stream == {"answer": 1}
        await pair.settle()

        with pytest.raises(StreamClosed) as info:
            await stream.send({"late": True})
        assert not isinstance(info.value, StreamReset)
        assert not isinstance(info.value, ProtocolError)

        cancelled = pair.dialer.open({"q": 2})
        await pair.settle()
        await cancelled.cancel()
        with pytest.raises(StreamReset) as reset_info:
            await cancelled.send({"late": True})
        assert reset_info.value.code is ResetCode.CANCELLED
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- cancellation


async def test_cancel_is_immediate_and_discards_in_flight(make_pair):
    """WSM-ERR-012: close locally at once; payloads still in flight are discarded."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send({"n": 1})
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.cancel("user navigated away")
        assert stream.state is StreamState.CLOSED
        assert stream.closed.is_set()

        pair.dialer_socket.inject(pair.dialer._codec.encode(Frame("data", stream=stream.id, payload={"late": True})))
        await pair.settle()
        assert pair.dialer.is_open
    finally:
        await pair.stop()


async def test_remote_cancel_cancels_the_handler_task(make_pair):
    """WSM-ERR-013: the handler observes CancelledError at its next await."""
    pair = make_pair()
    observed: list[str] = []

    async def handler(payload: Any, _stream: Stream) -> None:
        _ = payload
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            observed.append("cancelled")
            raise

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.cancel()
        await pair.settle()
        assert observed == ["cancelled"]
    finally:
        await pair.stop()


async def test_local_cancellation_sends_reset_and_reraises(make_pair):
    """WSM-ERR-014: CancelledError out of an iteration resets the stream and keeps propagating."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()

        async def consume() -> None:
            async for _ in stream:
                pass

        task = asyncio.create_task(consume())
        await pair.settle()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await pair.settle()
        assert any(f.type == "reset" and f.code == int(ResetCode.CANCELLED) for f in pair.sent_by("dialer"))
    finally:
        await pair.stop()


async def test_remote_application_error_raises_remote_error(make_pair):
    """WSM-ERR-015: `RemoteError` with `.payload` out of both the await and the async for."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("no such report")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(RemoteError) as info:
            await pair.dialer.open({"q": 1})
        assert info.value.payload == {"type": "ValueError", "message": "no such report"}

        with pytest.raises(RemoteError):
            async for _ in pair.dialer.open({"q": 2}):
                pass
    finally:
        await pair.stop()


async def test_socket_death_makes_cancel_and_reset_noops(make_pair):
    """WSM-RCN-041: nothing goes out on a dead socket, and neither call raises."""
    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()
        await stream.cancel()
        await stream.reset(ResetCode.NO_ERROR)
        assert stream.closed.is_set()
    finally:
        await pair.stop()


async def test_trailers_are_populated_when_the_stream_ends(make_pair):
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.end({"rows": 1}, trailers={"checksum": "abc"})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        assert await stream == {"rows": 1}
        await pair.settle()
        assert stream.trailers == {"checksum": "abc"}
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- edges


async def test_send_with_end_half_closes_in_one_call(make_pair):
    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.send({"last": True}, end=True)
        assert stream.state is StreamState.HALF_CLOSED_LOCAL
    finally:
        await pair.stop()


async def test_awaiting_a_stream_that_ends_without_a_payload_raises(make_pair):
    """An await that hung here would be a spinner nobody can explain."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.end()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(ProtocolError, match="without producing a payload"):
            await pair.dialer.open({"q": 1})
    finally:
        await pair.stop()


async def test_a_second_failure_on_a_closed_stream_is_ignored(make_pair):
    """Socket death after a reset must not overwrite the reason the stream actually died."""
    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.cancel()
        first_cause = stream._close_cause
        await pair.dialer_socket.drop()
        await pair.settle()
        assert stream._close_cause is first_cause
    finally:
        await pair.stop()


async def test_awaiting_after_the_stream_was_already_reset(make_pair):
    """The memoized future is created on first await, and may already have an answer waiting."""
    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.cancel()
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.CANCELLED
    finally:
        await pair.stop()


async def test_awaiting_after_the_payload_already_arrived(make_pair):
    """A payload that landed before the first await is what that await resolves with."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"early": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        assert await stream == {"early": True}
    finally:
        await pair.stop()


async def test_awaiting_after_the_stream_ended_empty(make_pair):
    """The queue holds only the end sentinel, so the future must not resolve with it."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.end()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        with pytest.raises(ProtocolError):
            await stream
    finally:
        await pair.stop()


async def test_result_without_a_timeout_is_the_plain_future(make_pair):
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"ok": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        assert await stream.result() == {"ok": True}
    finally:
        await pair.stop()


async def test_stream_repr_names_its_state(pair: Pair):
    stream = pair.dialer.open({"q": 1})
    assert repr(stream) == f"<Stream {stream.id} open>"


# --------------------------------------------------------------------------- cancelled awaits, unsendable codes


async def test_cancelling_an_await_sends_reset_cancelled(make_pair):
    """WSM-ERR-014 for the awaiting shapes, not only for `async for`.

    The rule names `await stream.result()` explicitly: a consumer that walks away from an await
    must not leave the remote producing for nobody.
    """
    for shape in ("result", "await"):
        pair = make_pair()
        pair.acceptor.on_stream(_echo_handler)
        pair.start()
        try:
            stream = pair.dialer.open({"q": 1})
            await pair.settle()

            waiter = asyncio.create_task(stream.result() if shape == "result" else _await_shape(stream))
            await pair.settle()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            await pair.settle()

            codes = [f.code for f in pair.sent_by("dialer") if f.type == "reset"]
            assert int(ResetCode.CANCELLED) in codes, f"{shape}: no reset(CANCELLED) went out"
        finally:
            await pair.stop()


async def _await_shape(stream: Stream) -> Any:
    return await stream


async def test_a_cancelled_await_does_not_destroy_the_memoized_future(make_pair):
    """WSM-API-011: the shield is what keeps a second await from inheriting the first's cancellation."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await asyncio.sleep(0.02)
        await stream.reply({"late": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        waiter = asyncio.create_task(stream.result())
        await pair.settle()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        # The stream is reset by the cancellation, so a second await reports that - not a
        # CancelledError leaked from the first awaiter.
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.CANCELLED
    finally:
        await pair.stop()


@pytest.mark.parametrize("code", [ResetCode.CONNECTION_CLOSED, 5, 42, -1])
async def test_the_application_cannot_put_an_unsendable_code_on_the_wire(code: Any, pair: Pair):
    """§8.1: code 9 is synthesised locally and never sent; 5 is retired; the rest do not exist."""
    stream = pair.dialer.open({"q": 1})
    with pytest.raises(ProtocolError):
        await stream.reset(code)
    await pair.settle()
    assert [f.code for f in pair.sent_by("dialer") if f.type == "reset"] == []


async def test_an_unconsumed_failure_reports_no_never_retrieved_warning(make_pair, capsys):
    """The Python side of WSM-API-016: a failure nobody reads must not become asyncio noise."""
    import gc

    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        waiter = asyncio.create_task(stream.result())
        await pair.settle()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await pair.settle()
    finally:
        await pair.stop()

    del stream, waiter
    gc.collect()
    await asyncio.sleep(0)
    assert "never retrieved" not in capsys.readouterr().err


# --------------------------------------------------------------------------- leading headers


async def test_leading_headers_are_sendable_once_and_only_first(make_pair):
    """WSM-API-024: one chance per peer per stream, and it is spent by the frame."""
    pair = make_pair()
    answered: list[Stream] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        answered.append(stream)
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": "export"}, end=True)
        await pair.settle()
        answering = answered[0]

        await answering.send_headers({"content-type": "text/csv"})
        # The chance is spent by the *frame*, not by the argument: a second set raises whether it
        # rides `send_headers`, `send` or `end`, and a port that only guarded the first would let two
        # sets onto a wire whose rule is one.
        with pytest.raises(ProtocolError):
            await answering.send_headers({"late": True})
        with pytest.raises(ProtocolError):
            await answering.send({"row": 1}, headers={"late": True})
        with pytest.raises(ProtocolError):
            await answering.end(headers={"late": True})

        # Still sendable without them, which is the half a "raises on the second call" check misses:
        # refusing the headers must not have refused the frame.
        await answering.send({"row": 1})
        await pair.settle()
        data = pair.frames_of_type("acceptor", "data")
        assert [frame.headers for frame in data] == [{"content-type": "text/csv"}, None]
        # ABSENT and not None: a headers frame carries no payload key at all (D1).
        assert data[0].payload is ABSENT
    finally:
        await pair.stop()


async def test_leading_headers_on_a_locally_opened_stream_point_at_open(make_pair):
    pair = make_pair()
    pair.acceptor.on_stream(_echo_handler)
    pair.start()
    try:
        # The opener's first frame was the `open`, so its one chance is already spent - and the error
        # has to name `open()` rather than repeat the rule, since the caller has somewhere to put them.
        stream = pair.dialer.open({"q": 1})
        with pytest.raises(ProtocolError, match=r"open\(\)"):
            await stream.send_headers({"trace": "abc"})
    finally:
        await pair.stop()


async def test_leading_headers_arrive_before_the_first_payload(make_pair):
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send_headers({"content-type": "text/csv"})
        await stream.end({"row": 1})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": "export"}, end=True)
        # The ordering is the whole feature: a consumer that learns the content type only after the
        # body has started has learned it too late. `headers_arrived` fires on the frame that carried
        # them, which arrived before the one carrying the payload.
        await asyncio.wait_for(stream.reply_headers_arrived.wait(), 2)
        assert stream.reply_headers == {"content-type": "text/csv"}
        assert await stream == {"row": 1}
    finally:
        await pair.stop()


async def test_leading_headers_can_ride_the_first_payload(make_pair):
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"rows": 2}, headers={"content-type": "application/json"})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1}, end=True)
        assert await stream == {"rows": 2}
        await asyncio.wait_for(stream.reply_headers_arrived.wait(), 2)
        assert stream.reply_headers == {"content-type": "application/json"}
        # One frame, not two: `reply` carried them rather than announcing them separately.
        assert len(pair.frames_of_type("acceptor", "data")) == 1
    finally:
        await pair.stop()


async def test_headers_after_the_first_frame_reset_the_stream_and_nothing_else(make_lone):
    """The receiving half of WSM-FRM-016, which the local handle refuses to produce."""
    lone = make_lone()
    lone.peer.on_stream(_echo_handler)
    lone.start()
    try:
        lone.inject(Frame("open", stream=1, payload={"q": 1}))
        lone.inject(Frame("open", stream=3, payload={"q": 2}))
        lone.inject(Frame("data", stream=1, payload={"chunk": 1}))
        await lone.settle()
        assert lone.frames_of_type("reset") == []

        lone.inject(Frame("data", stream=1, headers={"late": True}, payload={"chunk": 2}))
        await lone.settle()

        resets = lone.frames_of_type("reset")
        assert [(frame.stream, frame.code) for frame in resets] == [(1, int(ResetCode.PROTOCOL_ERROR))]
        # Stream-level: the connection and every other stream on it are untouched (§4.3).
        assert lone.frames_of_type("goaway") == []
        assert 3 in lone.peer.streams
    finally:
        await lone.stop()


async def test_reply_headers_arrived_settles_on_every_path(make_pair):
    """WSM-API-025: including the paths on which no headers will ever come."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.cancel("not answering this one")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        # A remote that resets before answering sends no headers and never will. The rule makes that
        # settle rather than hang: this wait is the assertion, and a port that only set the event on
        # a first frame would fail it by timing out.
        refused = pair.dialer.open({"q": 1})
        await asyncio.wait_for(refused.reply_headers_arrived.wait(), 2)
        assert refused.reply_headers == {}
    finally:
        await pair.stop()


async def test_an_opens_headers_are_the_acceptors_from_construction(make_pair):
    pair = make_pair()
    answered: list[Stream] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        answered.append(stream)
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1}, headers={"trace": "abc123"}, end=True)
        await pair.settle()
        assert answered[0].headers == {"trace": "abc123"}
        # The open's headers are not the answer's: this handler has announced nothing yet.
        assert answered[0].reply_headers == {}
    finally:
        await pair.stop()
