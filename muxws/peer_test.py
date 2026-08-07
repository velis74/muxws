"""`Peer`: id allocation, retention, dispatch, and what socket death does to every shape."""

from __future__ import annotations

import asyncio
import logging
import re

from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ConnectionLost, ProtocolError, RemoteError, ResetCode, StreamReset, StreamTimeout
from muxws.frames import ABSENT, Frame
from muxws.peer import default_error_serializer, Peer
from muxws.stream import Stream
from muxws.transports.memory import memory_pair

# --------------------------------------------------------------------------- ids


async def test_parity_and_monotonicity(make_pair):
    """WSM-SID-002/004: dialer odd, acceptor even, and never reused after close."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.dialer.on_stream(_hold)
    pair.start()
    try:
        assert [pair.dialer.open({}).id for _ in range(3)] == [1, 3, 5]
        assert [pair.acceptor.open({}).id for _ in range(3)] == [2, 4, 6]
        await pair.settle()

        for stream in list(pair.dialer.streams.values()):
            await stream.reset(ResetCode.NO_ERROR)
        await pair.settle()
        assert pair.dialer.open({}).id == 7, "an id is never reused, even after the stream closes"
    finally:
        await pair.stop()


async def test_concurrent_opens_produce_increasing_ids_on_the_wire(make_pair):
    """WSM-SID-006/WSM-INV-005: allocation and enqueue are one indivisible step.

    Fifty opens from fifty separate tasks. If anything suspended between taking the id and queueing
    the frame, the wire would carry a non-monotonic sequence - a protocol error this peer would be
    committing against itself.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:

        async def opener() -> int:
            await asyncio.sleep(0)
            return pair.dialer.open({"n": 1}).id

        ids = await asyncio.gather(*[opener() for _ in range(50)])
        await pair.settle(40)

        on_wire = [frame.stream for frame in pair.frames_of_type("dialer", "open")]
        assert on_wire == sorted(on_wire), "wire order must be allocation order"
        assert on_wire == sorted(ids)[: len(on_wire)]
        assert len(set(ids)) == 50
    finally:
        await pair.stop()


async def test_peer_id_is_prefix_plus_monotonic_counter(make_pair):
    """WSM-API-009: reuse is the failure being prevented, so nothing recycles a closed peer's id."""
    seen: list[str] = []
    for _ in range(100):
        pair = make_pair()
        seen.extend([pair.dialer.id, pair.acceptor.id])

    for peer_id in seen:
        assert re.fullmatch(r"[0-9a-f]{3}-\d+", peer_id), peer_id
    assert len(set(seen)) == len(seen), "ids must not repeat within a process"

    prefixes = {peer_id.split("-")[0] for peer_id in seen}
    assert len(prefixes) == 1, "the prefix is drawn once per process"
    counters = [int(peer_id.split("-")[1]) for peer_id in seen]
    assert counters == sorted(counters), "the counter never rewinds"


# --------------------------------------------------------------------------- retention


async def test_frame_for_closed_id_is_ignored(make_pair):
    """WSM-STM-002: expected during a normal race - no reset, no connection error, at most a counter."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.reset(ResetCode.NO_ERROR)
        await pair.settle()

        before = len(pair.sent_by("dialer"))
        pair.dialer_socket.inject(pair.dialer._codec.encode(Frame("data", stream=stream.id, payload={"late": True})))
        await pair.settle()

        assert pair.dialer.is_open
        assert len(pair.sent_by("dialer")) == before, "nothing goes out for a late frame"
        assert pair.dialer._ignored_late_frames == 1
    finally:
        await pair.stop()


async def test_frame_above_high_water_mark_kills_connection(make_pair):
    """WSM-STM-003/WSM-INV-006: a genuine id-space disagreement must not be swallowed."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        await pair.settle()
        pair.acceptor_socket.inject(pair.acceptor._codec.encode(Frame("data", stream=99, payload={"a": 1})))
        await pair.settle()

        goaway = pair.frames_of_type("acceptor", "goaway")
        assert goaway
        assert goaway[-1].code == int(ResetCode.PROTOCOL_ERROR)
        assert pair.acceptor.is_open is False
    finally:
        await pair.stop()


@pytest.mark.parametrize(
    ("name", "frame"),
    [
        ("wrong-parity", Frame("open", stream=2, payload={})),
        ("not-monotonic", None),
    ],
)
async def test_wrong_parity_and_non_monotonic_open_kill_connection(name: str, frame: Frame | None, make_pair):
    """WSM-SID-005: both are connection-level."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        if name == "not-monotonic":
            pair.acceptor_socket.inject(pair.acceptor._codec.encode(Frame("open", stream=5, payload={})))
            await pair.settle()
            frame = Frame("open", stream=3, payload={})
        pair.acceptor_socket.inject(pair.acceptor._codec.encode(frame))
        await pair.settle()

        assert pair.frames_of_type("acceptor", "goaway"), name
        assert pair.acceptor.is_open is False
    finally:
        await pair.stop()


async def test_data_after_end_resets_only_that_stream(make_pair):
    """WSM-STM-020/021: ILL-S resets that stream and leaves the connection - and others - alone."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        first = pair.dialer.open({"q": 1})
        second = pair.dialer.open({"q": 2})
        await pair.settle()

        codec = pair.dialer._codec
        pair.dialer_socket.inject(codec.encode(Frame("data", stream=first.id, payload={}, end=True)))
        await pair.settle()
        pair.dialer_socket.inject(codec.encode(Frame("data", stream=first.id, payload={"more": True})))
        await pair.settle()

        resets = [f for f in pair.frames_of_type("dialer", "reset") if f.stream == first.id]
        assert resets
        assert resets[-1].code == int(ResetCode.PROTOCOL_ERROR)
        assert pair.dialer.is_open, "the connection survives a stream-level error"
        assert second.state.value != "closed", "other streams are unaffected"
    finally:
        await pair.stop()


async def test_no_bookkeeping_survives_a_closed_stream(make_pair):
    """WSM-STM-001: two high-water integers and the live map, and nothing else."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        for _ in range(200):
            await pair.dialer.request({"q": 1})
        await pair.settle()

        assert pair.dialer.streams == {}
        assert pair.acceptor.streams == {}
        assert isinstance(pair.dialer._highest_local_open, int)
        assert isinstance(pair.acceptor._highest_remote_open, int)
        # Nothing keyed by a closed stream may linger anywhere on the peer.
        for value in vars(pair.dialer).values():
            if isinstance(value, dict) and value is not pair.dialer.tags:
                assert len(value) == 0, "a per-closed-stream map is a per-connection memory leak"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- dispatch


async def test_unknown_frame_type_is_ignored(make_pair, caplog):
    """WSM-FRM-002: dropped, logged once, connection alive, nothing goes out."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        before = len(pair.sent_by("acceptor"))
        with caplog.at_level(logging.INFO, logger="muxws.frames"):
            pair.acceptor_socket.inject('{"type":"widget","stream":1}')
            await pair.settle()

        assert pair.acceptor.is_open
        assert len(pair.sent_by("acceptor")) == before
        assert sum("widget" in record.getMessage() for record in caplog.records) == 1
    finally:
        await pair.stop()


async def test_no_handler_refuses_with_refused(make_pair):
    """WSM-STM-033: nothing ran, so the opener may safely retry elsewhere."""
    pair = make_pair()
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.REFUSED

        resets = pair.frames_of_type("acceptor", "reset")
        assert resets
        assert resets[-1].code == int(ResetCode.REFUSED)
    finally:
        await pair.stop()


async def test_handler_raising_produces_application_error(make_pair):
    """WSM-STM-034/WSM-INV-008: APPLICATION_ERROR **always**, even after the handler already sent.

    REFUSED promises the operation definitively did not happen. A handler that debits an account and
    then raises would, under REFUSED, be inviting the client to retry the debit.
    """
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.send({"partial": True})
        raise ValueError("halfway through")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        with pytest.raises(RemoteError) as info:
            async for _ in stream:
                pass

        assert info.value.code is ResetCode.APPLICATION_ERROR
        resets = pair.frames_of_type("acceptor", "reset")
        assert resets[-1].code == int(ResetCode.APPLICATION_ERROR)
        assert resets[-1].code != int(ResetCode.REFUSED)
        assert resets[-1].reason == "halfway through"
        assert resets[-1].payload == {"type": "ValueError", "message": "halfway through"}
    finally:
        await pair.stop()


async def test_error_serializer_hook_replaces_the_default(make_pair):
    """WSM-ERR-006: the hook's return value becomes the reset's payload; None means send none."""
    pair = make_pair(error_serializer=lambda _exc: {"redacted": True})

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("secret detail")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(RemoteError) as info:
            await pair.dialer.open({"q": 1})
        assert info.value.payload == {"redacted": True}
        assert "secret detail" not in str(pair.frames_of_type("acceptor", "reset")[-1].payload)
    finally:
        await pair.stop()


async def test_error_serializer_returning_none_sends_no_payload(make_pair):
    pair = make_pair(error_serializer=lambda _exc: None)

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("nope")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(RemoteError):
            await pair.dialer.open({"q": 1})
        reset = pair.frames_of_type("acceptor", "reset")[-1]
        assert reset.payload is ABSENT
        assert reset.reason == "nope"
    finally:
        await pair.stop()


async def test_error_serializer_is_per_peer(make_pair):
    """WSM-ERR-008: one process, two peers, two answers to the same exception."""
    redacting = make_pair(error_serializer=lambda _exc: {"redacted": True})
    verbose = make_pair(error_serializer=default_error_serializer)

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("detail")

    for pair in (redacting, verbose):
        pair.acceptor.on_stream(handler)
        pair.start()
    try:
        with pytest.raises(RemoteError) as redacted_info:
            await redacting.dialer.open({})
        with pytest.raises(RemoteError) as verbose_info:
            await verbose.dialer.open({})

        assert redacted_info.value.payload == {"redacted": True}
        assert verbose_info.value.payload == {"type": "ValueError", "message": "detail"}
    finally:
        await redacting.stop()
        await verbose.stop()


async def test_handler_returning_ends_stream_implicitly(make_pair):
    """WSM-STM-035: the opener sees `data(end=true)` without the handler writing one."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        collected = [item async for item in stream]
        await pair.settle()

        assert collected == []
        ends = [f for f in pair.frames_of_type("acceptor", "data") if f.end]
        assert ends
        assert ends[-1].stream == stream.id
    finally:
        await pair.stop()


async def test_second_on_stream_replaces_and_logs(make_pair, caplog):
    """WSM-STM-030: exactly one handler; a second replaces the first and says so."""
    pair = make_pair()
    calls: list[str] = []

    async def first(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        calls.append("first")

    async def second(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        calls.append("second")

    pair.acceptor.on_stream(first)
    with caplog.at_level(logging.WARNING, logger="muxws.frames"):
        pair.acceptor.on_stream(second)
    pair.start()
    try:
        await pair.dialer.notify({"q": 1})
        await pair.settle()
        assert calls == ["second"]
        assert any("on_stream" in record.message for record in caplog.records)
    finally:
        await pair.stop()


async def test_fragmented_open_reaches_handler_whole(make_pair):
    """WSM-STM-031/032: the handler runs once, with the payload reassembled, also on `stream.payload`."""
    pair = make_pair()
    received: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        received.append((payload, stream.payload))

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        codec = pair.acceptor._codec
        whole = {"body": "x" * 40}
        encoded = codec.encode_payload(whole)
        half = len(encoded) // 2
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=1, fragment=encoded[:half], more=True)))
        await pair.settle()
        assert received == [], "a fragmented open must not reach the application in pieces"

        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=1, fragment=encoded[half:], end=True)))
        await pair.settle()

        assert len(received) == 1
        assert received[0] == (whole, whole)
    finally:
        await pair.stop()


async def test_notify_returns_none_and_leaves_no_handle(make_pair):
    """WSM-API-005: no `Stream`, no awaitable, nothing to consume."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        result = await pair.dialer.notify({"event": "tick"})
        assert result is None
        await pair.settle()
        opens = pair.frames_of_type("dialer", "open")
        assert opens[-1].end is True
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- socket death


async def test_socket_death_fails_every_shape(make_pair):
    """WSM-RCN-041/WSM-STM-014 **(spec)**: every shape fails, none hangs.

    The whole assertion block runs under a deadline, so a shape that hangs is reported as a hang -
    a caller who sees no error, no log and no timeout, just a spinner that never stops, is the exact
    failure WSM-INV-011 names.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    closes: list[Any] = []
    pair.dialer.on_close(closes.append)

    async def body() -> None:
        awaited = pair.dialer.open({"shape": "await"})
        iterated = pair.dialer.open({"shape": "iterate"})
        sender = pair.dialer.open({"shape": "send"})
        await pair.settle()

        request_task = asyncio.create_task(pair.dialer.request({"shape": "request"}))
        await_task = asyncio.create_task(_await_shape(awaited))
        iterate_task = asyncio.create_task(_iterate_shape(iterated))
        await pair.settle()

        await pair.dialer_socket.drop()
        await pair.settle()

        with pytest.raises(ConnectionLost):
            await await_task
        with pytest.raises(ConnectionLost):
            await iterate_task
        with pytest.raises(ConnectionLost):
            await request_task
        with pytest.raises(ConnectionLost):
            await sender.send({"late": True})
        with pytest.raises(ConnectionLost):
            await sender.end()

        # cancel() and reset() are no-ops, and every stream reports itself closed.
        await sender.cancel()
        await sender.reset(ResetCode.NO_ERROR)
        for stream in (awaited, iterated, sender):
            assert stream.closed.is_set()

        # A second await gets the same error rather than hanging (WSM-API-010).
        with pytest.raises(ConnectionLost):
            await awaited

    try:
        await asyncio.wait_for(body(), timeout=5.0)
    except asyncio.TimeoutError:  # pragma: no cover - the failure this test exists to report
        pytest.fail("a stream shape hung instead of failing with ConnectionLost (WSM-INV-011)")
    finally:
        await pair.stop()

    assert len(closes) == 1, "on_close fires once per loss, after every stream has failed"
    assert closes[0].will_retry is False


async def _await_shape(stream: Stream) -> Any:
    return await stream


async def _iterate_shape(stream: Stream) -> list[Any]:
    collected: list[Any] = []
    async for item in stream:
        collected.append(item)
    # A clean end would read as "the export finished", which is exactly the lie WSM-RCN-041 forbids.
    raise AssertionError("an async for must raise on socket death, not terminate normally")


async def test_open_while_disconnected_raises_connection_lost(make_pair):
    """WSM-RCN-042/WSM-INV-010: nothing is buffered for a next socket."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        await pair.dialer_socket.drop()
        await pair.settle()

        with pytest.raises(ConnectionLost):
            pair.dialer.open({"q": 1})
        with pytest.raises(ConnectionLost):
            await pair.dialer.notify({"q": 1})
        with pytest.raises(ConnectionLost):
            await pair.dialer.request({"q": 1})
        assert pair.dialer.is_open is False
    finally:
        await pair.stop()


async def test_connection_closed_code_never_appears_on_the_wire(make_pair):
    """Reset code 9 is synthesised locally and MUST NEVER be sent."""
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


# --------------------------------------------------------------------------- observability


async def test_on_frame_sees_both_directions_with_byte_lengths(make_pair):
    """WSM-OBS-003: `(direction, frame, byte_length)`, before encode and after decode."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    seen: list[tuple[str, str, int]] = []
    pair.dialer.on_frame(lambda direction, frame, length: seen.append((direction, frame.type, length)))
    pair.start()
    try:
        await pair.dialer.request({"q": 1})
        await pair.settle()
        assert ("tx", "open", seen[0][2]) == seen[0]
        assert any(direction == "rx" for direction, _, _ in seen)
        assert all(length > 0 for _, _, length in seen)
    finally:
        await pair.stop()


async def test_payload_contents_never_appear_in_a_log_record(make_pair, caplog):
    """WSM-OBS-002: application data routinely contains secrets."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        with caplog.at_level(logging.DEBUG, logger="muxws.frames"):
            await pair.dialer.request({"password": "hunter2-sentinel"})
            await pair.settle()
        for record in caplog.records:
            assert "hunter2-sentinel" not in record.getMessage()
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- helpers


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


async def _reply_now(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.reply({"ok": True})


# --------------------------------------------------------------------------- edges


async def test_roles_are_readable(make_pair):
    pair = make_pair()
    assert pair.dialer.is_dialer is True
    assert pair.acceptor.is_dialer is False
    assert "dialer" in repr(pair.dialer)
    assert "acceptor" in repr(pair.acceptor)


async def test_request_that_ends_without_a_payload_raises(make_pair):
    """A unary call that returned nothing would be indistinguishable from one that returned None."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.end()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(ProtocolError, match="without a payload"):
            await pair.dialer.request({"q": 1})
    finally:
        await pair.stop()


async def test_request_timeout_resets_and_raises(make_pair):
    """WSM-ERR-011: the remote is told to stop working, and the caller is told why."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        with pytest.raises(StreamTimeout):
            await pair.dialer.request({"q": 1}, timeout=0.02)
        await pair.settle()
        assert any(f.type == "reset" and f.code == int(ResetCode.TIMEOUT) for f in pair.sent_by("dialer"))
    finally:
        await pair.stop()


async def test_request_with_a_timeout_that_does_not_expire(make_pair):
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        assert await pair.dialer.request({"q": 1}, timeout=5.0) == {"ok": True}
    finally:
        await pair.stop()


async def test_a_binary_codec_uses_send_bytes():
    """WSM-CDC-002/WSM-API-021: the branch comes from `codec.binary`, never from sniffing."""

    class BinaryCodec(JsonCodec):
        binary = True

        def encode(self, frame: Frame) -> bytes:  # type: ignore[override]
            return super().encode(frame).encode("utf-8")

        def decode(self, message: str | bytes) -> Frame:
            return super().decode(message)

    left, right = memory_pair()
    codec = BinaryCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)
    acceptor.on_stream(_reply_now)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    try:
        assert await dialer.request({"q": 1}) == {"ok": True}
        assert all(isinstance(message, bytes) for message in left.sent)
    finally:
        await left.drop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("frame_type", ["open", "data", "reset"])
async def test_a_stream_frame_with_no_stream_id_kills_the_connection(frame_type: str, make_pair):
    """A stream-level frame that names no stream cannot be attributed to anything."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.acceptor_socket.inject(f'{{"type":"{frame_type}","code":0}}')
        await pair.settle()
        assert pair.acceptor.is_open is False
        assert pair.frames_of_type("acceptor", "goaway")
    finally:
        await pair.stop()


async def test_fragmented_data_reaches_the_consumer_whole(make_pair):
    """The receive side reassembles a fragmented `data` payload exactly as it does an `open` one."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        codec = pair.acceptor._codec
        encoded = codec.encode_payload({"rows": list(range(20))})
        half = len(encoded) // 2
        pair.acceptor._enqueue(Frame("data", stream=stream.id, fragment=encoded[:half], more=True))
        pair.acceptor._enqueue(Frame("data", stream=stream.id, fragment=encoded[half:], end=True))

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        assert await pair.dialer.open({"q": 1}) == {"rows": list(range(20))}
    finally:
        await pair.stop()


async def test_a_fragmented_open_that_ends_on_its_last_fragment(make_pair):
    """`end` rides the closing fragment, so the stream is half-closed the moment it is dispatched."""
    pair = make_pair()
    states: list[str] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        states.append(stream.state.value)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        codec = pair.acceptor._codec
        encoded = codec.encode_payload({"body": "y" * 30})
        half = len(encoded) // 2
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=1, fragment=encoded[:half], more=True)))
        await pair.settle()
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=1, fragment=encoded[half:], end=True)))
        await pair.settle()
        assert states == ["half_closed_remote"]
    finally:
        await pair.stop()


async def test_a_handler_whose_stream_died_mid_flight_ends_quietly(make_pair):
    """WSM-STM-035's implicit end must not itself raise when the stream is already gone."""
    pair = make_pair()
    finished = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await asyncio.sleep(0)
        await stream.reset(ResetCode.NO_ERROR)
        finished.set()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        await pair.settle()
        assert finished.is_set()
        assert pair.acceptor.is_open
    finally:
        await pair.stop()
