"""The three receive-side caps, and the observability hooks (§4.3, §12).

All three are local defences. None is announced or has a remote counterpart, and a sender learns of
one only from the reset it causes (WSM-CON-031, WSM-FRG-035, WSM-STM-036).
"""

from __future__ import annotations

import logging

from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ProtocolError, ResetCode, StreamRefused, StreamReset
from muxws.fragment import encoded_length, MAX_FRAME_BYTES, split_frame
from muxws.frames import Frame
from muxws.peer import Peer
from muxws.stream import Stream
from muxws.transports.memory import memory_pair

# --------------------------------------------------------------------------- frame size


async def test_a_message_at_the_constant_is_accepted(make_pair):
    """WSM-FRG-004/031: a receiver MUST NOT reject a message at or below `MAX_FRAME_BYTES`."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        codec = pair.acceptor._codec
        # Sized so the whole encoded message lands just under the constant.
        payload = {"body": "x" * (MAX_FRAME_BYTES - 200)}
        assert encoded_length(codec.encode(Frame("open", stream=1, payload=payload))) <= MAX_FRAME_BYTES

        assert await pair.dialer.request(payload) == {"ok": True}
        assert pair.acceptor.is_open
    finally:
        await pair.stop()


async def test_an_over_cap_message_resets_that_stream_only(make_lone):
    """WSM-FRG-031: the **whole encoded message** is measured, and the connection survives."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_reply_now)
    lone.start()
    try:
        oversize = lone.codec.encode(Frame("open", stream=1, payload={"body": "x" * 4000}))
        assert encoded_length(oversize) > 512
        lone.inject(oversize)
        await lone.settle()

        resets = lone.frames_of_type("reset")
        assert resets
        assert resets[-1].code == int(ResetCode.PAYLOAD_TOO_LARGE)
        assert lone.peer.is_open, "a size violation is stream-level, not connection-level"
    finally:
        await lone.stop()


async def test_the_fragment_field_alone_is_not_what_is_measured(make_lone):
    """WSM-FRG-031 says the entire encoded message, envelope included - never the field."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_hold)
    lone.start()
    try:
        codec = lone.codec
        # A fragment field comfortably under the cap, in an envelope that pushes the message over it.
        fragment = "y" * 400
        frame = Frame("open", stream=1, fragment=fragment, more=True, headers={"h": "z" * 300})
        assert len(fragment) < 512
        assert encoded_length(codec.encode(frame)) > 512

        lone.inject(frame)
        await lone.settle()
        assert lone.frames_of_type("reset")
    finally:
        await lone.stop()


def test_a_cap_below_the_envelope_floor_is_rejected_at_construction():
    """WSM-FRG-034: the constructor raises; a cap below the envelope would make the splitter loop forever."""
    _, socket = memory_pair()
    with pytest.raises(ProtocolError, match="WSM-FRG-034"):
        Peer(socket, codec=JsonCodec(), is_dialer=False, max_frame_bytes=16)


# --------------------------------------------------------------------------- max_payload_bytes


async def test_oversize_payload_resets_on_the_crossing_fragment(make_lone):
    """WSM-FRG-032/WSM-INV-017 **(spec)**: the reset goes out **before the final fragment arrives**.

    The receiver does not assemble the payload to measure it, because assembling it allocates the
    memory the limit protects. The partial buffer is dropped in the same step.
    """
    lone = make_lone(max_frame_bytes=512, max_payload_bytes=2000)
    lone.peer.on_stream(_hold)
    lone.start()
    try:
        parts = split_frame(Frame("open", stream=1, payload={"body": "x" * 9000}, end=True), 512, lone.codec)
        assert len(parts) > 6

        crossing = None
        for index, part in enumerate(parts):
            lone.inject(part)
            await lone.settle()
            if lone.frames_of_type("reset"):
                crossing = index
                break

        assert crossing is not None, "the limit was never enforced"
        assert crossing < len(parts) - 1, "the reset must not wait for the last fragment"
        assert lone.frames_of_type("reset")[-1].code == int(ResetCode.PAYLOAD_TOO_LARGE)
        assert lone.peer.is_open
        assert lone.peer.streams == {}, "the partial buffer must be released with the stream"
    finally:
        await lone.stop()


async def test_max_payload_bytes_defaults_to_64_mib_and_is_never_announced(make_pair):
    """WSM-FRG-035: a local receiver setting, and a sender may learn it only from a reset."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        assert pair.acceptor._max_payload_bytes == 67_108_864
        await pair.dialer.request({"q": 1})
        await pair.settle()
        for who in ("dialer", "acceptor"):
            for frame in pair.sent_by(who):
                assert "67108864" not in str(frame)
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- concurrency


async def test_open_beyond_receiver_limit_is_refused_and_opener_raises_nothing_locally(make_pair):
    """WSM-STM-036 **(spec)**: refused without invoking the handler, and `open()` never fails for it."""
    pair = make_pair(max_concurrent_streams=3)
    handled: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        handled.append(payload)
        await stream.closed.wait()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        for index in range(3):
            pair.dialer.open({"n": index})
        await pair.settle()
        assert len(handled) == 3

        # One more. `open()` must not raise - the limit is the receiver's alone (WSM-API-004).
        extra = pair.dialer.open({"n": 3})
        with pytest.raises(StreamRefused):
            await extra

        assert len(handled) == 3, "the handler must not run for a refused open"
        refusals = [f for f in pair.frames_of_type("acceptor", "reset") if f.code == int(ResetCode.REFUSED)]
        assert refusals
        assert pair.acceptor.is_open
    finally:
        await pair.stop()


async def test_own_opens_do_not_count_against_the_limit(make_pair):
    """WSM-STM-037: the limit bounds work the *remote* can impose."""
    pair = make_pair(max_concurrent_streams=3)
    pair.acceptor.on_stream(_hold)
    pair.dialer.on_stream(_hold)
    pair.start()
    try:
        for index in range(5):
            pair.acceptor.open({"mine": index})
        await pair.settle()

        for index in range(3):
            pair.dialer.open({"theirs": index})
        await pair.settle()
        assert pair.acceptor._remote_stream_count() == 3
        assert len([f for f in pair.frames_of_type("acceptor", "reset") if f.code == int(ResetCode.REFUSED)]) == 0
    finally:
        await pair.stop()


async def test_closing_a_stream_frees_a_slot(make_pair):
    pair = make_pair(max_concurrent_streams=2)
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        first = pair.dialer.open({"n": 0})
        pair.dialer.open({"n": 1})
        await pair.settle()

        refused = pair.dialer.open({"n": 2})
        with pytest.raises(StreamReset):
            await refused
        await first.reset(ResetCode.NO_ERROR)
        await pair.settle()

        accepted = pair.dialer.open({"n": 3})
        await pair.settle()
        assert accepted.id in pair.acceptor.streams
    finally:
        await pair.stop()


async def test_the_limit_is_never_announced_and_never_checked_by_the_sender(make_pair):
    """WSM-STM-036/WSM-INV-007: the sender does not count its opens against the receiver's limit."""
    import inspect

    from muxws import peer as module

    source = inspect.getsource(module.Peer.open)
    assert "max_concurrent" not in source, "open() must not consult the concurrency limit"

    pair = make_pair(max_concurrent_streams=1)
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        for index in range(5):
            pair.dialer.open({"n": index})  # none of these may raise
        await pair.settle()
        for who in ("dialer", "acceptor"):
            for frame in pair.sent_by(who):
                assert "max_concurrent_streams" not in str(frame)
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- observability


async def test_on_frame_fires_before_encode_and_after_decode_with_byte_length(make_pair):
    """WSM-OBS-003: the handler always sees the logical frame, and the encoded length."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    seen: list[tuple[str, Frame, int]] = []
    pair.dialer.on_frame(lambda d, f, n: seen.append((d, f, n)))
    pair.start()
    try:
        await pair.dialer.request({"q": 1})
        await pair.settle()

        codec = pair.dialer._codec
        assert seen[0][0] == "tx"
        assert any(entry[0] == "rx" for entry in seen)
        for direction, frame, length in seen:
            assert isinstance(frame, Frame), "the hook sees the logical frame, not bytes"
            assert length == encoded_length(codec.encode(frame)), f"{direction} length was not the encoded one"
    finally:
        await pair.stop()


async def test_the_frame_log_line_shape_at_debug(make_pair, caplog):
    """WSM-OBS-001: one line per frame, carrying `conn=` so two lines can be tied together."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        with caplog.at_level(logging.DEBUG, logger="muxws.frames"):
            await pair.dialer.request({"q": 1})
            await pair.settle()

        lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("muxws conn=")]
        assert lines
        assert any(f"conn={pair.dialer.id}" in line for line in lines)
        assert any("dir=tx" in line for line in lines)
        assert any("dir=rx" in line for line in lines)
        assert all("bytes=" in line for line in lines)
    finally:
        await pair.stop()


async def test_payload_contents_never_appear_in_any_log_record(make_pair, caplog):
    """WSM-OBS-002: application data routinely contains secrets."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        with caplog.at_level(logging.DEBUG):
            await pair.dialer.request({"password": "hunter2-sentinel"})
            await pair.settle()
        assert caplog.records, "the level was raised, so something must have been logged"
        for record in caplog.records:
            assert "hunter2-sentinel" not in record.getMessage()
    finally:
        await pair.stop()


async def test_window_update_is_never_sent(make_pair):
    """WSM-BPR-001: reserved in v1, and the only flow control is the two local defences."""
    pair = make_pair(max_concurrent_streams=2, max_payload_bytes=4000)
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        for _ in range(6):
            pair.dialer.open({"q": 1})
        await pair.settle()
        for who in ("dialer", "acceptor"):
            assert not [f for f in pair.sent_by(who) if f.type == "window_update"]
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- the id of an over-cap open


async def test_an_over_cap_open_counts_toward_the_high_water_mark(make_lone):
    """WSM-FRG-031/WSM-STM-002: a later frame on the id of an over-cap `open` is ignored, not a WSM-STM-003 error."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_reply_now)
    lone.start()
    try:
        lone.inject(Frame("open", stream=1, payload={"body": "x" * 4000}))
        await lone.settle()
        lone.inject(Frame("data", stream=1, payload={"chunk": 1}, end=True))
        await lone.settle()

        assert [(f.stream, f.code) for f in lone.frames_of_type("reset")] == [(1, int(ResetCode.PAYLOAD_TOO_LARGE))]
        assert not lone.frames_of_type("goaway")
        assert lone.peer.is_open

        lone.inject(Frame("open", stream=3, payload={"q": 1}, end=True))
        await lone.settle()
        assert [f.payload for f in lone.frames_of_type("data") if f.stream == 3] == [{"ok": True}]
    finally:
        await lone.stop()


@pytest.mark.parametrize("stream_id", [2, 1], ids=["wrong-parity", "not-above-the-previous-open"])
async def test_an_over_cap_open_with_an_invalid_id_is_a_connection_error(make_lone, stream_id):
    """WSM-SID-005: the id of an over-cap `open` is validated, and an invalid one ends the connection."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_reply_now)
    lone.start()
    try:
        lone.inject(Frame("open", stream=1, payload={"q": 1}, end=True))
        await lone.settle()
        assert lone.peer.streams == {}
        lone.inject(Frame("open", stream=stream_id, payload={"body": "x" * 4000}))
        await lone.settle()

        assert [f.code for f in lone.frames_of_type("goaway")] == [int(ResetCode.PROTOCOL_ERROR)]
        assert not lone.peer.is_open
    finally:
        await lone.stop()


async def test_an_over_cap_frame_on_a_closed_id_is_ignored(make_lone):
    """WSM-STM-002: an over-cap `data` on the id of a rejected `open` is ignored, with no second reset."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_reply_now)
    lone.start()
    try:
        lone.inject(Frame("open", stream=1, payload={"body": "x" * 4000}))
        await lone.settle()
        lone.inject(Frame("data", stream=1, payload={"body": "x" * 4000}, end=True))
        await lone.settle()

        assert [(f.stream, f.code) for f in lone.frames_of_type("reset")] == [(1, int(ResetCode.PAYLOAD_TOO_LARGE))]
        assert not lone.frames_of_type("goaway")
        assert lone.peer.is_open
    finally:
        await lone.stop()


async def test_an_over_cap_frame_above_the_high_water_mark_is_a_connection_error(make_lone):
    """WSM-STM-003: an over-cap `data` on an id the remote never opened ends the connection."""
    lone = make_lone(max_frame_bytes=512)
    lone.peer.on_stream(_reply_now)
    lone.start()
    try:
        lone.inject(Frame("data", stream=5, payload={"body": "x" * 4000}))
        await lone.settle()

        assert not lone.frames_of_type("reset")
        assert [f.code for f in lone.frames_of_type("goaway")] == [int(ResetCode.PROTOCOL_ERROR)]
        assert not lone.peer.is_open
    finally:
        await lone.stop()


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


async def _reply_now(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.reply({"ok": True})
