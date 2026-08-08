"""Connection liveness and orderly shutdown (§6)."""

from __future__ import annotations

import asyncio
import re

from typing import Any

import pytest

from muxws.errors import ConnectionClosed, ConnectionGoingAway, ConnectionLost, ResetCode, StreamRefused
from muxws.frames import Frame
from muxws.lifecycle import GoawayState, MAX_STREAM_ID, new_nonce, PingRegistry
from muxws.stream import Stream

# --------------------------------------------------------------------------- establishment


async def test_no_frame_is_required_before_any_other(make_pair):
    """WSM-CON-030: no handshake phase. A peer may open a stream on its first frame."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        await pair.dialer.request({"q": 1})
        await pair.settle()

        first_dialer = pair.sent_by("dialer")[0]
        assert first_dialer.type == "open", f"the dialer's first frame was {first_dialer.type!r}"
        first_acceptor = pair.sent_by("acceptor")[0]
        assert first_acceptor.type == "data", f"the acceptor's first frame was {first_acceptor.type!r}"
    finally:
        await pair.stop()


async def test_no_settings_frame_is_ever_emitted(make_pair):
    """WSM-CON-031: no limit, version or capability appears on the wire in any form."""
    forbidden = {
        "settings",
        "ack",
        "protocol_version",
        "extensions",
        "max_frame_bytes",
        "max_concurrent_streams",
        "max_payload_bytes",
    }
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        await pair.dialer.request({"q": 1})
        await pair.dialer.ping()
        await pair.dialer.close()
        await pair.settle()

        for who in ("dialer", "acceptor"):
            for frame in pair.sent_by(who):
                assert frame.type != "settings"
                for word in forbidden:
                    assert not hasattr(frame, word) or getattr(frame, word, None) is None
    finally:
        await pair.stop()


async def test_connection_level_frames_omit_stream(make_pair):
    """WSM-FRM-015: `ping`, `pong` and `goaway` are connection-level."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        await pair.dialer.ping()
        await pair.dialer.close()
        await pair.settle()

        for who in ("dialer", "acceptor"):
            for frame in pair.sent_by(who):
                if frame.type in ("ping", "pong", "goaway"):
                    assert frame.stream in (None, 0), f"{frame.type} carried stream={frame.stream}"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- ping / pong


async def test_ping_nonce_is_echoed_verbatim(make_pair):
    """WSM-CON-010: the exact nonce string, promptly."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        await pair.dialer.ping()
        await pair.settle()

        sent = [f.nonce for f in pair.sent_by("dialer") if f.type == "ping"]
        echoed = [f.nonce for f in pair.sent_by("acceptor") if f.type == "pong"]
        assert sent
        assert echoed == sent
    finally:
        await pair.stop()


async def test_pong_needs_no_application_involvement(make_pair):
    """WSM-CON-010: no `on_stream` handler anywhere, and the ping still round-trips."""
    pair = make_pair()
    pair.start()
    try:
        elapsed = await pair.dialer.ping()
        assert elapsed >= 0
    finally:
        await pair.stop()


async def test_ping_returns_round_trip_time_in_seconds(make_pair):
    """WSM-CON-012: seconds as a float in Python; milliseconds in TypeScript."""
    pair = make_pair()
    pair.start()
    try:
        elapsed = await pair.dialer.ping()
        assert isinstance(elapsed, float)
        # An in-memory round trip is fast. If this were milliseconds the number would be ~1000x this.
        assert 0 <= elapsed < 1.0
    finally:
        await pair.stop()


async def test_ping_timeout_raises_and_does_not_kill_the_connection(make_pair):
    """The call fails; the connection is untouched. M5b turns this into liveness detection."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        # Swallow every pong on the way back in.
        original = pair.dialer._dispatch

        async def swallow_pongs(frame: Frame) -> bool:
            if frame.type == "pong":
                return True
            return await original(frame)

        pair.dialer._dispatch = swallow_pongs  # type: ignore[method-assign]

        with pytest.raises(ConnectionClosed, match="no pong"):
            await pair.dialer.ping(timeout=0.05)

        assert pair.dialer.is_open, "a lost pong is not a lost connection - that is M5b's job"
        pair.dialer._dispatch = original  # type: ignore[method-assign]
        assert await pair.dialer.request({"q": 1}) == {"ok": True}
    finally:
        await pair.stop()


async def test_unknown_pong_nonce_is_ignored(make_pair):
    """A late echo of a ping whose deadline already expired is not an error."""
    pair = make_pair()
    pair.start()
    try:
        pair.dialer_socket.inject(pair.dialer._codec.encode(Frame("pong", nonce="nobody-waited-for-this")))
        await pair.settle()
        assert pair.dialer.is_open
    finally:
        await pair.stop()


async def test_pings_are_keyed_by_nonce_not_by_order(make_pair):
    """Matching by position would credit a late echo to the next ping and invent an RTT."""
    pair = make_pair()
    pair.start()
    try:
        registry = PingRegistry()
        first = registry.open("aaa")
        second = registry.open("bbb")
        assert len(registry) == 2

        assert registry.settle("bbb", 0.5) is True
        assert second.result() == 0.5
        assert not first.done()

        assert registry.settle("zzz", 0.1) is False
        registry.give_up("aaa")
        assert len(registry) == 0
    finally:
        await pair.stop()


def test_a_nonce_is_not_guessable():
    """`secrets`, not `random`: the reconnect jitter in M5b is the opposite case."""
    nonces = {new_nonce() for _ in range(50)}
    assert len(nonces) == 50
    assert all(re.fullmatch(r"[0-9a-f]{16}", nonce) for nonce in nonces)


async def test_socket_death_fails_a_pending_ping(make_pair):
    """Nobody is going to answer, so nobody should keep waiting."""
    pair = make_pair()
    pair.start()
    try:
        original = pair.dialer._dispatch

        async def swallow_pongs(frame: Frame) -> bool:
            return True if frame.type == "pong" else await original(frame)

        pair.dialer._dispatch = swallow_pongs  # type: ignore[method-assign]
        pinging = asyncio.create_task(pair.dialer.ping(timeout=5.0))
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        with pytest.raises((ConnectionLost, ConnectionClosed)):
            await asyncio.wait_for(pinging, timeout=1.0)
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- goaway


async def test_goaway_carries_code_reason_and_last_stream(make_pair):
    """WSM-CON-020: `last_stream` is the **remote's** parity, not ours."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        pair.dialer.open({"q": 2})
        await pair.settle()

        await pair.acceptor.close(ResetCode.NO_ERROR, "shutting down")
        await pair.settle()

        goaway = pair.frames_of_type("acceptor", "goaway")[-1]
        assert goaway.code == int(ResetCode.NO_ERROR)
        assert goaway.reason == "shutting down"
        # The acceptor allocates even ids; the streams it promises to finish are the dialer's odd ones.
        assert goaway.last_stream == 3
        assert goaway.last_stream % 2 == 1, "last_stream must carry the *other* peer's parity"
    finally:
        await pair.stop()


async def test_after_sending_goaway_incoming_opens_are_refused():
    """WSM-CON-021: nothing new runs here, and this peer opens nothing either.

    A lone acceptor with injected frames, rather than a live pair: sending a `goaway` and then
    feeding it an `open` its counterpart never made would leave the two peers disagreeing about the
    id space, and the dialer would kill the connection for a reset it cannot account for - correctly,
    and entirely beside the point being tested here.
    """
    from muxws.codecs.json_ import JsonCodec
    from muxws.peer import Peer
    from muxws.transports.memory import memory_pair

    handled: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        handled.append(payload)
        await stream.reply({"ok": True})

    codec = JsonCodec()
    _, socket = memory_pair()
    acceptor = Peer(socket, codec=codec, is_dialer=False)
    acceptor.on_stream(handler)
    serving = asyncio.create_task(acceptor.serve())
    try:
        acceptor._send_goaway(ResetCode.NO_ERROR, "going")
        for _ in range(12):
            await asyncio.sleep(0)

        socket.inject(codec.encode(Frame("open", stream=1, payload={"q": 1}, end=True)))
        for _ in range(12):
            await asyncio.sleep(0)

        assert handled == [], "a handler ran after this peer said it was going away"
        resets = [codec.decode(m) for m in socket.sent if codec.decode(m).type == "reset"]
        assert resets
        assert resets[-1].code == int(ResetCode.REFUSED), "nothing ran, so the opener may retry elsewhere"

        with pytest.raises(ConnectionGoingAway):
            acceptor.open({"q": 2})
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_open_after_receiving_goaway_raises_synchronously(make_pair):
    """WSM-CON-022/WSM-API-004: out of the call, not out of an await."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.dialer_socket.inject(
            pair.dialer._codec.encode(Frame("goaway", code=int(ResetCode.NO_ERROR), last_stream=0))
        )
        await pair.settle()

        with pytest.raises(ConnectionGoingAway):
            pair.dialer.open({"q": 1})
        with pytest.raises(ConnectionGoingAway):
            await pair.dialer.notify({"q": 1})
    finally:
        await pair.stop()


async def test_streams_above_last_stream_are_reset_refused_locally(make_pair):
    """WSM-CON-023: never processed, so safe to retry - and nothing goes out for them."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        below = pair.dialer.open({"q": 1})
        above = pair.dialer.open({"q": 2})
        await pair.settle()
        before = len(pair.sent_by("dialer"))

        pair.dialer_socket.inject(
            pair.dialer._codec.encode(Frame("goaway", code=int(ResetCode.NO_ERROR), last_stream=below.id))
        )
        await pair.settle()

        with pytest.raises(StreamRefused):
            await above
        assert below.state.value != "closed", "a stream at or below the cut-off keeps going"
        assert len(pair.sent_by("dialer")) == before, "nothing is sent for a stream the remote never saw"
    finally:
        await pair.stop()


async def test_streams_at_or_below_last_stream_finish_within_drain(make_pair):
    """WSM-CON-024: work the remote promised to complete is allowed to complete."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await asyncio.sleep(0.02)
        await stream.reply({"finished": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        closing = asyncio.create_task(pair.acceptor.close(drain=2.0))
        assert await asyncio.wait_for(stream, timeout=1.0) == {"finished": True}
        await closing
    finally:
        await pair.stop()


async def test_drain_deadline_closes_the_socket_with_streams_still_live(make_pair):
    """WSM-CON-024: then the socket closes regardless, and those streams fail locally."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()

        await asyncio.wait_for(pair.acceptor.close(drain=0.05), timeout=2.0)
        await pair.settle()

        assert pair.acceptor.is_open is False
        assert stream.closed.is_set()
    finally:
        await pair.stop()


async def test_close_sends_goaway_then_drains_then_closes(make_pair):
    """WSM-CON-025: assert the order, because the order is the whole rule."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        await pair.dialer.request({"q": 1})
        await pair.dialer.close()
        await pair.settle()

        types = [f.type for f in pair.sent_by("dialer")]
        assert "goaway" in types
        assert types.index("goaway") == len(types) - 1, "goaway must be the last frame out"
        assert pair.dialer_socket.is_closed
        assert pair.dialer.is_open is False
    finally:
        await pair.stop()


async def test_close_is_idempotent(make_pair):
    pair = make_pair()
    pair.start()
    try:
        await pair.dialer.close()
        await pair.dialer.close()
        assert len([f for f in pair.sent_by("dialer") if f.type == "goaway"]) == 1
    finally:
        await pair.stop()


async def test_id_exhaustion_sends_goaway_drains_and_closes(make_pair):
    """WSM-SID-007 asks for four things, not one.

    Refusing to open again is the easy quarter. The rule also requires the exhausting peer to send
    `goaway`, let in-flight streams drain, and then close - none of which `open()` can do by
    returning, and all of which were missing until this test asked for them.
    """
    assert MAX_STREAM_ID == 2**31 - 1

    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        # One id left. Taking it is what triggers the shutdown.
        pair.dialer._next_id = MAX_STREAM_ID
        last = pair.dialer.open({"q": 1}, end=True)
        assert last.id == MAX_STREAM_ID

        # Still synchronous: the shutdown was scheduled, not awaited (WSM-API-001).
        assert pair.dialer._exhaustion_task is not None

        with pytest.raises(ConnectionGoingAway, match="exhausted"):
            pair.dialer.open({"q": 2})

        await asyncio.wait_for(pair.dialer._exhaustion_task, timeout=5.0)
        await pair.settle()

        goaway = pair.frames_of_type("dialer", "goaway")
        assert goaway, "the exhausting peer must say goodbye rather than just refusing"
        assert goaway[-1].code == int(ResetCode.NO_ERROR)
        assert "exhausted" in (goaway[-1].reason or "")
        assert pair.dialer.is_open is False, "and must then close"
        assert pair.dialer_socket.is_closed
    finally:
        await pair.stop()


async def test_the_last_stream_still_gets_its_drain_window(make_pair):
    """The stream allocated at exhaustion is in flight, and in-flight work drains (WSM-SID-007)."""
    pair = make_pair()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await asyncio.sleep(0.02)
        await stream.reply({"finished": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer._next_id = MAX_STREAM_ID
        last = pair.dialer.open({"q": 1}, end=True)
        assert await asyncio.wait_for(last, timeout=2.0) == {"finished": True}
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- the state object


def test_goaway_state_tracks_the_two_directions_separately():
    """Having sent one and having received one mean different things."""
    state = GoawayState()
    assert state.is_going_away is False

    state.sent = True
    assert state.is_going_away is True
    # With nothing received, no cut-off is known and every stream gets its drain window.
    assert state.survives_drain(999) is True

    state.remote_last_stream = 5
    assert state.survives_drain(5) is True
    assert state.survives_drain(7) is False


# --------------------------------------------------------------------------- helpers


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


async def _reply_now(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.reply({"ok": True})
