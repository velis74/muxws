"""`Peer`: id allocation, retention, dispatch, and what socket death does to every shape."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import logging
import re

from collections.abc import Awaitable, Callable
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


async def test_closing_a_peer_does_not_free_its_id(make_pair):
    """WSM-API-009's fourth clause. Reuse is the failure being prevented, so it needs a closed peer.

    Counting distinct ids among peers that were never closed cannot fail against a scheme that
    recycles the ids of closed connections - which is precisely the scheme the rule forbids.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    closed_ids = {pair.dialer.id, pair.acceptor.id}
    await pair.stop()

    assert pair.dialer.is_open is False
    assert pair.acceptor.is_open is False

    fresh = make_pair()
    fresh_ids = {fresh.dialer.id, fresh.acceptor.id}
    assert closed_ids.isdisjoint(fresh_ids), "a closed peer's id must not be reissued"
    assert min(int(i.split("-")[1]) for i in fresh_ids) > max(int(i.split("-")[1]) for i in closed_ids)


#: Every spelling a caller might reach for if they wanted to name a stream themselves.
_ID_ARGUMENT_NAMES = frozenset({"id", "sid", "stream", "stream_id", "streamid"})

#: The public callables that take an argument by one of those names, and why each is not the rule's
#: subject. Held as a set so a **new** one fails here rather than being discovered by a peer that
#: receives an id its counterpart never allocated.
#:
#: - `Frame` is the wire envelope itself, not a way to open a stream. The conformance runner and the
#:   injection fixtures build frames by hand precisely because that is not the application's API.
#: - `stream_id` on the `StreamReset` family is an **output**: which stream failed. Nothing is
#:   allocated by naming it, and an error that could not say would be useless.
#: - `Stream.__init__` is the library's own constructor, reached only with a `Peer` in hand; every
#:   caller-facing route to a `Stream` (`open`, `notify`, `request`, and dispatch to `on_stream`)
#:   allocates the id itself. See the note in SPEC.md's Appendix B: this is the one place the rule's
#:   absolute wording and the implementation do not quite meet, and it is deliberately pinned rather
#:   than left to be rediscovered.
_ID_ARGUMENTS_THAT_ARE_NOT_AN_ALLOCATION = frozenset(
    {
        ("ConnectionLost.__init__", "stream_id"),
        ("Frame.__init__", "stream"),
        ("RemoteError.__init__", "stream_id"),
        ("Stream.__init__", "stream_id"),
        ("StreamRefused.__init__", "stream_id"),
        ("StreamReset.__init__", "stream_id"),
        ("StreamTimeout.__init__", "stream_id"),
    }
)


def _public_callables() -> list[tuple[str, Any]]:
    """`(qualified name, callable)` for everything reachable from `muxws.__all__`."""
    import muxws

    found: list[tuple[str, Any]] = []
    for name in muxws.__all__:
        obj = getattr(muxws, name)
        if inspect.isclass(obj):
            for attr in dir(obj):
                if attr.startswith("_") and attr != "__init__":
                    continue
                member = inspect.getattr_static(obj, attr, None)
                if isinstance(member, property) or not callable(member):
                    continue
                found.append((f"{name}.{attr}", getattr(obj, attr)))
        elif callable(obj):
            found.append((name, obj))
    return found


def test_no_public_entry_point_lets_a_caller_supply_a_stream_id():
    """WSM-SID-001: the library allocates ids; no API anywhere takes one.

    `test_parity_and_monotonicity` covers the allocator, which is the half that has a value to
    compare. This is the other half, and it has no value at all - it is an argument that must not
    exist - so the only way to assert it is over the signatures. A `peer.open(stream_id=...)` added
    for a test harness would satisfy every behavioural test in this file: the parity of an id the
    caller chose is the caller's business, and monotonicity would hold for as long as the caller
    kept counting upwards.
    """
    found = {
        (qualified, parameter)
        for qualified, function in _public_callables()
        for parameter in _signature_of(function).parameters
        if parameter.lower() in _ID_ARGUMENT_NAMES
    }
    assert found == _ID_ARGUMENTS_THAT_ARE_NOT_AN_ALLOCATION


def _signature_of(function: Any) -> inspect.Signature:
    try:
        return inspect.signature(function)
    except (TypeError, ValueError):  # a C-level callable has none to read
        return inspect.Signature()


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
        # Nothing keyed by a closed stream may linger anywhere on the peer, in a container of any
        # kind. Checking only dicts would pass a `self._seen_ids: set[int]` growing without bound.
        for name, value in vars(pair.dialer).items():
            if name == "tags" or not isinstance(value, (dict, list, set, frozenset, tuple)):
                continue
            assert len(value) == 0, f"peer.{name} still holds {len(value)} entries after 200 closed streams"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- dispatch


async def test_unknown_frame_type_is_ignored(make_pair, caplog):
    """WSM-FRM-002: dropped, logged once, connection alive, nothing goes out.

    All three halves in one test, because any one of them alone passes for the wrong reason: a peer
    that never received the frame is also alive and also quiet, and a peer that answered it with a
    `reset` also logged something. The arrival witness is what rules the first out - `on_frame` fires
    after decode and before dispatch (WSM-OBS-003), so it sees exactly the frame dispatch is about to
    be asked to ignore.

    `stream=1` is deliberate. It is above this acceptor's high-water mark, so an implementation that
    let an unrecognised type fall through to the stream-frame path would kill the connection
    (WSM-STM-003) rather than fail some subtler assertion.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    arrived: list[Frame] = []
    pair.acceptor.on_frame(lambda direction, frame, _length: arrived.append(frame) if direction == "rx" else None)
    pair.start()
    try:
        before = pair.sent_by("acceptor")
        with caplog.at_level(logging.INFO, logger="muxws.frames"):
            pair.acceptor_socket.inject('{"type":"widget","stream":1}')
            await pair.settle()

        assert [frame.type for frame in arrived] == ["widget"], "the frame must actually have arrived"
        assert pair.acceptor.is_open
        assert pair.sent_by("acceptor") == before, "an unknown type is never answered"
        assert pair.acceptor.streams == {}, "nor does it open anything"

        lines = [record for record in caplog.records if "widget" in record.getMessage()]
        assert len(lines) == 1, [record.getMessage() for record in lines]
        # WSM-OBS-001 puts every frame line under one named logger; a line nobody can filter on is
        # not the "logging once" the rule asks for.
        assert lines[0].name == "muxws.frames"
        assert lines[0].levelno == logging.INFO
    finally:
        await pair.stop()


async def test_unknown_envelope_field_is_ignored(make_lone):
    """WSM-FRM-001: a field this generation has never heard of changes nothing whatever.

    `frames_test.py` proves the decoder drops the key. That is not the rule: a receiver could drop it
    and still behave differently - refuse the frame, log a warning, take a slower path. So this
    replays the same exchange twice, once with `"colour": "red"` on the `open` and once without, and
    compares the two peers' **whole wires**. Equality across the two runs is the only formulation
    that cannot pass while the extra field is quietly acted on somewhere downstream, and it is what
    WSM-CON-009 rests on - additive revisions are safe to receive precisely because of this.

    A `Lone` peer rather than a pair: a real counterpart would see the reply to a stream it never
    opened and kill the connection, correctly and entirely beside the point.
    """
    with_field = '{"type":"open","stream":1,"payload":{"q":1},"end":true,"colour":"red"}'
    without = '{"type":"open","stream":1,"payload":{"q":1},"end":true}'

    async def replay(message: str) -> tuple[list[str | bytes], list[Frame], list[Any], bool]:
        lone = make_lone()
        seen: list[Any] = []
        arrived: list[Frame] = []

        async def handler(payload: Any, stream: Stream) -> None:
            seen.append(payload)
            await stream.reply({"ok": True})

        lone.peer.on_stream(handler)
        lone.peer.on_frame(lambda direction, frame, _length: arrived.append(frame) if direction == "rx" else None)
        lone.start()
        try:
            lone.inject(message)
            await lone.settle()
            return list(lone.socket.sent), arrived, seen, lone.peer.is_open
        finally:
            await lone.stop()

    tainted_wire, tainted_rx, tainted_seen, tainted_alive = await replay(with_field)
    clean_wire, clean_rx, clean_seen, clean_alive = await replay(without)

    # Non-vacuity: two silent peers also have equal wires. The handler must have run and answered.
    assert clean_seen == [{"q": 1}]
    assert clean_wire, "the control exchange produced no frames at all"
    assert tainted_alive is clean_alive is True
    assert tainted_seen == clean_seen, "the payload the handler saw"
    assert tainted_wire == clean_wire, "an unknown envelope field changed what went out"
    # The wire is the loudest witness but not the finest: a decoder that let the extra key disturb a
    # *quiet* field - `end`, `more`, `code` - can produce an identical answer to this one exchange and
    # a different one to the next. `on_frame` fires after decode and before dispatch (WSM-OBS-003),
    # so this compares the frames the peer actually acted on, field for field.
    assert tainted_rx == clean_rx, "an unknown envelope field changed the frame the peer dispatched"


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


@dataclasses.dataclass
class _Rig:
    """What one shape is handed: a peer, a way to say it is armed, and the death it is waiting on."""

    name: str
    peer: Peer
    armed: asyncio.Event
    dropped: asyncio.Event
    watched: list[Stream]
    stream: Stream | None = None

    def open(self) -> Stream:
        self.stream = self.peer.open({"shape": self.name})
        self.watched.append(self.stream)
        return self.stream

    def arm(self) -> None:
        """Say this shape is in position. The socket is not dropped until every shape has said so."""
        self.armed.set()


async def _await_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    await stream


async def _result_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    await stream.result()


async def _iterate_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    async for _item in stream:
        pass
    # A clean end would read as "the export finished", which is exactly the lie WSM-RCN-041 forbids.
    raise AssertionError("an async for must raise on socket death, not terminate normally")


async def _request_shape(rig: _Rig) -> None:
    rig.arm()
    await rig.peer.request({"shape": rig.name})


async def _send_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    while True:
        # Mid-send when the socket dies, rather than sending once and waiting: a sender that had
        # already stopped would be testing `send()` on a closed stream, which is a different rule.
        await stream.send({"shape": rig.name})
        await asyncio.sleep(0)


async def _cancel_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    await rig.dropped.wait()
    await stream.cancel()
    await stream.reset(ResetCode.NO_ERROR)


#: Every shape WSM-RCN-041 names, with what the socket dying under it must do to that shape. **This
#: map is the only place they are listed**: the runner below opens one stream per entry, waits for
#: all of them to be armed, kills the socket once, and checks each outcome against the expectation
#: recorded here - so adding a seventh shape is one line in one place (§6).
_DEATH_SHAPES: dict[str, tuple[Callable[[_Rig], Awaitable[None]], type[BaseException] | None]] = {
    "await stream": (_await_shape, ConnectionLost),
    "await stream.result()": (_result_shape, ConnectionLost),
    "async for": (_iterate_shape, ConnectionLost),
    "in-flight request()": (_request_shape, ConnectionLost),
    "sender mid-send()": (_send_shape, ConnectionLost),
    # `cancel()` and `reset()` are the shapes WSM-RCN-041 makes **no-ops** rather than failures, so
    # `None` is the expectation. They belong here anyway: "does not raise" is worth nothing if it
    # hangs, and hanging is what this test detects.
    "a stream awaiting cancel()": (_cancel_shape, None),
}


async def test_socket_death_fails_every_shape(make_pair):
    """WSM-RCN-041/WSM-STM-014 **(spec)**: every shape ends, none hangs.

    One stream per shape, all of them live at the same instant, and one socket death underneath all
    of them at once. What each shape must do is read from `_DEATH_SHAPES` rather than written out
    here, so the list of shapes has exactly one home.

    **This test fails by hang detection.** A shape that never finishes is named in the failure,
    because the failure WSM-INV-011 describes is not an exception: it is a caller who sees no error,
    no log and no timeout, just a spinner that never stops. Asserting on an exception nobody raised
    would report that as a deadline somewhere else entirely, long after the fact, without saying
    which of the six shapes was the one that never came back.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()

    closes: list[Any] = []
    #: What the world looked like at the instant `on_close` ran. WSM-STM-014 puts the synthesised
    #: failures *before* it, so a handler that fired first would observe streams still live - and
    #: counting the calls alone cannot tell the two orderings apart.
    world_at_close: list[dict[str, Any]] = []
    watched: list[Stream] = []
    dropped = asyncio.Event()

    def record(reason: Any) -> None:
        closes.append(reason)
        world_at_close.append(
            {
                "live_streams": len(pair.dialer.streams),
                "all_closed": [stream.closed.is_set() for stream in watched],
                "states": sorted({stream.state.value for stream in watched}),
            }
        )

    pair.dialer.on_close(record)
    rigs = {name: _Rig(name, pair.dialer, asyncio.Event(), dropped, watched) for name in _DEATH_SHAPES}
    tasks = {name: asyncio.create_task(shape(rigs[name])) for name, (shape, _) in _DEATH_SHAPES.items()}

    try:
        await asyncio.wait_for(asyncio.gather(*(rig.armed.wait() for rig in rigs.values())), 5.0)
        await pair.settle()

        await pair.dialer_socket.drop()
        dropped.set()
        await pair.settle()

        _, pending = await asyncio.wait(tasks.values(), timeout=5.0)
        if pending:  # pragma: no cover - the failure this test exists to report
            hung = sorted(name for name, task in tasks.items() if task in pending)
            for task in pending:
                task.cancel()
            pytest.fail(f"these shapes hung instead of ending when the socket died: {hung} (WSM-INV-011)")

        for name, (_, expected) in _DEATH_SHAPES.items():
            outcome = tasks[name].exception()
            if expected is None:
                assert outcome is None, f"{name} must be a no-op after death, and it raised {outcome!r}"
            else:
                assert isinstance(outcome, expected), f"{name} ended with {outcome!r}, not {expected.__name__}"

        # The memoized future was resolved once, so a second await gets the same error (WSM-API-010).
        with pytest.raises(ConnectionLost):
            await rigs["await stream"].stream
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await pair.stop()

    assert len(closes) == 1, "on_close fires once per loss, after every stream has failed"
    assert closes[0].will_retry is False
    snapshot = world_at_close[0]
    assert snapshot["live_streams"] == 0, "on_close ran while the peer still held live streams"
    assert all(snapshot["all_closed"]), f"on_close ran before every stream closed: {snapshot}"
    assert snapshot["states"] == ["closed"], snapshot


async def test_nothing_attempted_while_disconnected_appears_on_the_new_socket(make_pair):
    """WSM-RCN-042/WSM-INV-010 **(spec)**: nothing is buffered for a next socket.

    Every call an application can make during the gap raises, and the socket that ends the gap
    carries none of them. A queue that flushed into a server which has forgotten the sender turns a
    failure that would have reached a call site into silent misdelivery - which no test on the
    calling side can detect, because the call site was told nothing.
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

        # And the id space starts empty rather than resuming where the gap left off (WSM-RCN-031).
        assert pair.dialer.open({"after": "the gap"}).id == 1
        await pair.settle()
        assert [pair.dialer._codec.decode(message).payload for message in fresh.sent] == [{"after": "the gap"}]

        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
    finally:
        await pair.stop()


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
    """Reset code 9 is synthesised locally and MUST NEVER be sent.

    Reading the socket alone cannot prove this, and that is worth saying: the socket the streams died
    on is dead, so a peer that *did* synthesise a `reset(9)` for each of them would enqueue every one
    and put none of them on any wire this test can read. The assertion has to be what was **queued**,
    which is the last point at which the decision is still this peer's own.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    queued: list[Frame] = []

    def watch(peer: Any) -> None:
        original = peer._enqueue

        def record(frame: Frame) -> None:
            queued.append(frame)
            original(frame)

        peer._enqueue = record

    watch(pair.dialer)
    watch(pair.acceptor)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        await pair.settle()
        await pair.dialer_socket.drop()
        await pair.settle()

        assert any(frame.type == "open" for frame in queued), "the watch has to be on the real path"
        for frame in queued:
            assert frame.code != int(ResetCode.CONNECTION_CLOSED), f"code 9 was queued for the wire: {frame}"
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


async def test_a_throwing_on_frame_handler_does_not_break_the_connection(make_pair, caplog):
    """WSM-OBS-003: an observer must not be able to break what it observes.

    `on_frame` is called from inside the read loop and from inside the write loop. A handler that
    raised - a metrics counter, a debug print with a bad format string - took the read loop down with
    it, which the peer then reports as a socket that died. The connection an application was watching
    is ended by the watching.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    seen: list[str] = []

    def explode(_direction: str, _frame: Frame, _length: int) -> None:
        raise RuntimeError("the application's frame counter is broken")

    pair.dialer.on_frame(explode)
    pair.dialer.on_frame(lambda direction, _frame, _length: seen.append(direction))
    pair.start()
    try:
        with caplog.at_level("ERROR"):
            answer = await pair.dialer.request({"q": 1})

        assert answer == {"ok": True}, "the connection carried on working"
        assert pair.dialer.is_open is True
        assert {"tx", "rx"} <= set(seen), "and the handler behind the broken one still ran"
        assert any("on_frame handler raised" in record.getMessage() for record in caplog.records)
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


async def test_an_unknown_reset_code_resets_the_stream_without_killing_the_peer(make_pair):
    """A peer of another generation - or one still using the retired 5 - must be heard, not crashed on.

    Regression: converting the wire value straight to `ResetCode` raised `ValueError` out of the read
    loop, which left `is_open` true, `on_close` unfired, and every pending await hanging against a
    queue no writer was draining.
    """
    for wire_code in (5, 42):
        pair = make_pair()
        pair.acceptor.on_stream(_hold)
        pair.start()
        closes: list[Any] = []
        pair.dialer.on_close(closes.append)
        try:
            stream = pair.dialer.open({"q": 1})
            await pair.settle()
            pair.dialer_socket.inject(f'{{"type":"reset","stream":{stream.id},"code":{wire_code},"reason":"old peer"}}')
            await pair.settle()

            assert pair.dialer.is_open, f"code {wire_code} must not kill the connection"
            assert closes == []
            with pytest.raises(StreamReset) as info:
                await stream
            assert info.value.code == wire_code
            assert stream.closed.is_set()
        finally:
            await pair.stop()


async def test_a_read_loop_failure_closes_the_peer_rather_than_zombifying_it(make_pair):
    """A peer that reports itself open while its read loop is dead is worse than one that closed."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    closes: list[Any] = []
    pair.dialer.on_close(closes.append)
    try:

        async def exploding(_frame: Frame) -> bool:
            raise RuntimeError("simulated bug in dispatch")

        pair.dialer._dispatch = exploding  # type: ignore[method-assign]
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        pair.dialer_socket.inject('{"type":"data","stream":1,"payload":{}}')
        await pair.settle()

        assert pair.dialer.is_open is False
        assert len(closes) == 1
        assert stream.closed.is_set()
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- audit regressions


async def test_a_wrong_parity_open_cannot_pose_as_a_fragment_continuation(make_pair):
    """WSM-SID-005: parity is checked before anything else, whatever reassembly is in flight.

    Regression: "is this a continuation?" was inferred from whether *some* assembler was running on
    that id. A stream this peer opened, receiving fragmented `data`, therefore accepted an `open`
    carrying our own parity as a continuation - dispatching a handler for a stream we opened, and
    skipping the connection-level error the rule requires.
    """
    pair = make_pair()
    handled: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = stream
        handled.append(payload)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        codec = pair.acceptor._codec
        pair.acceptor.open({"mine": True})  # the acceptor's own stream 2
        await pair.settle()

        encoded = codec.encode_payload({"x": 1})
        pair.acceptor_socket.inject(codec.encode(Frame("data", stream=2, fragment=encoded[:3], more=True)))
        await pair.settle()
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=2, fragment=encoded[3:])))
        await pair.settle()

        assert handled == [], "a handler ran for a stream the local peer opened"
        assert pair.acceptor.is_open is False
        assert pair.frames_of_type("acceptor", "goaway")
    finally:
        await pair.stop()


async def test_a_duplicate_open_is_still_a_connection_error_mid_reassembly(make_pair):
    """The same hole from the other side: a re-used id must not be laundered by a fragment."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        codec = pair.acceptor._codec
        encoded = codec.encode_payload({"x": 1})
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=3, fragment=encoded[:3], more=True)))
        await pair.settle()
        pair.acceptor_socket.inject(codec.encode(Frame("open", stream=1, fragment=encoded[3:])))
        await pair.settle()

        assert pair.acceptor.is_open is False, "an open below the high-water mark must be ILL-C"
    finally:
        await pair.stop()


async def test_an_unencodable_frame_fails_its_stream_without_wedging_the_connection(make_pair):
    """Regression: a codec that could not encode a frame took the writer task down in silence.

    Nothing drained the queue afterwards, every later send sat in it forever, and the peer went on
    reporting itself open - the same shape as the read-loop zombie, from the other end.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        # Bytes are not a payload type under JSON, and the codec refuses rather than base64-ing.
        await stream.send({"blob": b"\x00\xff"})
        await pair.settle()

        assert pair.dialer.is_open, "one unencodable frame must not end the connection"
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.INTERNAL_ERROR

        # The writer is still alive: a later request still completes.
        assert await pair.dialer.request({"q": 2}) == {"ok": True}
    finally:
        await pair.stop()


async def test_an_error_serializer_that_raises_still_produces_the_reset(make_pair):
    """WSM-STM-034 is unconditional - a broken hook must not turn a failure into silence."""

    def exploding(_exc: BaseException) -> Any:
        raise RuntimeError("the serializer itself is broken")

    pair = make_pair(error_serializer=exploding)

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("handler said no")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        with pytest.raises(RemoteError) as info:
            await pair.dialer.request({"q": 1})
        assert info.value.code is ResetCode.APPLICATION_ERROR
        reset = pair.frames_of_type("acceptor", "reset")[-1]
        assert reset.code == int(ResetCode.APPLICATION_ERROR)
        assert reset.reason == "handler said no"
        assert reset.payload is ABSENT
    finally:
        await pair.stop()


async def test_repeated_sends_on_a_reset_stream_do_not_grow_one_traceback(make_pair):
    """Re-raising the stored instance appends a frame to its traceback on every call."""
    import traceback

    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        await pair.settle()
        await stream.cancel()

        depths: list[int] = []
        for _ in range(5):
            try:
                await stream.send({"late": True})
            except StreamReset as exc:
                depths.append(len(traceback.extract_tb(exc.__traceback__)))
        assert len(set(depths)) == 1, f"the traceback grows on every raise: {depths}"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- what muxws refuses to interpret
#
# Four rules whose whole content is an absence: no argument, no default, no meaning read out of a
# header or a payload key. None of them can be witnessed by watching a correct exchange succeed,
# because they are all satisfied by an implementation that does the extra thing and gets away with
# it. Each test below therefore either reads a signature or compares two runs that must not differ.


async def test_open_takes_zero_mandatory_arguments_and_defaults_payload_to_null(make_pair):
    """WSM-API-003: `peer.open()` on its own is legal, and the payload it sends is an explicit null.

    A promise to every caller, and the reason it needs a witness is that it is the sort of thing a
    later refactor makes required without noticing - `open(payload)` positional-and-mandatory reads
    perfectly well in every call site the suite already has, because every one of them passes a
    payload. The default is also *not* `ABSENT`: WSM-API-003 says `null`, and a receiver that
    distinguishes "no payload key" from "payload: null" (D1) would see the two as different frames.
    """
    parameters = inspect.signature(Peer.open).parameters
    mandatory = [
        name
        for name, parameter in parameters.items()
        if name != "self" and parameter.default is inspect.Parameter.empty
    ]
    assert mandatory == [], f"open() must be callable with nothing: {mandatory} are still required"
    assert parameters["payload"].default is None
    assert parameters["payload"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD

    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open()
        assert isinstance(stream, Stream)
        await pair.settle()
        [opened] = pair.frames_of_type("dialer", "open")
        assert opened.payload is None, "the default is null on the wire, not an omitted key"
        assert opened.headers is None
        assert opened.end is False
    finally:
        await pair.stop()


async def test_request_arms_no_deadline_unless_it_is_given_one(make_pair, monkeypatch):
    """WSM-ERR-010: `request()` has no default timeout, and nothing quietly supplies one.

    The behavioural half alone cannot fail: a default of thirty seconds is indistinguishable from no
    default in any test anybody would be willing to wait for. So this watches the deadline machinery
    itself. `_collect_unary` reaches `asyncio.wait_for` only on the `timeout is not None` branch, and
    the spy below records every arming - with the second half of the test as its control, because a
    spy that never fires is exactly as convincing about a default as no spy at all.

    What a default would cost, and why it is worth a test: a slow handler and a caller who never
    passed a timeout would get `StreamTimeout` out of a call site that mentions no deadline, and the
    remote would get `reset(TIMEOUT)` telling it to abandon work nobody cancelled.
    """
    timeout_parameter = inspect.signature(Peer.request).parameters["timeout"]
    assert timeout_parameter.default is None
    assert timeout_parameter.kind is inspect.Parameter.KEYWORD_ONLY

    armed: list[float | None] = []
    real_wait_for = asyncio.wait_for

    async def spy(awaitable: Any, timeout: Any = None, *args: Any, **options: Any) -> Any:
        armed.append(timeout)
        return await real_wait_for(awaitable, timeout, *args, **options)

    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        monkeypatch.setattr(asyncio, "wait_for", spy)
        pending = asyncio.ensure_future(pair.dialer.request({"q": 1}))
        await pair.settle(40)
        await asyncio.sleep(0.05)

        assert armed == [], f"a deadline was armed for a caller who asked for none: {armed}"
        assert not pending.done(), "request() must wait until the stream ends, is reset, or dies"
        assert pair.frames_of_type("dialer", "reset") == [], "nothing timed out, so nothing was reset"

        # The control. Without it every assertion above is also satisfied by a spy wired to nothing.
        with pytest.raises(StreamTimeout):
            await pair.dialer.request({"q": 2}, timeout=0.02)
        assert armed == [0.02]

        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
    finally:
        monkeypatch.undo()
        await pair.stop()


#: Per-stream headers an implementation might be tempted to act on: two spellings of the same
#: standard credential header, a cookie, an API key, a structured credential, an expiry that has
#: already passed, and a scope list. Anything muxws re-authenticated with, or normalised, or
#: stripped before handing the stream to the application, fails the assertions below.
_HEADERS_THAT_LOOK_LIKE_AUTHENTICATION: dict[str, Any] = {
    "authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.7T4",
    "Authorization": "a second spelling, because muxws does not fold case either",
    "cookie": "session=8f14e45fceea167a; Path=/; HttpOnly",
    "x-api-key": "0123456789abcdef",
    "credential": {"kind": "mtls", "fingerprint": "de:ad:be:ef"},
    "expires_at": 0,
    "scope": ["read", "write"],
}


async def test_per_stream_headers_arrive_unchanged_and_change_nothing(make_pair):
    """WSM-AUT-002: headers are the application's, and muxws does not read them.

    Two exchanges, identical but for the headers, and the wire is compared frame for frame with the
    `headers` field taken off. That comparison is the rule: "muxws MUST NOT interpret them" means
    the peer that was handed them behaves exactly like the peer that was not. Asserting only that
    the handler received them back would pass against a peer that also re-authenticated on
    `authorization`, refused on the expired `expires_at`, or lower-cased every key on the way
    through - and `expires_at: 0` is there because a library that had opinions about expiry would
    have them about that value first.
    """
    seen: list[dict[str, Any]] = []

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        seen.append(stream.headers)
        await stream.reply({"ok": True})

    async def exchange(headers: dict[str, Any] | None) -> tuple[Any, list[Frame], list[Frame]]:
        pair = make_pair()
        pair.acceptor.on_stream(handler)
        pair.start()
        try:
            result = await pair.dialer.request({"q": 1}, headers=headers)
            await pair.settle()
            return result, pair.sent_by("dialer"), pair.sent_by("acceptor")
        finally:
            await pair.stop()

    with_headers, dialer_with, acceptor_with = await exchange(_HEADERS_THAT_LOOK_LIKE_AUTHENTICATION)
    without, dialer_without, acceptor_without = await exchange(None)

    # Delivered whole: every key, both spellings, the nested object and the list unflattened.
    assert seen[0] == _HEADERS_THAT_LOOK_LIKE_AUTHENTICATION
    assert list(seen[0]) == list(_HEADERS_THAT_LOOK_LIKE_AUTHENTICATION), "no key was reordered away"
    assert seen[1] == {}, "a stream opened without headers gets an empty mapping, not the last one's"
    assert dialer_with[0].headers == _HEADERS_THAT_LOOK_LIKE_AUTHENTICATION, "verbatim on the wire"

    # And they bought nothing and cost nothing: the same result, the same frames, the same order.
    assert with_headers == without == {"ok": True}
    stripped = [dataclasses.replace(frame, headers=None) for frame in dialer_with]
    assert stripped == dialer_without, "the headers changed what this peer sent"
    assert acceptor_with == acceptor_without, "the headers changed what the remote sent back"


class _WatchedPayload(dict):
    """A payload that records every key muxws looks up on it, for WSM-FRM-006.

    A plain equality check cannot see a peer that *read* `payload["type"]` and happened to do
    nothing with it yet; this can. `json.dumps` walks a dict subclass by iterating its items rather
    than by subscripting it, so the codec leaves no trace here - which is what makes a trace mean
    something.
    """

    def __init__(self, *args: Any, **options: Any) -> None:
        super().__init__(*args, **options)
        self.looked_up: list[Any] = []

    def __getitem__(self, key: Any) -> Any:
        self.looked_up.append(key)
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        self.looked_up.append(key)
        return super().get(key, default)


#: A payload made entirely of words the envelope also uses, plus the `kind` field WSM-FRM-006 names
#: first. Every value here contradicts the envelope it shadows, so anything muxws took from it would
#: show up as a wrong frame rather than as a coincidence.
_PAYLOAD_SPELLED_LIKE_AN_ENVELOPE: dict[str, Any] = {
    "type": "reset",
    "stream": 99,
    "end": True,
    "more": True,
    "fragment": "not a fragment",
    "code": 3,
    "reason": "the application's word for something, not muxws'",
    "trailers": {"checksum": "deadbeef"},
    "headers": {"authorization": "not read here either"},
    "nonce": "8f14e45fceea167a",
    "last_stream": 7,
    "payload": {"kind": "an application's own discriminator, nested inside its own payload"},
    "kind": "the reserved key WSM-FRM-006 names first",
}


async def test_a_payload_spelled_like_an_envelope_is_carried_and_never_read(make_pair):
    """WSM-FRM-006 / WSM-INV-016: muxws defines no vocabulary inside `payload`.

    The rule is what makes two independent consumers able to share one socket without nesting their
    own vocabulary inside an imposed one - so the failure it prevents is not a crash, it is a
    release of muxws being required every time either consumer changes a message.

    Three things are asserted, and the second is the one no round-trip test would catch. The payload
    survives both directions unchanged; **every** envelope field that this payload has a lookalike
    for stays at its own default, so `stream: 99` did not become the frame's stream and `end: true`
    did not half-close anything; and no key of it was ever subscripted.
    """
    pair = make_pair()
    echoed: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        echoed.append(payload)
        await stream.reply(payload)

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        sent = _WatchedPayload(_PAYLOAD_SPELLED_LIKE_AN_ENVELOPE)
        # `open`, not `request`: `request` sets `end=True` itself, which would make the frame's
        # `end` agree with the payload's for the wrong reason.
        stream = pair.dialer.open(sent)
        assert await stream == _PAYLOAD_SPELLED_LIKE_AN_ENVELOPE
        await pair.settle()

        assert echoed == [_PAYLOAD_SPELLED_LIKE_AN_ENVELOPE], "it arrived as it was sent"
        assert sent.looked_up == [], f"muxws read {sent.looked_up} out of an application payload"

        [opened] = pair.frames_of_type("dialer", "open")
        assert (opened.type, opened.stream) == ("open", 1)
        assert (opened.end, opened.more, opened.fragment) == (False, False, None)
        assert (opened.code, opened.reason, opened.trailers, opened.headers) == (None, None, None, None)
        assert (opened.nonce, opened.last_stream) == (None, None)
        assert opened.payload == _PAYLOAD_SPELLED_LIKE_AN_ENVELOPE, "nested, not merged"

        # Nothing objected to any of it: no reset, no goaway, both peers still connected.
        assert pair.frames_of_type("dialer", "reset") == []
        assert pair.frames_of_type("acceptor", "reset") == []
        assert pair.frames_of_type("acceptor", "goaway") == []
        assert pair.dialer.is_open is True
        assert pair.acceptor.is_open is True
    finally:
        await pair.stop()


async def test_an_envelope_lookalike_payload_survives_being_fragmented(make_pair):
    """WSM-FRM-006 again, across the one path that does take the payload apart.

    The splitter cuts the codec's encoding of the payload into byte ranges (WSM-FRG-011), so a
    fragment of this payload is a string with `"type":"reset"` visibly inside it. A reassembler that
    peeked at a fragment's contents, or a receiver that merged a reassembled payload into the
    envelope, has its one chance here.
    """
    pair = make_pair(max_frame_bytes=200)
    reassembled: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        reassembled.append(payload)
        await stream.reply({"ok": True})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        big = dict(_PAYLOAD_SPELLED_LIKE_AN_ENVELOPE, filler="z" * 2048)
        stream = pair.dialer.open(big)
        assert await stream == {"ok": True}
        await pair.settle(60)

        pieces = [frame for frame in pair.sent_by("dialer") if frame.fragment is not None]
        assert len(pieces) > 1, "the payload must actually have been cut for this to prove anything"
        assert all(piece.type == "open" and piece.stream == 1 for piece in pieces)
        assert any('"type":"reset"' in str(piece.fragment) for piece in pieces), "the bait must be on the wire"
        # The receiver reassembled it whole and read nothing out of it on the way.
        assert reassembled == [big]
        assert pair.frames_of_type("acceptor", "goaway") == []
    finally:
        await pair.stop()


#: WSM-API-008's partition, spelled out. `connect`, `accept`, `serve`, `notify`, `request`, `ping`,
#: `close` and every `Stream` send method are async; everything else must not be.
#:
#: Two entries the rule's sentence does not name, and why they belong: `Stream.result` and
#: `Stream.cancel`/`reset` wait or send, which is the property the rule is drawn around, and
#: `SocketAdapter`'s four members are the transport seam of WSM-API-021 - the thing a socket is,
#: not a call the application makes.
_MUST_BE_ASYNC = frozenset(
    {
        "Peer.close",
        "Peer.notify",
        "Peer.ping",
        "Peer.request",
        "Peer.serve",
        "SocketAdapter.close",
        "SocketAdapter.receive",
        "SocketAdapter.send_bytes",
        "SocketAdapter.send_text",
        "Stream.cancel",
        "Stream.end",
        "Stream.reply",
        "Stream.reset",
        "Stream.result",
        "Stream.send",
        "accept",
        "connect",
        "serve",
    }
)


def test_the_public_api_is_async_exactly_where_the_rule_says():
    """WSM-API-008, in the only place it lives: the signatures.

    Compared by equality in both directions, because the rule has two halves and the interesting
    one is the second. `peer.open()` being **synchronous** is WSM-API-001 and the whole reason a
    stream can be opened and its id put on the wire in one indivisible step (WSM-INV-005); an
    `async def open` would still pass every behavioural test in this suite, since `await`ing it
    reads the same at every call site. So would an `async def on_stream`, and a registration that
    suspends is a registration an acceptor can race a pushed stream against (WSM-STM-033).
    """
    import muxws

    surface = _public_callables()
    assert len(surface) > 100, "the walker found almost nothing to check"
    asynchronous = {name for name, function in surface if inspect.iscoroutinefunction(function)}
    assert asynchronous == _MUST_BE_ASYNC

    # Named individually as well, because a set of eighteen strings is easy to edit and hard to
    # read: these are the ones §5.1 lists, plus the one it forbids.
    for name in ("connect", "accept", "serve"):
        assert inspect.iscoroutinefunction(getattr(muxws, name)), name
    assert not inspect.iscoroutinefunction(Peer.open)
    assert not inspect.iscoroutinefunction(Peer.on_stream)


async def test_the_opening_payload_is_never_looked_at_for_routing(make_pair):
    """WSM-AUT-004 / WSM-STM-030: one handler, and it gets everything.

    A routing library is what muxws would become by accident, one convenience at a time - a `path`
    key honoured "just for dispatch", a handler table keyed on it - and the shape of the accident is
    that every existing test keeps passing, because every existing test sends one payload to one
    handler. So this sends payloads that *ask* to be routed: two different paths and methods, a bare
    string with no keys to match on at all, and a null. All four reach the same handler, in the
    order they were opened, and none of them is refused.

    `test_second_on_stream_replaces_and_logs` is the companion: there is one handler slot, not a
    table, so there is nowhere for a route to be registered even if something wanted to match one.
    """
    assert list(inspect.signature(Peer.on_stream).parameters) == ["self", "handler"]
    for absence in ("route", "add_route", "routes", "handlers", "dispatch", "handler_for"):
        assert not hasattr(Peer, absence), f"Peer.{absence} is a routing table by another name"

    pair = make_pair()
    arrived: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        arrived.append(payload)
        await stream.reply({"seen": len(arrived)})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        asking_to_be_routed = [
            {"path": "/reports/export", "method": "POST"},
            {"path": "/reports/list", "method": "GET"},
            "a bare string with nothing to match on",
            None,
        ]
        results = [await pair.dialer.request(payload) for payload in asking_to_be_routed]

        assert arrived == asking_to_be_routed, "one handler, every payload, in order"
        assert results == [{"seen": 1}, {"seen": 2}, {"seen": 3}, {"seen": 4}]
        assert pair.frames_of_type("acceptor", "reset") == [], "nothing was refused for not matching"
    finally:
        await pair.stop()


async def test_no_hook_can_intercept_an_error_on_its_way_to_the_call_site(make_pair):
    """WSM-ERR-003: errors are raised where they are awaited, and there is nowhere else to put them.

    The rule has two halves and the suite only had the first. That every shape raises is asserted
    all over `socket_death_test.py`; that the raise cannot be *diverted* is asserted nowhere, and an
    `on_error` hook is the single most natural thing to add to a peer that already has four hooks.
    What it would cost is the failure this rule prevents: an error handled somewhere other than the
    call site is an error the caller's `try` never sees, and the await either hangs or returns a
    value that was never sent.

    So: the four hooks are named exhaustively, and the exchange below registers every one of them
    before failing a stream. They all fire, they all see the reset go past, and the exception still
    comes out of the `await`.
    """
    assert {name for name in dir(Peer) if name.startswith("on_")} == {
        "on_close",
        "on_frame",
        "on_reconnect",
        "on_stream",
    }

    pair = make_pair()
    frames_seen: list[str] = []
    closes_seen: list[Any] = []

    async def handler(payload: Any, _stream: Stream) -> None:
        _ = payload
        raise ValueError("handler said no")

    pair.acceptor.on_stream(handler)
    pair.dialer.on_frame(lambda direction, frame, _length: frames_seen.append(f"{direction}:{frame.type}"))
    pair.dialer.on_close(closes_seen.append)
    pair.start()
    try:
        with pytest.raises(RemoteError) as info:
            await pair.dialer.request({"q": 1})
        assert info.value.code is ResetCode.APPLICATION_ERROR

        # The observer saw it and did not consume it - `on_frame` is observability (WSM-OBS-003),
        # not a handler, and a `reset` reaching it is not a `reset` that was dealt with.
        assert "rx:reset" in frames_seen
        assert closes_seen == [], "the connection is fine; one stream failed"
    finally:
        await pair.stop()
