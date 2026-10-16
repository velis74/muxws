"""`Peer`: id allocation, retention, dispatch, and the outcome of socket death for every stream shape."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import re
import warnings

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

import muxws

from muxws.codecs.json_ import JsonCodec
from muxws.errors import (
    ConnectionGoingAway,
    ConnectionLost,
    ProtocolError,
    RemoteError,
    ResetCode,
    StreamReset,
    StreamTimeout,
)
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

    Fifty opens from fifty separate tasks. A suspension between taking the id and queueing the frame
    would put a non-monotonic id sequence on the wire, which is a protocol error by the sending peer.
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
    """WSM-API-009: a peer id is a prefix plus a counter, and a closed peer's id is not reused."""
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
    """WSM-API-009, fourth clause: a peer created after another closed gets a new id.

    The test closes peers before creating new ones; distinct ids among peers that are all still open
    do not detect a scheme that reuses the ids of closed connections.
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

#: The public callables that take an argument by one of those names and are exempt from the rule.
#: Any callable that takes such an argument and is not listed here fails the test.
#:
#: - `Frame` is the wire envelope, not a way to open a stream. The conformance runner and the
#:   injection fixtures build frames directly; frames are not part of the application API.
#: - `stream_id` on the `StreamReset` family is an output: it names the stream that failed. Naming it
#:   allocates nothing.
#: - `Stream.__init__` is the library's own constructor and requires a `Peer`; every caller-facing
#:   route to a `Stream` (`open`, `notify`, `request`, and dispatch to `on_stream`) allocates the id
#:   itself. SPEC.md Appendix B notes this exception to the rule's wording.
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


def _is_defined_here(cls: type, attr: str) -> bool:
    """Return whether `attr` is defined by muxws rather than inherited from `str`, `int` or `Exception`.

    The rule applies only to muxws's own definitions. Inherited members also vary by interpreter
    version: since Python 3.11, `dir()` of a `str`-mixin enum lists every `str` method.
    """
    for base in cls.__mro__:
        if attr in vars(base):
            return getattr(base, "__module__", "").split(".")[0] == "muxws"
    return False


def _public_callables() -> list[tuple[str, Any]]:
    """`(qualified name, callable)` for everything muxws itself defines under `muxws.__all__`."""
    import muxws

    found: list[tuple[str, Any]] = []
    for name in muxws.__all__:
        obj = getattr(muxws, name)
        if inspect.isclass(obj):
            for attr in dir(obj):
                if attr.startswith("_") and attr != "__init__":
                    continue
                if not _is_defined_here(obj, attr):
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

    `test_parity_and_monotonicity` covers the allocator. This test checks the signatures for an
    argument that names a stream id. Behavioural tests do not detect such an argument: a
    `peer.open(stream_id=...)` passes them as long as the caller supplies ids of the right parity in
    increasing order.
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
        # No container of any kind on the peer may hold a key of a closed stream. Checking only dicts
        # would miss an unbounded `self._seen_ids: set[int]`.
        for name, value in vars(pair.dialer).items():
            if name == "tags" or not isinstance(value, (dict, list, set, frozenset, tuple)):
                continue
            assert len(value) == 0, f"peer.{name} still holds {len(value)} entries after 200 closed streams"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- dispatch


async def test_unknown_frame_type_is_ignored(make_pair, caplog):
    """WSM-FRM-002: dropped, logged once, connection alive, nothing goes out.

    The three conditions are asserted together: a peer that never received the frame is also alive
    and silent, and a peer that answered it with a `reset` also logs. `on_frame` fires after decode
    and before dispatch (WSM-OBS-003), so it confirms the frame arrived.

    `stream=1` is above this acceptor's high-water mark, so an implementation that passes an
    unrecognised type to the stream-frame path kills the connection (WSM-STM-003).
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
        # WSM-OBS-001 requires every frame log line to use the one named logger.
        assert lines[0].name == "muxws.frames"
        assert lines[0].levelno == logging.INFO
    finally:
        await pair.stop()


async def test_each_unknown_frame_type_is_logged_once(make_pair, caplog):
    """WSM-FRM-002: an unknown frame type that arrives repeatedly produces one log line per type."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        with caplog.at_level(logging.INFO, logger="muxws.frames"):
            for frame_type in ("widget", "widget", "gadget", "widget", "gadget"):
                pair.acceptor_socket.inject(json.dumps({"type": frame_type, "stream": 1}))
            await pair.settle()

        messages = [record.getMessage() for record in caplog.records]
        assert len([message for message in messages if "'widget'" in message]) == 1, messages
        assert len([message for message in messages if "'gadget'" in message]) == 1, messages
        assert pair.acceptor.is_open
    finally:
        await pair.stop()


@pytest.mark.parametrize("frame_type", ["5", "[1]", '{"a":1}', '{"toString":1}', "null"])
async def test_a_non_string_frame_type_fails_the_connection_with_frame_logging_on(make_pair, caplog, frame_type):
    """WSM-FRM-005: a `type` that is not a string is a malformed envelope, not an unknown frame type.

    With frame logging at DEBUG level the read loop sends goaway(PROTOCOL_ERROR) and closes the socket.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        with caplog.at_level(logging.DEBUG, logger="muxws.frames"):
            pair.acceptor_socket.inject(f'{{"type":{frame_type},"stream":1}}')
            await pair.settle()
        goaways = pair.frames_of_type("acceptor", "goaway")
        assert [frame.code for frame in goaways] == [int(ResetCode.PROTOCOL_ERROR)]
        assert pair.acceptor.is_open is False
        assert pair.acceptor_socket.is_closed
    finally:
        await pair.stop()


async def test_unknown_envelope_field_is_ignored(make_lone):
    """WSM-FRM-001: an unknown envelope field changes no behaviour.

    `frames_test.py` checks that the decoder drops the key. This test checks behaviour: it runs the
    same exchange twice, once with `"colour": "red"` on the `open` and once without, and compares the
    two peers' whole wires. WSM-CON-009 (additive revisions are safe to receive) depends on this rule.

    The test uses a `Lone` peer: a real counterpart would receive the reply to a stream it never
    opened and kill the connection.
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

    # Two silent peers also have equal wires, so the handler must have run and answered.
    assert clean_seen == [{"q": 1}]
    assert clean_wire, "the control exchange produced no frames at all"
    assert tainted_alive is clean_alive is True
    assert tainted_seen == clean_seen, "the payload the handler saw"
    assert tainted_wire == clean_wire, "an unknown envelope field changed what went out"
    # A decoder that let the extra key change `end`, `more` or `code` can still produce an identical
    # answer for this exchange. `on_frame` fires after decode and before dispatch (WSM-OBS-003), so
    # this compares the decoded frames field by field.
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

    REFUSED means the operation did not happen and may be retried. For a handler that debits an
    account and then raises, REFUSED would cause the client to retry the debit.
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
    """The arguments for one shape: a peer, a callback that marks it armed, and the socket-death event."""

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
        """Mark this shape as armed. The socket is dropped after every shape is armed."""
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
    # A clean end would report the export as finished; WSM-RCN-041 requires an error.
    raise AssertionError("an async for must raise on socket death, not terminate normally")


async def _request_shape(rig: _Rig) -> None:
    rig.arm()
    await rig.peer.request({"shape": rig.name})


async def _send_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    while True:
        # The sender keeps sending until the socket dies. A sender that sent once and waited would
        # test `send()` on a closed stream, which is a different rule.
        await stream.send({"shape": rig.name})
        await asyncio.sleep(0)


async def _cancel_shape(rig: _Rig) -> None:
    stream = rig.open()
    rig.arm()
    await rig.dropped.wait()
    await stream.cancel()
    await stream.reset(ResetCode.NO_ERROR)


#: Every shape WSM-RCN-041 names, with the required outcome when the socket dies. The shapes are
#: listed only here: the runner below opens one stream per entry, waits until all are armed, kills
#: the socket once, and checks each outcome against this map. Adding a shape is one entry (§6).
_DEATH_SHAPES: dict[str, tuple[Callable[[_Rig], Awaitable[None]], type[BaseException] | None]] = {
    "await stream": (_await_shape, ConnectionLost),
    "await stream.result()": (_result_shape, ConnectionLost),
    "async for": (_iterate_shape, ConnectionLost),
    "in-flight request()": (_request_shape, ConnectionLost),
    "sender mid-send()": (_send_shape, ConnectionLost),
    # WSM-RCN-041 makes `cancel()` and `reset()` no-ops, so the expected outcome is `None`. They are
    # listed so that the test also detects them hanging.
    "a stream awaiting cancel()": (_cancel_shape, None),
}


async def test_socket_death_fails_every_shape(make_pair):
    """WSM-RCN-041/WSM-STM-014 **(spec)**: every shape ends, none hangs.

    One stream per shape, all live at the same time, and one socket death under all of them. The
    expected outcome of each shape is read from `_DEATH_SHAPES`.

    The test fails by hang detection and names each shape that did not finish. The failure WSM-INV-011
    describes raises no exception: the caller gets no error, no log and no timeout.
    """
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()

    closes: list[Any] = []
    #: State observed when `on_close` ran. WSM-STM-014 requires the synthesised failures before
    #: `on_close`; a handler that ran first would observe live streams. Call counts alone do not
    #: distinguish the two orders.
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
        if pending:  # pragma: no cover - reached only when the test fails
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

    Every call an application makes while disconnected raises, and the next socket carries none of
    them. A queue flushed into a server that no longer knows the sender delivers frames without any
    error reaching the call site.
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

        # The id space of the new socket starts empty (WSM-RCN-031).
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

    The socket the streams died on is closed, so a `reset(9)` enqueued for each of them would reach no
    wire this test can read. The test therefore asserts on the frames that were queued.
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
    """WSM-OBS-003: an exception from `on_frame` does not affect the connection.

    `on_frame` is called from the read loop and from the write loop. If its exception propagated, it
    would stop the read loop, and the peer would report the socket as dead.
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
    """A unary call that ends without a payload raises; returning would be indistinguishable from a None payload."""
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
    """WSM-ERR-011: on timeout the remote receives reset(TIMEOUT) and the caller gets StreamTimeout."""
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


@pytest.mark.parametrize("timeout", [None, 5.0])
async def test_cancelling_a_request_resets_its_stream_with_or_without_a_timeout(make_pair, timeout):
    """WSM-ERR-011: a cancelled caller sends reset(CANCELLED) and leaves no stream and no task behind."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        caller = asyncio.create_task(pair.dialer.request({"q": 1}, timeout=timeout))
        await pair.settle()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        await pair.settle()

        resets = [f.code for f in pair.sent_by("dialer") if f.type == "reset"]
        assert resets == [int(ResetCode.CANCELLED)]
        assert pair.dialer._streams == {}, "the stream is gone"
        assert pair.acceptor._streams == {}, "and the remote stopped working on it"
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
    """`end` is set on the last fragment, so the stream is half-closed when it is dispatched."""
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
    """A reset code this peer does not know, including the retired 5, resets the stream and keeps the peer open.

    Converting the wire value directly to `ResetCode` raises `ValueError` in the read loop, which
    leaves `is_open` true, `on_close` not called, and every pending await blocked on a queue that no
    writer drains.
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


async def test_a_read_loop_failure_closes_the_peer(make_pair):
    """An exception in the read loop closes the peer; `is_open` does not stay true with a stopped read loop."""
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
        assert closes[0].code == 1011
        assert stream.closed.is_set()
        assert pair.dialer_socket.is_closed, "a peer that reports itself closed has closed its socket"
        assert [frame.code for frame in pair.frames_of_type("dialer", "goaway")] == [int(ResetCode.INTERNAL_ERROR)]
    finally:
        await pair.stop()


async def test_an_exception_outside_dispatch_closes_the_peer_and_the_socket(make_pair):
    """An exception the read loop raises outside `_dispatch` marks the peer closed (1011) and closes its socket."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    closes: list[Any] = []
    pair.dialer.on_close(closes.append)

    def exploding(*_args: Any) -> None:
        raise RuntimeError("simulated bug outside dispatch")

    pair.dialer._report_frame = exploding  # type: ignore[method-assign]
    pair.start()
    try:
        pair.dialer_socket.inject('{"type":"ping","nonce":"n"}')
        await pair.settle()

        assert pair.dialer.is_open is False
        assert [reason.code for reason in closes] == [1011]
        assert pair.dialer_socket.is_closed
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- shutdown


class _WrappedSocket:
    """Delegates to a `MemorySocket`. With `suspend`, every send waits 1 ms before it reaches the wire."""

    def __init__(self, inner: Any, *, suspend: bool = False, failing_close: bool = False) -> None:
        self._inner = inner
        self._suspend = suspend
        self._failing_close = failing_close

    async def send_text(self, text: str) -> None:
        if self._suspend:
            await asyncio.sleep(0.001)
        await self._inner.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        if self._suspend:
            await asyncio.sleep(0.001)
        await self._inner.send_bytes(data)

    async def receive(self) -> str | bytes:
        return await self._inner.receive()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._inner.close(code, reason)
        if self._failing_close:
            raise RuntimeError("the adapter failed to close")


#: (send suspends, reply size): a socket whose send suspends, and a reply of more than 1000 fragments.
_DRAIN_CASES = [pytest.param(True, 4_000, id="suspending-send"), pytest.param(False, 200_000, id="many-fragments")]


def _assert_reply_and_goaway_sent(frames: list[Frame]) -> None:
    pieces = [frame for frame in frames if frame.type == "data"]
    assert len(pieces) > 1, "the reply must have been fragmented"
    assert pieces[-1].more is not True, "the last fragment of the reply must be on the wire"
    assert pieces[-1].end is True
    assert [frame for frame in frames if frame.type == "goaway"], "goaway must be on the wire"


@pytest.mark.parametrize(("suspend", "size"), _DRAIN_CASES)
async def test_close_sends_every_queued_fragment_before_closing_the_socket(make_pair, suspend: bool, size: int):
    """WSM-CON-025/WSM-FRG-017: `close()` closes the socket only after every queued fragment and `goaway` are sent."""
    pair = make_pair(max_frame_bytes=200)
    pair.acceptor._socket = _WrappedSocket(pair.acceptor_socket, suspend=suspend)  # type: ignore[assignment]
    blob = {"blob": "x" * size}
    replied = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply(blob)
        replied.set()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1}, end=True)
        await asyncio.wait_for(replied.wait(), 5)
        await asyncio.wait_for(pair.acceptor.close(drain=5), 10)

        _assert_reply_and_goaway_sent(pair.sent_by("acceptor"))
        assert await asyncio.wait_for(stream.result(), 5) == blob
    finally:
        await pair.stop()


@pytest.mark.parametrize(("suspend", "size"), _DRAIN_CASES)
async def test_a_connection_failure_sends_every_queued_fragment_and_its_goaway(make_pair, suspend: bool, size: int):
    """ILL-C: the socket closes only after `goaway(PROTOCOL_ERROR)` and every fragment queued before it are sent."""
    pair = make_pair(max_frame_bytes=200)
    pair.acceptor._socket = _WrappedSocket(pair.acceptor_socket, suspend=suspend)  # type: ignore[assignment]
    replied = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"blob": "x" * size})
        replied.set()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1}, end=True)
        await asyncio.wait_for(replied.wait(), 5)
        pair.acceptor_socket.inject('{"type":"open","code":0}')
        for _ in range(2000):
            if not pair.acceptor.is_open:
                break
            await asyncio.sleep(0.001)

        assert pair.acceptor.is_open is False
        frames = pair.sent_by("acceptor")
        _assert_reply_and_goaway_sent(frames)
        assert [frame.code for frame in frames if frame.type == "goaway"] == [int(ResetCode.PROTOCOL_ERROR)]
    finally:
        await pair.stop()


async def test_a_connection_failure_refuses_local_opens_while_it_drains(make_pair):
    """WSM-CON-021/ILL-C: once `goaway(PROTOCOL_ERROR)` is queued, `open()` raises `ConnectionGoingAway`."""
    pair = make_pair(max_frame_bytes=200)
    pair.acceptor._socket = _WrappedSocket(pair.acceptor_socket, suspend=True)  # type: ignore[assignment]
    replied = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"blob": "x" * 40_000})
        replied.set()

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1}, end=True)
        await asyncio.wait_for(replied.wait(), 5)
        pair.acceptor_socket.inject('{"type":"open","code":0}')
        for _ in range(2000):
            if any(frame.type == "goaway" for frame in pair.sent_by("acceptor")):
                break
            await asyncio.sleep(0.001)

        assert pair.acceptor.is_open is True, "the drain must still be sending the reply"
        with pytest.raises(ConnectionGoingAway):
            pair.acceptor.open({"q": 2})
    finally:
        await pair.stop()


async def test_a_read_loop_failure_refuses_local_opens_while_it_drains(make_pair):
    """WSM-CON-021: once `goaway(INTERNAL_ERROR)` is queued, `open()` raises `ConnectionGoingAway`."""
    pair = make_pair(max_frame_bytes=200)
    pair.acceptor._socket = _WrappedSocket(pair.acceptor_socket, suspend=True)  # type: ignore[assignment]
    replied = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload
        await stream.reply({"blob": "x" * 40_000})
        replied.set()

    async def exploding(_frame: Frame) -> bool:
        raise RuntimeError("simulated bug in dispatch")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1}, end=True)
        await asyncio.wait_for(replied.wait(), 5)
        pair.acceptor._dispatch = exploding  # type: ignore[method-assign]
        pair.acceptor_socket.inject('{"type":"ping","nonce":"n"}')
        for _ in range(2000):
            if any(frame.type == "goaway" for frame in pair.sent_by("acceptor")):
                break
            await asyncio.sleep(0.001)

        assert pair.acceptor.is_open is True, "the drain must still be sending the reply"
        with pytest.raises(ConnectionGoingAway):
            pair.acceptor.open({"q": 2})
    finally:
        await pair.stop()


async def test_close_marks_the_peer_dead_when_the_adapter_close_raises(make_pair, caplog):
    """WSM-CON-025/WSM-STM-014: an exception from the adapter's `close()` is logged; the peer still dies."""
    pair = make_pair()
    pair.acceptor._socket = _WrappedSocket(pair.acceptor_socket, failing_close=True)  # type: ignore[assignment]
    pair.acceptor.on_stream(_hold)
    closes: list[Any] = []
    pair.acceptor.on_close(closes.append)
    pair.start()
    try:
        stream = pair.acceptor.open({"q": 1})
        await pair.settle()
        with caplog.at_level(logging.WARNING, logger="muxws"):
            await pair.acceptor.close(drain=0.01)

        assert pair.acceptor.is_open is False
        assert stream.closed.is_set()
        assert [reason.code for reason in closes] == [1000]
        assert any("closing the socket failed" in record.getMessage() for record in caplog.records)
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- hostile frames and broken hooks


async def test_a_wrong_parity_open_cannot_pose_as_a_fragment_continuation(make_pair):
    """WSM-SID-005: parity is checked before anything else, whatever reassembly is in flight.

    If continuation were inferred from an assembler running on that id, a stream this peer opened
    that is receiving fragmented `data` would accept an `open` with this peer's own parity as a
    continuation. That dispatches a handler for a locally opened stream and skips the
    connection-level error the rule requires.
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
    """An `open` that reuses an id is a connection error even while a fragment on that id is being reassembled."""
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


@pytest.mark.parametrize(
    "message",
    [
        '{"type":"open","stream":"x"}',
        '{"type":"open","stream":-1}',
        '{"type":"open","stream":1,"headers":"x"}',
        '{"type":"open","stream":1,"fragment":5}',
        '{"type":"open","stream":1,"end":"yes"}',
        '{"type":"data","stream":true}',
        '{"type":"ping","nonce":5}',
        '{"type":5}',
    ],
)
async def test_an_envelope_field_of_the_wrong_type_fails_the_connection(make_pair, message: str):
    """WSM-FRM-005/WSM-STM-023: a wrong-typed envelope field sends goaway(PROTOCOL_ERROR) and closes the socket."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        pair.acceptor_socket.inject(message)
        await pair.settle()

        goaways = pair.frames_of_type("acceptor", "goaway")
        assert [frame.code for frame in goaways] == [int(ResetCode.PROTOCOL_ERROR)]
        assert pair.acceptor.is_open is False
        assert pair.acceptor_socket.is_closed
    finally:
        await pair.stop()


async def test_a_connection_level_error_closes_the_socket_with_a_reason_that_fits_a_close_frame():
    """WSM-STM-023: the 1002 close carries at most 123 UTF-8 bytes of reason, so the socket closes.

    The socket raises on a longer reason, as the `websockets` adapter does. A text message on a msgpack
    connection produces a longer reason; the `goaway` and `on_close` carry it whole.
    """
    from muxws.codecs.msgpack_ import MsgpackCodec

    class CloseFrameLimit:
        def __init__(self, inner: Any) -> None:
            self.inner = inner
            self.closed_with: list[tuple[int, str]] = []

        async def close(self, code: int = 1000, reason: str = "") -> None:
            if len(reason.encode("utf-8")) > 123:
                raise ValueError("control frame too long")
            self.closed_with.append((code, reason))
            await self.inner.close(code, reason)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    _, inner = memory_pair()
    socket = CloseFrameLimit(inner)
    codec = MsgpackCodec()
    peer = Peer(socket, codec=codec, is_dialer=False)  # type: ignore[arg-type]
    closes: list[Any] = []
    peer.on_close(closes.append)
    serving = asyncio.create_task(peer.serve())
    try:
        inner.inject('{"type":"ping","nonce":"n"}')
        await asyncio.wait_for(serving, 2.0)

        goaways = [frame for frame in map(codec.decode, inner.sent) if frame.type == "goaway"]
        assert [frame.code for frame in goaways] == [int(ResetCode.PROTOCOL_ERROR)]
        assert len(goaways[0].reason.encode("utf-8")) > 123
        assert [code for code, _ in socket.closed_with] == [1002]
        assert goaways[0].reason.startswith(socket.closed_with[0][1])
        assert [(reason.code, reason.reason) for reason in closes] == [(1002, goaways[0].reason)]
        assert inner.is_closed
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_close_sends_a_close_frame_reason_cut_to_123_bytes():
    """`close()` cuts its reason for the WebSocket close frame; the `goaway` and `on_close` carry it whole.

    The socket raises on a longer reason, as the `websockets` adapter does.
    """
    closed_with: list[tuple[int, str]] = []

    class CloseFrameLimit:
        def __init__(self, inner: Any) -> None:
            self.inner = inner

        async def close(self, code: int = 1000, reason: str = "") -> None:
            if len(reason.encode("utf-8")) > 123:
                raise ValueError("control frame too long")
            closed_with.append((code, reason))
            await self.inner.close(code, reason)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    _, inner = memory_pair()
    codec = JsonCodec()
    peer = Peer(CloseFrameLimit(inner), codec=codec, is_dialer=True)  # type: ignore[arg-type]
    closes: list[Any] = []
    peer.on_close(closes.append)
    serving = asyncio.create_task(peer.serve())
    reason = "é" * 100
    try:
        await asyncio.wait_for(peer.close(reason=reason, drain=1.0), 2.0)

        goaways = [frame for frame in map(codec.decode, inner.sent) if frame.type == "goaway"]
        assert [frame.reason for frame in goaways] == [reason]
        assert closed_with == [(1000, "é" * 61)]
        assert [(close.code, close.reason) for close in closes] == [(1000, reason)]
        assert inner.is_closed
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


def test_a_close_frame_reason_is_cut_at_a_character_boundary():
    """A reason over 123 UTF-8 bytes is cut to at most 123 bytes without splitting a character."""
    from muxws.peer import _close_frame_reason

    assert _close_frame_reason("short") == "short"
    assert _close_frame_reason("a" * 200) == "a" * 123
    assert _close_frame_reason("a" + "é" * 100) == "a" + "é" * 61
    assert _close_frame_reason("aa" + "é" * 100) == "aa" + "é" * 60


async def test_an_undecodable_reassembled_payload_resets_only_that_stream():
    """WSM-STM-020/024: a reassembled payload the codec cannot decode resets that stream with PROTOCOL_ERROR.

    No handler runs for the stream, and the connection stays open.
    """
    _, socket = memory_pair()
    codec = JsonCodec()
    peer = Peer(socket, codec=codec, is_dialer=False)
    handled: list[Any] = []

    async def handler(payload: Any, stream: Stream) -> None:
        handled.append(payload)
        await stream.closed.wait()

    peer.on_stream(handler)
    serving = asyncio.create_task(peer.serve())
    try:
        socket.inject(codec.encode(Frame("open", stream=1, fragment='{"a":', more=True)))
        socket.inject(codec.encode(Frame("open", stream=1, fragment="]")))
        for _ in range(12):
            await asyncio.sleep(0)

        sent = [codec.decode(message) for message in socket.sent]
        assert [(frame.type, frame.stream, frame.code) for frame in sent] == [
            ("reset", 1, int(ResetCode.PROTOCOL_ERROR))
        ]
        assert handled == []
        assert peer.is_open
        assert peer.streams == {}
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_mixed_text_and_binary_fragments_reset_only_that_stream():
    """WSM-STM-020/024: one payload's fragments that mix text and binary values reset that stream.

    The stream is reset with PROTOCOL_ERROR, the connection stays open, and other streams are unaffected.
    """
    from muxws.codecs.msgpack_ import MsgpackCodec

    left, right = memory_pair()
    codec = MsgpackCodec()
    dialer = Peer(left, codec=codec, is_dialer=True)
    acceptor = Peer(right, codec=codec, is_dialer=False)
    acceptor.on_stream(_hold)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    try:
        stream = dialer.open({"q": 1})
        other = dialer.open({"q": 2})
        for _ in range(12):
            await asyncio.sleep(0)
        left.inject(codec.encode(Frame("data", stream=stream.id, fragment="\x81", more=True)))
        left.inject(codec.encode(Frame("data", stream=stream.id, fragment=b"\xa1a\x01")))
        for _ in range(12):
            await asyncio.sleep(0)

        sent = [codec.decode(message) for message in left.sent]
        resets = [frame for frame in sent if frame.type == "reset" and frame.stream == stream.id]
        assert [frame.code for frame in resets] == [int(ResetCode.PROTOCOL_ERROR)]
        with pytest.raises(StreamReset):
            await stream
        assert dialer.is_open
        assert not other.closed.is_set()
    finally:
        await left.drop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_an_unencodable_frame_for_a_removed_stream_closes_the_socket(make_pair):
    """A frame that cannot be encoded and has no live stream closes the socket and reports code 1011.

    The error serializer returns a payload the codec cannot encode; the stream is already removed when
    the writer encodes its reset.
    """
    pair = make_pair(error_serializer=lambda _exc: {"blob": object()})
    closes: list[Any] = []
    pair.acceptor.on_close(closes.append)

    async def handler(payload: Any, stream: Stream) -> None:
        _ = (payload, stream)
        raise ValueError("handler said no")

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        pair.dialer.open({"q": 1})
        await pair.settle()

        assert pair.acceptor.is_open is False
        assert [reason.code for reason in closes] == [1011]
        assert pair.acceptor_socket.is_closed
    finally:
        await pair.stop()


async def test_an_unsendable_connection_level_frame_closes_the_socket(make_pair):
    """A connection-level frame whose send raises closes the socket with 1011 and marks the peer closed."""

    class FailingPings:
        def __init__(self, inner: Any) -> None:
            self.inner = inner
            self.closed_with: int | None = None

        async def send_text(self, text: str) -> None:
            if '"ping"' in text:
                raise RuntimeError("the transport refused this message")
            await self.inner.send_text(text)

        async def close(self, code: int = 1000, reason: str = "") -> None:
            self.closed_with = code
            await self.inner.close(code, reason)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    pair = make_pair()
    socket = FailingPings(pair.dialer_socket)
    pair.dialer._socket = socket  # type: ignore[assignment]
    pair.start()
    try:
        with pytest.raises(muxws.ConnectionClosed):
            await pair.dialer.ping(timeout=1.0)
        await pair.settle()

        assert pair.dialer.is_open is False
        assert socket.closed_with == 1011
        assert pair.dialer_socket.is_closed
    finally:
        await pair.stop()


async def test_an_unencodable_frame_fails_its_stream_without_wedging_the_connection(make_pair):
    """A frame the codec cannot encode fails its own stream and leaves the writer task running.

    If the writer task stopped, nothing would drain the queue: every later send would stay queued
    while the peer reports itself open.
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


async def test_nothing_more_is_sent_on_a_stream_after_a_later_queued_frame_fails_to_encode(make_pair):
    """A frame that fails to encode when the writer advances past the previous frame on its lane
    resets the stream, and the frames queued behind it on that lane are not sent."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.start()
    try:
        stream = pair.dialer.open({"q": 1})
        # Queued behind the `open`, so the writer slices it when it advances after sending the `open`.
        await stream.send({"blob": b"\x00\xff"})
        await stream.send({"n": 3})
        await pair.settle()

        sent = [frame for frame in pair.sent_by("dialer") if frame.stream == stream.id]
        assert [frame.type for frame in sent] == ["open", "reset"]
        # The reset carries the codec's own error, as when the rotation fails to encode a frame.
        with pytest.raises(Exception) as refused:  # noqa: PT011 - the codec's error type is not part of the contract
            JsonCodec().encode(Frame("data", stream=stream.id, payload={"blob": b"\x00\xff"}))
        assert sent[1].reason == str(refused.value)
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.INTERNAL_ERROR
        assert pair.dialer.is_open
    finally:
        await pair.stop()


async def test_an_open_that_fails_to_encode_fails_its_stream_and_sends_nothing(make_pair):
    """An `open` the codec cannot encode is not sent, and neither is a `reset` for its id: the remote
    has not seen the id, and a `reset` above its high-water mark would end the connection (WSM-STM-003)."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    pair.start()
    try:
        stream = pair.dialer.open({"blob": b"\x00\xff"})
        await pair.settle()

        assert [frame for frame in pair.sent_by("dialer") if frame.stream == stream.id] == []
        with pytest.raises(StreamReset) as info:
            await stream
        assert info.value.code is ResetCode.INTERNAL_ERROR
        assert pair.dialer.is_open
        assert pair.acceptor.is_open
        assert await pair.dialer.request({"q": 2}) == {"ok": True}
    finally:
        await pair.stop()


async def test_an_error_serializer_that_raises_still_produces_the_reset(make_pair):
    """WSM-STM-034: the reset is sent even when the error serializer raises."""

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
# Four rules that each forbid something: a mandatory argument, a default timeout, and reading a
# header or a payload key. A successful exchange does not show that the forbidden behaviour is
# absent, so each test below either inspects a signature or compares two runs that must be equal.


async def test_open_takes_zero_mandatory_arguments_and_defaults_payload_to_null(make_pair):
    """WSM-API-003: `peer.open()` on its own is legal, and the payload it sends is an explicit null.

    Other tests always pass a payload, so they do not detect a mandatory `payload` parameter. The
    default is `null`, not `ABSENT`: a receiver that distinguishes "no payload key" from
    "payload: null" (D1) treats the two as different frames.
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
    """WSM-ERR-010: `request()` has no default timeout.

    A long default timeout is not observable in a test of practical duration, so the test spies on
    the deadline mechanism. `_collect_unary` calls `asyncio.wait_for` only when `timeout is not None`,
    and the spy records every call. The second half of the test passes a timeout and checks that the
    spy records it.

    With a default timeout, a caller that passed no timeout to a slow handler would get
    `StreamTimeout`, and the remote would receive `reset(TIMEOUT)`.
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

        # Control: the spy records a timeout that is passed.
        with pytest.raises(StreamTimeout):
            await pair.dialer.request({"q": 2}, timeout=0.02)
        assert armed == [0.02]

        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
    finally:
        monkeypatch.undo()
        await pair.stop()


#: Per-stream headers with authentication-like content: two spellings of the same standard credential
#: header, a cookie, an API key, a structured credential, an expiry in the past, and a scope list. The
#: assertions below fail if muxws authenticates with, normalises or strips any of them.
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

    Two exchanges that differ only in headers; the wires are compared frame by frame with the
    `headers` field removed. "muxws MUST NOT interpret them" means the peer that receives headers
    behaves the same as the peer that does not. Checking only that the handler received the headers
    would not detect a peer that authenticates on `authorization`, refuses the expired `expires_at`,
    or lower-cases the keys. `expires_at: 0` is a timestamp in the past.
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

    # Every key, both spellings, the nested object and the list are delivered unchanged.
    assert seen[0] == _HEADERS_THAT_LOOK_LIKE_AUTHENTICATION
    assert list(seen[0]) == list(_HEADERS_THAT_LOOK_LIKE_AUTHENTICATION), "no key was reordered away"
    assert seen[1] == {}, "a stream opened without headers gets an empty mapping, not the last one's"
    assert dialer_with[0].headers == _HEADERS_THAT_LOOK_LIKE_AUTHENTICATION, "verbatim on the wire"

    # The headers change nothing else: same result, same frames, same order.
    assert with_headers == without == {"ok": True}
    stripped = [dataclasses.replace(frame, headers=None) for frame in dialer_with]
    assert stripped == dialer_without, "the headers changed what this peer sent"
    assert acceptor_with == acceptor_without, "the headers changed what the remote sent back"


async def test_the_answering_sides_headers_arrive_unchanged_and_change_nothing(make_pair):
    """WSM-AUT-002 for answer headers (WSM-FRM-016): muxws does not interpret them either.

    Two exchanges that differ only in the headers the acceptor sends; the dialer's own frames are
    compared with `headers` removed. The headers include `content-type`, `status: 500` and
    `error: True`, so decoding by content type or raising on status would change the result.
    """
    announced = {
        "content-type": "application/octet-stream",
        "status": 500,
        "error": True,
        "code": 3,
        "type": "reset",
        "expires_at": 0,
    }
    seen: list[dict[str, Any]] = []

    async def exchange(headers: dict[str, Any] | None) -> tuple[Any, list[Frame], list[Frame]]:
        async def handler(payload: Any, stream: Stream) -> None:
            _ = payload
            if headers is None:
                await stream.reply({"ok": True})
            else:
                await stream.reply({"ok": True}, headers=headers)

        pair = make_pair()
        pair.acceptor.on_stream(handler)
        pair.start()
        try:
            stream = pair.dialer.open({"q": 1}, end=True)
            result = await stream
            await asyncio.wait_for(stream.reply_headers_arrived.wait(), 2)
            seen.append(stream.reply_headers)
            await pair.settle()
            return result, pair.sent_by("dialer"), pair.sent_by("acceptor")
        finally:
            await pair.stop()

    with_headers, dialer_with, acceptor_with = await exchange(announced)
    without, dialer_without, acceptor_without = await exchange(None)

    # The headers are delivered unchanged on the opener's handle; an answer without headers gives an
    # empty mapping.
    assert seen[0] == announced
    assert list(seen[0]) == list(announced), "no key was reordered away"
    assert seen[1] == {}
    assert acceptor_with[0].headers == announced, "verbatim on the wire"

    # `status: 500` and `error: True` raised no exception, and the opener's frames are identical to
    # the exchange without headers.
    assert with_headers == without == {"ok": True}
    assert dialer_with == dialer_without, "the answer's headers changed what the opener sent"
    stripped = [dataclasses.replace(frame, headers=None) for frame in acceptor_with]
    assert stripped == acceptor_without, "the answer's headers changed something beside themselves"


class _WatchedPayload(dict):
    """A payload that records every key muxws looks up on it, for WSM-FRM-006.

    An equality check does not detect a peer that reads `payload["type"]` without acting on it; this
    recorder does. `json.dumps` iterates a dict subclass's items without subscripting it, so encoding
    records no keys, and every recorded key is a lookup by muxws.
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


#: A payload whose keys are envelope field names, plus the `kind` field WSM-FRM-006 names. Every value
#: differs from the actual envelope value, so any value muxws read from it would produce a wrong frame.
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

    The rule lets two independent consumers share one socket without nesting their messages inside a
    muxws-defined vocabulary, and without a muxws release when a message format changes.

    Three assertions: the payload arrives unchanged in both directions; every envelope field that
    has a same-named payload key keeps its own value, so `stream: 99` is not the frame's stream and
    `end: true` half-closes nothing; and no payload key was subscripted. A round-trip test does not
    check the second.
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
        # `open`, not `request`: `request` sets `end=True` itself, so the frame's `end` would equal
        # the payload's.
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

        # No reset, no goaway, both peers still connected.
        assert pair.frames_of_type("dialer", "reset") == []
        assert pair.frames_of_type("acceptor", "reset") == []
        assert pair.frames_of_type("acceptor", "goaway") == []
        assert pair.dialer.is_open is True
        assert pair.acceptor.is_open is True
    finally:
        await pair.stop()


async def test_an_envelope_lookalike_payload_survives_being_fragmented(make_pair):
    """WSM-FRM-006 for a fragmented payload.

    The splitter cuts the codec's encoding of the payload into byte ranges (WSM-FRG-011), so a
    fragment of this payload is a string containing `"type":"reset"`. The test fails if the
    reassembler reads a fragment's contents or the receiver merges the reassembled payload into the
    envelope.
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
        # The receiver reassembled the whole payload and read no key from it.
        assert reassembled == [big]
        assert pair.frames_of_type("acceptor", "goaway") == []
    finally:
        await pair.stop()


#: WSM-API-008's partition, spelled out. `connect`, `accept`, `serve`, `notify`, `request`, `ping`,
#: `close` and every `Stream` send method are async; everything else must not be.
#:
#: Entries the rule does not name: `Stream.result` and `Stream.cancel`/`reset` wait or send, so they
#: are async; `SocketAdapter`'s four members are the transport interface of WSM-API-021 and are async.
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
        "Stream.send_headers",
        "accept",
        "connect",
        "serve",
    }
)


def test_the_public_api_is_async_exactly_where_the_rule_says():
    """WSM-API-008: checked on the signatures.

    The set of async callables is compared for equality, so both an async callable that should be
    sync and the reverse fail. `peer.open()` is synchronous (WSM-API-001), which lets a stream be
    opened and its id enqueued in one step (WSM-INV-005); behavioural tests do not detect an
    `async def open`. An async `on_stream` would let a pushed stream arrive before registration
    completes (WSM-STM-033).
    """
    import muxws

    surface = _public_callables()
    assert len(surface) > 60, "the walker found almost nothing to check"
    asynchronous = {name for name, function in surface if inspect.iscoroutinefunction(function)}
    assert asynchronous == _MUST_BE_ASYNC

    # The ones §5.1 lists, plus the one it forbids, asserted individually.
    for name in ("connect", "accept", "serve"):
        assert inspect.iscoroutinefunction(getattr(muxws, name)), name
    assert not inspect.iscoroutinefunction(Peer.open)
    assert not inspect.iscoroutinefunction(Peer.on_stream)


async def test_the_opening_payload_is_never_looked_at_for_routing(make_pair):
    """WSM-AUT-004 / WSM-STM-030: one handler, and it gets everything.

    The payloads contain routing-like content: two different paths and methods, a string with no
    keys, and a null. All four reach the same handler in the order they were opened, and none is
    refused. Other tests send one payload to one handler and do not detect routing on a `path` key.

    `test_second_on_stream_replaces_and_logs` checks that there is one handler slot, not a table.
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
    """WSM-ERR-003: errors are raised where they are awaited, and no hook receives them instead.

    `socket_death_test.py` checks that every shape raises. This test checks that no hook diverts the
    error: an error handled outside the call site is not seen by the caller's `try`, and the await
    either hangs or returns a value that was never sent.

    The test lists the four hooks exhaustively and registers all of them before failing a stream.
    All four fire, all see the reset, and the exception is still raised from the `await`.
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

        # `on_frame` observed the reset without consuming it; it is an observer (WSM-OBS-003), not a
        # handler.
        assert "rx:reset" in frames_seen
        assert closes_seen == [], "the connection is fine; one stream failed"
    finally:
        await pair.stop()


# --------------------------------------------------------------------------- header interceptors


async def test_open_interceptors_shape_the_headers_of_every_open(make_pair):
    """WSM-API-026/027: in registration order, on a copy, before the frame is built, for every opening call."""
    pair = make_pair()
    received: list[dict[str, Any]] = []

    async def record(_payload: Any, stream: Stream) -> None:
        received.append(dict(stream.headers))
        await stream.reply({"ok": True})

    pair.acceptor.on_stream(record)
    calls: list[tuple[str, Any]] = []
    kept: list[dict[str, Any]] = []

    def first(headers: dict[str, Any], payload: Any) -> None:
        calls.append(("first", payload))
        headers["trace"] = "t-1"
        headers["attempt"] = 2
        kept.append(headers)

    def second(headers: dict[str, Any], _payload: Any) -> None:
        calls.append(("second", dict(headers)))
        headers.pop("drop-me", None)

    unregister_first = pair.dialer.intercept_open(first)
    unregister_second = pair.dialer.intercept_open(second)
    pair.start()
    try:
        callers = {"attempt": 1, "drop-me": True, "own": "x"}
        stream = pair.dialer.open({"n": 1}, headers=callers, end=True)
        kept[0]["late"] = "after return"
        await stream

        assert callers == {"attempt": 1, "drop-me": True, "own": "x"}, "the caller's mapping is untouched"
        assert calls == [("first", {"n": 1}), ("second", {"attempt": 2, "drop-me": True, "own": "x", "trace": "t-1"})]
        assert stream.headers == {"attempt": 2, "own": "x", "trace": "t-1"}
        assert received == [{"attempt": 2, "own": "x", "trace": "t-1"}], "a change after return never reaches the frame"

        await pair.dialer.request("r")
        await pair.dialer.notify("n")
        await pair.settle()
        assert [headers["trace"] for headers in received] == ["t-1", "t-1", "t-1"], "request() and notify() too"

        unregister_first()
        unregister_first()
        unregister_second()
        before = len(pair.dialer_socket.sent)
        await pair.dialer.request("bare")
        assert '"headers"' not in pair.dialer_socket.sent[before], "no interceptor left, no headers field"

        pair.dialer.intercept_open(lambda _headers, _payload: None)
        before = len(pair.dialer_socket.sent)
        await pair.dialer.request("bare")
        assert '"headers"' not in pair.dialer_socket.sent[before], "an interceptor that adds nothing changes no byte"
    finally:
        await pair.stop()


async def test_an_open_interceptor_that_raises_fails_the_open_and_spends_nothing(make_pair):
    """WSM-API-027: the interceptor's own exception at the call site, no id allocated, nothing enqueued."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    later: list[str] = []

    def refuse(_headers: dict[str, Any], _payload: Any) -> None:
        raise ValueError("no ambient token")

    unregister = pair.dialer.intercept_open(refuse)
    pair.dialer.intercept_open(lambda _headers, _payload: later.append("ran"))
    pair.start()
    try:
        with pytest.raises(ValueError, match="no ambient token"):
            pair.dialer.open("x")
        with pytest.raises(ValueError, match="no ambient token"):
            await pair.dialer.request("x")
        assert later == [], "the interceptors after the one that raised do not run"
        await pair.settle()
        assert pair.dialer_socket.sent == [], "nothing reached the wire"
        unregister()

        async def asynchronous(headers: dict[str, Any], _payload: Any) -> None:
            headers["late"] = True

        unregister = pair.dialer.intercept_open(asynchronous)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(TypeError, match="awaitable"):
                pair.dialer.open("x")
        assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
        unregister()

        unregister = pair.dialer.intercept_open(lambda headers, _payload: headers.__setitem__(7, "seven"))
        with pytest.raises(TypeError, match="non-string"):
            pair.dialer.open("x")
        unregister()

        assert pair.dialer.open("x").id == 1, "no failed open spent an id"
        assert pair.dialer.is_open
    finally:
        await pair.stop()


async def test_reply_headers_interceptors_shape_the_first_frame_of_every_answer(make_pair):
    """WSM-API-028: on a copy, once per stream the remote opened, whichever call sends the first frame."""
    pair = make_pair()
    seen: list[tuple[dict[str, Any], str]] = []
    kept: list[dict[str, Any]] = []
    announced = {"content-type": "text/csv"}
    on_own_streams: list[Any] = []

    def stamp(headers: dict[str, Any], stream: Stream) -> None:
        seen.append((dict(headers), stream.payload))
        headers["served-by"] = "acceptor"
        headers.pop("drop-me", None)
        kept.append(headers)

    async def handler(payload: Any, stream: Stream) -> None:
        if payload == "announce":
            await stream.send_headers(announced)
            kept[-1]["late"] = "after return"
            await stream.send("row")
            await stream.end("last")
        elif payload == "reply":
            await stream.reply("done", headers={"drop-me": 1})

    pair.acceptor.on_stream(handler)
    unregister = pair.acceptor.intercept_reply_headers(stamp)
    pair.dialer.intercept_reply_headers(lambda _headers, stream: on_own_streams.append(stream.id))
    pair.start()
    try:
        stream = pair.dialer.open("announce")
        await stream.end()
        await asyncio.wait_for(stream.closed.wait(), 5.0)
        assert stream.reply_headers == {"content-type": "text/csv", "served-by": "acceptor"}
        assert announced == {"content-type": "text/csv"}, "the caller's mapping is untouched"

        stream = pair.dialer.open("reply", end=True)
        assert await asyncio.wait_for(stream, 5.0) == "done"
        assert stream.reply_headers == {"served-by": "acceptor"}

        stream = pair.dialer.open("implicit", end=True)
        await asyncio.wait_for(stream.closed.wait(), 5.0)
        assert stream.reply_headers == {"served-by": "acceptor"}, "the implicit end of a returning handler too"

        assert seen == [({"content-type": "text/csv"}, "announce"), ({"drop-me": 1}, "reply"), ({}, "implicit")]
        assert on_own_streams == [], "never on a stream this peer opened"

        unregister()
        stream = pair.dialer.open("implicit", end=True)
        await stream.closed.wait()
        assert pair.frames_of_type("acceptor", "data")[-1].headers is None, "no interceptor left, no headers field"
    finally:
        await pair.stop()


async def test_a_reply_headers_interceptor_that_raises_fails_the_call_and_spends_nothing(make_pair):
    """WSM-API-028: the call raises and sends nothing, the first frame stays unsent, and at the
    implicit end of a handler the failure is reported as the handler's."""
    pair = make_pair()
    sent_before_answer: list[int] = []

    def refuse(_headers: dict[str, Any], _stream: Stream) -> None:
        raise ValueError("no tenant")

    async def asynchronous(headers: dict[str, Any], _stream: Stream) -> None:
        headers["late"] = True

    async def handler(payload: Any, stream: Stream) -> None:
        if payload == "implicit":
            return
        unregister = pair.acceptor.intercept_reply_headers(refuse)
        with pytest.raises(ValueError, match="no tenant"):
            await stream.send_headers({"a": 1})
        unregister()
        unregister = pair.acceptor.intercept_reply_headers(asynchronous)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(TypeError, match="awaitable"):
                await stream.send("x")
        assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
        unregister()
        sent_before_answer.append(len(pair.acceptor_socket.sent))
        await stream.reply("ok", headers={"a": 1})

    pair.acceptor.on_stream(handler)
    pair.start()
    try:
        stream = pair.dialer.open("explicit", end=True)
        assert await asyncio.wait_for(stream, 5.0) == "ok"
        assert sent_before_answer == [0], "a failed call put nothing on the wire"
        assert stream.reply_headers == {"a": 1}, "and left the first frame for the next call"

        pair.acceptor.intercept_reply_headers(refuse)
        with pytest.raises(RemoteError, match="no tenant"):
            await pair.dialer.request("implicit", timeout=5.0)
        assert pair.dialer.is_open
        assert pair.acceptor.is_open
    finally:
        await pair.stop()


async def test_serve_registers_the_interceptors_it_is_given():
    """WSM-API-026: `serve()` builds its peer itself, so the interceptors are its arguments."""
    left, right = memory_pair()
    dialer = Peer(left, codec=JsonCodec(), is_dialer=True)
    seen: list[Any] = []

    async def echo(_payload: Any, stream: Stream) -> None:
        await stream.reply(dict(stream.headers))

    tasks = [
        asyncio.create_task(
            muxws.serve(
                right,
                handler=echo,
                codec=JsonCodec(),
                reply_headers_interceptors=[lambda headers, _stream: headers.update(served="yes")],
                incoming_headers_interceptors=[
                    lambda headers, _stream: seen.append(dict(headers)),
                    lambda _headers, _stream: seen.append("second"),
                ],
            )
        ),
        asyncio.create_task(dialer.serve()),
    ]
    try:
        stream = dialer.open("q", headers={"trace": "t-1"}, end=True)
        assert await asyncio.wait_for(stream, 5.0) == {"trace": "t-1"}
        assert stream.reply_headers == {"served": "yes"}
        assert seen == [{"trace": "t-1"}, "second"]
    finally:
        await left.drop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_incoming_interceptors_see_each_streams_leading_headers_once(make_pair):
    """WSM-API-029: the remote's open on one side, the remote's first data on the other, read-only."""
    pair = make_pair()
    order: list[str] = []
    seen_by_acceptor: list[tuple[dict[str, Any], bool]] = []
    seen_by_dialer: list[tuple[dict[str, Any], int, bool]] = []

    async def handler(payload: Any, stream: Stream) -> None:
        order.append("handler")
        if payload == "reset":
            await stream.reset(ResetCode.APPLICATION_ERROR, "no answer")
            return
        await stream.send_headers({"content-type": "text/csv"})
        await stream.send("row")
        await stream.end("last")

    def acceptor_side(headers: Any, stream: Stream) -> None:
        order.append("interceptor")
        seen_by_acceptor.append((dict(headers), stream.local))
        with pytest.raises(TypeError):
            headers["x"] = 1

    pair.acceptor.on_stream(handler)
    pair.acceptor.intercept_incoming_headers(acceptor_side)
    pair.dialer.intercept_incoming_headers(
        lambda headers, stream: seen_by_dialer.append((dict(headers), stream.id, stream.local))
    )
    pair.start()
    try:
        stream = pair.dialer.open("go", headers={"trace": "abc"})
        assert [item async for item in stream] == ["row", "last"]
        assert order == ["interceptor", "handler"], "before the handler runs"
        assert seen_by_acceptor == [({"trace": "abc"}, False)]
        assert seen_by_dialer == [({"content-type": "text/csv"}, stream.id, True)], "once, on the answer"

        with pytest.raises(StreamReset):
            await pair.dialer.request("reset")
        await pair.settle()
        assert len(seen_by_dialer) == 1, "a reset is not an answer"
        assert seen_by_acceptor[-1] == ({}, False), "an open without headers is seen with an empty mapping"
    finally:
        await pair.stop()


async def test_an_incoming_interceptor_that_raises_is_logged_and_changes_nothing(make_pair, caplog):
    """WSM-API-029: logged, the next interceptor still runs, and stream and connection carry on."""
    pair = make_pair()
    pair.acceptor.on_stream(_reply_now)
    after: list[Any] = []

    def broken(_headers: Any, _stream: Stream) -> None:
        raise RuntimeError("interceptor bug")

    def cancelled(_headers: Any, _stream: Stream) -> None:
        raise asyncio.CancelledError

    async def asynchronous(_headers: Any, _stream: Stream) -> None:
        after.append("never awaited")

    pair.acceptor.intercept_incoming_headers(broken)
    pair.acceptor.intercept_incoming_headers(cancelled)
    pair.acceptor.intercept_incoming_headers(asynchronous)
    pair.acceptor.intercept_incoming_headers(lambda headers, _stream: after.append(dict(headers)))
    pair.dialer.intercept_incoming_headers(broken)
    pair.start()
    try:
        with caplog.at_level(logging.ERROR, logger="muxws.frames"), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert await pair.dialer.request("q", headers={"k": "v"}) == {"ok": True}
        assert after == [{"k": "v"}]
        assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
        messages = [record.getMessage() for record in caplog.records]
        assert sum("incoming headers interceptor raised" in message for message in messages) == 3
        assert any("returned an awaitable" in message for message in messages)
        assert pair.dialer.is_open
        assert pair.acceptor.is_open
    finally:
        await pair.stop()
