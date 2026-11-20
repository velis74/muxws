"""White-box tests of `on_exchange` recording and `Exchange.as_dict` (WSM-OBS-004)."""

from __future__ import annotations

import json
import logging

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from muxws.errors import ResetCode, StreamRefused
from muxws.exchange import Exchange, ExchangeSide
from muxws.frames import Frame
from muxws.stream import Stream


def _side(payloads=(), *, headers=None, trailers=None, count=None, ended=True) -> ExchangeSide:
    return ExchangeSide(headers, tuple(payloads), len(payloads) if count is None else count, trailers, ended)


def _exchange(request: ExchangeSide, response: ExchangeSide | None = None, **overrides) -> Exchange:
    values = {
        "conn": "abc-1",
        "stream": 1,
        "opened_by": "local",
        "oneway": False,
        "request": request,
        "response": response or _side(),
        "outcome": "ok",
        "reset_by": None,
        "code": None,
        "reason": None,
        "started_at": datetime(2026, 11, 19, 9, 32, 22, 359876, tzinfo=timezone.utc),
        "first_response": 0.0012344,
        "duration": 0.25,
    }
    values.update(overrides)
    return Exchange(**values)


# ---------------------------------------------------------------- as_dict


def test_as_dict_key_order_timestamp_and_milliseconds():
    result = _exchange(_side(["hi"])).as_dict()

    assert list(result) == [
        "ts",
        "conn",
        "stream",
        "opened_by",
        "oneway",
        "request",
        "response",
        "outcome",
        "reset_by",
        "code",
        "reason",
        "first_response_ms",
        "duration_ms",
    ]
    assert list(result["request"]) == ["headers", "payloads", "payload_count", "trailers", "ended"]
    assert result["ts"] == "2026-11-19T09:32:22.359Z"
    assert result["first_response_ms"] == 1.234
    assert result["duration_ms"] == 250.0


def test_as_dict_converts_a_non_utc_start_to_utc():
    start = datetime(2026, 11, 19, 10, 32, 22, 5000, tzinfo=timezone(timedelta(hours=1)))
    assert _exchange(_side(), started_at=start).as_dict()["ts"] == "2026-11-19T09:32:22.005Z"


def test_as_dict_null_first_response():
    assert _exchange(_side(), first_response=None).as_dict()["first_response_ms"] is None


def test_binary_values_become_base64_everywhere():
    request = _side(
        [b"\x00\x01", {"blob": bytearray(b"ab"), "list": [memoryview(b"c")]}],
        headers={"key": b"\xff"},
        trailers={"sig": (b"x",)},
    )
    result = _exchange(request).as_dict()["request"]

    assert result["payloads"] == ["AAE=", {"blob": "YWI=", "list": ["Yw=="]}]
    assert result["headers"] == {"key": "/w=="}
    assert result["trailers"] == {"sig": ["eA=="]}
    json.dumps(result)


def test_as_dict_is_a_fresh_structure():
    headers = {"auth": "token"}
    payload = {"user": {"pin": "x"}}
    exchange = _exchange(_side([payload], headers=headers))

    result = exchange.as_dict()
    result["request"]["headers"]["auth"] = "***"
    result["request"]["payloads"][0]["user"]["pin"] = "***"

    assert headers == {"auth": "token"}
    assert payload == {"user": {"pin": "x"}}
    assert exchange.request.payloads[0] is payload


def test_trim_keeps_payloads_within_the_limit_as_values():
    result = _exchange(_side([{"a": 1}, "abc"])).as_dict(trim_payload_size=7)
    assert result["request"]["payloads"] == [{"a": 1}, "abc"]


def test_trim_replaces_a_longer_payload_with_marked_text():
    result = _exchange(_side([{"name": "abcdefghij"}])).as_dict(trim_payload_size=10)
    # Compact JSON is {"name":"abcdefghij"}: 21 bytes, of which 11 are cut off.
    assert result["request"]["payloads"] == ['{"name":"a…(trimmed 11 bytes)…']


@pytest.mark.parametrize(
    ("limit", "kept", "cut"),
    [
        # "aé€😀" as JSON is "\"aé€😀\"": 1 + 1 + 2 + 3 + 4 + 1 = 12 bytes.
        (2, '"a', 10),
        (3, '"a', 10),
        (4, '"aé', 8),
        (5, '"aé', 8),
        (6, '"aé', 8),
        (7, '"aé€', 5),
        (10, '"aé€', 5),
        (11, '"aé€😀', 1),
    ],
)
def test_trim_cuts_back_to_a_utf8_character_boundary(limit, kept, cut):
    result = _exchange(_side(["aé€😀"])).as_dict(trim_payload_size=limit)
    assert result["request"]["payloads"] == [f"{kept}…(trimmed {cut} bytes)…"]


def test_trim_measures_binary_as_its_base64_text():
    # b"\x00" * 6 is "AAAAAAAA" in base64; quoted, 10 bytes.
    exchange = _exchange(_side([b"\x00" * 6]))
    assert exchange.as_dict(trim_payload_size=10)["request"]["payloads"] == ["AAAAAAAA"]
    assert exchange.as_dict(trim_payload_size=4)["request"]["payloads"] == ['"AAA…(trimmed 6 bytes)…']


def test_trim_zero_trims_every_payload_and_leaves_headers_and_trailers():
    exchange = _exchange(_side([None, 1], headers={"h": "x" * 50}, trailers={"t": "y" * 50}))
    result = exchange.as_dict(trim_payload_size=0)["request"]
    assert result["payloads"] == ["…(trimmed 4 bytes)…", "…(trimmed 1 bytes)…"]
    assert result["headers"] == {"h": "x" * 50}
    assert result["trailers"] == {"t": "y" * 50}


def test_trim_serialises_unknown_values_with_str():
    when = datetime(2026, 1, 2, tzinfo=timezone.utc)
    result = _exchange(_side([when])).as_dict(trim_payload_size=5)
    text = json.dumps(str(when))
    assert result["request"]["payloads"] == [f"{text[:5]}…(trimmed {len(text) - 5} bytes)…"]


# The same inputs and expected strings are in `ts/exchange.spec.ts` (WSM-OBS-011).
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            [1.0, -0.0, 1.5, 1e21, 1e-7, 1.2345678901234568e20, 0.000001, 2.5e-8, 1e300, -1e-300],
            "[1,0,1.5,1e+21,1e-7,123456789012345680000,0.000001,2.5e-8,1e+300,-1e-300]",
        ),
        ([float("nan"), float("inf"), float("-inf"), 9007199254740991], "[null,null,null,9007199254740991]"),
        ({"b": 1, "2": 2, "a": 3, "1": 4, "01": 5, "4294967295": 6}, '{"1":4,"2":2,"b":1,"a":3,"01":5,"4294967295":6}'),
        ({2: "a", 1.5: "b", 1.0: "c"}, '{"1":"c","2":"a","1.5":"b"}'),
        (["\ud800x", "\x00\x1f\x7f é"], '["\\ud800x","\\u0000\\u001f\x7f é"]'),
        ({"bin": b"\x00\x01", "nested": [{"x": [True, None]}]}, '{"bin":"AAE=","nested":[{"x":[true,null]}]}'),
    ],
)
def test_trim_text_is_the_same_in_both_ports(payload, expected):
    limit = len(expected.encode("utf-8")) - 1
    result = _exchange(_side([payload])).as_dict(trim_payload_size=limit)
    assert result["request"]["payloads"] == [f"{expected[:-1]}…(trimmed 1 bytes)…"]


def test_trim_measures_an_integral_float_as_an_integer():
    assert _exchange(_side([1.0, 100.0])).as_dict(trim_payload_size=3)["request"]["payloads"] == [1.0, 100.0]


def test_trim_serialises_a_value_json_cannot_write_with_str():
    result = _exchange(_side([{"a": Decimal("150")}])).as_dict(trim_payload_size=10)
    assert result["request"]["payloads"] == ['{"a":"150"…(trimmed 1 bytes)…']


@pytest.mark.parametrize("limit", [-1, 1.5, "10", True])
def test_trim_rejects_a_negative_or_non_integer_limit(limit):
    with pytest.raises(ValueError, match="trim_payload_size"):
        _exchange(_side()).as_dict(trim_payload_size=limit)


# ---------------------------------------------------------------- recording on a live pair


@pytest.mark.parametrize("value", [-1, 2.0, "3", True])
async def test_on_exchange_rejects_a_negative_or_non_integer_max_payloads(make_pair, value):
    built = make_pair()
    with pytest.raises(ValueError, match="max_payloads"):
        built.dialer.on_exchange(lambda _exchange: None, max_payloads=value)
    assert built.dialer._exchange_handlers == []


async def test_nothing_is_recorded_without_a_handler(make_pair):
    built = make_pair()
    created: list[Stream] = []

    async def handler(_payload, stream):
        created.append(stream)
        await stream.send(1)
        await stream.end(2, trailers={"t": 1})

    built.acceptor.on_stream(handler)
    built.start()
    try:
        stream = built.dialer.open({"q": 1}, headers={"h": 1}, end=True)
        assert stream._exchange is None
        assert [item async for item in stream] == [1, 2]
        await built.settle()
        assert created[0]._exchange is None
    finally:
        await built.stop()


async def test_max_payloads_slices_per_handler(make_pair):
    built = make_pair()
    seen: dict[str, Exchange] = {}
    built.dialer.on_exchange(lambda exchange: seen.__setitem__("zero", exchange), max_payloads=0)
    built.dialer.on_exchange(lambda exchange: seen.__setitem__("two", exchange), max_payloads=2)
    built.dialer.on_exchange(lambda exchange: seen.__setitem__("default", exchange))

    async def handler(_payload, stream):
        for index in range(5):
            await stream.send(index)
        await stream.end()

    built.acceptor.on_stream(handler)
    built.start()
    try:
        stream = built.dialer.open("go", end=True)
        assert stream._exchange.response.limit == 100
        assert [item async for item in stream] == [0, 1, 2, 3, 4]
        await built.settle()
    finally:
        await built.stop()

    assert seen["zero"].response.payloads == ()
    assert seen["two"].response.payloads == (0, 1)
    assert seen["default"].response.payloads == (0, 1, 2, 3, 4)
    assert {exchange.response.payload_count for exchange in seen.values()} == {5}
    assert seen["two"].request.payloads == ("go",)
    assert seen["zero"].request.payloads == ()


async def test_records_up_to_the_largest_limit_and_none_is_unlimited(make_pair):
    built = make_pair()
    built.dialer.on_exchange(lambda _exchange: None, max_payloads=3)
    built.dialer.on_exchange(lambda _exchange: None, max_payloads=1)
    assert built.dialer._exchange_limit == 3
    built.dialer.on_exchange(lambda _exchange: None, max_payloads=None)
    assert built.dialer._exchange_limit is None
    built.dialer.on_exchange(lambda _exchange: None, max_payloads=7)
    assert built.dialer._exchange_limit is None


async def test_a_limit_registered_later_gets_what_was_recorded(make_pair):
    built = make_pair()
    seen: list[Exchange] = []
    built.dialer.on_exchange(lambda _exchange: None, max_payloads=1)

    async def handler(_payload, stream):
        await stream.send(1)
        await stream.send(2)
        await stream.end(3)

    built.acceptor.on_stream(handler)
    built.start()
    try:
        stream = built.dialer.open(None, end=True)
        built.dialer.on_exchange(seen.append, max_payloads=None)
        assert [item async for item in stream] == [1, 2, 3]
        await built.settle()
    finally:
        await built.stop()

    assert seen[0].response.payloads == (1,)
    assert seen[0].response.payload_count == 3


async def test_a_raising_handler_is_logged_and_the_others_still_run(make_pair, caplog):
    built = make_pair()
    seen: list[Exchange] = []

    def broken(_exchange):
        raise RuntimeError("handler failure")

    built.dialer.on_exchange(broken)
    built.dialer.on_exchange(seen.append)
    built.acceptor.on_stream(lambda payload, stream: stream.reply(payload))
    built.start()
    try:
        with caplog.at_level(logging.ERROR, logger="muxws.frames"):
            assert await built.dialer.request("a") == "a"
            await built.settle()
            assert await built.dialer.request("b") == "b"
            await built.settle()
        assert built.dialer.is_open
    finally:
        await built.stop()

    assert [exchange.outcome for exchange in seen] == ["ok", "ok"]
    records = [record for record in caplog.records if "on_exchange handler raised" in record.getMessage()]
    assert len(records) == 2
    assert all(record.name == "muxws.frames" and record.levelno == logging.ERROR for record in records)
    assert records[0].exc_info[0] is RuntimeError


async def test_the_recording_is_released_when_the_exchange_fires(make_pair):
    built = make_pair()
    built.dialer.on_exchange(lambda _exchange: None)
    built.acceptor.on_stream(lambda payload, stream: stream.reply(payload))
    built.start()
    try:
        stream = built.dialer.open("x", end=True)
        assert stream._exchange is not None
        assert await stream == "x"
        await built.settle()
        assert stream._exchange is None
    finally:
        await built.stop()


async def test_a_stream_the_remote_goaway_refuses_is_a_remote_reset(make_pair):
    """A local stream above the remote's `last_stream` fails with REFUSED caused by the remote (WSM-CON-023)."""
    built = make_pair()
    seen: list[Exchange] = []
    built.dialer.on_exchange(seen.append)
    built.acceptor.on_stream(lambda _payload, stream: stream.closed.wait())
    built.start()
    try:
        stream = built.dialer.open({"q": 1})
        await built.settle()
        built.dialer_socket.inject(
            built.dialer._codec.encode(Frame("goaway", code=int(ResetCode.NO_ERROR), last_stream=0))
        )
        await built.settle()
        with pytest.raises(StreamRefused):
            await stream
    finally:
        await built.stop()

    [exchange] = seen
    assert exchange.outcome == "reset"
    assert exchange.reset_by == "remote"
    assert exchange.code == ResetCode.REFUSED.value
    assert exchange.reason == f"the remote went away before processing stream {stream.id}"


async def test_a_refusal_reports_the_reason_on_the_wire_on_both_peers(make_pair):
    """The refusing peer's exchange carries the reason of the `reset(REFUSED)` it sends."""
    built = make_pair()
    on_dialer: list[Exchange] = []
    on_acceptor: list[Exchange] = []
    built.dialer.on_exchange(on_dialer.append)
    built.acceptor.on_exchange(on_acceptor.append)
    built.start()
    try:
        with pytest.raises(StreamRefused):
            await built.dialer.request({"q": 1})
        await built.settle()
    finally:
        await built.stop()

    [reset] = built.frames_of_type("acceptor", "reset")
    assert reset.reason == "no on_stream handler"
    assert [(exchange.reset_by, exchange.reason) for exchange in on_dialer] == [("remote", reset.reason)]
    assert [(exchange.reset_by, exchange.reason) for exchange in on_acceptor] == [("local", reset.reason)]


async def test_a_refusal_after_this_peer_sent_goaway_reports_the_reason_on_the_wire(make_pair):
    """An open that arrives after this peer sent `goaway` is refused with the reason it sends."""
    built = make_pair()
    on_acceptor: list[Exchange] = []
    built.acceptor.on_exchange(on_acceptor.append)
    built.acceptor.on_stream(lambda payload, stream: stream.reply(payload))
    built.start()
    try:
        built.acceptor._send_goaway(ResetCode.NO_ERROR, None)
        stream = built.dialer.open({"q": 1}, end=True)
        with pytest.raises(StreamRefused):
            await stream
        await built.settle()
    finally:
        await built.stop()

    [reset] = built.frames_of_type("acceptor", "reset")
    assert reset.reason == "this peer is going away"
    assert [(exchange.outcome, exchange.reset_by, exchange.reason) for exchange in on_acceptor] == [
        ("reset", "local", reset.reason)
    ]


async def test_a_coroutine_handler_is_closed_without_running_and_logged(make_pair, caplog):
    built = make_pair()
    ran: list[Exchange] = []
    seen: list[Exchange] = []

    async def asynchronous(exchange):
        ran.append(exchange)

    built.dialer.on_exchange(asynchronous)
    built.dialer.on_exchange(seen.append)
    built.acceptor.on_stream(lambda payload, stream: stream.reply(payload))
    built.start()
    try:
        with caplog.at_level(logging.ERROR, logger="muxws.frames"):
            assert await built.dialer.request("a") == "a"
            await built.settle()
    finally:
        await built.stop()

    assert ran == []
    assert len(seen) == 1
    records = [record for record in caplog.records if "returned a coroutine" in record.getMessage()]
    assert len(records) == 1
    assert records[0].name == "muxws.frames"
    assert records[0].levelno == logging.ERROR
