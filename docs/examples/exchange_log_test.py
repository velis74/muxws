"""White-box tests of `ExchangeLog` for what a live connection does not produce deterministically."""

import json

from typing import Any

import pytest

from exchange_log import ExchangeLog, trim_text

from muxws import CloseReason, Frame, JsonCodec, split_frame

CODEC = JsonCodec()


class FakePeer:
    """The three members `ExchangeLog.attach` uses, driven by the test."""

    def __init__(self) -> None:
        self.id = "t-0"
        self.frame_handlers: list[Any] = []
        self.close_handlers: list[Any] = []

    def on_frame(self, handler: Any) -> None:
        self.frame_handlers.append(handler)

    def on_close(self, handler: Any) -> None:
        self.close_handlers.append(handler)

    def frame(self, direction: str, frame: Frame) -> None:
        for handler in self.frame_handlers:
            handler(direction, frame, len(CODEC.encode(frame).encode("utf-8")))

    def close(self) -> None:
        for handler in self.close_handlers:
            handler(CloseReason(code=1006, reason="", was_clean=False, will_retry=False))


def attached(**options: Any) -> tuple[FakePeer, list[dict[str, Any]]]:
    lines: list[str] = []
    ticks = iter(range(1000))
    log = ExchangeLog(CODEC, write=lines.append, clock=lambda: next(ticks) / 1000, **options)
    peer = FakePeer()
    log.attach(peer)
    return peer, lines  # type: ignore[return-value]


def records(lines: list[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in lines]  # type: ignore[arg-type]


def test_fragmented_payloads_are_logged_once_each_and_bytes_count_every_fragment() -> None:
    peer, lines = attached(trim_payload_size=None)
    request = {"rows": ["é" * 300] * 3}
    response = list(range(400))
    request_fragments = split_frame(
        Frame("open", stream=1, payload=request, headers={"op": "put"}, end=True), 256, CODEC
    )
    response_fragments = split_frame(Frame("data", stream=1, payload=response, end=True), 256, CODEC)
    assert len(request_fragments) > 2
    assert len(response_fragments) > 2

    for fragment in request_fragments:
        peer.frame("tx", fragment)
    for fragment in response_fragments:
        peer.frame("rx", fragment)

    (record,) = records(lines)
    assert record["request"]["payloads"] == [request]
    assert record["request"]["headers"] == {"op": "put"}
    assert record["response"]["payloads"] == [response]
    assert record["request"]["bytes"] == sum(len(CODEC.encode(f).encode()) for f in request_fragments)
    assert record["response"]["bytes"] == sum(len(CODEC.encode(f).encode()) for f in response_fragments)
    assert record["outcome"] == "ok"


def test_fragments_of_two_sides_interleave_without_mixing() -> None:
    peer, lines = attached(trim_payload_size=None)
    peer.frame("rx", Frame("open", stream=2, payload="go"))
    upload = split_frame(Frame("data", stream=2, payload="u" * 900, end=True), 256, CODEC)
    download = split_frame(Frame("data", stream=2, payload="d" * 900, end=True), 256, CODEC)
    for received, sent in zip(upload, download, strict=True):
        peer.frame("rx", received)
        peer.frame("tx", sent)

    (record,) = records(lines)
    assert record["opened_by"] == "remote"
    assert record["request"]["payloads"] == ["go", "u" * 900]
    assert record["response"]["payloads"] == ["d" * 900]


def test_lost_connection_writes_every_unfinished_stream_and_starts_a_new_id_space() -> None:
    peer, lines = attached()
    peer.frame("tx", Frame("open", stream=1, payload=1, end=True))
    peer.frame("rx", Frame("data", stream=1, payload=2))
    peer.frame("rx", Frame("open", stream=2, payload=3))
    peer.close()

    lost = records(lines)
    assert [(r["stream"], r["outcome"], r["code"], r["reason"]) for r in lost] == [
        (1, "lost", None, None),
        (2, "lost", None, None),
    ]
    assert lost[0]["response"]["payloads"] == [2]
    assert lost[1]["first_response_ms"] is None

    peer.frame("tx", Frame("open", stream=1, payload="again", end=True))
    peer.frame("rx", Frame("data", stream=1, payload="ok", end=True))
    assert records(lines)[2]["request"]["payloads"] == ["again"]


def test_trim_cuts_on_a_character_boundary() -> None:
    # "ab€€" is 10 bytes; a 4-byte limit falls inside the first three-byte '€'.
    text = json.dumps("ab€€", ensure_ascii=False)
    assert trim_text(text, 4) == '"ab…(trimmed 7 bytes)…'
    assert trim_text(text, 6) == '"ab€…(trimmed 4 bytes)…'
    assert trim_text(text, len(text.encode())) is None
    assert trim_text(text, None) is None


def test_trimmed_payload_is_a_string_and_short_one_stays_a_json_value() -> None:
    peer, lines = attached(trim_payload_size=10)
    peer.frame("tx", Frame("open", stream=1, payload={"k": "ččččč"}, end=True))
    peer.frame("rx", Frame("data", stream=1, payload=[1, 2], end=True))

    (record,) = records(lines)
    assert record["request"]["payloads"] == ['{"k":"čč…(trimmed 8 bytes)…']
    assert record["response"]["payloads"] == [[1, 2]]


def test_dropped_fragmented_payloads_are_counted_without_reassembly() -> None:
    peer, lines = attached(max_messages=1)
    peer.frame("tx", Frame("open", stream=1, payload="first"))
    for fragment in split_frame(Frame("data", stream=1, payload="x" * 900, end=True), 256, CODEC):
        peer.frame("tx", fragment)
    peer.frame("rx", Frame("data", stream=1, end=True))

    (record,) = records(lines)
    assert record["request"]["payloads"] == ["first"]
    assert record["request"]["dropped"] == 1


def test_a_failing_write_on_close_drops_only_its_stream_and_the_next_socket_is_logged(caplog: Any) -> None:
    lines: list[str] = []

    def write(line: str) -> None:
        if not lines and '"stream":1' in line:
            lines.append("failed")
            raise OSError("disk full")
        lines.append(line)

    log = ExchangeLog(CODEC, write=write)
    peer = FakePeer()
    log.attach(peer)
    peer.frame("tx", Frame("open", stream=1, payload="a"))
    peer.frame("tx", Frame("open", stream=3, payload="b"))
    peer.close()

    assert [r["stream"] for r in records(lines[1:])] == [3]
    assert "exchange log dropped stream 1" in caplog.text

    peer.frame("tx", Frame("open", stream=1, payload="again", end=True))
    peer.frame("rx", Frame("data", stream=1, payload="ok", end=True))
    assert records(lines[2:])[0]["request"]["payloads"] == ["again"]


def test_a_failing_write_on_a_frame_drops_only_its_stream() -> None:
    lines: list[str] = []

    def write(line: str) -> None:
        if '"boom"' in line:
            raise OSError("disk full")
        lines.append(line)

    log = ExchangeLog(CODEC, write=write)
    peer = FakePeer()
    log.attach(peer)
    peer.frame("tx", Frame("open", stream=1, payload="boom", end=True))
    peer.frame("tx", Frame("open", stream=3, payload="fine", end=True))
    peer.frame("rx", Frame("data", stream=1, payload="ok", end=True))
    peer.frame("rx", Frame("data", stream=3, payload="ok", end=True))

    assert [r["stream"] for r in records(lines)] == [3]


def test_a_raising_redact_or_include_drops_only_its_stream(caplog: Any) -> None:
    def redact(payload: Any, _side: str) -> Any:
        if payload == "secret":
            raise ValueError("cannot redact")
        return payload

    def include(_headers: dict[str, Any] | None, payload: Any) -> bool:
        if payload == "odd":
            raise ValueError("cannot decide")
        return True

    peer, lines = attached(redact=redact, include=include)
    peer.frame("tx", Frame("open", stream=1, payload="secret", end=True))
    peer.frame("tx", Frame("open", stream=3, payload="odd", end=True))
    peer.frame("tx", Frame("open", stream=5, payload="plain", end=True))
    for stream in (1, 3, 5):
        peer.frame("rx", Frame("data", stream=stream, payload="ok", end=True))

    assert [r["stream"] for r in records(lines)] == [5]
    assert caplog.text.count("exchange log dropped stream") == 2


def test_include_decides_a_stream_that_ends_before_its_opening_payload_is_complete() -> None:
    seen: list[tuple[Any, Any]] = []

    def include(headers: dict[str, Any] | None, payload: Any) -> bool:
        seen.append((headers, payload))
        return headers is None or "authorization" not in headers

    peer, lines = attached(include=include)
    for stream, headers in ((2, {"authorization": "Bearer x"}), (4, {"trace": "t"})):
        opening = Frame("open", stream=stream, payload="x" * 900, headers=headers, end=True)
        peer.frame("rx", split_frame(opening, 256, CODEC)[0])
    peer.close()

    assert seen == [({"authorization": "Bearer x"}, None), ({"trace": "t"}, None)]
    assert [(r["stream"], r["outcome"]) for r in records(lines)] == [(4, "lost")]


def test_non_finite_numbers_and_lone_surrogates_are_logged_as_strict_json() -> None:
    peer, lines = attached()
    (handler,) = peer.frame_handlers  # the JSON codec cannot encode NaN; msgpack carries it
    handler("tx", Frame("open", stream=1, payload=[float("nan"), float("inf")], end=True), 20)
    handler("rx", Frame("data", stream=1, payload="a\ud800b", end=True), 20)

    (line,) = lines
    record = json.loads(line, parse_constant=lambda name: pytest.fail(f"{name} in the line"))
    assert record["request"]["payloads"] == [[None, None]]
    assert record["response"]["payloads"] == ["a\ud800b"]
    assert "\\ud800" in line
    line.encode("utf-8")


def test_include_sees_the_reassembled_opening_payload() -> None:
    seen: list[Any] = []

    def include(_headers: dict[str, Any] | None, payload: Any) -> bool:
        seen.append(payload)
        return isinstance(payload, dict) and payload["keep"]

    peer, lines = attached(include=include)
    for stream, keep in ((1, True), (3, False)):
        for fragment in split_frame(Frame("open", stream=stream, payload={"keep": keep, "pad": "x" * 600}), 256, CODEC):
            peer.frame("tx", fragment)
        peer.frame("tx", Frame("data", stream=stream, end=True))
        peer.frame("rx", Frame("data", stream=stream, payload="r", end=True))

    assert [payload["keep"] for payload in seen] == [True, False]
    (record,) = records(lines)
    assert record["stream"] == 1
    assert record["request"]["payloads"] == [{"keep": True, "pad": "x" * 600}]


def test_bytes_and_bytes_keys_are_logged_as_base64() -> None:
    peer, lines = attached()
    (handler,) = peer.frame_handlers  # the JSON codec carries neither; msgpack carries both
    handler("tx", Frame("open", stream=1, payload={b"\x01\x02": b"\x03", (1, 2): 3}, end=True), 20)
    handler("rx", Frame("data", stream=1, payload=b"\x01\x02\x03", end=True), 20)

    (record,) = records(lines)
    assert record["request"]["payloads"] == [{"AQI=": "Aw==", "(1, 2)": 3}]
    assert record["response"]["payloads"] == ["AQID"]


def test_frames_without_a_stream_are_ignored() -> None:
    peer, lines = attached()
    peer.frame("tx", Frame("ping", nonce="n"))
    peer.frame("rx", Frame("pong", nonce="n"))
    peer.frame("rx", Frame("goaway", last_stream=0))
    peer.frame("tx", Frame("open", stream=1, payload=1, end=True))
    peer.frame("rx", Frame("data", stream=1, payload=2, end=True))

    assert [r["stream"] for r in records(lines)] == [1]


def test_a_payload_of_exactly_trim_payload_size_bytes_stays_a_json_value() -> None:
    payload = {"k": "č" * 4}
    size = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    peer, lines = attached(trim_payload_size=size)
    peer.frame("tx", Frame("open", stream=1, payload=payload, end=True))
    peer.frame("rx", Frame("data", stream=1, payload=[payload, 1], end=True))

    (record,) = records(lines)
    assert record["request"]["payloads"] == [payload]
    assert isinstance(record["response"]["payloads"][0], str)
