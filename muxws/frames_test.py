import json

from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ProtocolError
from muxws.frames import ABSENT, Frame, from_mapping, to_mapping

CORPUS_PATH = Path(__file__).parent.parent / "conformance" / "frames" / "v1-frames.json"
CORPUS: list[dict[str, Any]] = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
CORPUS_IDS = [case["name"] for case in CORPUS]


def frame_from_fixture(mapping: dict[str, Any]) -> Frame:
    """Fixtures spell a frame as its envelope, so an absent payload key means ABSENT (D1)."""
    return from_mapping(mapping)


@pytest.fixture
def codec() -> JsonCodec:
    return JsonCodec()


@pytest.mark.parametrize("case", CORPUS, ids=CORPUS_IDS)
def test_conformance_wire_decodes_to_frame(case: dict[str, Any], codec: JsonCodec):
    """WSM-CDC-005, WSM-TST-001: decode(json_wire) == frame, comparing parsed objects."""
    assert codec.decode(case["json_wire"]) == frame_from_fixture(case["frame"])


@pytest.mark.parametrize("case", CORPUS, ids=CORPUS_IDS)
def test_conformance_round_trips(case: dict[str, Any], codec: JsonCodec):
    """WSM-CDC-005: decode(encode(frame)) == frame, never a byte comparison."""
    frame = frame_from_fixture(case["frame"])
    assert codec.decode(codec.encode(frame)) == frame


@pytest.mark.parametrize("case", CORPUS, ids=CORPUS_IDS)
def test_conformance_wire_is_equivalent_json(case: dict[str, Any], codec: JsonCodec):
    """The pinned wire and our own encoding parse to the same object (never compared as bytes)."""
    frame = frame_from_fixture(case["frame"])
    assert json.loads(codec.encode(frame)) == json.loads(case["json_wire"])


def test_canonical_key_order():
    """WSM-CDC-005: the one test permitted to assert key order - type, stream, then alphabetical."""
    frame = Frame(
        "data",
        stream=7,
        payload={"rows": 1},
        end=True,
        trailers={"checksum": "x"},
        reason="because",
        code=0,
    )
    assert list(to_mapping(frame)) == ["type", "stream", "code", "end", "payload", "reason", "trailers"]


def test_unknown_fields_are_dropped(codec: JsonCodec):
    """WSM-FRM-001, D2: a receiver ignores unknown envelope fields rather than preserving them."""
    with_extra = codec.decode('{"type":"data","stream":1,"payload":{"a":1},"colour":"red"}')
    without = codec.decode('{"type":"data","stream":1,"payload":{"a":1}}')
    assert with_extra == without
    assert not hasattr(with_extra, "colour")


def test_unknown_frame_type_decodes(codec: JsonCodec):
    """WSM-FRM-002, D4: an unrecognised type survives decoding; it is the peer that ignores it."""
    frame = codec.decode('{"type":"window_update","stream":1}')
    assert frame.type == "window_update"


def test_missing_type_and_payload_with_fragment_raise():
    """WSM-FRM-004, WSM-FRM-005: both are connection-level protocol errors."""
    with pytest.raises(ProtocolError, match="type"):
        from_mapping({"stream": 1})
    with pytest.raises(ProtocolError, match="fragment"):
        from_mapping({"type": "data", "stream": 1, "payload": 1, "fragment": "x"})


def test_absent_payload_is_not_null(codec: JsonCodec):
    """D1: ABSENT omits the key entirely; None emits an explicit null."""
    assert "payload" not in to_mapping(Frame("data", stream=1))
    assert to_mapping(Frame("data", stream=1, payload=None))["payload"] is None
    assert codec.encode(Frame("data", stream=1)) == '{"type":"data","stream":1}'
    assert codec.encode(Frame("data", stream=1, payload=None)) == '{"type":"data","stream":1,"payload":null}'
    assert codec.decode('{"type":"data","stream":1}').payload is ABSENT
    assert codec.decode('{"type":"data","stream":1,"payload":null}').payload is None


def test_no_settings_frame_exists(codec: JsonCodec):
    """WSM-CON-031, WSM-FRM-001: there is no settings frame and no field that could carry one."""
    from dataclasses import fields

    names = {f.name for f in fields(Frame)}
    assert "settings" not in names
    assert "ack" not in names
    assert "protocol_version" not in names
    assert "extensions" not in names

    decoded = codec.decode('{"type":"data","stream":1,"settings":{"max_frame_bytes":1},"ack":true}')
    assert decoded == Frame("data", stream=1)


def test_frame_is_frozen_and_compares_by_value():
    """D3: frozen, slotted, and equal when its fields are equal."""
    a = Frame("data", stream=1, payload={"x": 1})
    b = Frame("data", stream=1, payload={"x": 1})
    assert a == b
    with pytest.raises(AttributeError):
        a.stream = 2  # type: ignore[misc]


def test_absent_sentinel_is_falsy_and_names_itself():
    """D1: ABSENT reads well in a condition and in a test failure."""
    assert not ABSENT
    assert repr(ABSENT) == "ABSENT"


def test_corpus_covers_the_shapes_the_brief_requires():
    """The corpus is an acceptance criterion of its own; a silently shrinking one is worthless."""
    names = {case["name"] for case in CORPUS}
    required = {
        "unary-open-with-end",
        "open-with-headers",
        "data-end-with-trailers",
        "fragmented-open-first",
        "fragmented-open-last",
        "reset-with-structured-error",
        "ping",
        "pong",
        "goaway",
        "data-with-explicit-null-payload",
        "data-end-with-no-payload",
        "payload-with-non-bmp-characters",
    }
    assert required <= names
    assert not any(case["frame"]["type"] == "settings" for case in CORPUS)


def test_absent_survives_copy_and_deepcopy():
    """A frozen frame is routinely deep-copied; the sentinel must stay the same object."""
    from copy import copy, deepcopy

    assert copy(ABSENT) is ABSENT
    assert deepcopy(ABSENT) is ABSENT
    frame = Frame("data", stream=1)
    assert deepcopy(frame).payload is ABSENT
