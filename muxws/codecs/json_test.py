import json

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ProtocolError
from muxws.frames import Frame


@pytest.fixture
def codec() -> JsonCodec:
    return JsonCodec()


def test_binary_is_declared_false(codec: JsonCodec):
    """WSM-CDC-001/002: `binary` is declared, never inferred from a value's type."""
    assert codec.name == "json"
    assert codec.binary is False
    assert isinstance(JsonCodec.binary, bool)


def test_bytes_payload_is_rejected_not_base64ed(codec: JsonCodec):
    """WSM-CDC-008: bytes are a payload type only under a binary codec, and are never base64ed."""
    with pytest.raises(TypeError, match="WSM-CDC-008"):
        codec.encode(Frame("data", stream=1, payload=b"\x00\xff"))
    with pytest.raises(TypeError, match="WSM-CDC-008"):
        codec.encode_payload({"blob": b"\x00\xff"})


def test_undecodable_message_is_a_protocol_error(codec: JsonCodec):
    """WSM-FRM-005: a message the codec refuses to decode is a connection-level protocol error."""
    with pytest.raises(ProtocolError):
        codec.decode("{this is not json")
    with pytest.raises(ProtocolError, match="object"):
        codec.decode("[1,2,3]")


def test_encoding_is_compact_and_not_ascii_escaped(codec: JsonCodec):
    """WSM-FRG-016 needs both ports to emit the same bytes, which pins separators and escaping."""
    encoded = codec.encode(Frame("data", stream=1, payload={"a": 1, "b": "č"}))
    assert " " not in encoded
    assert "\\u" not in encoded
    assert "č" in encoded


def test_payload_round_trip_is_independent_of_the_envelope(codec: JsonCodec):
    """The payload-level half of the port: what fragmentation slices and reassembles."""
    for payload in ({"a": [1, 2]}, "plain", 17, None, [], {"non-bmp": "𝕄"}):
        assert codec.decode_payload(codec.encode_payload(payload)) == payload


def test_undecodable_reassembled_payload_is_a_protocol_error(codec: JsonCodec):
    with pytest.raises(ProtocolError, match="reassembled"):
        codec.decode_payload("{truncated")


def test_nan_and_infinity_are_refused(codec: JsonCodec):
    """Neither has a JSON representation; emitting the JavaScript-only spelling would not round-trip."""
    with pytest.raises(ValueError, match="(?i)nan|infinit|not.*compliant"):
        codec.encode_payload(float("nan"))
    with pytest.raises(ValueError, match="(?i)inf|not.*compliant"):
        codec.encode_payload(float("inf"))


def test_decode_accepts_bytes_as_well_as_str(codec: JsonCodec):
    """A text codec may still be handed bytes by a transport; decoding must not care."""
    assert codec.decode(b'{"type":"ping","nonce":"a"}') == Frame("ping", nonce="a")


def test_encode_output_parses_back_to_the_same_object(codec: JsonCodec):
    frame = Frame("open", stream=1, headers={"trace": "x"}, payload={"a": 1}, end=True)
    assert json.loads(codec.encode(frame)) == {
        "type": "open",
        "stream": 1,
        "end": True,
        "headers": {"trace": "x"},
        "payload": {"a": 1},
    }


def test_unencodable_non_bytes_value_is_refused_too(codec: JsonCodec):
    """The default hook refuses everything it cannot represent, not just bytes."""
    with pytest.raises(TypeError, match="cannot encode set"):
        codec.encode_payload({"tags": {1, 2}})


@pytest.mark.parametrize(
    ("value", "python_form", "javascript_form"),
    [
        (1.0, "1.0", "1"),
        (-0.0, "-0.0", "0"),
        (100.0, "100.0", "100"),
        (1e16, "1e+16", "10000000000000000"),
        (1e-7, "1e-07", "1e-7"),
        (1e-6, "1e-06", "0.000001"),
        (12345678901234567890, "12345678901234567890", "12345678901234567000"),
    ],
)
def test_number_forms_that_the_two_ports_spell_differently(
    value: float | int, python_form: str, javascript_form: str, codec: JsonCodec
):
    """Pins the known WSM-FRG-016 divergence so it cannot silently widen.

    Fragment boundaries are cut over the encoded payload, so any value the two ports spell
    differently is cut differently by them. That is invisible on the wire - the sender chooses the
    cuts and the receiver only concatenates - but it bounds what the shared boundary corpus may
    contain. See GAPS.md.
    """
    assert codec.encode_payload(value) == python_form
    assert python_form != javascript_form


@pytest.mark.parametrize("value", [0, 1, -1, 42, 9007199254740992, 0.1, 1.5, 1e21, True, False, None, "text"])
def test_number_and_scalar_forms_the_two_ports_agree_on(value: object, codec: JsonCodec):
    """The complement of the list above: everything here is safe in the shared boundary corpus."""
    encoded = codec.encode_payload(value)
    assert encoded == encoded.strip()
    assert codec.decode_payload(encoded) == value
