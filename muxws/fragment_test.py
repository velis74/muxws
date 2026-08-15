import json

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ProtocolError
from muxws.fragment import Assembler, encoded_length, iter_fragments, MAX_FRAME_BYTES, split_frame
from muxws.frames import ABSENT, Frame, from_mapping, to_mapping

_CONFORMANCE = Path(__file__).parent.parent / "conformance" / "frames"
CORPUS: list[dict[str, Any]] = json.loads((_CONFORMANCE / "v1-frames.json").read_text(encoding="utf-8"))
BOUNDARIES: list[dict[str, Any]] = json.loads(
    (_CONFORMANCE / "v1-fragment-boundaries.json").read_text(encoding="utf-8")
)

#: Payloads chosen to exercise the splitter rather than the corpus: plain text, 3- and 4-byte
#: codepoints, control characters whose JSON escaping expands them sixfold, and a nested structure.
PAYLOADS: dict[str, Any] = {
    "ascii": {"body": "abcdefghij" * 400},
    "three-byte-codepoints": {"body": "č š ž — ъ ђ " * 300},
    "four-byte-codepoints": {"body": "🛰️𝕄𝕦𝕩🌍" * 300},
    "control-characters": {"body": "".join(chr(c) for c in range(1, 32)) * 200},
    "mixed": {"rows": [{"id": i, "name": f"vrstica {i} 🛰️"} for i in range(200)]},
    "bare-string": "x" * 5000,
    "bare-list": list(range(2000)),
}


@pytest.fixture
def codec() -> JsonCodec:
    return JsonCodec()


def _reassemble(parts: list[Frame], codec: JsonCodec) -> Any:
    """Feed `split_frame`'s output back through an Assembler, whether or not it fragmented."""
    if len(parts) == 1 and parts[0].fragment is None:
        return parts[0].payload
    assembler = Assembler()
    result: Any = ABSENT
    for part in parts:
        result = assembler.feed(part, codec)
    return result


def test_max_frame_bytes_is_the_protocol_constant():
    """WSM-FRG-004: 64 KiB, never negotiated, never announced, never read from configuration."""
    assert MAX_FRAME_BYTES == 65_536


def test_encoded_length_counts_utf8_bytes_not_characters():
    """WSM-FRG-002: a non-BMP character is four bytes, whatever any language calls its length."""
    assert encoded_length("abc") == 3
    assert encoded_length("č") == 2
    assert encoded_length("𝕄") == 4
    assert encoded_length(b"\x00\xff") == 2


@pytest.mark.parametrize("cap", [64, 96, 128, 256, 512, 1024, 4096])
@pytest.mark.parametrize("name", list(PAYLOADS), ids=list(PAYLOADS))
def test_slice_point_sweep_never_exceeds_cap(name: str, cap: int, codec: JsonCodec):
    """WSM-FRG-001/003: every produced encoded message is <= cap, in bytes of the codec's output."""
    frame = Frame("data", stream=1, payload=PAYLOADS[name], end=True)
    for part in split_frame(frame, cap, codec):
        assert encoded_length(codec.encode(part)) <= cap


@pytest.mark.parametrize("name", ["three-byte-codepoints", "four-byte-codepoints"])
def test_slice_point_inside_multibyte_codepoint_moves_back(name: str, codec: JsonCodec):
    """WSM-FRG-012: slices cut at codepoint boundaries; every fragment is valid UTF-8 on its own."""
    frame = Frame("data", stream=1, payload=PAYLOADS[name], end=True)
    parts = split_frame(frame, 96, codec)
    assert len(parts) > 1
    for part in parts:
        assert isinstance(part.fragment, str)
        # Re-encoding and decoding a fragment in isolation only works if it holds whole codepoints.
        assert part.fragment.encode("utf-8").decode("utf-8") == part.fragment
        assert not any(0xD800 <= ord(ch) <= 0xDFFF for ch in part.fragment)


def test_control_character_payload_is_resplit_not_emitted_over_cap(codec: JsonCodec):
    """WSM-FRG-014: JSON escaping expands control characters sixfold; the loop is the guarantee."""
    frame = Frame("data", stream=1, payload=PAYLOADS["control-characters"], end=True)
    parts = split_frame(frame, 128, codec)
    assert len(parts) > 1
    sizes = [encoded_length(codec.encode(p)) for p in parts]
    assert max(sizes) <= 128
    # The reservation alone (min(512, cap//2) = 64) would have allowed a 64-byte slice, which escapes
    # to ~384 bytes. Landing under the cap therefore proves the verify-and-re-split loop ran.
    assert _reassemble(parts, codec) == PAYLOADS["control-characters"]


def test_end_and_trailers_only_on_the_last_fragment(codec: JsonCodec):
    """WSM-FRG-020/021: more on all but the last; end/trailers on the last; headers on the first."""
    frame = Frame(
        "open",
        stream=1,
        payload=PAYLOADS["ascii"],
        headers={"trace": "abc"},
        end=True,
        trailers={"checksum": "d"},
    )
    parts = split_frame(frame, 256, codec)
    assert len(parts) > 2

    assert [p.more for p in parts] == [True] * (len(parts) - 1) + [False]
    assert [p.end for p in parts] == [False] * (len(parts) - 1) + [True]
    assert [p.trailers for p in parts] == [None] * (len(parts) - 1) + [{"checksum": "d"}]
    assert parts[0].headers == {"trace": "abc"}
    assert all(p.headers is None for p in parts[1:])
    assert all(p.payload is ABSENT for p in parts)


def test_headers_are_never_fragmented(codec: JsonCodec):
    """WSM-FRG-021: headers ride the first fragment whole; they are never sliced."""
    headers = {"trace": "t" * 300}
    frame = Frame("open", stream=1, payload=PAYLOADS["ascii"], headers=headers)
    parts = split_frame(frame, 1024, codec)
    assert parts[0].headers == headers


def test_a_frame_too_big_on_headers_alone_cannot_be_split(codec: JsonCodec):
    """A payload can be fragmented; an envelope cannot. The receiver rejects it (WSM-FRG-021)."""
    frame = Frame("open", stream=1, headers={"trace": "t" * 5000})
    with pytest.raises(ProtocolError, match="headers are never fragmented"):
        split_frame(frame, 256, codec)


@pytest.mark.parametrize("name", list(PAYLOADS), ids=list(PAYLOADS))
def test_assembler_round_trips_every_corpus_payload(name: str, codec: JsonCodec):
    """WSM-FRG-030: concatenate, then hand the result to the codec on the frame without `more`."""
    payload = PAYLOADS[name]
    parts = split_frame(Frame("data", stream=1, payload=payload, end=True), 256, codec)

    assembler = Assembler()
    assert not assembler.in_progress
    for part in parts[:-1]:
        assert assembler.feed(part, codec) is ABSENT
        assert assembler.in_progress
    assert assembler.feed(parts[-1], codec) == payload
    assert not assembler.in_progress


def test_assembler_tracks_accumulated_bytes_before_reassembly(codec: JsonCodec):
    """The receiver enforces max_payload_bytes against this as fragments arrive (WSM-FRG-032)."""
    parts = split_frame(Frame("data", stream=1, payload=PAYLOADS["ascii"]), 256, codec)
    assembler = Assembler()
    seen = 0
    for part in parts[:-1]:
        assembler.feed(part, codec)
        assert assembler.byte_length > seen
        seen = assembler.byte_length
    assembler.reset()
    assert assembler.byte_length == 0
    assert not assembler.in_progress


def test_assembler_rejects_a_frame_with_no_fragment(codec: JsonCodec):
    with pytest.raises(ProtocolError, match="no fragment"):
        Assembler().feed(Frame("data", stream=1, payload={"a": 1}), codec)


def test_fitting_frame_is_returned_untouched(codec: JsonCodec):
    frame = Frame("data", stream=1, payload={"a": 1})
    assert split_frame(frame, MAX_FRAME_BYTES, codec) == [frame]


def test_cap_below_envelope_floor_raises(codec: JsonCodec):
    """WSM-FRG-034: raise at once rather than be discovered later as an infinite split loop."""
    frame = Frame("data", stream=1, payload=PAYLOADS["ascii"])
    with pytest.raises(ProtocolError, match="envelope"):
        split_frame(frame, 32, codec)
    with pytest.raises(ProtocolError, match="too small"):
        split_frame(frame, 1, codec)


def test_split_requires_a_codec():
    """Boundaries are defined over the codec's output, so there is no codec-free default."""
    with pytest.raises(ProtocolError, match="requires a codec"):
        split_frame(Frame("data", stream=1, payload={"a": 1}), 256, None)


def test_split_is_pure(codec: JsonCodec):
    """WSM-FRG-015: same arguments, same result, and neither argument mutated."""
    payload = deepcopy(PAYLOADS["mixed"])
    frame = Frame("data", stream=1, payload=payload, headers={"trace": "x"}, end=True)
    before = deepcopy(payload)

    first = split_frame(frame, 512, codec)
    second = split_frame(frame, 512, codec)

    assert first == second
    assert payload == before
    assert frame.payload == before


def test_fragments_are_contiguous_and_reassemble_in_order(codec: JsonCodec):
    """WSM-FRG-017: fragments of one payload are contiguous on their own stream."""
    payload = PAYLOADS["mixed"]
    parts = split_frame(Frame("data", stream=1, payload=payload), 512, codec)
    rejoined = "".join(str(p.fragment) for p in parts)
    assert codec.decode_payload(rejoined) == payload


@pytest.mark.parametrize("case", CORPUS, ids=[c["name"] for c in CORPUS])
def test_corpus_payloads_survive_a_small_cap(case: dict[str, Any], codec: JsonCodec):
    """Every corpus frame that carries a payload fragments and reassembles at a hostile cap."""
    envelope = case["frame"]
    if "payload" not in envelope:
        pytest.skip("no payload to fragment")
    frame = Frame("data", stream=1, payload=envelope["payload"])
    parts = split_frame(frame, 96, codec)
    for part in parts:
        assert encoded_length(codec.encode(part)) <= 96
    assert _reassemble(parts, codec) == envelope["payload"]


class _BinaryJsonCodec:
    """A toy binary codec, so the byte-boundary half of the splitter has something to exercise.

    The real binary codec is msgpack; this one exists only to prove that `_take` slices bytes at
    byte boundaries and that `binary` is declared rather than inferred. `latin-1` round-trips any
    byte sequence one-to-one, which is all the envelope needs.
    """

    name = "binary-json"
    binary = True

    def encode(self, frame: Frame) -> bytes:
        mapping = to_mapping(frame)
        if isinstance(mapping.get("fragment"), bytes):
            mapping["fragment"] = mapping["fragment"].decode("latin-1")
        return json.dumps(mapping, separators=(",", ":")).encode("utf-8")

    def decode(self, message: str | bytes) -> Frame:
        mapping = json.loads(message)
        if "fragment" in mapping:
            mapping["fragment"] = mapping["fragment"].encode("latin-1")
        return from_mapping(mapping)

    def encode_payload(self, payload: Any) -> bytes:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    def decode_payload(self, data: str | bytes) -> Any:
        return json.loads(data)


def test_binary_codec_slices_at_byte_boundaries():
    """WSM-FRG-003/012: under a binary codec the unit is the byte and the length is the buffer's."""
    codec = _BinaryJsonCodec()
    payload = {"body": "abcdefghij" * 200}
    parts = split_frame(Frame("data", stream=1, payload=payload, end=True), 256, codec)

    assert len(parts) > 1
    for part in parts:
        assert isinstance(part.fragment, bytes)
        assert encoded_length(codec.encode(part)) <= 256

    assembler = Assembler()
    result: Any = ABSENT
    for part in parts:
        result = assembler.feed(part, codec)
    assert result == payload


@pytest.mark.parametrize("case", BOUNDARIES, ids=[c["name"] for c in BOUNDARIES])
def test_both_ports_agree_on_fragment_boundaries(case: dict[str, Any], codec: JsonCodec):
    """WSM-FRG-016: the pinned fragments are the contract both ports are held to.

    A boundary disagreement between Python and TypeScript is invisible until two peers of different
    languages try to reassemble each other's payloads, at which point it looks like corruption. This
    fixture turns it into a failing unit test in whichever port drifted.
    """
    frame = Frame("data", stream=1, payload=case["payload"], end=True)
    parts = split_frame(frame, case["cap"], codec)
    assert [part.fragment for part in parts] == case["fragments"]
    assert _reassemble(parts, codec) == case["payload"]


def test_a_fragment_sequence_always_terminates(codec: JsonCodec):
    """WSM-FRG-020/030: the last fragment must carry `more: false`, even if it carries no bytes.

    Trailers larger than the reservation make the closing frame too big at every slice point. A loop
    that answered that by emitting middle fragments until the payload ran out would leave a sequence
    with no terminator: the receiver's assembler never fires, the payload never reaches the
    application, and nothing anywhere reports an error.
    """
    frame = Frame("data", stream=1, payload={"body": "x" * 900}, end=True, trailers={"checksum": "d" * 130})
    parts = split_frame(frame, 256, codec)

    assert parts[-1].more is False
    assert parts[-1].end is True
    assert parts[-1].trailers == {"checksum": "d" * 130}
    assert all(part.more for part in parts[:-1])

    assembler = Assembler()
    result: Any = ABSENT
    for part in parts:
        result = assembler.feed(part, codec)
    assert result == frame.payload
    assert not assembler.in_progress


def test_trailers_too_large_for_the_cap_raise_rather_than_split(codec: JsonCodec):
    """`trailers` ride the closing fragment whole, exactly as `headers` ride the first one."""
    frame = Frame("data", stream=1, payload={"a": 1}, end=True, trailers={"x": "y" * 500})
    with pytest.raises(ProtocolError, match="closing envelope"):
        split_frame(frame, 256, codec)


def test_the_closing_fragment_may_be_empty(codec: JsonCodec):
    """A terminator carrying no payload bytes is still a terminator, and reassembles cleanly."""
    frame = Frame("data", stream=1, payload={"body": "x" * 900}, end=True, trailers={"checksum": "d" * 130})
    parts = split_frame(frame, 256, codec)
    rejoined = "".join(str(part.fragment) for part in parts)
    assert codec.decode_payload(rejoined) == frame.payload


def test_how_much_encoding_one_megabyte_costs():
    """What splitting a megabyte costs the sender, pinned as a ceiling.

    `iter_fragments` asks the codec "does the rest fit?" on every pass, and the reservation
    `min(512, cap // 2)` is short of what JSON escaping needs often enough that the binary search in
    `_largest_fitting_count` runs on nearly every fragment rather than as the exceptional path its
    comment describes. Nothing here is incorrect - the cuts are right and the function stays pure -
    but a large payload costs the sender far more encoding than it should, and that cost is
    synchronous: it blocks the event loop, which is the same latency WSM-INV-004 exists to prevent
    arriving by another road.

    The reservation may not simply be raised to make the search rare. It decides the boundary
    whenever its first guess *fits* - the search runs only when it does not - so a larger guess cuts
    in different places, and fragment boundaries are frozen: WSM-FRG-016 requires both ports to cut
    identically and `conformance/frames/` pins where. Changing the cost here is a generation
    concern, not an optimisation.

    The ceiling is a ceiling, not the value, so this records the cost without tripping on every
    unrelated change.
    """
    codec = JsonCodec()
    calls = 0
    encoded_bytes = 0
    original = codec.encode

    def counted(frame: Frame) -> str:
        nonlocal calls, encoded_bytes
        calls += 1
        rendered = original(frame)
        encoded_bytes += len(rendered)
        return rendered

    # `encode`, the frame-level call: that is what the splitter asks "does this fit?" with, and each
    # of those questions renders the whole candidate.
    codec.encode = counted  # type: ignore[method-assign]
    payload = {"rows": [{"i": index, "name": f"row-{index}"} for index in range(40_000)]}
    frame = Frame("data", stream=1, payload=payload, end=True)
    fragments = list(iter_fragments(frame, MAX_FRAME_BYTES, codec))

    assert len(fragments) > 8, "the payload must actually fragment for this to measure anything"
    per_fragment = calls / len(fragments)
    ratio = encoded_bytes / max(1, len(codec.encode_payload(payload)))
    report = (
        f"{calls} encodes for {len(fragments)} fragments ({per_fragment:.1f} each), rendering "
        f"{encoded_bytes:,} bytes for a {len(codec.encode_payload(payload)):,}-byte payload "
        f"({ratio:.1f}x). The splitter is re-encoding the remaining payload on every pass; see this "
        f"test's docstring."
    )
    assert per_fragment < 40, report
    assert ratio < 40, report
