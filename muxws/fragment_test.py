import json
import random

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ProtocolError
from muxws.fragment import (
    _floor_boundary,
    _next_boundary,
    Assembler,
    encoded_length,
    iter_fragments,
    MAX_FRAME_BYTES,
    split_frame,
)
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


def _take(text: str, start: int, budget: int) -> str:
    """The reservation cut of `iter_fragments`, on its own, so the invariant can be asserted on it.

    `start` is a byte offset on a codepoint boundary, as it is inside the splitter; the answer is the
    longest prefix of the remaining UTF-8 that fits `budget` bytes and ends on a boundary.
    """
    data = text.encode("utf-8")
    return data[start : _floor_boundary(data, min(start + budget, len(data)), text=True)].decode("utf-8")


def _first_codepoint_width(text: str, start: int) -> int | None:
    """Bytes in the codepoint at byte offset `start`, or None when nothing is left.

    Read off the string rather than off the byte pattern, so the invariant below is checked against
    what UTF-8 means and not against the way the splitter reads it.
    """
    remainder = text.encode("utf-8")[start:].decode("utf-8")
    return len(remainder[0].encode("utf-8")) if remainder else None


def _boundaries(text: str) -> list[int]:
    """Every byte offset a fragment may end on, including 0 and the end of the string."""
    offsets, cursor = [0], 0
    for character in text:
        cursor += len(character.encode("utf-8"))
        offsets.append(cursor)
    return offsets


def _assert_take_invariant(text: str, start: int, budget: int) -> None:
    """The four properties that make the two ports cut in the same place (WSM-FRG-012/016)."""
    data = text.encode("utf-8")
    case = f"text={text!r} start={start} budget={budget}"
    taken = _take(text, start, budget).encode("utf-8")

    # 1. a prefix of what is left, and 2. inside the budget.
    assert data[start:].startswith(taken), case
    assert len(taken) <= budget, case

    # 3. maximal: either the string is spent, or the next codepoint would overrun the budget.
    next_width = _first_codepoint_width(text, start + len(taken))
    assert next_width is None or len(taken) + next_width > budget, case

    # 4. empty exactly when the first codepoint does not fit - never as a way of making progress.
    first_width = _first_codepoint_width(text, start)
    assert (taken == b"") == (first_width is None or first_width > budget), case


def test_the_take_invariant_holds_over_generated_strings_offsets_and_budgets():
    """Prefix, within budget, maximal, and empty only when the first codepoint does not fit.

    Seeded, so a failure names a case that reproduces instead of one that vanishes on the rerun. The
    sweep is exhaustive per string - every codepoint boundary as an offset, every budget from zero to
    past the end of the string - because a slicer goes wrong at an edge and not in the middle.
    """
    rng = random.Random(20260814)  # noqa: S311 - a fixed seed is the point; nothing here is a secret
    pool = "aZ0 -\nčšžđé—ъ中あ\ufeff🛰𝕄🌍😀"
    cases = 0
    for _ in range(150):
        text = "".join(rng.choice(pool) for _ in range(rng.randrange(0, 11)))
        for start in _boundaries(text):
            for budget in range(len(text.encode("utf-8")) - start + 3):
                _assert_take_invariant(text, start, budget)
                cases += 1
    assert cases > 3_000, f"{cases} cases is fewer than this sweep is meant to generate"


def test_a_budget_smaller_than_the_first_codepoint_takes_nothing():
    """The empty take is an answer, not a failure: the caller turns it into a whole unit or an error."""
    assert _take("🌍a", 0, 3) == ""
    assert _take("🌍a", 0, 4) == "🌍"
    assert _take("č", 0, 1) == ""
    assert _take("č", 0, 2) == "č"


def test_a_budget_landing_exactly_on_a_boundary_takes_the_whole_codepoint():
    """Nothing is given back at an exact fit; the byte at the cut begins the next codepoint."""
    assert _take("ččč", 0, 4) == "čč"
    assert _take("ččč", 0, 5) == "čč"
    assert _take("ččč", 0, 6) == "ččč"
    assert _take("a🌍", 0, 5) == "a🌍"


def test_four_byte_codepoints_are_taken_whole_or_not_at_all():
    """A 4-byte codepoint is a surrogate pair in the TypeScript port and indivisible in both."""
    text = "𝕄𝕦𝕩"
    assert _take(text, 4, 3) == ""
    assert _take(text, 4, 8) == "𝕦𝕩"
    for budget in range(14):
        assert _take(text, 0, budget) == text[: budget // 4]


def test_an_offset_at_the_end_of_the_string_takes_nothing():
    """The splitter reaches this on the pass where the payload is spent and only a terminator is due."""
    assert _take("abč", 4, 100) == ""
    assert _take("", 0, 100) == ""


def test_a_budget_of_zero_takes_nothing():
    for text in ("", "a", "č", "🌍", "abc"):
        assert _take(text, 0, 0) == ""


def test_an_all_non_ascii_string_cuts_only_between_codepoints():
    """No ASCII anywhere: every cut point the budget lands on is inside a codepoint half the time."""
    text = "ъђћјљњ" * 4
    data = text.encode("utf-8")
    assert len(data) == 2 * len(text)
    for budget in range(len(data) + 2):
        assert len(_take(text, 0, budget).encode("utf-8")) == min(budget - budget % 2, len(data))


def test_a_binary_payload_may_be_cut_at_every_byte():
    """WSM-FRG-003: under a binary codec the unit is the byte, and 0x80..0xBF are ordinary payload."""
    data = bytes(range(256))
    for cut in range(len(data) + 1):
        assert _floor_boundary(data, cut, text=False) == cut
    assert _next_boundary(data, 0x80, text=False) == 0x81


def test_the_first_codepoint_is_taken_whole_even_when_it_overruns_the_budget():
    """What the splitter does with an empty take: advance by one unit, then verify against the cap."""
    data = "🌍č".encode()
    assert _next_boundary(data, 0, text=True) == 4
    assert _next_boundary(data, 4, text=True) == 6


class _BareCodec:
    """A codec whose fragment frames are the fragment and nothing else.

    It lets the reservation be narrower than one codepoint while a fragment carrying that codepoint
    still fits under the cap. Under any real envelope the cap would have to be smaller than the
    envelope for that to happen and the splitter would raise instead (WSM-FRG-034), which leaves the
    one-unit floor unobservable.
    """

    name = "bare"
    binary = False

    def encode(self, frame: Frame) -> str:
        if isinstance(frame.fragment, str):
            return frame.fragment
        return json.dumps(to_mapping(frame), separators=(",", ":"))

    def decode(self, message: str | bytes) -> Frame:
        return from_mapping(json.loads(message))

    def encode_payload(self, payload: Any) -> str:
        return str(payload)

    def decode_payload(self, data: str | bytes) -> Any:
        return data


def test_a_codepoint_wider_than_the_reservation_is_taken_whole():
    """WSM-FRG-013: a cap of 5 reserves 2, leaving a budget of 3 against four-byte codepoints.

    The budget floors onto the offset the fragment starts at, and a fragment carrying nothing leaves
    the loop no way forward, so the unit is taken whole and the cap check below decides its fate.
    """
    parts = split_frame(Frame("data", stream=1, payload="𝕄" * 5), 5, _BareCodec())

    assert [part.fragment for part in parts] == ["𝕄"] * 5
    assert "".join(str(part.fragment) for part in parts) == "𝕄" * 5


def test_a_codepoint_wider_than_the_whole_budget_raises_rather_than_looping(codec: JsonCodec):
    """WSM-FRG-034: an empty take must not become a fragment sequence that never advances."""
    with pytest.raises(ProtocolError, match="indivisible unit"):
        split_frame(Frame("data", stream=1, payload="🌍🌍"), 4, codec)


def test_generated_unicode_payloads_fragment_and_reassemble_at_hostile_caps(codec: JsonCodec):
    """Mixed-width text at small caps: every fragment is whole codepoints and the whole sequence joins.

    Seeded for the same reason as the invariant sweep - the failing payload has to survive the rerun.
    """
    rng = random.Random(4711)  # noqa: S311 - a fixed seed, so the failing payload survives the rerun
    pool = 'aZ0 "\\\nčšž—ъ中\ufeff🛰𝕄🌍'
    for _ in range(40):
        payload = {"body": "".join(rng.choice(pool) for _ in range(rng.randrange(200, 900)))}
        cap = rng.choice([64, 96, 128, 200, 512])
        parts = split_frame(Frame("data", stream=1, payload=payload, end=True), cap, codec)
        for part in parts:
            assert encoded_length(codec.encode(part)) <= cap
            assert isinstance(part.fragment, str)
            assert not any(0xD800 <= ord(ch) <= 0xDFFF for ch in part.fragment)
        assert _reassemble(parts, codec) == payload


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

    The real binary codec is msgpack; this one exists only to prove that the splitter slices bytes at
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
