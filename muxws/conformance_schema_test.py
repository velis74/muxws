"""Schema checks over the shared corpus.

M1 writes `conformance/invalid/` and proves it well-formed; **M2 executes it**. A fixture that has
quietly lost a field, or a required case that has quietly disappeared, would otherwise show up as a
suite that passes by testing nothing.
"""

import json

from pathlib import Path
from typing import Any

import pytest

CONFORMANCE = Path(__file__).parent.parent / "conformance"
INVALID_FILES = sorted((CONFORMANCE / "invalid").glob("*.json"))
FRAME_FILES = [CONFORMANCE / "frames" / "v1-frames.json"]
BOUNDARY_FILES = [CONFORMANCE / "frames" / "v1-fragment-boundaries.json"]

#: The eight cases WSM-TST-003 enumerates, by fixture name.
REQUIRED_INVALID_CASES = {
    "open-wrong-parity",
    "open-id-not-monotonic",
    "data-after-end",
    "frame-over-max-frame-bytes",
    "undecodable-message",
    "fragment-interrupted-by-non-fragment",
    "data-above-high-water-mark",
    "data-for-closed-id",
}


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", INVALID_FILES, ids=[p.stem for p in INVALID_FILES])
def test_invalid_fixtures_parse(path: Path):
    """WSM-TST-003: each fixture declares what goes out and whether the connection survives."""
    fixture = _load(path)
    assert set(fixture) >= {"name", "description", "inbound", "expect_out", "connection_survives"}
    assert fixture["name"] == path.stem
    assert isinstance(fixture["description"], str)
    assert fixture["description"]
    assert isinstance(fixture["inbound"], list)
    assert fixture["inbound"]
    assert isinstance(fixture["expect_out"], list)
    assert isinstance(fixture["connection_survives"], bool)

    for entry in fixture["inbound"]:
        assert isinstance(entry, dict)
        # An entry is either a literal message for the codec, or a frame envelope.
        assert "raw" in entry or "type" in entry
    for entry in fixture["expect_out"]:
        assert entry["type"] in {"reset", "goaway"}
        assert isinstance(entry["code"], int)
        assert entry["code"] != 5, "reset code 5 is retired and must never appear"


def test_every_invalid_case_of_wsm_tst_003_has_a_fixture():
    """Without this, a deleted fixture is a silently passing suite."""
    assert {p.stem for p in INVALID_FILES} == REQUIRED_INVALID_CASES


def test_connection_level_cases_die_and_stream_level_cases_survive():
    """WSM-STM-020/023: the discriminator is whether the peers can still agree about other streams."""
    survival = {p.stem: _load(p)["connection_survives"] for p in INVALID_FILES}
    assert survival == {
        "open-wrong-parity": False,
        "open-id-not-monotonic": False,
        "undecodable-message": False,
        "data-above-high-water-mark": False,
        "data-after-end": True,
        "frame-over-max-frame-bytes": True,
        "fragment-interrupted-by-non-fragment": True,
        "data-for-closed-id": True,
    }


def test_a_surviving_connection_never_emits_goaway():
    """`goaway` ends the connection by definition, so the two columns cannot disagree."""
    for path in INVALID_FILES:
        fixture = _load(path)
        if fixture["connection_survives"]:
            assert all(out["type"] != "goaway" for out in fixture["expect_out"]), path.stem


@pytest.mark.parametrize("path", FRAME_FILES, ids=[p.stem for p in FRAME_FILES])
def test_frame_fixtures_are_name_frame_wire_triples(path: Path):
    """WSM-TST-001: a list of {name, frame, json_wire} read verbatim by both pytest and vitest."""
    corpus = _load(path)
    assert isinstance(corpus, list)
    assert corpus
    names = [case["name"] for case in corpus]
    assert len(names) == len(set(names)), "fixture names must be unique - they become test ids"

    for case in corpus:
        assert set(case) == {"name", "frame", "json_wire"}
        assert "type" in case["frame"]
        # Shape only: the pinned wire parses to an object carrying the same frame type. This used to
        # attempt `parsed == frame` as well, with the type check as an `or` fallback - and the
        # fallback is true for every well-formed triple, so the comparison beside it could never fail
        # the test. It read as proof of round-tripping while proving nothing.
        #
        # The semantic check is `decode(json_wire) == frame`, and it belongs where the codec is: this
        # module asserts that a fixture is *well-formed*, `conformance_test.py` asserts that it is
        # *true* (WSM-TST-001, WSM-CDC-005).
        wire = json.loads(case["json_wire"])
        assert isinstance(wire, dict)
        assert wire["type"] == case["frame"]["type"]


def test_no_fixture_anywhere_mentions_a_settings_frame():
    """WSM-CON-031: no limit, version or capability appears on the wire in any form."""
    forbidden = {"settings", "ack", "protocol_version", "extensions", "max_concurrent_streams", "max_payload_bytes"}
    for path in INVALID_FILES + FRAME_FILES + BOUNDARY_FILES:
        text = path.read_text(encoding="utf-8")
        for word in forbidden:
            assert f'"{word}"' not in text, f"{path.name} carries {word!r}"


def test_max_frame_bytes_is_a_runner_instruction_never_a_wire_value():
    """WSM-FRG-005/WSM-TST-002: it may sit at the top level; it must never be inside a frame."""
    for path in INVALID_FILES:
        fixture = _load(path)
        for entry in fixture["inbound"] + fixture["expect_out"]:
            assert "max_frame_bytes" not in entry


@pytest.mark.parametrize("path", BOUNDARY_FILES, ids=[p.stem for p in BOUNDARY_FILES])
def test_boundary_fixtures_are_name_cap_payload_fragments(path: Path):
    """WSM-FRG-016's fixture: the exact slices both ports must produce for a payload at a cap."""
    corpus = _load(path)
    assert isinstance(corpus, list)
    assert corpus
    names = [case["name"] for case in corpus]
    assert len(names) == len(set(names))

    for case in corpus:
        assert set(case) == {"name", "cap", "payload", "fragments"}
        assert isinstance(case["cap"], int)
        assert case["cap"] > 0
        assert len(case["fragments"]) > 1, f"{case['name']} does not actually fragment"
        assert all(isinstance(fragment, str) for fragment in case["fragments"])


def test_every_boundary_hazard_has_a_case():
    """Astral planes, multi-byte codepoints and escape expansion are the three ways this drifts."""
    names = {case["name"] for case in _load(BOUNDARY_FILES[0])}
    assert {"astral-plane", "two-byte-codepoints", "three-byte-codepoints", "control-characters"} <= names
