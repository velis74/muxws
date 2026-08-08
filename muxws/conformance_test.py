"""Replays `conformance/invalid/*.json` against a live peer (WSM-TST-003).

M1 wrote these fixtures and proved them well-formed. This is where they first run: each one injects
frames no correct implementation would send, and asserts both what goes out and whether the
connection survives. The survival column is the whole point - it is the difference between "the
peers can still agree about every other stream" and "they cannot" (WSM-STM-024).
"""

from __future__ import annotations

import asyncio
import json

from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.frames import Frame, from_mapping
from muxws.peer import Peer
from muxws.stream import Stream
from muxws.transports.memory import memory_pair

INVALID_DIR = Path(__file__).parent.parent / "conformance" / "invalid"
FIXTURES = sorted(INVALID_DIR.glob("*.json"))


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


#: Empty since M5a. It held `frame-over-max-frame-bytes` while receive-side size enforcement was
#: still that milestone's work, marked `xfail(strict=True)` so that implementing the cap would fail
#: the suite as an unexpected pass rather than leave a skip to rot. It did exactly that.
NOT_YET_ENFORCED: dict[str, str] = {}


@pytest.mark.parametrize("path", FIXTURES, ids=[p.stem for p in FIXTURES])
async def test_invalid_fixtures(path: Path, request: pytest.FixtureRequest):
    """WSM-TST-003: the declared frames go out, and `connection_survives` matches."""
    if path.stem in NOT_YET_ENFORCED:
        request.node.add_marker(pytest.mark.xfail(reason=NOT_YET_ENFORCED[path.stem], strict=True))

    fixture = json.loads(path.read_text(encoding="utf-8"))
    codec = JsonCodec()

    # The local peer is an acceptor (even ids); the remote that misbehaves is a dialer (odd ids).
    _, acceptor_socket = memory_pair()
    peer = Peer(
        acceptor_socket,
        codec=codec,
        is_dialer=False,
        max_frame_bytes=fixture.get("max_frame_bytes", 65_536),
    )
    peer.on_stream(_hold)
    serving = asyncio.create_task(peer.serve())

    try:
        for entry in fixture["inbound"]:
            message = entry["raw"] if "raw" in entry else codec.encode(from_mapping(entry))
            acceptor_socket.inject(message)
            for _ in range(12):
                await asyncio.sleep(0)

        for _ in range(12):
            await asyncio.sleep(0)

        emitted = [codec.decode(message) for message in acceptor_socket.sent]
        _assert_expected_frames(emitted, fixture)
        assert peer.is_open == fixture["connection_survives"], fixture["description"]
    finally:
        serving.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):  # noqa: PT012, B017
            await serving
            raise AssertionError("unreachable")


def _assert_expected_frames(emitted: list[Frame], fixture: dict[str, Any]) -> None:
    """`expect_out` is a subset match: the listed keys must appear, in order, on some frame.

    Asserting equality instead would make every fixture invalid the moment an optional field is
    added, which is the opposite of what WSM-FRM-001 asks of a receiver.
    """
    expected = fixture["expect_out"]
    if not expected:
        assert emitted == [], f"{fixture['name']}: nothing should have gone out, got {emitted}"
        return

    remaining = list(emitted)
    for wanted in expected:
        for index, frame in enumerate(remaining):
            if all(getattr(frame, key, None) == value for key, value in wanted.items()):
                remaining = remaining[index + 1 :]
                break
        else:
            raise AssertionError(f"{fixture['name']}: no frame matching {wanted} in {emitted}")


def test_every_fixture_is_executed():
    """A fixture that stopped being collected would be a silently passing suite."""
    assert len(FIXTURES) == 8
