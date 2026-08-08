"""Replays `conformance/sequences/*.json` against a live peer pair (WSM-TST-002).

The invalid corpus next door injects frames no correct implementation would send and asserts what
comes back. This one is the opposite: **both** peers are real, nothing is injected, and the fixture
is a script of ordinary API calls with assertions interleaved. What it proves is that the two
language ports agree about *sequences* - which frame goes out when, which stream survives which
event - and not merely about how one frame is spelled.

Two properties of the schema do the work, and both exist to keep one corpus honest in two languages:

- **Streams are named by `stream_ref`, an ordinal, never by a raw id.** Ordinal 1 is the first stream
  the script opens. The runner resolves it to whatever id that peer actually allocated, so the same
  fixture replays unchanged when the roles are swapped; a raw id would bake one side's parity into
  the corpus. `last_stream_ref` is the same resolution applied to `goaway.last_stream`, which carries
  the *other* peer's parity (WSM-CON-020).
- **`expect_frame` is a subset match, not equality.** The listed keys must hold and everything else is
  ignored, so a later revision that adds an optional field does not invalidate the corpus - which is
  what WSM-FRM-001 asks of a receiver, applied to the test suite.

`ts/conformance.spec.ts` is this file's twin and reads the same files. If one runner needs a field
the other does not read, the fixture is wrong.
"""

from __future__ import annotations

import asyncio
import json

from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ConnectionLost, RemoteError, ResetCode, StreamRefused, StreamReset, StreamTimeout
from muxws.frames import Frame
from muxws.peer import Peer
from muxws.stream import Stream
from muxws.transports.memory import memory_pair, MemorySocket

SEQUENCES_DIR = Path(__file__).parent.parent / "conformance" / "sequences"
FIXTURES = sorted(SEQUENCES_DIR.glob("*.json"))

#: Both runners carry this number and both assert it. M6 raises it as it completes the corpus; until
#: then a fixture that stopped being collected would be a suite that passes by testing nothing.
EXPECTED_FIXTURES = 1

#: Wall-clock ceiling on any one waiting step. A fixture that cannot make progress must fail as a
#: named assertion rather than hang the suite.
STEP_TIMEOUT = 2.0

#: How many turns of the event loop a bounded poll is given before it gives up.
POLL_TURNS = 400

#: `expect_error` names a class rather than a code, because the class is the part WSM-ERR-004 fixes
#: across the two languages.
ERROR_CLASSES: dict[str, type[BaseException]] = {
    "ConnectionLost": ConnectionLost,
    "RemoteError": RemoteError,
    "StreamRefused": StreamRefused,
    "StreamReset": StreamReset,
    "StreamTimeout": StreamTimeout,
}


class Replay:
    """One fixture, one live pair, one pass over `steps`."""

    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.codec = JsonCodec()

        options: dict[str, Any] = {}
        if "max_frame_bytes" in fixture:
            # An instruction to the runner, never a wire value (WSM-TST-002/WSM-FRG-005).
            options["max_frame_bytes"] = fixture["max_frame_bytes"]

        dialer_socket, acceptor_socket = memory_pair()
        self.sockets: dict[str, MemorySocket] = {"dialer": dialer_socket, "acceptor": acceptor_socket}
        self.peers: dict[str, Peer] = {
            "dialer": Peer(dialer_socket, codec=self.codec, is_dialer=True, **options),
            "acceptor": Peer(acceptor_socket, codec=self.codec, is_dialer=False, **options),
        }

        #: Ordinal -> the id that stream actually got. Filled by every step that opens one.
        self.ordinals: list[int] = []
        #: Both peers' view of every stream, by id: the opener's handle and the receiver's.
        self.by_id: dict[str, dict[int, Stream]] = {"dialer": {}, "acceptor": {}}
        #: `as` labels, for `expect_result` / `expect_error`.
        self.refs: dict[str, Stream] = {}
        #: In-flight `close()` calls, awaited by an `await_close` step.
        self.closing: dict[str, asyncio.Task[None]] = {}
        #: How far `expect_frame` has consumed each peer's wire; frames match forwards, in order.
        self.cursor: dict[str, int] = {"dialer": 0, "acceptor": 0}
        self.serving: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        for who, peer in self.peers.items():
            peer.on_stream(self._recorder(who))
        self.serving = [asyncio.create_task(peer.serve()) for peer in self.peers.values()]

    def _recorder(self, who: str):
        """The one incoming-stream handler: record the stream, then hold it open.

        Holding matters. WSM-STM-035 ends a stream the moment its handler returns, so a handler that
        returned here would close every inbound stream before the script could `reply` on it.
        """

        async def handler(_payload: Any, stream: Stream) -> None:
            self.by_id[who][stream.id] = stream
            await stream.closed.wait()

        return handler

    async def stop(self) -> None:
        await self.sockets["dialer"].drop()
        await self.settle()
        for task in list(self.closing.values()) + self.serving:
            task.cancel()
        for task in list(self.closing.values()) + self.serving:
            # Whatever a cancelled serve() or close() raises on the way out is teardown noise.
            await asyncio.gather(task, return_exceptions=True)

    async def settle(self, rounds: int = 12) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)

    # ------------------------------------------------------------------ the script

    async def run(self) -> None:
        for index, step in enumerate(self.fixture["steps"]):
            try:
                await self.step(step)
            except AssertionError as exc:
                raise AssertionError(f"{self.fixture['name']} step {index}: {exc}") from exc

    async def step(self, step: dict[str, Any]) -> None:
        if "settle" in step:
            await self.settle(int(step["settle"]))
        elif "call" in step:
            await self.do_call(step)
        elif "expect_frame" in step:
            self.expect_frame(step)
        elif "expect_no_frame" in step:
            self.expect_no_frame(step)
        elif "expect_result" in step:
            await self.expect_result(step["expect_result"])
        elif "expect_error" in step:
            await self.expect_error(step["expect_error"])
        elif "expect_closed" in step:
            await self.expect_closed(step["expect_closed"])
        else:
            raise AssertionError(f"no step kind in {step!r}")

    async def do_call(self, step: dict[str, Any]) -> None:
        """The calls this milestone's corpus needs. M6 adds the rest, deliberately and in both runners."""
        who = step["peer"]
        peer = self.peers[who]
        call = step["call"]

        if call == "open":
            stream = peer.open(step.get("payload"), headers=step.get("headers"), end=step.get("end", False))
            self.ordinals.append(stream.id)
            self.by_id[who][stream.id] = stream
            if "as" in step:
                self.refs[step["as"]] = stream
        elif call == "reply":
            await self.stream_for(who, step["stream_ref"]).reply(step.get("payload"))
        elif call == "close":
            # Durations are milliseconds in the corpus and in TypeScript, seconds in Python
            # (WSM-CON-012); the runner converts so the fixture does not have to carry both.
            drain = float(step.get("drain_ms", 10_000)) / 1000
            code = ResetCode(step.get("code", int(ResetCode.NO_ERROR)))
            # Started rather than awaited: `close()` sends `goaway`, *then* drains, and the steps
            # after this one are what the drain window is there to let happen (WSM-CON-025).
            self.closing[who] = asyncio.create_task(peer.close(code, step.get("reason"), drain))
        elif call == "await_close":
            task = self.closing.get(who)
            if task is None:
                raise AssertionError(f"await_close: {who} has no close() in flight")
            await asyncio.wait_for(task, STEP_TIMEOUT)
        else:
            raise AssertionError(f"the runner does not implement the call {call!r}")

    # ------------------------------------------------------------------ resolution

    def id_of(self, ordinal: int) -> int:
        """`stream_ref: n` -> the id the n-th stream in the script actually got (WSM-TST-002)."""
        if not 1 <= ordinal <= len(self.ordinals):
            raise AssertionError(f"stream_ref {ordinal} names a stream this script has not opened")
        return self.ordinals[ordinal - 1]

    def stream_for(self, who: str, ordinal: int) -> Stream:
        stream_id = self.id_of(ordinal)
        stream = self.by_id[who].get(stream_id)
        if stream is None:
            raise AssertionError(f"{who} has no stream for ordinal {ordinal} (id {stream_id})")
        return stream

    def resolve(self, wanted: dict[str, Any]) -> dict[str, Any]:
        """Turn the two ordinal-bearing keys into the ids this run allocated."""
        resolved: dict[str, Any] = {}
        for key, value in wanted.items():
            if key == "stream_ref":
                resolved["stream"] = self.id_of(int(value))
            elif key == "last_stream_ref":
                resolved["last_stream"] = self.id_of(int(value))
            else:
                resolved[key] = value
        return resolved

    def frames_sent_by(self, who: str) -> list[Frame]:
        return [self.codec.decode(message) for message in self.sockets[who].sent]

    @staticmethod
    def matches(frame: Frame, wanted: dict[str, Any]) -> bool:
        """A **subset** match: the listed keys, and nothing about the rest (WSM-TST-002)."""
        return all(getattr(frame, key, None) == value for key, value in wanted.items())

    # ------------------------------------------------------------------ assertions

    def expect_frame(self, step: dict[str, Any]) -> None:
        who = step["peer"]
        wanted = self.resolve(step["expect_frame"])
        frames = self.frames_sent_by(who)
        for index in range(self.cursor[who], len(frames)):
            if self.matches(frames[index], wanted):
                self.cursor[who] = index + 1
                return
        raise AssertionError(f"{who} sent no frame matching {wanted}; it sent {frames[self.cursor[who] :]}")

    def expect_no_frame(self, step: dict[str, Any]) -> None:
        """Not "not yet", but "not at all": the whole of that peer's wire is searched."""
        who = step["peer"]
        wanted = self.resolve(step["expect_no_frame"])
        offending = [frame for frame in self.frames_sent_by(who) if self.matches(frame, wanted)]
        if offending:
            raise AssertionError(
                f"{who} sent {offending}, and this fixture says it must send nothing matching {wanted}"
            )

    async def expect_result(self, spec: dict[str, Any]) -> None:
        stream = self.refs[spec["ref"]]
        value = await asyncio.wait_for(stream.result(), STEP_TIMEOUT)
        if value != spec["value"]:
            raise AssertionError(f"{spec['ref']} produced {value!r}, expected {spec['value']!r}")

    async def expect_error(self, spec: dict[str, Any]) -> None:
        stream = self.refs[spec["ref"]]
        expected = ERROR_CLASSES[spec["error"]]
        with pytest.raises(expected) as caught:
            await asyncio.wait_for(stream.result(), STEP_TIMEOUT)
        if "code" in spec and int(getattr(caught.value, "code", -1)) != spec["code"]:
            raise AssertionError(f"{spec['ref']} failed with code {getattr(caught.value, 'code', None)}")

    async def expect_closed(self, spec: dict[str, Any]) -> None:
        """A bounded poll, not a single read: the other end learns of a close one turn later."""
        who = spec["peer"]
        want_socket = bool(spec.get("socket", False))
        for _ in range(POLL_TURNS):
            if not self.peers[who].is_open and (not want_socket or self.sockets[who].is_closed):
                return
            await asyncio.sleep(0)
        raise AssertionError(
            f"{who} is still open (peer.is_open={self.peers[who].is_open}, "
            f"socket.is_closed={self.sockets[who].is_closed})"
        )


@pytest.mark.parametrize("path", FIXTURES, ids=[p.stem for p in FIXTURES])
async def test_sequence_fixtures(path: Path):
    """WSM-TST-002: the script replays over the in-memory transport, step for step."""
    fixture = json.loads(path.read_text(encoding="utf-8"))
    assert fixture["name"] == path.stem
    replay = Replay(fixture)
    replay.start()
    try:
        await replay.run()
    finally:
        await replay.stop()


def test_the_sequence_corpus_is_collected():
    """An empty `conformance/sequences/` is a suite that passes by testing nothing."""
    assert FIXTURES, f"no sequence fixtures were collected from {SEQUENCES_DIR}"
    assert len(FIXTURES) == EXPECTED_FIXTURES, (
        f"{len(FIXTURES)} sequence fixtures, expected {EXPECTED_FIXTURES}; "
        f"ts/conformance.spec.ts pins the same number and must be raised with this one"
    )
