"""The Python conformance runner: `conformance/sequences/` and `conformance/invalid/` (M6 §3).

`ts/conformance.spec.ts` is this file's twin and reads the same files, under the same schema
(`conformance/README.md`). If one runner needs a field the other does not read, the fixture is wrong.

Two corpora, two shapes of proof:

- **`sequences/`** - both peers are real, nothing is injected, and the fixture is a script of
  ordinary API calls with assertions interleaved. What it proves is that the ports agree about
  *sequences* - which frame goes out when, which stream survives which event - and not merely about
  how one frame is spelled. Every fixture is replayed **twice, with the roles swapped**, which is
  what `stream_ref`-as-an-ordinal buys and the whole point of the corpus: a raw id would bake one
  side's parity in, and the second pass is what proves it did not.
- **`invalid/`** - one real peer, fed messages no correct implementation would send, asserting both
  which frame goes out and whether the connection survives. The survival column is the point: it is
  the difference between "the peers can still agree about every other stream" and "they cannot"
  (WSM-STM-024).

Nothing here may change peer behaviour. A failing fixture means a rule was implemented wrongly in
M1-M5b; the fix belongs to the milestone that owns the rule, not to a fixture edit.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re

from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.codecs.msgpack_ import MsgpackCodec
from muxws.errors import ConnectionLost, RemoteError, ResetCode, StreamRefused, StreamReset, StreamTimeout
from muxws.frames import ABSENT, Frame, from_mapping
from muxws.peer import Peer
from muxws.stream import Stream
from muxws.transports.memory import memory_pair, MemorySocket

ROOT = Path(__file__).parent.parent
CONFORMANCE = ROOT / "conformance"
SEQUENCES_DIR = CONFORMANCE / "sequences"
INVALID_DIR = CONFORMANCE / "invalid"
FRAMES_DIR = CONFORMANCE / "frames"

SEQUENCES = sorted(SEQUENCES_DIR.glob("*.json"))
INVALID = sorted(INVALID_DIR.glob("*.json"))

#: Both runners carry these two numbers and both assert them, and
#: `test_both_runners_collect_the_same_number_of_fixtures` reads the TypeScript file to prove they
#: still agree. A fixture that stopped being collected would otherwise be a suite that passes by
#: testing nothing.
EXPECTED_SEQUENCE_FIXTURES = 13
EXPECTED_INVALID_FIXTURES = 8

#: The eight cases WSM-TST-003 enumerates, **by name**. Enumerated rather than counted: a count alone
#: passes when one case is deleted and another duplicated under a new name.
REQUIRED_INVALID_CASES = frozenset(
    {
        "open-wrong-parity",
        "open-id-not-monotonic",
        "data-after-end",
        "frame-over-max-frame-bytes",
        "undecodable-message",
        "fragment-interrupted-by-non-fragment",
        "data-above-high-water-mark",
        "data-for-closed-id",
    }
)

#: The freeze (M6 §5). sha256 over `conformance/frames/`, sorted by file name, each file contributing
#: `name`, a NUL, its bytes, a NUL. `ts/conformance.spec.ts` recomputes the same digest from the same
#: bytes and reads **this** literal out of this file, so there is exactly one checked-in digest and a
#: port that reads a different corpus fails. Changing the JSON wire after 1.0 means editing the line
#: below, deliberately, in the same commit as the change.
JSON_WIRE_DIGEST = "8edb30f1a73248a36ad08208e3706df60868534a180aa58bb58ffae1ef587961"

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

#: WSM-CON-031, WSM-CON-009, WSM-PKG-005: no limit and no version, in any frame, in any form. Checked
#: against the **envelope keys on the wire** rather than against a decoded `Frame`, because
#: `from_mapping` drops unknown keys - decoding first would hide exactly the key this is looking for.
FORBIDDEN_ENVELOPE_KEYS = frozenset(
    {"ack", "protocol_version", "extensions", "max_frame_bytes", "max_concurrent_streams", "max_payload_bytes"}
)

#: Reserved in v1 and never sent (WSM-BPR-001). There is no `settings` frame at all (WSM-CON-031).
FORBIDDEN_FRAME_TYPES = frozenset({"settings", "window_update"})


class Error(Exception):
    """What a fixture's `{"handler": "raise"}` payload makes the receiving handler raise.

    The class name is load-bearing and is why this is not a `RuntimeError`. The default error
    serializer sends `type(exc).__name__` (Python) and `error.name` (TypeScript), so the
    `reset(APPLICATION_ERROR)` payload a fixture pins is only the same in both languages if the class
    is spelled the same in both. `Error` is the one name that already exists in JavaScript.
    """


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def substitute_bytes(value: Any) -> Any:
    """Resolve every `{"$bytes": [...]}` placeholder into the byte string those integers spell.

    JSON has no byte type, so a fixture that needs one under a binary codec (WSM-CDC-008) has to
    spell it. This is fixture *notation*, resolved before the first step runs; it is never a payload
    shape and never reaches the wire. A runner that passed the mapping through unresolved would send
    `{"$bytes": [...]}` as an ordinary object and pass the fixture while testing nothing, which is
    what `test_the_bytes_placeholder_is_resolved_before_a_step_runs` exists to prevent.

    Only a mapping whose **sole** key is `$bytes` is a placeholder; `$bytes` alongside anything else
    is an ordinary payload key, so an application object can still carry one.
    """
    if isinstance(value, dict):
        if set(value) == {"$bytes"}:
            return bytes(value["$bytes"])
        return {key: substitute_bytes(item) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute_bytes(item) for item in value]
    return value


def _load_sequence(path: Path) -> dict[str, Any]:
    """A sequence fixture with its `$bytes` placeholders already resolved (WSM-CDC-008)."""
    return substitute_bytes(_load(path))


# --------------------------------------------------------------------------- sequences


class Replay:
    """One fixture, one live pair, one pass over `steps` - in one of the two role assignments."""

    def __init__(self, fixture: dict[str, Any], *, swapped: bool = False, codec: Any = None) -> None:
        self.fixture = fixture
        self.swapped = swapped
        # The codec is a parameter, not a constant, because WSM-CDC-007 requires every shipped codec
        # to run this corpus with a peer at each end - a corpus only the default codec ever replays
        # proves nothing about the seam it is supposed to prove.
        self.codec = JsonCodec() if codec is None else codec

        options: dict[str, Any] = {}
        for key in ("max_frame_bytes", "max_concurrent_streams"):
            if key in fixture:
                # An instruction to the runner, never a wire value (WSM-TST-002, WSM-CON-031).
                options[key] = fixture[key]

        dialer_socket, acceptor_socket = memory_pair()
        self.sockets: dict[str, MemorySocket] = {"dialer": dialer_socket, "acceptor": acceptor_socket}
        # The two roles keep their names; which of them holds the odd parity is what swaps. That is
        # the whole of a role swap, and it is why a fixture may never write a raw stream id: every
        # id in the second pass is the other one (WSM-TST-002).
        self.peers: dict[str, Peer] = {
            "dialer": Peer(dialer_socket, codec=self.codec, is_dialer=not swapped, **options),
            "acceptor": Peer(acceptor_socket, codec=self.codec, is_dialer=swapped, **options),
        }

        #: Ordinal -> the id that stream actually got. Filled by every step that opens one.
        self.ordinals: list[int] = []
        #: Both peers' view of every stream, by id: the opener's handle and the receiver's.
        self.by_id: dict[str, dict[int, Stream]] = {"dialer": {}, "acceptor": {}}
        #: `as` labels bound to a stream, for a later `expect_result` / `expect_error`.
        self.refs: dict[str, Stream] = {}
        #: `as` labels bound to work already in flight - a `request`, an `iterate` - which is a
        #: different thing from a stream: the value those steps assert is the call's return, and the
        #: call was started rather than awaited so that the steps after it could run.
        self.tasks: dict[str, asyncio.Task[Any]] = {}
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

    def _recorder(self, who: str) -> Callable[[Any, Stream], Coroutine[Any, Any, None]]:
        """The one incoming-stream handler: record the stream, then hold it open.

        Holding matters. WSM-STM-035 ends a stream the moment its handler returns, so a handler that
        returned here would close every inbound stream before the script could `reply` on it.

        The `{"handler": "raise"}` payload is the only way a script of ordinary API calls can reach
        the handler-failure path of WSM-STM-034 - `stream.reset(APPLICATION_ERROR)` sends the same
        code but carries no serialized error object, which is the half the fixture is pinning.
        """

        async def handler(payload: Any, stream: Stream) -> None:
            self.by_id[who][stream.id] = stream
            if isinstance(payload, dict) and payload.get("handler") == "raise":
                raise Error(str(payload.get("message", "the handler raised")))
            await stream.closed.wait()

        return handler

    async def stop(self) -> None:
        await self.sockets["dialer"].drop()
        await self.settle()
        pending = [*self.tasks.values(), *self.closing.values(), *self.serving]
        for task in pending:
            task.cancel()
        for task in pending:
            # Whatever a cancelled serve(), close() or collector raises on the way out is teardown
            # noise: the fixture's assertions have already run.
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
        elif "inject" in step:
            self.inject(step)
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
        """Dispatch one `call` step. An unimplemented call is a hard failure, never a skip."""
        call = step["call"]
        handler = getattr(self, f"call_{call}", None)
        if handler is None:
            raise AssertionError(f"the runner does not implement the call {call!r}")

        expected = step.get("raises")
        if expected is None:
            await handler(step)
            return
        # `raises` is how a fixture states that the *call* fails - a producer whose stream the
        # consumer cancelled, say. The alternative is asserting on frames, which cannot see that the
        # local handle refuses to send at all (WSM-ERR-009).
        with pytest.raises(ERROR_CLASSES[expected]):
            await handler(step)

    async def call_open(self, step: dict[str, Any]) -> None:
        peer = self.peers[step["peer"]]
        stream = peer.open(step.get("payload"), headers=step.get("headers"), end=step.get("end", False))
        self._adopt(step, stream)

    async def call_request(self, step: dict[str, Any]) -> None:
        """`peer.request(...)`, **started and not awaited** (WSM-API-006).

        The steps after a `request` are the assertions about the frames it produced, so awaiting it
        here would deadlock every fixture that uses it. The stream it allocated is then discovered
        through the public `peer.streams` map rather than returned, because `request()` deliberately
        hands back a value and not a handle.
        """
        peer = self.peers[step["peer"]]
        timeout = step.get("timeout_ms")
        before = set(peer.streams)
        task = asyncio.ensure_future(
            peer.request(
                step.get("payload"),
                headers=step.get("headers"),
                # Milliseconds in the corpus and in TypeScript, seconds in Python (WSM-CON-012).
                timeout=None if timeout is None else float(timeout) / 1000,
            )
        )
        stream = await self._discover(peer, before)
        self._adopt(step, stream, task=task)

    async def call_notify(self, step: dict[str, Any]) -> None:
        peer = self.peers[step["peer"]]
        before = set(peer.streams)
        await peer.notify(step.get("payload"), headers=step.get("headers"))
        self._adopt(step, await self._discover(peer, before))

    async def call_send(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        await stream.send(step.get("payload"), end=step.get("end", False))

    async def call_end(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        # An absent `payload` key is `ABSENT`, not `null`: they are different frames (D1).
        await stream.end(step["payload"] if "payload" in step else ABSENT, trailers=step.get("trailers"))

    async def call_reply(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        await stream.reply(step.get("payload"), trailers=step.get("trailers"))

    async def call_cancel(self, step: dict[str, Any]) -> None:
        await self.stream_for(step["peer"], step["stream_ref"]).cancel(step.get("reason"))

    async def call_iterate(self, step: dict[str, Any]) -> None:
        """Start consuming a stream as an async iterator; the collected list is the ref's value.

        Started rather than awaited, and bound to a label, so that iteration and the frames that feed
        it can be asserted in the same script: `expect_result` on the label compares the whole list.
        """
        stream = self.stream_for(step["peer"], step["stream_ref"])

        async def collect() -> list[Any]:
            return [item async for item in stream]

        self.tasks[step["as"]] = asyncio.ensure_future(collect())

    async def call_close(self, step: dict[str, Any]) -> None:
        peer = self.peers[step["peer"]]
        # Durations are milliseconds in the corpus and in TypeScript, seconds in Python
        # (WSM-CON-012); the runner converts so the fixture does not have to carry both.
        drain = float(step.get("drain_ms", 10_000)) / 1000
        code = ResetCode(step.get("code", int(ResetCode.NO_ERROR)))
        # Started rather than awaited: `close()` sends `goaway`, *then* drains, and the steps after
        # this one are what the drain window is there to let happen (WSM-CON-025).
        self.closing[step["peer"]] = asyncio.create_task(peer.close(code, step.get("reason"), drain))

    async def call_await_close(self, step: dict[str, Any]) -> None:
        task = self.closing.get(step["peer"])
        if task is None:
            raise AssertionError(f"await_close: {step['peer']} has no close() in flight")
        await asyncio.wait_for(task, STEP_TIMEOUT)

    def _adopt(self, step: dict[str, Any], stream: Stream, *, task: asyncio.Task[Any] | None = None) -> None:
        """Give the stream this step opened its ordinal, and bind `as` if the step carries one."""
        self.ordinals.append(stream.id)
        self.by_id[step["peer"]][stream.id] = stream
        if "as" not in step:
            return
        if task is not None:
            self.tasks[step["as"]] = task
        else:
            self.refs[step["as"]] = stream

    async def _discover(self, peer: Peer, before: set[int]) -> Stream:
        """The stream a call opened without handing it back, found through the public map.

        A bounded poll rather than a single read: `request()` is a coroutine, so in Python the
        `open()` inside it does not run until the task is first scheduled. TypeScript reaches the
        same `open()` synchronously, which is exactly the kind of difference a shared corpus must not
        be able to see.
        """
        for _ in range(POLL_TURNS):
            fresh = [stream for stream_id, stream in peer.streams.items() if stream_id not in before]
            if fresh:
                return fresh[0]
            await asyncio.sleep(0)
        raise AssertionError("the call opened no stream")

    def inject(self, step: dict[str, Any]) -> None:
        """Deliver one message **to** the named peer, as if its remote had sent it.

        The envelope is encoded as it is written, without passing through `from_mapping`, which is
        the whole point for the two extension-point fixtures: `from_mapping` drops unknown keys
        (WSM-FRM-001), so a frame built through it could never carry the unknown field whose
        toleration is being asserted.
        """
        who = step["peer"]
        self.sockets[who].inject(self.codec.encode_payload(self.resolve(step["inject"])))

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

    async def _value_of(self, ref: str) -> Any:
        """Await whatever the label was bound to: a call in flight, or a stream's own result."""
        task = self.tasks.get(ref)
        if task is not None:
            # Shielded: a step that times out must not cancel the call underneath it, or the failure
            # reported would be a reset this fixture never asked for.
            return await asyncio.wait_for(asyncio.shield(task), STEP_TIMEOUT)
        stream = self.refs.get(ref)
        if stream is None:
            raise AssertionError(f"no step labelled {ref!r} with 'as'")
        return await asyncio.wait_for(stream.result(), STEP_TIMEOUT)

    async def expect_result(self, spec: dict[str, Any]) -> None:
        value = await self._value_of(spec["ref"])
        if value != spec["value"]:
            raise AssertionError(f"{spec['ref']} produced {value!r}, expected {spec['value']!r}")

    async def expect_error(self, spec: dict[str, Any]) -> None:
        expected = ERROR_CLASSES[spec["error"]]
        with pytest.raises(expected) as caught:
            await self._value_of(spec["ref"])
        if "code" in spec and int(getattr(caught.value, "code", -1)) != spec["code"]:
            raise AssertionError(f"{spec['ref']} failed with code {getattr(caught.value, 'code', None)}")
        if "payload" in spec and getattr(caught.value, "payload", None) != spec["payload"]:
            raise AssertionError(f"{spec['ref']} carried {getattr(caught.value, 'payload', None)!r}")

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


async def replay(path: Path, *, swapped: bool, codec: Any = None) -> Replay:
    """Run one sequence fixture end to end and hand back the finished `Replay` for inspection."""
    fixture = _load_sequence(path)
    assert fixture["name"] == path.stem, f"{path.name} carries the name {fixture['name']!r}"
    codec = JsonCodec() if codec is None else codec
    _skip_unless_this_codec_can_run(fixture, codec.name)

    replaying = Replay(fixture, swapped=swapped, codec=codec)
    replaying.start()
    try:
        await replaying.run()
    finally:
        await replaying.stop()
    return replaying


def _skip_unless_this_codec_can_run(fixture: dict[str, Any], codec_name: str) -> None:
    """A fixture written for another codec is **skipped with a reason**, never quietly passed.

    `bytes-payload-under-binary-codec` is the one that needs it: bytes are a payload type only under
    a binary codec (WSM-CDC-008), and there is nothing a JSON-configured pass can assert about it. A
    skip nobody can see is the same as a missing test, so the reason names the fixture and both
    codecs - and the msgpack pass below skips nothing, which is what stops this from being a fixture
    every configuration skips (WSM-CDC-007).
    """
    required = fixture.get("requires_codec")
    if required is not None and required != codec_name:
        pytest.skip(f"{fixture['name']} requires the {required} codec; this runner is configured with {codec_name}")


@pytest.mark.parametrize("swapped", [False, True], ids=["dialer-opens", "roles-swapped"])
@pytest.mark.parametrize("path", SEQUENCES, ids=[p.stem for p in SEQUENCES])
async def test_sequence_corpus_replays_in_both_role_assignments(path: Path, swapped: bool):
    """WSM-TST-002: the script replays step for step, in both role assignments.

    The second pass is not decoration. Every stream id in it is the other parity, so a fixture that
    wrote a raw id - or a runner that resolved an ordinal to a constant - fails here and only here.
    """
    await replay(path, swapped=swapped)


@pytest.mark.parametrize("swapped", [False, True], ids=["dialer-opens", "roles-swapped"])
@pytest.mark.parametrize("path", SEQUENCES, ids=[p.stem for p in SEQUENCES])
async def test_sequence_corpus_replays_under_msgpack(path: Path, swapped: bool):
    """WSM-CDC-007: the second shipped codec runs the whole corpus with a real peer at each end.

    Not a duplicate of the pass above. A codec that ships without this is a codec whose only proof is
    `decode(encode(frame)) == frame` over a bag of frames - which cannot see anything a *sequence*
    exposes: the send path picking `send_bytes` from `codec.binary` (WSM-CDC-002), fragment sizes
    measured as buffer length rather than character count (WSM-FRG-003), or a payload type JSON does
    not have surviving both directions (WSM-CDC-008). It is also the pass that runs
    `bytes-payload-under-binary-codec`, which every JSON-configured pass skips.
    """
    await replay(path, swapped=swapped, codec=MsgpackCodec())


def test_the_binary_codec_fixture_is_replayed_rather_than_skipped_everywhere():
    """The fixture the JSON pass skips must be a fixture some pass runs (WSM-CDC-007).

    Written as an assertion about the corpus rather than trusted to the parametrization above,
    because the failure it guards against is silent: a `requires_codec` naming a codec no pass is
    configured with produces a fixture every configuration skips, which reads in the report as
    covered while being reachable by no line of the library.
    """
    configured = {JsonCodec.name, MsgpackCodec.name}
    for path in SEQUENCES:
        required = _load(path).get("requires_codec")
        assert required is None or required in configured, (
            f"{path.stem} requires the {required} codec and no pass in this runner is configured "
            f"with it, so nothing runs it; configure a pass or drop the fixture (WSM-CDC-007)"
        )


def test_the_bytes_placeholder_is_resolved_before_a_step_runs():
    """`{"$bytes": [...]}` is notation, never a payload shape (WSM-CDC-008).

    A runner that left the placeholder as a mapping would send an ordinary object, JSON and msgpack
    would both carry it happily, and the fixture would pass while asserting nothing about bytes. So
    assert both halves: the corpus really does carry the placeholder, and loading really does
    resolve it.
    """
    raw = json.dumps(_load(SEQUENCES_DIR / "bytes-payload-under-binary-codec.json"))
    assert '"$bytes"' in raw, "the fixture no longer carries the placeholder this test is about"

    resolved = _load_sequence(SEQUENCES_DIR / "bytes-payload-under-binary-codec.json")
    found: list[bytes] = []

    def walk(value: Any) -> None:
        if isinstance(value, bytes):
            found.append(value)
        elif isinstance(value, dict):
            assert set(value) != {"$bytes"}, "a $bytes placeholder survived loading"
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(resolved)
    assert found == [b"\x00\xff\x10"] * 3, f"the placeholders resolved to {found!r}"


def test_the_sequence_corpus_is_collected():
    """An empty `conformance/sequences/` is a suite that passes by testing nothing."""
    assert SEQUENCES, f"no sequence fixtures were collected from {SEQUENCES_DIR}"
    assert len(SEQUENCES) == EXPECTED_SEQUENCE_FIXTURES


def test_the_only_fixture_the_json_pass_skips_is_the_binary_codec_one():
    """A skip nobody can see is the same as a missing test, so the skip set itself is pinned.

    `pytest -rs` names the fixture and the reason; this is what stops a second fixture quietly
    acquiring a `requires_codec` and shrinking the corpus without anyone noticing. The msgpack pass
    skips nothing, so this names the JSON pass rather than "this runner".
    """
    declared = {path.stem: _load(path)["requires_codec"] for path in SEQUENCES if "requires_codec" in _load(path)}
    assert declared == {"bytes-payload-under-binary-codec": "msgpack"}


def test_both_runners_collect_the_same_number_of_fixtures():
    """One corpus, two runners: the counts must be the same number, not two claims about it.

    Read out of the TypeScript source rather than asserted twice in parallel, because two independent
    constants drift silently - which is the whole failure mode `conformance/README.md` exists to
    prevent (M6 §8).
    """
    source = (ROOT / "ts" / "conformance.spec.ts").read_text(encoding="utf-8")
    for name, mine in (
        ("EXPECTED_SEQUENCE_FIXTURES", EXPECTED_SEQUENCE_FIXTURES),
        ("EXPECTED_INVALID_FIXTURES", EXPECTED_INVALID_FIXTURES),
    ):
        found = re.search(rf"^const {name} = (\d+);$", source, re.MULTILINE)
        assert found is not None, f"ts/conformance.spec.ts declares no {name}"
        assert int(found.group(1)) == mine, f"{name}: TypeScript says {found.group(1)}, Python says {mine}"


async def _wire_of_the_whole_corpus() -> list[tuple[str, str | bytes]]:
    """Every message both peers put on the wire, over every sequence fixture, with its fixture name."""
    wire: list[tuple[str, str | bytes]] = []
    for path in SEQUENCES:
        fixture = _load(path)
        if fixture.get("requires_codec", JsonCodec.name) != JsonCodec.name:
            continue
        replaying = await replay(path, swapped=False)
        for socket in replaying.sockets.values():
            wire.extend((path.stem, message) for message in socket.sent)
    assert wire, "the corpus produced no frames at all"
    return wire


async def test_no_limit_and_no_version_appears_on_the_wire():
    """WSM-CON-031, WSM-CON-009, WSM-PKG-005: replay everything and look at what actually went out.

    There is no `settings` frame and no announced limit of any kind; the `muxws.v1.` subprotocol
    prefix is the only version anywhere. This replaces the retired extension-advertisement test
    (WSM-FRM-003), and it reads the **envelope on the wire** rather than a decoded `Frame`, which
    would have dropped the offending key before the assertion ever saw it.
    """
    for name, message in await _wire_of_the_whole_corpus():
        envelope = json.loads(message)
        assert envelope["type"] not in FORBIDDEN_FRAME_TYPES, f"{name} put a {envelope['type']} frame on the wire"
        carried = FORBIDDEN_ENVELOPE_KEYS & set(envelope)
        assert not carried, f"{name} put {sorted(carried)} on the wire in a {envelope['type']} frame"


async def test_window_update_is_never_sent():
    """WSM-BPR-001: `window_update` is a reserved frame type in v1 and nothing may send it.

    Separate from the test above, though it replays the same corpus, because it is a separate promise:
    flow control is *reserved*, not merely unannounced, and the day someone implements it this is the
    test that must be deleted on purpose.
    """
    for name, message in await _wire_of_the_whole_corpus():
        assert json.loads(message)["type"] != "window_update", f"{name} sent a window_update frame"


# --------------------------------------------------------------------------- invalid


async def _hold(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


@pytest.mark.parametrize("path", INVALID, ids=[p.stem for p in INVALID])
async def test_invalid_corpus_produces_the_declared_frame_and_survival(path: Path):
    """WSM-TST-003: the declared frames go out, and `connection_survives` matches."""
    fixture = _load(path)
    assert fixture["name"] == path.stem
    codec = JsonCodec()

    # The peer under test is an acceptor (even ids); the misbehaving remote is a dialer (odd ids).
    # Deliberately **not** role-swappable, unlike a sequence fixture: the whole point is hand-written
    # messages no API call on either side could have produced, and an ordinal cannot name a stream
    # that was never opened.
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
        await asyncio.gather(serving, return_exceptions=True)


def _assert_expected_frames(emitted: list[Frame], fixture: dict[str, Any]) -> None:
    """`expect_out` is an ordered subset match: the listed keys must appear, in order, on some frame.

    Asserting equality instead would make every fixture invalid the moment an optional field is
    added, which is the opposite of what WSM-FRM-001 asks of a receiver.
    """
    expected = fixture["expect_out"]
    if not expected:
        # Not "no assertion": `data-for-closed-id` needs to say that a late frame below the
        # high-water mark provokes **nothing at all** (WSM-STM-002), and this is the only way to.
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


def test_every_invalid_case_of_wsm_tst_003_has_a_fixture():
    """The eight cases, by name. Without this, a deleted fixture is a silently passing suite."""
    assert {path.stem for path in INVALID} == set(REQUIRED_INVALID_CASES)
    assert len(INVALID) == EXPECTED_INVALID_FIXTURES


# --------------------------------------------------------------------------- the freeze


def corpus_digest() -> str:
    """sha256 over `conformance/frames/`, defined so that two languages compute the same number.

    File name, a NUL, the file's bytes, a NUL, in file-name order. The name is hashed too, so that
    renaming a file is a change to the corpus rather than a no-op.
    """
    digest = hashlib.sha256()
    for path in sorted(FRAMES_DIR.glob("*.json"), key=lambda p: p.name):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(path.read_bytes())
        digest.update(b"\x00")
    return digest.hexdigest()


def test_json_wire_is_frozen():
    """The freeze is a test, not a promise (M6 §5).

    The JSON wire form is frozen at 1.0. Adding or changing a triple changes this digest - that is
    the point: the change is then a deliberate edit of the line above, in the same commit, rather
    than something that slips through because every other assertion compares parsed objects.
    """
    assert corpus_digest() == JSON_WIRE_DIGEST, (
        "the conformance/frames/ corpus changed. If that was deliberate, update JSON_WIRE_DIGEST in "
        "this file; the JSON wire form is frozen at 1.0 and ts/conformance.spec.ts reads this literal."
    )


def test_the_digest_covers_every_file_in_the_frames_corpus():
    """A digest over an empty glob is a digest that can never fail."""
    covered = sorted(path.name for path in FRAMES_DIR.glob("*.json"))
    assert covered == ["v1-fragment-boundaries.json", "v1-frames.json"]
