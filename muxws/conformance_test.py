"""The Python conformance runner: `conformance/sequences/` and `conformance/invalid/`.

`ts/conformance.spec.ts` is the TypeScript counterpart and reads the same files under the same
schema (`conformance/README.md`). A field that only one runner reads indicates a defective fixture.

The two corpora test different things:

- **`sequences/`** - both peers are real, nothing is injected, and the fixture is a script of
  ordinary API calls with interleaved assertions. It checks that the implementations agree about
  sequences (which frame is sent when, which stream survives which event), not only about the
  encoding of single frames. Every fixture runs twice, with the roles swapped. `stream_ref` is an
  ordinal rather than a raw id, so a fixture does not depend on either side's parity.
- **`invalid/`** - one real peer receives messages no correct implementation sends; the fixture
  asserts which frame is sent and whether the connection survives. Connection survival determines
  whether the peers still agree about every other stream (WSM-STM-024).

The runner does not change peer behaviour. A failing fixture indicates a wrongly implemented rule;
the fix belongs in the implementation, not in the fixture.
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

#: Both runners declare and assert these two numbers, and
#: `test_both_runners_collect_the_same_number_of_fixtures` reads the TypeScript file to check that
#: they agree. A fixture that is no longer collected fails the count.
EXPECTED_SEQUENCE_FIXTURES = 17
EXPECTED_INVALID_FIXTURES = 9

#: The nine cases WSM-TST-003 enumerates, by name. A count alone passes when one case is deleted and
#: another is duplicated under a new name.
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
        "headers-on-a-later-frame",
    }
)

#: Digest of the frozen JSON wire corpus: sha256 over `conformance/frames/`, sorted by file name, each
#: file contributing `name`, a NUL, its bytes, a NUL. `ts/conformance.spec.ts` computes the same
#: digest and reads this literal from this file, so there is one checked-in digest and an
#: implementation that reads a different corpus fails. A change to the JSON wire requires
#: updating this line in the same commit.
JSON_WIRE_DIGEST = "216c9462b79a866970deb9fb3718d85588ff52ec45db8ddf124b8c48569999b9"

#: Wall-clock limit, in seconds, on any one waiting step. A fixture that cannot make progress fails
#: with a named assertion instead of hanging the suite.
STEP_TIMEOUT = 2.0

#: How many turns of the event loop a bounded poll is given before it gives up.
POLL_TURNS = 400

#: `expect_error` names a class rather than a code, because WSM-ERR-004 defines the same class names in
#: both languages.
ERROR_CLASSES: dict[str, type[BaseException]] = {
    "ConnectionLost": ConnectionLost,
    "RemoteError": RemoteError,
    "StreamRefused": StreamRefused,
    "StreamReset": StreamReset,
    "StreamTimeout": StreamTimeout,
}

#: WSM-CON-031, WSM-CON-009, WSM-PKG-005: no frame carries a limit or a version. Checked against the
#: envelope keys on the wire, not a decoded `Frame`, because `from_mapping` drops unknown keys.
FORBIDDEN_ENVELOPE_KEYS = frozenset(
    {"ack", "protocol_version", "extensions", "max_frame_bytes", "max_concurrent_streams", "max_payload_bytes"}
)

#: Reserved in v1 and never sent (WSM-BPR-001). There is no `settings` frame at all (WSM-CON-031).
FORBIDDEN_FRAME_TYPES = frozenset({"settings", "window_update"})


class Error(Exception):
    """The exception the receiving handler raises for a fixture's `{"handler": "raise"}` payload.

    The class name appears on the wire: the default error serializer sends `type(exc).__name__`
    (Python) and `error.name` (TypeScript), so the `reset(APPLICATION_ERROR)` payload a fixture
    asserts is the same in both languages only if the class name is. `Error` is the JavaScript
    built-in name.
    """


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def substitute_bytes(value: Any) -> Any:
    """Resolve every `{"$bytes": [...]}` placeholder into the byte string those integers spell.

    JSON has no byte type, so a fixture that needs bytes under a binary codec (WSM-CDC-008) uses this
    notation. It is resolved before the first step runs and never reaches the wire. An unresolved
    placeholder would be sent as an ordinary object and the fixture would not test bytes;
    `test_the_bytes_placeholder_is_resolved_before_a_step_runs` checks the resolution.

    Only a mapping whose sole key is `$bytes` is a placeholder; `$bytes` alongside other keys is an
    ordinary payload key.
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
        # The codec is a parameter, because WSM-CDC-007 requires every shipped codec to run this
        # corpus with a peer at each end.
        self.codec = JsonCodec() if codec is None else codec

        options: dict[str, Any] = {}
        for key in ("max_frame_bytes", "max_concurrent_streams"):
            if key in fixture:
                # An instruction to the runner, never a wire value (WSM-TST-002, WSM-CON-031).
                options[key] = fixture[key]

        dialer_socket, acceptor_socket = memory_pair()
        self.sockets: dict[str, MemorySocket] = {"dialer": dialer_socket, "acceptor": acceptor_socket}
        # The two roles keep their names; a role swap changes only which of them has the odd parity.
        # Every id in the second pass has the other parity, so a fixture never contains a raw stream
        # id (WSM-TST-002).
        self.peers: dict[str, Peer] = {
            "dialer": Peer(dialer_socket, codec=self.codec, is_dialer=not swapped, **options),
            "acceptor": Peer(acceptor_socket, codec=self.codec, is_dialer=swapped, **options),
        }

        #: Ordinal -> the id allocated to that stream. Filled by every step that opens one.
        self.ordinals: list[int] = []
        #: Both peers' view of every stream, by id: the opener's handle and the receiver's.
        self.by_id: dict[str, dict[int, Stream]] = {"dialer": {}, "acceptor": {}}
        #: `as` labels bound to a stream, for a later `expect_result` / `expect_error`.
        self.refs: dict[str, Stream] = {}
        #: `as` labels bound to calls in flight (`request`, `iterate`). The asserted value is the
        #: call's return value; the call is started, not awaited, so that the following steps run.
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

        WSM-STM-035 ends a stream when its handler returns, so the handler waits; otherwise every
        inbound stream would close before the script could `reply` on it.

        The `{"handler": "raise"}` payload makes the handler raise, which reaches the handler-failure
        path of WSM-STM-034. `stream.reset(APPLICATION_ERROR)` sends the same code but no serialized
        error object, which the fixture asserts. The `{"handler": "return"}` payload makes it return at
        once, which ends a oneway stream (WSM-STM-040).
        """

        async def handler(payload: Any, stream: Stream) -> None:
            self.by_id[who][stream.id] = stream
            if isinstance(payload, dict) and payload.get("handler") == "raise":
                raise Error(str(payload.get("message", "the handler raised")))
            if isinstance(payload, dict) and payload.get("handler") == "return":
                return
            await stream.closed.wait()

        return handler

    async def stop(self) -> None:
        await self.sockets["dialer"].drop()
        await self.settle()
        pending = [*self.tasks.values(), *self.closing.values(), *self.serving]
        for task in pending:
            task.cancel()
        for task in pending:
            # Exceptions from a cancelled serve(), close() or collector during teardown are ignored;
            # the fixture's assertions have already run.
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
        elif "expect_headers" in step:
            await self.expect_headers(step["expect_headers"])
        elif "expect_error" in step:
            await self.expect_error(step["expect_error"])
        elif "expect_closed" in step:
            await self.expect_closed(step["expect_closed"])
        else:
            raise AssertionError(f"no step kind in {step!r}")

    async def do_call(self, step: dict[str, Any]) -> None:
        """Dispatch one `call` step. An unimplemented call fails the test; it is not skipped."""
        call = step["call"]
        handler = getattr(self, f"call_{call}", None)
        if handler is None:
            raise AssertionError(f"the runner does not implement the call {call!r}")

        expected = step.get("raises")
        if expected is None:
            await handler(step)
            return
        # `raises` states that the call itself fails, for example a send by a producer whose stream
        # the consumer cancelled. Frames do not show that the local handle refused to send
        # (WSM-ERR-009).
        with pytest.raises(ERROR_CLASSES[expected]):
            await handler(step)

    async def call_open(self, step: dict[str, Any]) -> None:
        peer = self.peers[step["peer"]]
        stream = peer.open(step.get("payload"), headers=step.get("headers"), end=step.get("end", False))
        self._adopt(step, stream)

    async def call_request(self, step: dict[str, Any]) -> None:
        """`peer.request(...)`, **started and not awaited** (WSM-API-006).

        The steps after a `request` assert on the frames it produced, so awaiting it here would
        deadlock. The stream it allocated is found through the public `peer.streams` map, because
        `request()` returns a value, not a handle.
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
        """`peer.notify(...)`, awaited. The stream is closed at once and not in `peer.streams`
        (WSM-STM-041), so its ordinal is the id of the `open` it put on the wire."""
        who = step["peer"]
        before = {frame.stream for frame in self.frames_sent_by(who) if frame.type == "open"}
        await self.peers[who].notify(step.get("payload"), headers=step.get("headers"))
        for _ in range(POLL_TURNS):
            fresh = [
                frame.stream
                for frame in self.frames_sent_by(who)
                if frame.type == "open" and frame.stream not in before
            ]
            if fresh:
                self.ordinals.append(fresh[0])
                return
            await asyncio.sleep(0)
        raise AssertionError("the notify put no open on the wire")

    async def call_send(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        await stream.send(step.get("payload"), end=step.get("end", False), headers=step.get("headers"))

    async def call_send_headers(self, step: dict[str, Any]) -> None:
        """The answering side's leading headers, on a frame with no payload at all (WSM-API-024)."""
        stream = self.stream_for(step["peer"], step["stream_ref"])
        await stream.send_headers(step["headers"])

    async def call_end(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        # An absent `payload` key is `ABSENT`, not `null`: they are different frames.
        await stream.end(
            step["payload"] if "payload" in step else ABSENT,
            trailers=step.get("trailers"),
            headers=step.get("headers"),
        )

    async def call_reply(self, step: dict[str, Any]) -> None:
        stream = self.stream_for(step["peer"], step["stream_ref"])
        await stream.reply(step.get("payload"), trailers=step.get("trailers"), headers=step.get("headers"))

    async def call_cancel(self, step: dict[str, Any]) -> None:
        await self.stream_for(step["peer"], step["stream_ref"]).cancel(step.get("reason"))

    async def call_iterate(self, step: dict[str, Any]) -> None:
        """Start consuming a stream as an async iterator; the collected list is the ref's value.

        Started, not awaited, and bound to a label, so the script can also assert on the frames that
        feed the iteration; `expect_result` on the label compares the whole list.
        """
        stream = self.stream_for(step["peer"], step["stream_ref"])

        async def collect() -> list[Any]:
            return [item async for item in stream]

        self.tasks[step["as"]] = asyncio.ensure_future(collect())

    async def call_close(self, step: dict[str, Any]) -> None:
        peer = self.peers[step["peer"]]
        # Durations are milliseconds in the corpus and in TypeScript, seconds in Python
        # (WSM-CON-012); the runner converts them.
        drain = float(step.get("drain_ms", 10_000)) / 1000
        code = ResetCode(step.get("code", int(ResetCode.NO_ERROR)))
        # Started, not awaited: `close()` sends `goaway` and then drains, and the following steps run
        # during the drain window (WSM-CON-025).
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

        A bounded poll, not a single read: `request()` is a coroutine, so in Python the `open()`
        inside it runs only when the task is first scheduled. TypeScript calls `open()`
        synchronously; the poll keeps the fixture independent of that difference.
        """
        for _ in range(POLL_TURNS):
            fresh = [stream for stream_id, stream in peer.streams.items() if stream_id not in before]
            if fresh:
                return fresh[0]
            await asyncio.sleep(0)
        raise AssertionError("the call opened no stream")

    def inject(self, step: dict[str, Any]) -> None:
        """Deliver one message **to** the named peer, as if its remote had sent it.

        The envelope is encoded as written, without `from_mapping`: `from_mapping` drops unknown keys
        (WSM-FRM-001), and the two extension-point fixtures send an unknown field.
        """
        who = step["peer"]
        self.sockets[who].inject(self.codec.encode_payload(self.resolve(step["inject"])))

    # ------------------------------------------------------------------ resolution

    def id_of(self, ordinal: int) -> int:
        """`stream_ref: n` -> the id allocated to the n-th stream in the script (WSM-TST-002)."""
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
        """Assert that no matching frame was sent; the whole of that peer's wire is searched."""
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
            # Shielded: a step that times out does not cancel the call, which would reset the stream.
            return await asyncio.wait_for(asyncio.shield(task), STEP_TIMEOUT)
        stream = self.refs.get(ref)
        if stream is None:
            raise AssertionError(f"no step labelled {ref!r} with 'as'")
        return await asyncio.wait_for(stream.result(), STEP_TIMEOUT)

    async def expect_result(self, spec: dict[str, Any]) -> None:
        value = await self._value_of(spec["ref"])
        if value != spec["value"]:
            raise AssertionError(f"{spec['ref']} produced {value!r}, expected {spec['value']!r}")

    async def expect_headers(self, spec: dict[str, Any]) -> None:
        """Assert the headers a peer's stream handle exposes (WSM-API-025).

        `expect_frame` checks that headers were sent; this checks the attribute that exposes them.
        `of` is required and selects the attribute. For `reply` the step waits on
        `reply_headers_arrived`, which is set when the value is final; waiting a fixed number of turns
        would pass against an implementation that never sets the event.
        """
        stream = self.stream_for(spec["peer"], spec["stream_ref"])
        of = spec["of"]
        if of not in ("open", "reply"):
            raise AssertionError(f'expect_headers needs "of": "open" or "reply", not {of!r}')
        if of == "reply":
            try:
                await asyncio.wait_for(stream.reply_headers_arrived.wait(), STEP_TIMEOUT)
            except asyncio.TimeoutError:
                raise AssertionError(
                    f"{spec['peer']} stream {spec['stream_ref']} never saw reply headers arrive"
                ) from None
        held = stream.headers if of == "open" else stream.reply_headers
        if held != spec["value"]:
            raise AssertionError(f"{of} headers were {held!r}, expected {spec['value']!r}")

    async def expect_error(self, spec: dict[str, Any]) -> None:
        expected = ERROR_CLASSES[spec["error"]]
        with pytest.raises(expected) as caught:
            await self._value_of(spec["ref"])
        if "code" in spec and int(getattr(caught.value, "code", -1)) != spec["code"]:
            raise AssertionError(f"{spec['ref']} failed with code {getattr(caught.value, 'code', None)}")
        if "payload" in spec and getattr(caught.value, "payload", None) != spec["payload"]:
            raise AssertionError(f"{spec['ref']} carried {getattr(caught.value, 'payload', None)!r}")

    async def expect_closed(self, spec: dict[str, Any]) -> None:
        """A bounded poll, not a single read: the other end observes a close one turn later."""
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
    """A fixture written for another codec is skipped with a reason naming the fixture and both codecs.

    `bytes-payload-under-binary-codec` requires a binary codec: bytes are a payload type only under a
    binary codec (WSM-CDC-008). The msgpack pass below runs it (WSM-CDC-007).
    """
    required = fixture.get("requires_codec")
    if required is not None and required != codec_name:
        pytest.skip(f"{fixture['name']} requires the {required} codec; this runner is configured with {codec_name}")


@pytest.mark.parametrize("swapped", [False, True], ids=["dialer-opens", "roles-swapped"])
@pytest.mark.parametrize("path", SEQUENCES, ids=[p.stem for p in SEQUENCES])
async def test_sequence_corpus_replays_in_both_role_assignments(path: Path, swapped: bool):
    """WSM-TST-002: the script replays step for step, in both role assignments.

    Every stream id in the second pass has the other parity, so a fixture with a raw id, or a runner
    that resolves an ordinal to a constant, fails here.
    """
    await replay(path, swapped=swapped)


@pytest.mark.parametrize("swapped", [False, True], ids=["dialer-opens", "roles-swapped"])
@pytest.mark.parametrize("path", SEQUENCES, ids=[p.stem for p in SEQUENCES])
async def test_sequence_corpus_replays_under_msgpack(path: Path, swapped: bool):
    """WSM-CDC-007: the second shipped codec runs the whole corpus with a real peer at each end.

    A round-trip test `decode(encode(frame)) == frame` does not cover what a sequence exercises: the
    send path choosing `send_bytes` from `codec.binary` (WSM-CDC-002), fragment sizes measured as
    buffer length rather than character count (WSM-FRG-003), and a payload type JSON does not have
    arriving unchanged in both directions (WSM-CDC-008). This pass also runs
    `bytes-payload-under-binary-codec`, which every JSON-configured pass skips.
    """
    await replay(path, swapped=swapped, codec=MsgpackCodec())


def test_every_codec_specific_fixture_is_run_by_some_pass():
    """The fixture the JSON pass skips must be a fixture some pass runs (WSM-CDC-007).

    Asserted over the corpus, not left to the parametrization above: a `requires_codec` naming a
    codec no pass is configured with produces a fixture every configuration skips, which the report
    does not flag as a failure.
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

    An unresolved placeholder would be sent as an ordinary object, which JSON and msgpack both carry,
    and the fixture would not test bytes. The test asserts that the corpus contains the placeholder
    and that loading resolves it.
    """
    raw = json.dumps(_load(SEQUENCES_DIR / "bytes-payload-under-binary-codec.json"))
    assert '"$bytes"' in raw, "the fixture contains no $bytes placeholder"

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
    """`conformance/sequences/` is not empty."""
    assert SEQUENCES, f"no sequence fixtures were collected from {SEQUENCES_DIR}"
    assert len(SEQUENCES) == EXPECTED_SEQUENCE_FIXTURES


def test_the_only_fixture_the_json_pass_skips_is_the_binary_codec_one():
    """The set of fixtures the JSON pass skips is fixed.

    `pytest -rs` names the fixture and the reason; this test fails when another fixture acquires a
    `requires_codec`. The msgpack pass skips nothing, so this names the JSON pass.
    """
    declared = {path.stem: _load(path)["requires_codec"] for path in SEQUENCES if "requires_codec" in _load(path)}
    assert declared == {"bytes-payload-under-binary-codec": "msgpack"}


def test_both_runners_collect_the_same_number_of_fixtures():
    """Both runners declare the same fixture counts.

    The TypeScript counts are read from its source, so the two declarations cannot differ without
    this test failing (`conformance/README.md`).
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
    assert wire, "the corpus produced no frames"
    return wire


async def test_no_limit_and_no_version_appears_on_the_wire():
    """WSM-CON-031, WSM-CON-009, WSM-PKG-005: replay the whole corpus and check every sent envelope.

    There is no `settings` frame and no announced limit; the `muxws.v1.` subprotocol prefix is the
    only version. The envelope on the
    wire is checked, not a decoded `Frame`, which drops unknown keys.
    """
    for name, message in await _wire_of_the_whole_corpus():
        envelope = json.loads(message)
        assert envelope["type"] not in FORBIDDEN_FRAME_TYPES, f"{name} put a {envelope['type']} frame on the wire"
        carried = FORBIDDEN_ENVELOPE_KEYS & set(envelope)
        assert not carried, f"{name} put {sorted(carried)} on the wire in a {envelope['type']} frame"


async def test_window_update_is_never_sent():
    """WSM-BPR-001: `window_update` is a reserved frame type in v1 and nothing may send it.

    Separate from the test above, though it replays the same corpus: the rule is WSM-BPR-001, not
    WSM-CON-031.
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
    # Unlike sequence fixtures, these are not role-swappable: the messages are hand-written, no API
    # call produces them, and an ordinal cannot name a stream that was never opened.
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

    With an equality match, adding an optional field would invalidate every fixture; WSM-FRM-001
    requires receivers to ignore unknown fields.
    """
    expected = fixture["expect_out"]
    if not expected:
        # An empty list asserts that nothing is sent: `data-for-closed-id` requires that a late frame
        # below the high-water mark causes no response (WSM-STM-002).
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
    """The nine required cases are present, by name."""
    assert {path.stem for path in INVALID} == set(REQUIRED_INVALID_CASES)
    assert len(INVALID) == EXPECTED_INVALID_FIXTURES


# --------------------------------------------------------------------------- the freeze


def corpus_digest() -> str:
    """sha256 over `conformance/frames/`, computed identically in both languages.

    File name, a NUL, the file's bytes, a NUL, in file-name order. The name is hashed, so renaming a
    file changes the digest.
    """
    digest = hashlib.sha256()
    for path in sorted(FRAMES_DIR.glob("*.json"), key=lambda p: p.name):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(path.read_bytes())
        digest.update(b"\x00")
    return digest.hexdigest()


def test_json_wire_is_frozen():
    """The JSON wire corpus matches `JSON_WIRE_DIGEST`.

    The JSON wire form of `muxws.v1.json` is frozen. Adding or changing a triple changes this digest, so the
    change requires editing `JSON_WIRE_DIGEST` in the same commit. Other assertions compare parsed
    objects and do not detect byte-level changes.
    """
    assert corpus_digest() == JSON_WIRE_DIGEST, (
        "the conformance/frames/ corpus changed. If the change is intended, update JSON_WIRE_DIGEST in "
        "this file; the JSON wire form of muxws.v1.json is frozen and ts/conformance.spec.ts reads this literal."
    )


def test_the_digest_covers_every_file_in_the_frames_corpus():
    """`conformance/frames/` is not empty, so the digest covers files."""
    covered = sorted(path.name for path in FRAMES_DIR.glob("*.json"))
    assert covered == ["v1-fragment-boundaries.json", "v1-frames.json"]
