"""Python runner for the live cross-language interop matrix.

Run as an acceptor:          python interop/runner.py accept <port>
Accept on a socket file:     python interop/runner.py accept-unix <path>
Run the WSM-TST-004 script:  python interop/runner.py dial ws://127.0.0.1:<port>
     ... over a socket file: python interop/runner.py dial ws+unix://<path>:/ws
Run the WSM-TST-005 script:  python interop/runner.py reconnect-dial ws://127.0.0.1:<port>
Serve the corpus:            python interop/runner.py corpus-accept <control-port>
Conduct the corpus:          python interop/runner.py corpus-dial 127.0.0.1:<control-port>

`interop/runner.ts` has the same entry points, and `interop/drive.sh` pairs them in both role
assignments. A rule the two implementations handle differently fails with a named check instead of a
hang.

`dial` takes only a URL, so the Unix pairing uses the same mode with `ws+unix:///path/to.sock:/route`
in place of the TCP URL. The pairing checks that both implementations split that URL the same way:
pathname and search up to the first colon are the filesystem path, the rest is the HTTP request
target.

Checks use `check()` instead of `assert`: this file is not a `*_test.py`, so ruff's S101 applies, and
`assert` statements are removed under `python -O`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import socket
import sys

from pathlib import Path
from typing import Any

import websockets

import muxws

from muxws.frames import ABSENT, Frame
from muxws.stream import Stream
from muxws.subprotocol import offer
from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

#: How many streams the acceptor pushes back when asked (WSM-TST-004's "server push").
PUSH_COUNT = 3
#: Rows an `export` produces, the last one carrying `end` and trailers.
EXPORT_ROWS = 5
#: Pause between export rows. Without it the acceptor can enqueue all five rows before any other
#: handler is scheduled, and the interleaving assertion in `run_tst_004` depends on scheduling.
EXPORT_GAP = 0.01
#: Interval between rows of the producer that is cancelled mid-flight.
DRIP_GAP = 0.02
#: Rows the `slow` stream produces; it is the stream that must survive the goaway drain.
SLOW_ROWS = 3
SLOW_GAP = 0.05

#: The hello replayed on every connection the reconnect scenario's dialer makes (WSM-RCN-020/027).
#: Nested, so a replay that rebuilds the payload instead of replaying the captured bytes can produce
#: different bytes.
HELLO_PAYLOAD: Any = {"who": "interop", "caps": ["a", "b"], "nested": {"n": 1}}
HELLO_HEADERS: dict[str, Any] = {"session": "interop-session"}

#: Short delays so that WSM-TST-005's jittered retries complete within a CI job without a 30 s wait.
#: The jitter fraction keeps its default: the scenario asserts that the delays are dispersed.
RECONNECT = muxws.Reconnect(initial_delay=0.05, max_delay=0.5)


def check(condition: bool, what: str) -> None:
    if not condition:
        raise SystemExit(f"interop FAILED: {what}")


def register_configured_codec() -> None:
    """Register the codec `MUXWS_CODEC` selects. This is done by the application, not the library.

    WSM-CDC-013 forbids the library from dynamically importing or probing for a codec; an application
    may choose which codec module to import. An unknown name exits at startup and does not fall back
    to JSON (WSM-INV-015).
    """
    name = muxws.settings.codec
    if name == "json":
        # `muxws/__init__.py` already registered it (WSM-CDC-004).
        return
    if name == "msgpack":
        from muxws.codecs.msgpack_ import MsgpackCodec

        muxws.register_codec("msgpack", MsgpackCodec())
        return
    raise SystemExit(f"interop FAILED: the interop runner knows no codec named {name!r}")


def encoding_of(frame: Frame) -> str:
    """The encoded frame as a hex string.

    A binary codec produces bytes and JSON produces text; hex gives one comparable form for the
    byte-identity check of WSM-RCN-027.
    """
    encoded = muxws.resolve_codec().encode(frame)
    return (encoded if isinstance(encoded, bytes) else encoded.encode("utf-8")).hex()


def emit(**fields: Any) -> None:
    """Print one JSON object per line on stdout; `interop/drive.sh` reads these lines."""
    print(json.dumps(fields), flush=True)


# --------------------------------------------------------------------------- the acceptor


def make_handler(peer: muxws.Peer, state: dict[str, Any]) -> Any:
    """Build the per-connection handler; the opening payload's `action` selects the behaviour.

    The handler is built per connection so that it can use `peer` (a server push is an ordinary
    `open()` from the acceptor) and so that `state` is not shared between connections; the reconnect
    scenario gives this process two connections.
    """

    async def handler(payload: Any, stream: Stream) -> None:
        action = (payload or {}).get("action")

        if action is None:
            # The hello. It carries no marker on the wire (WSM-RCN-021); it is acknowledged by this
            # handler returning, which ends the stream implicitly (WSM-RCN-022/WSM-STM-035). Rejecting
            # a payload without `action` would reset the hello and the reconnect scenario would fail
            # on its first connection.
            if payload != HELLO_PAYLOAD:
                raise ValueError(f"opened with neither an action nor the hello: {payload!r}")

        elif action == "echo":
            await stream.reply({"echo": payload.get("value")})

        elif action == "export":
            for index in range(EXPORT_ROWS - 1):
                await stream.send({"row": index})
                await asyncio.sleep(EXPORT_GAP)
            await stream.end({"row": EXPORT_ROWS - 1}, trailers={"rows": str(EXPORT_ROWS)})

        elif action == "push":
            count = int(payload.get("count", PUSH_COUNT))
            for index in range(count):
                await peer.notify({"event": "tick", "index": index})
            await stream.reply({"pushed": count})

        elif action == "drip":
            await drip(stream, state)

        elif action == "drip-report":
            await stream.reply({"stopped": state["drip_stopped"], "sent": state["drip_sent"]})

        elif action == "slow":
            for index in range(SLOW_ROWS - 1):
                await stream.send({"row": index})
                await asyncio.sleep(SLOW_GAP)
            await stream.end({"row": SLOW_ROWS - 1})

        elif action == "raise":
            raise ValueError("interop handler said no")

        elif action == "forever":
            await stream.closed.wait()

        elif action == "big":
            # Larger than MAX_FRAME_BYTES once encoded, so the reply is fragmented while other streams
            # are in flight (WSM-FRG-019 round robin).
            await stream.reply({"blob": "š" * 40_000})

        elif action == "goaway":
            # Scheduled, not awaited: `peer.close()` drains the streams in flight, including this
            # handler's stream, so awaiting it here would deadlock. The task is stored in `state`
            # because an unreferenced `create_task` result can be garbage-collected during the drain.
            state["shutdown"] = asyncio.create_task(peer.close(reason="interop goaway"))

        else:
            # Raised, not exited: the error resets one stream (WSM-ERR-006). Exiting the process would
            # appear to the dialer as a lost socket.
            raise ValueError(f"unknown action {action!r}")

    return handler


async def drip(stream: Stream, state: dict[str, Any]) -> None:
    """Send rows until the consumer cancels the stream; the remote side of the cancel-mid-flight check.

    Both exits set `drip_stopped`: under WSM-ERR-013 an inbound `reset(CANCELLED)` cancels the handler
    task, and a reset that arrives between two sends raises `StreamClosed` from the send. Which one
    occurs depends on timing.
    """
    try:
        while True:
            await stream.send({"row": state["drip_sent"]})
            state["drip_sent"] += 1
            await asyncio.sleep(DRIP_GAP)
    except asyncio.CancelledError:
        state["drip_stopped"] = True
        # Re-raised so that the task completes as cancelled.
        raise
    except (muxws.StreamClosed, muxws.StreamReset):
        state["drip_stopped"] = True


async def serve_one_connection(connection: Any) -> None:
    """Serve one accepted connection, over TCP or a Unix socket file.

    `accept_forever` and `accept_unix_forever` both pass this coroutine to their server, so the two
    acceptors differ only in how they bind (WSM-API-021).
    """
    # Report the HTTP request target. Neither acceptor routes on it, so a dialer that sends `/` or
    # drops the query string still passes every assertion in the script; the driver compares this
    # value with the route in the `ws+unix://<path>:/route` URL.
    request = connection.request
    emit(event="accepted", target=None if request is None else request.path)
    peer = await muxws.accept(WebsocketsSocket(connection))
    state: dict[str, Any] = {"drip_sent": 0, "drip_stopped": False, "shutdown": None}
    seen_open = False

    def journal(direction: str, frame: Frame, _length: int) -> None:
        """Report the first inbound `open` of this connection, re-encoded.

        The driver compares this line across the two acceptor processes to check WSM-TST-005's
        byte-identical hello on the receiving side. The first open of a connection is the hello
        (WSM-RCN-023).
        """
        nonlocal seen_open
        if direction == "rx" and frame.type == "open" and not seen_open:
            seen_open = True
            emit(event="first-open", encoding=encoding_of(frame))

    peer.on_frame(journal)
    peer.on_stream(make_handler(peer, state))
    # A dialer disconnecting ends this connection; the read loop's `ConnectionClosed` is not an error.
    with contextlib.suppress(muxws.ConnectionClosed):
        await peer.serve()


async def accept_forever(port: int) -> None:
    """Serve until the driver kills the process (SIGKILL in the reconnect scenario)."""
    async with websockets.serve(
        serve_one_connection, "127.0.0.1", port, select_subprotocol=muxws.select_subprotocol
    ) as service:
        emit(role="python-acceptor", port=service.sockets[0].getsockname()[1], codec=muxws.settings.codec)
        await asyncio.Future()


async def accept_unix_forever(path: str) -> None:
    """The acceptor bound to a Unix socket file, for the Unix variant of WSM-TST-004.

    It passes the same `select_subprotocol` hook as the TCP acceptor, so a dialer offering an
    unsupported codec gets HTTP 400 and must raise `CodecMismatch` (WSM-CDC-022/024). The driver runs
    the unmodified WSM-TST-004 script against it.

    The driver's readiness signal is the `path` line printed after `listen`; the socket file already
    exists after `bind`, so waiting for the file can connect before `listen`.
    """
    # Without AF_UNIX (Windows) the bind fails with an event-loop error about a missing
    # `create_unix_server`; this check exits with a clear message first.
    if not hasattr(socket, "AF_UNIX"):
        raise SystemExit("this platform has no AF_UNIX, so the unix scenario cannot run here")
    # Imported after the check so that an unsupported platform reaches the message above first.
    from websockets.asyncio.server import unix_serve

    async with unix_serve(serve_one_connection, path, select_subprotocol=muxws.select_subprotocol):
        emit(role="python-acceptor", path=path, codec=muxws.settings.codec)
        await asyncio.Future()


# --------------------------------------------------------------------------- WSM-TST-004


class Journal:
    """Every frame the peer saw, in order, as `(direction, type, stream)`.

    Interleaving is checked on wire order: two calls that overlap in Python can still be serialised
    on the socket.
    """

    def __init__(self) -> None:
        self.entries: list[tuple[str, str, int | None]] = []
        self.goaway = asyncio.Event()
        self.goaway_frame: Frame | None = None

    def record(self, direction: str, frame: Frame, _length: int) -> None:
        self.entries.append((direction, frame.type, frame.stream))
        if direction == "rx" and frame.type == "goaway" and self.goaway_frame is None:
            self.goaway_frame = frame
            self.goaway.set()


async def run_tst_004(peer: muxws.Peer, journal: Journal, pushes: list[Any], *, label: str) -> None:
    """Concurrent unary requests interleaved with a streaming export and a server push, one stream
    cancelled mid-flight, then a `goaway` shutdown (WSM-TST-004)."""
    export_rows: list[Any] = []
    export_id = 0

    async def consume_export() -> None:
        nonlocal export_id
        stream = peer.open({"action": "export"})
        export_id = stream.id
        async for row in stream:
            export_rows.append(row)

    # Six concurrent streams. The export and the fragmented `big` reply produce multiple frames each,
    # so concurrency shows up in the wire order.
    echoed_1, echoed_2, echoed_3, big, _, pushed = await asyncio.gather(
        peer.request({"action": "echo", "value": 1}),
        peer.request({"action": "echo", "value": 2}),
        peer.request({"action": "echo", "value": 3}),
        peer.request({"action": "big"}),
        consume_export(),
        peer.request({"action": "push", "count": PUSH_COUNT}),
    )

    check([echoed_1, echoed_2, echoed_3] == [{"echo": 1}, {"echo": 2}, {"echo": 3}], f"{label}: unary answers crossed")
    check(len(big["blob"]) == 40_000, f"{label}: fragmented payload came back as {len(big['blob'])} chars")
    check(big["blob"][0] == "š", f"{label}: fragmented payload lost its non-ASCII content")
    check(export_rows == [{"row": index} for index in range(EXPORT_ROWS)], f"{label}: export gave {export_rows!r}")
    check(pushed == {"pushed": PUSH_COUNT}, f"{label}: the push request answered {pushed!r}")
    # Compared without order: v1 does not order delivery across streams (WSM-STM-001).
    check(
        sorted(push["index"] for push in pushes) == list(range(PUSH_COUNT))
        and {push["event"] for push in pushes} == {"tick"},
        f"{label}: the server pushed {pushes!r}",
    )

    # Between the export's first and last inbound frame there must be an inbound frame of another
    # stream. Only inbound frames are counted: the six outbound opens are sent together regardless of
    # how the acceptor answers.
    #
    # This assertion does not test the round-robin writer; the export handler's pauses satisfy it
    # without one. WSM-FRG-019 is tested by
    # `writer_test.py::test_round_robin_selects_across_streams_not_fifo` and the
    # `small-frame-overtakes-a-fragmented-payload` fixture. This check asserts that neither
    # implementation answers streams one at a time. Asserting that the `big` reply's fragments are
    # non-contiguous would depend on another stream having data queued at that moment, and would be
    # timing-dependent.
    inbound = [entry for entry in journal.entries if entry[0] == "rx"]
    export_positions = [index for index, entry in enumerate(inbound) if entry[2] == export_id]
    check(bool(export_positions), f"{label}: no inbound frame for the export stream {export_id}")
    foreign = [
        inbound[index] for index in range(export_positions[0], export_positions[-1]) if inbound[index][2] != export_id
    ]
    check(bool(foreign), f"{label}: the export was answered without a single other stream interleaved")

    await run_application_error(peer, label=label)
    await run_cancel_mid_flight(peer, label=label)
    await run_goaway_shutdown(peer, journal, label=label)

    emit(role=label, ok=True)


async def run_application_error(peer: muxws.Peer, *, label: str) -> None:
    """A handler that raises becomes `reset(APPLICATION_ERROR)` carrying the serialized payload."""
    raised: muxws.RemoteError | None = None
    try:
        await peer.request({"action": "raise"})
    except muxws.RemoteError as error:
        raised = error
    check(raised is not None, f"{label}: a raising handler did not produce RemoteError")

    payload = raised.payload or {}
    # `message` is the same in both languages; `type` is not. WSM-ERR-006's default serializer
    # reports the remote's exception class name: "Error" from TypeScript, "ValueError" from Python.
    check(payload.get("message") == "interop handler said no", f"{label}: application error was {payload!r}")
    check(
        isinstance(payload.get("type"), str) and bool(payload["type"]),
        f"{label}: the application error carried no type name: {payload!r}",
    )


async def run_cancel_mid_flight(peer: muxws.Peer, *, label: str) -> None:
    """One stream cancelled mid-flight; the remote producer must stop (WSM-TST-004)."""
    drip_stream = peer.open({"action": "drip"})
    seen = 0
    async for _row in drip_stream:
        seen += 1
        if seen == 3:
            break
    check(seen == 3, f"{label}: the drip stream produced only {seen} rows before the cancel")

    await drip_stream.cancel("interop cancel mid-flight")
    check(drip_stream.closed.is_set(), f"{label}: cancel did not close the stream locally")

    # Polled until a deadline instead of a single fixed sleep. A producer that did not receive the
    # reset never reports `stopped`, and the check fails at the deadline.
    deadline = asyncio.get_running_loop().time() + 5.0
    report: dict[str, Any] = {}
    while asyncio.get_running_loop().time() < deadline:
        report = await peer.request({"action": "drip-report"})
        if report.get("stopped"):
            break
        await asyncio.sleep(DRIP_GAP)
    check(bool(report.get("stopped")), f"{label}: reset(CANCELLED) did not stop the remote producer: {report!r}")
    check(int(report.get("sent", 0)) >= seen, f"{label}: the producer reports fewer rows than arrived: {report!r}")


async def run_goaway_shutdown(peer: muxws.Peer, journal: Journal, *, label: str) -> None:
    """A `goaway` shutdown: streams at or below `last_stream` drain, later opens are refused."""
    slow = peer.open({"action": "slow"})
    rows: list[Any] = []
    draining = asyncio.create_task(collect(slow, rows))

    await peer.notify({"action": "goaway"})
    # Waits for the goaway frame itself; the checks below apply after it has arrived.
    await asyncio.wait_for(journal.goaway.wait(), 5.0)
    frame = journal.goaway_frame
    check(frame is not None, f"{label}: no goaway arrived")
    check(
        frame.last_stream is not None and frame.last_stream >= slow.id,
        f"{label}: goaway last_stream {frame.last_stream!r} excludes the in-flight stream {slow.id}",
    )

    refused: BaseException | None = None
    try:
        peer.open({"action": "echo", "value": 99})
    except muxws.ConnectionGoingAway as error:
        refused = error
    check(refused is not None, f"{label}: an open after goaway was not refused locally (WSM-CON-021)")

    await asyncio.wait_for(draining, 10.0)
    check(
        rows == [{"row": index} for index in range(SLOW_ROWS)],
        f"{label}: the stream inside last_stream did not drain to completion: {rows!r}",
    )


async def collect(stream: Stream, into: list[Any]) -> None:
    async for item in stream:
        into.append(item)


# --------------------------------------------------------------------------- WSM-TST-005


async def run_tst_005(url: str, *, label: str) -> None:
    """Kill the acceptor process with streams open, restart it, and hold the peer to WSM-TST-005."""
    hellos: list[str] = []
    reconnects: list[tuple[int, int]] = []
    closes: list[Any] = []

    def on_reconnect(attempt: int, _peer: muxws.Peer) -> None:
        # The second number checks ordering: when `on_reconnect` fires, the replayed hello has been
        # sent and acknowledged (WSM-RCN-023/030), so `hellos` is non-empty.
        reconnects.append((attempt, len(hellos)))

    peer = await muxws.connect(
        url,
        hello=HELLO_PAYLOAD,
        hello_headers=HELLO_HEADERS,
        reconnect=RECONNECT,
        on_close=closes.append,
        on_reconnect=on_reconnect,
    )

    def journal(direction: str, frame: Frame, _length: int) -> None:
        """Capture the encoding of every hello this peer sends after the first connection.

        Stream 1 is the hello: stream ids restart at 1 on every socket, and WSM-RCN-023 forbids any
        frame before it. The first connection's hello is sent inside `connect()`, before a handler
        can be registered, so the acceptor reports the first-connection encoding (see
        `serve_one_connection`).
        """
        if direction == "tx" and frame.type == "open" and frame.stream == 1:
            hellos.append(encoding_of(frame))

    peer.on_frame(journal)

    held = [peer.open({"action": "forever"}), peer.open({"action": "forever"})]
    # The driver kills the acceptor when it reads this, so the streams above must already exist.
    emit(role=label, event="streams-open", streams=[stream.id for stream in held])

    # Awaited with a deadline. Streams left pending after the socket is lost violate WSM-RCN-041;
    # the deadline turns that into a reported failure instead of a hung job.
    try:
        outcomes = await asyncio.wait_for(asyncio.gather(*held, return_exceptions=True), 60.0)
    except asyncio.TimeoutError:
        raise SystemExit(f"interop FAILED: {label}: the streams open at the kill never settled") from None
    for stream, outcome in zip(held, outcomes, strict=True):
        check(
            isinstance(outcome, muxws.ConnectionLost),
            f"{label}: stream {stream.id} was open when the acceptor died and raised {outcome!r}, "
            f"not ConnectionLost (WSM-TST-005)",
        )

    loop = peer._connection_loop
    deadline = asyncio.get_running_loop().time() + 60.0
    while not reconnects and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
    # Reports `delays`, not `attempts`: the attempt counter resets when a connection is established
    # (WSM-RCN-004), so it is zero for a peer that reconnected without calling `on_reconnect`.
    check(bool(reconnects), f"{label}: the dialer never announced a reconnect; it waited {loop.delays!r}")

    check(
        reconnects == [(1, 1)],
        f"{label}: on_reconnect fired {reconnects!r}; WSM-TST-005 wants exactly one, after the "
        f"replayed hello was acknowledged",
    )
    check(
        hellos == [encoding_of(Frame("open", stream=1, payload=HELLO_PAYLOAD, headers=HELLO_HEADERS, end=True))],
        f"{label}: the replayed hello was not the captured one, byte for byte (WSM-RCN-027)",
    )
    check_jitter_dispersed(loop.delays, label=label)

    # A request on the reconnected peer checks that the acceptor accepted the replayed hello.
    echoed = await peer.request({"action": "echo", "value": 7})
    check(echoed == {"echo": 7}, f"{label}: the reconnected peer answered {echoed!r}")
    check(len(closes) >= 1, f"{label}: losing the socket fired no on_close")

    attempts = len(loop.delays)
    # The peer has a heartbeat task and a supervisor task, and only `close()` stops the reconnect
    # helper (WSM-RCN-040). This peer retries indefinitely, so without `close()` it keeps dialing.
    await peer.close(reason="interop reconnect scenario complete")
    emit(role=label, ok=True, attempts=attempts, reconnections=loop.reconnections)


def check_jitter_dispersed(delays: list[float], *, label: str) -> None:
    """WSM-RCN-002: jitter is applied to every delay, including the capped ones.

    Asserted as dispersion, not as values. The check uses the delays whose unjittered value equals
    `max_delay`; without jitter they would all be exactly `max_delay`.
    """
    capped = [
        delay
        for attempt, delay in enumerate(delays)
        if muxws.unjittered_delay(attempt, RECONNECT) == RECONNECT.max_delay
    ]
    check(
        len(capped) >= 2,
        f"{label}: only {len(capped)} capped delays in {delays!r}; the acceptor was not held down "
        f"long enough for the schedule to reach max_delay twice",
    )
    check(
        len(set(capped)) > 1,
        f"{label}: every capped delay was {capped[0]!r}; the cap was applied without jitter (WSM-RCN-002)",
    )
    lowest = RECONNECT.max_delay * (1 - RECONNECT.jitter)
    highest = RECONNECT.max_delay * (1 + RECONNECT.jitter)
    outside = [delay for delay in capped if not lowest <= delay <= highest]
    check(not outside, f"{label}: capped delays {outside!r} fall outside +/-{RECONNECT.jitter} of the cap")


# --------------------------------------------------------------------------- WSM-CDC-007: the corpus
#
# The cross-language part of WSM-CDC-007: "a Python peer and a TypeScript peer, both configured with
# that codec, running the sequence corpus". `muxws/conformance_test.py` and `ts/conformance.spec.ts`
# replay `conformance/sequences/` with both peers in one process; this runs the corpus with one peer
# in each language.
#
# A sequence fixture scripts both peers as one ordered list of steps. The dialer process conducts: it
# reads the script, executes the steps whose `peer` is its own role, and sends every other step over
# a control channel to the acceptor process, which executes it through the same `Side` class and
# replies. The steps run in fixture order, as in the in-process runners, and every fixture can run.
#
# The control channel carries only a step index and the ordinal -> stream id table. Both processes
# load the fixture from disk, so each resolves `$bytes` placeholders (`conformance/README.md`) from
# the same file and no JSON encoding of bytes is needed.

#: Read from disk by both processes; not sent over the control channel.
CONFORMANCE = Path(__file__).resolve().parent.parent / "conformance"
SEQUENCES_DIR = CONFORMANCE / "sequences"

#: The file that declares the expected fixture count. `muxws/conformance_test.py` defines it and
#: `ts/conformance.spec.ts` checks against that file; this driver reads the same literal.
FIXTURE_COUNT_SOURCE = Path(__file__).resolve().parent.parent / "muxws" / "conformance_test.py"

#: Mirrors `test_the_only_fixture_the_json_pass_skips_is_the_binary_codec_one`. Listed explicitly
#: and compared with the corpus's `requires_codec` declarations, so a fixture that gains a
#: declaration fails the run instead of being skipped.
CODEC_SPECIFIC_FIXTURES = {"bytes-payload-under-binary-codec": "msgpack"}

#: Every step kind and every `call` of `conformance/README.md`. A fixture that uses anything else is
#: reported as unsupported by name and fails the run.
STEP_KINDS = (
    "settle",
    "call",
    "inject",
    "expect_frame",
    "expect_no_frame",
    "expect_result",
    "expect_headers",
    "expect_error",
    "expect_closed",
)
IMPLEMENTED_CALLS = frozenset(
    {
        "open",
        "request",
        "notify",
        "send",
        "send_headers",
        "end",
        "reply",
        "cancel",
        "iterate",
        "close",
        "await_close",
    }
)
#: The three calls that append an ordinal, in step order (`conformance/README.md`).
ALLOCATING_CALLS = frozenset({"open", "request", "notify"})

ERROR_CLASSES: dict[str, type[BaseException]] = {
    "ConnectionLost": muxws.ConnectionLost,
    "RemoteError": muxws.RemoteError,
    "StreamRefused": muxws.StreamRefused,
    "StreamReset": muxws.StreamReset,
    "StreamTimeout": muxws.StreamTimeout,
}

#: Timeout for one waiting step, in seconds. Larger than the in-process runners' 2 s because every
#: step crosses a socket and a process boundary. A stalled fixture fails by name.
CORPUS_STEP_TIMEOUT = 5.0

#: One quiescence window, and how many of them a `settle` may take before it gives up. See
#: `settle_across`.
QUIET = 0.03
QUIET_ROUNDS = 60

#: Timeout for one control-channel round trip, in seconds. A step that blocks in the other process
#: fails with an error instead of a hang.
CONTROL_TIMEOUT = 60.0

#: Event-loop turns `stream_for` waits for an inbound stream to be delivered. A preceding `settle`
#: guarantees delivery; this bound makes a fixture without one fail by name instead of on timing.
STREAM_ARRIVAL_TURNS = 400


class StepError(Exception):
    """A step that did not hold. The fixture name and step index are attached where it is caught."""


class Error(Exception):
    """What a fixture's `{"handler": "raise"}` payload makes the receiving handler raise.

    The class name appears on the wire. WSM-ERR-006's default serializer sends `type(exc).__name__`
    in Python and `error.name` in TypeScript, so the `reset(APPLICATION_ERROR)` payload that
    `handler-raises-produces-application-error` expects is the same in both languages only if the
    class names match. `Error` is the JavaScript built-in name; `muxws/conformance_test.py` declares
    the same class.
    """


def substitute_bytes(value: Any) -> Any:
    """Resolve every `{"$bytes": [...]}` placeholder into the byte string those integers spell.

    Same as the function in `muxws/conformance_test.py`, and run in both processes. JSON has no byte
    type; an unresolved placeholder would be sent as an ordinary map, and
    `bytes-payload-under-binary-codec` would not test bytes (WSM-CDC-008).
    """
    if isinstance(value, dict):
        if set(value) == {"$bytes"}:
            return bytes(value["$bytes"])
        return {key: substitute_bytes(item) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute_bytes(item) for item in value]
    return value


def load_fixture(name: str) -> dict[str, Any]:
    return substitute_bytes(json.loads((SEQUENCES_DIR / f"{name}.json").read_text(encoding="utf-8")))


def peer_options(fixture: dict[str, Any]) -> dict[str, Any]:
    """`max_frame_bytes` / `max_concurrent_streams` are instructions to the runner (WSM-TST-002).

    Both peers are constructed with them; neither is encoded into any frame (WSM-CON-031).
    """
    return {key: fixture[key] for key in ("max_frame_bytes", "max_concurrent_streams") if key in fixture}


def unsupported_reason(fixture: dict[str, Any]) -> str | None:
    """The reason this driver cannot replay the fixture, or None."""
    for index, step in enumerate(fixture["steps"]):
        kind = next((key for key in STEP_KINDS if key in step), None)
        if kind is None:
            return f"step {index} has no step kind this driver recognises"
        if kind == "call" and step["call"] not in IMPLEMENTED_CALLS:
            return f"step {index} calls {step['call']!r}, which this driver does not implement"
    return None


class Side:
    """One process's part of a cross-language replay: one peer, one journal, one role in the script.

    Both processes use this class, so a step is executed by the same code on either end of the
    socket. The conductor runs the steps whose `peer` is its own role and sends the rest to the other
    process over the control channel.
    """

    def __init__(self, role: str, peer: muxws.Peer, socket: WebsocketsSocket, codec: Any) -> None:
        self.role = role
        self.peer = peer
        self.socket = socket
        self.codec = codec
        #: Replaced with the conductor's table on every step; this side keeps no ordinal list of its own.
        self.ordinals: list[int] = []
        self.by_id: dict[int, Stream] = {}
        self.refs: dict[str, Stream] = {}
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self.closing: asyncio.Task[None] | None = None
        #: Frames this peer sent, in order. `on_frame` reports outbound wire frames after
        #: fragmentation, so `small-frame-overtakes-a-fragmented-payload` can assert on individual
        #: fragments (WSM-OBS-003, WSM-FRG-019).
        self.sent: list[Frame] = []
        #: Every frame in either direction. Only `settle_across` reads it, as a quiescence signal.
        self.frames = 0
        self.cursor = 0
        self.socket_closed = False
        peer.on_frame(self._journal)
        peer.on_stream(self._handler)

    def _journal(self, direction: str, frame: Frame, _length: int) -> None:
        self.frames += 1
        if direction == "tx":
            self.sent.append(frame)

    async def _handler(self, payload: Any, stream: Stream) -> None:
        """Record the inbound stream, then hold it open.

        WSM-STM-035 ends a stream when its handler returns; returning here would close every inbound
        stream before the script can `reply` on it.
        """
        self.by_id[stream.id] = stream
        if isinstance(payload, dict) and payload.get("handler") == "raise":
            raise Error(str(payload.get("message", "the handler raised")))
        await stream.closed.wait()

    # ------------------------------------------------------------------ the script

    async def execute(self, step: dict[str, Any], ordinals: list[int]) -> int | None:
        """Run one step as this side, and return the stream id it allocated, if any.

        `step["peer"]` is not read: the conductor routes steps (see `owner_of`, which inverts the
        owner for `inject`).
        """
        self.ordinals = list(ordinals)
        if "call" in step:
            return await self.do_call(step)
        if "inject" in step:
            await self.write_raw(step["inject"])
        elif "expect_frame" in step:
            self.expect_frame(step["expect_frame"])
        elif "expect_no_frame" in step:
            self.expect_no_frame(step["expect_no_frame"])
        elif "expect_result" in step:
            await self.expect_result(step["expect_result"])
        elif "expect_headers" in step:
            await self.expect_headers(step["expect_headers"])
        elif "expect_error" in step:
            await self.expect_error(step["expect_error"])
        elif "expect_closed" in step:
            await self.expect_closed(step["expect_closed"])
        else:
            raise StepError(f"no step kind in {step!r}")
        return None

    async def do_call(self, step: dict[str, Any]) -> int | None:
        call = step["call"]
        handler = getattr(self, f"corpus_{call}", None)
        if handler is None:
            raise StepError(f"the interop driver does not implement the call {call!r}")

        expected = step.get("raises")
        if expected is None:
            return await handler(step)
        if expected not in ERROR_CLASSES:
            raise StepError(f"'raises' names an unknown class {expected!r}")
        # `raises` states that the call itself fails, for example a send on a stream the consumer
        # cancelled. A frame matcher cannot express this: a peer that reset the stream and kept
        # producing sends the same frames (WSM-ERR-009).
        try:
            await handler(step)
        except ERROR_CLASSES[expected]:
            return None
        except Exception as exc:
            raise StepError(f"{call} raised {exc!r}, not {expected}") from exc
        raise StepError(f"{call} was expected to raise {expected} and did not")

    async def corpus_open(self, step: dict[str, Any]) -> int:
        stream = self.peer.open(step.get("payload"), headers=step.get("headers"), end=step.get("end", False))
        return self._adopt(step, stream)

    async def corpus_request(self, step: dict[str, Any]) -> int:
        """`peer.request(...)`, started and not awaited (WSM-API-006); later steps assert on the frames
        it produces."""
        timeout = step.get("timeout_ms")
        before = set(self.peer.streams)
        task = asyncio.ensure_future(
            self.peer.request(
                step.get("payload"),
                headers=step.get("headers"),
                # Milliseconds in the corpus, seconds in Python (WSM-CON-012).
                timeout=None if timeout is None else float(timeout) / 1000,
            )
        )
        return self._adopt(step, await self._discover(before), task=task)

    async def corpus_notify(self, step: dict[str, Any]) -> int:
        before = set(self.peer.streams)
        await self.peer.notify(step.get("payload"), headers=step.get("headers"))
        return self._adopt(step, await self._discover(before))

    async def corpus_send(self, step: dict[str, Any]) -> None:
        stream = await self.stream_for(step["stream_ref"])
        await stream.send(step.get("payload"), end=step.get("end", False), headers=step.get("headers"))

    async def corpus_send_headers(self, step: dict[str, Any]) -> None:
        """The answering side's leading headers, on a frame without a payload (WSM-API-024)."""
        await (await self.stream_for(step["stream_ref"])).send_headers(step["headers"])

    async def corpus_end(self, step: dict[str, Any]) -> None:
        stream = await self.stream_for(step["stream_ref"])
        # An absent `payload` key is `ABSENT`, not `null`: they are different frames.
        await stream.end(
            step["payload"] if "payload" in step else ABSENT,
            trailers=step.get("trailers"),
            headers=step.get("headers"),
        )

    async def corpus_reply(self, step: dict[str, Any]) -> None:
        stream = await self.stream_for(step["stream_ref"])
        await stream.reply(step.get("payload"), trailers=step.get("trailers"), headers=step.get("headers"))

    async def corpus_cancel(self, step: dict[str, Any]) -> None:
        await (await self.stream_for(step["stream_ref"])).cancel(step.get("reason"))

    async def corpus_iterate(self, step: dict[str, Any]) -> None:
        stream = await self.stream_for(step["stream_ref"])

        async def collect() -> list[Any]:
            return [item async for item in stream]

        self.tasks[step["as"]] = asyncio.ensure_future(collect())

    async def corpus_close(self, step: dict[str, Any]) -> None:
        drain = float(step.get("drain_ms", 10_000)) / 1000
        code = muxws.ResetCode(step.get("code", int(muxws.ResetCode.NO_ERROR)))
        # Started, not awaited: `close()` sends `goaway` and then drains, and the following steps run
        # during the drain window (WSM-CON-025).
        self.closing = asyncio.create_task(self.peer.close(code, step.get("reason"), drain))

    async def corpus_await_close(self, _step: dict[str, Any]) -> None:
        if self.closing is None:
            raise StepError(f"await_close: {self.role} has no close() in flight")
        await asyncio.wait_for(asyncio.shield(self.closing), CORPUS_STEP_TIMEOUT)

    def _adopt(self, step: dict[str, Any], stream: Stream, *, task: asyncio.Task[Any] | None = None) -> int:
        self.by_id[stream.id] = stream
        if "as" in step:
            if task is not None:
                self.tasks[step["as"]] = task
            else:
                self.refs[step["as"]] = stream
        return stream.id

    async def _discover(self, before: set[int]) -> Stream:
        """Find, through the public stream map, the stream a call opened without returning it.

        Polled for a bounded number of turns: `request()` is a coroutine, so its `open()` runs only
        when the task is first scheduled, whereas TypeScript opens the stream synchronously.
        """
        for _ in range(STREAM_ARRIVAL_TURNS):
            fresh = [stream for stream_id, stream in self.peer.streams.items() if stream_id not in before]
            if fresh:
                return fresh[0]
            await asyncio.sleep(0)
        raise StepError("the call opened no stream")

    async def write_raw(self, envelope: dict[str, Any]) -> None:
        """Put one hand-written envelope on this side's socket, bypassing the writer.

        Executes `inject` from the sending end. In-process, a runner hands the message to the
        receiving peer's socket; across two processes only the receiving peer's remote can send it,
        so the other side executes the step as a send. The envelope does not pass through
        `from_mapping`, which drops unknown keys (WSM-FRM-001); the fixtures inject unknown fields
        and unknown frame types.

        The writer is bypassed because no public API sends an envelope with exactly the given keys.
        Nothing else is in flight in the fixtures that use `inject`, and a WebSocket message is
        atomic, so this does not interleave with a fragment.
        """
        message = self.codec.encode_payload(self.resolve(envelope))
        if self.codec.binary:
            await self.socket.send_bytes(message)
        else:
            await self.socket.send_text(message)

    # ------------------------------------------------------------------ resolution

    def id_of(self, ordinal: int) -> int:
        if not 1 <= ordinal <= len(self.ordinals):
            raise StepError(f"stream_ref {ordinal} names a stream this script has not opened")
        return self.ordinals[ordinal - 1]

    async def stream_for(self, ordinal: int) -> Stream:
        stream_id = self.id_of(int(ordinal))
        for _ in range(STREAM_ARRIVAL_TURNS):
            stream = self.by_id.get(stream_id)
            if stream is not None:
                return stream
            await asyncio.sleep(0)
        raise StepError(f"{self.role} has no stream for ordinal {ordinal} (id {stream_id})")

    def resolve(self, wanted: dict[str, Any]) -> dict[str, Any]:
        """Turn the two ordinal-bearing keys into the ids this run allocated (WSM-TST-002)."""
        resolved: dict[str, Any] = {}
        for key, value in wanted.items():
            if key == "stream_ref":
                resolved["stream"] = self.id_of(int(value))
            elif key == "last_stream_ref":
                resolved["last_stream"] = self.id_of(int(value))
            else:
                resolved[key] = value
        return resolved

    @staticmethod
    def matches(frame: Frame, wanted: dict[str, Any]) -> bool:
        """A **subset** match: the listed keys, and nothing about the rest (WSM-TST-002)."""
        return all(getattr(frame, key, None) == value for key, value in wanted.items())

    # ------------------------------------------------------------------ assertions

    def expect_frame(self, wanted_raw: dict[str, Any]) -> None:
        wanted = self.resolve(wanted_raw)
        for index in range(self.cursor, len(self.sent)):
            if self.matches(self.sent[index], wanted):
                self.cursor = index + 1
                return
        raise StepError(f"{self.role} sent no frame matching {wanted}; it sent {self.sent[self.cursor :]}")

    def expect_no_frame(self, wanted_raw: dict[str, Any]) -> None:
        """Search every frame this peer has sent, not only those after the cursor."""
        wanted = self.resolve(wanted_raw)
        offending = [frame for frame in self.sent if self.matches(frame, wanted)]
        if offending:
            raise StepError(f"{self.role} sent {offending}, and this fixture says it must send nothing like {wanted}")

    async def _value_of(self, ref: str) -> Any:
        task = self.tasks.get(ref)
        if task is not None:
            # Shielded so that a step timeout does not cancel the underlying call and send a reset.
            return await asyncio.wait_for(asyncio.shield(task), CORPUS_STEP_TIMEOUT)
        stream = self.refs.get(ref)
        if stream is None:
            raise StepError(f"no step labelled {ref!r} with 'as' on {self.role}")
        return await asyncio.wait_for(stream.result(), CORPUS_STEP_TIMEOUT)

    async def expect_result(self, spec: dict[str, Any]) -> None:
        try:
            value = await self._value_of(spec["ref"])
        except asyncio.TimeoutError:
            raise StepError(f"{spec['ref']} never produced a result") from None
        if value != spec["value"]:
            raise StepError(f"{spec['ref']} produced {value!r}, expected {spec['value']!r}")

    async def expect_headers(self, spec: dict[str, Any]) -> None:
        """Compare the headers held on the attribute `of` names, once final (WSM-API-025)."""
        stream = await self.stream_for(spec["stream_ref"])
        of = spec["of"]
        if of not in ("open", "reply"):
            raise StepError(f'expect_headers needs "of": "open" or "reply", not {of!r}')
        if of == "reply":
            try:
                await asyncio.wait_for(stream.reply_headers_arrived.wait(), CORPUS_STEP_TIMEOUT)
            except asyncio.TimeoutError:
                raise StepError(f"stream {spec['stream_ref']} never saw reply headers arrive") from None
        held = stream.headers if of == "open" else stream.reply_headers
        if held != spec["value"]:
            raise StepError(f"{of} headers were {held!r}, expected {spec['value']!r}")

    async def expect_error(self, spec: dict[str, Any]) -> None:
        expected = ERROR_CLASSES.get(spec["error"])
        if expected is None:
            raise StepError(f"expect_error names an unknown class {spec['error']!r}")
        try:
            value = await self._value_of(spec["ref"])
        except expected as caught:
            if "code" in spec and int(getattr(caught, "code", -1)) != spec["code"]:
                raise StepError(f"{spec['ref']} failed with code {getattr(caught, 'code', None)}") from None
            if "payload" in spec and getattr(caught, "payload", None) != spec["payload"]:
                raise StepError(f"{spec['ref']} carried {getattr(caught, 'payload', None)!r}") from None
            return
        except asyncio.TimeoutError:
            raise StepError(f"{spec['ref']} neither failed nor answered") from None
        except Exception as other:
            raise StepError(f"{spec['ref']} failed with {other!r}, expected {spec['error']}") from other
        raise StepError(f"{spec['ref']} was expected to fail and produced {value!r}")

    async def expect_closed(self, spec: dict[str, Any]) -> None:
        """Polled for a bounded time: the other end observes a close at least one turn later."""
        want_socket = bool(spec.get("socket", False))
        for _ in range(QUIET_ROUNDS):
            if not self.peer.is_open and (not want_socket or self.socket_closed):
                return
            await asyncio.sleep(QUIET)
        raise StepError(
            f"{self.role} is still open (peer.is_open={self.peer.is_open}, socket_closed={self.socket_closed})"
        )

    # ------------------------------------------------------------------ teardown

    async def teardown(self) -> None:
        """Drop the socket and cancel whatever the script left running.

        The socket is closed without a `goaway`: most fixtures end with streams open, and a graceful
        close would wait out the drain window for each of them.
        """
        pending = [task for task in (*self.tasks.values(), self.closing) if task is not None]
        for task in pending:
            task.cancel()
        for task in pending:
            # Exceptions from cancelled collectors or close() are ignored; the fixture's assertions
            # have already run.
            await asyncio.gather(task, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.socket.close()


async def settle_across(side: Side, control: Control) -> None:
    """Execute `settle` across two processes: wait until neither peer sends or receives a frame.

    The corpus defines `settle: n` as n event-loop turns, "a lower bound on progress, never a
    duration" (`conformance/README.md`), for a transport that delivers within one event loop. Between
    two processes a turn count bounds nothing, so this driver waits until both peers are quiet: each
    side checks its frame counter across one `QUIET` window, and the step returns when neither saw a
    frame. A fixture whose next step needs a reset to have arrived
    (`cancel-mid-stream-stops-the-producer`) waits here until it has.
    """
    for _ in range(QUIET_ROUNDS):
        before = side.frames
        remote = await control.rpc({"cmd": "settle"})
        await asyncio.sleep(QUIET)
        if remote["quiet"] and side.frames == before:
            return
    raise StepError("the two peers never went quiet")


# --------------------------------------------------------------------------- the control channel


class Control:
    """The conductor's end of the line-delimited JSON control channel.

    A separate TCP socket, not the WebSocket under test: control messages on that connection would
    appear among the frames the fixtures assert on, including `expect_no_frame`.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        #: A command sent by `dispatch` whose acknowledgement has not been read. It is collected before
        #: the next command, so replies stay paired with their commands.
        self._pending: asyncio.Task[dict[str, Any]] | None = None

    async def rpc(self, message: dict[str, Any]) -> dict[str, Any]:
        await self._collect()
        self._send(message)
        return await self._answer(message)

    async def dispatch(self, message: dict[str, Any]) -> None:
        """Send a command and go straight on to the next step, collecting the answer later.

        Used only for `close`, which the corpus defines as "started and not awaited - `close()` sends
        `goaway` and then drains, and the steps after it are what the drain window exists to let
        happen" (`conformance/README.md`). `goaway-drains-then-closes` then opens a stream that must
        be sent before its opener receives the goaway (WSM-CON-021/023).

        In one process turn ordering guarantees this. Across two processes, the acceptor writes the
        goaway before the acknowledgement, so waiting for the acknowledgement lets the goaway arrive
        first. Not waiting lets the local `open()` run a full round trip earlier, and the acceptor
        receives the control message one event-loop turn before the open it must exclude from
        `last_stream`.
        """
        await self._collect()
        self._send(message)
        self._pending = asyncio.ensure_future(self._answer(message))

    async def _collect(self) -> None:
        pending, self._pending = self._pending, None
        if pending is not None:
            await pending

    def _send(self, message: dict[str, Any]) -> None:
        # Not drained: the messages are a few hundred bytes on a loopback socket, and `drain()` would
        # add a suspension point between sending a `dispatch` command and running the next step.
        self._writer.write((json.dumps(message) + "\n").encode("utf-8"))

    async def _answer(self, message: dict[str, Any]) -> dict[str, Any]:
        line = await asyncio.wait_for(self._reader.readline(), CONTROL_TIMEOUT)
        if not line:
            raise StepError("the acceptor's control channel closed mid-run")
        reply = json.loads(line)
        if not reply.get("ok"):
            raise StepError(f"the acceptor refused {message['cmd']!r}: {reply.get('error')}")
        return reply

    async def close(self) -> None:
        await self._collect()
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()


class CorpusAcceptor:
    """The acceptor process: one WebSocket server, one control channel, one fixture at a time."""

    def __init__(self, codec: Any) -> None:
        self.codec = codec
        self.url = ""
        self.fixture: dict[str, Any] | None = None
        self.options: dict[str, Any] = {}
        self.side: Side | None = None
        self.connected = asyncio.Event()
        self.done = asyncio.Event()

    async def handle_connection(self, connection: Any) -> None:
        # In addition to the handshake hook: checks that the dialer offered `muxws.v1.<codec>` first
        # (WSM-CDC-020).
        verify_negotiated(getattr(connection, "subprotocol", None), self.codec.name)
        socket = WebsocketsSocket(connection)
        peer = muxws.Peer(socket, codec=self.codec, is_dialer=False, **self.options)
        side = Side("acceptor", peer, socket, self.codec)
        self.side = side
        self.connected.set()
        try:
            with contextlib.suppress(muxws.ConnectionClosed):
                await peer.serve()
        finally:
            side.socket_closed = True

    async def handle_control(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                reply = await self.dispatch(json.loads(line))
            except Exception as exc:  # reported to the conductor, which reports failures
                reply = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            writer.write((json.dumps(reply) + "\n").encode("utf-8"))
            await writer.drain()
        self.done.set()

    async def dispatch(self, command: dict[str, Any]) -> dict[str, Any]:
        name = command["cmd"]
        if name == "begin":
            self.fixture = load_fixture(command["fixture"])
            self.options = peer_options(self.fixture)
            self.side = None
            self.connected.clear()
            return {"ok": True, "url": self.url}
        if name == "ready":
            await asyncio.wait_for(self.connected.wait(), CONTROL_TIMEOUT)
            return {"ok": True}
        if name == "step":
            if self.side is None or self.fixture is None:
                raise StepError("a step arrived before the fixture's connection did")
            step = self.fixture["steps"][command["index"]]
            return {"ok": True, "stream": await self.side.execute(step, command["ordinals"])}
        if name == "settle":
            if self.side is None:
                raise StepError("a settle arrived before the fixture's connection did")
            before = self.side.frames
            await asyncio.sleep(QUIET)
            return {"ok": True, "quiet": self.side.frames == before}
        if name == "end":
            if self.side is not None:
                await self.side.teardown()
            self.side = None
            return {"ok": True}
        raise StepError(f"unknown control command {name!r}")


async def corpus_accept(port: int) -> None:
    """Serve the corpus half: a WebSocket server for the peers, a TCP server for the script."""
    codec = muxws.resolve_codec()
    acceptor = CorpusAcceptor(codec)
    websocket_server = await websockets.serve(
        acceptor.handle_connection, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol
    )
    acceptor.url = f"ws://127.0.0.1:{websocket_server.sockets[0].getsockname()[1]}"
    control = await asyncio.start_server(acceptor.handle_control, "127.0.0.1", port)
    # The reported port is the control port, which `interop/drive.sh` passes to the conductor. The
    # conductor learns the WebSocket URL from the `begin` reply.
    emit(
        role="python-corpus-acceptor",
        port=control.sockets[0].getsockname()[1],
        codec=muxws.settings.codec,
    )
    async with control:
        await acceptor.done.wait()
    websocket_server.close()


# --------------------------------------------------------------------------- the conductor


def pinned_fixture_count() -> int:
    """Read `EXPECTED_SEQUENCE_FIXTURES` from `muxws/conformance_test.py`."""
    source = FIXTURE_COUNT_SOURCE.read_text(encoding="utf-8")
    found = re.search(r"^EXPECTED_SEQUENCE_FIXTURES = (\d+)$", source, re.MULTILINE)
    if found is None:
        raise SystemExit(f"interop FAILED: {FIXTURE_COUNT_SOURCE} declares no EXPECTED_SEQUENCE_FIXTURES")
    return int(found.group(1))


def owner_of(step: dict[str, Any], labels: dict[str, str]) -> str:
    """Which side executes this step.

    `inject` is inverted: the step means "deliver this message to peer X", and across processes only
    X's remote can send a message to X. Running it on X would write the frame to X's own outbound
    socket and the receiving peer would never see it.
    """
    if "inject" in step:
        return "acceptor" if step["peer"] == "dialer" else "dialer"
    for key in ("call", "expect_frame", "expect_no_frame"):
        if key in step:
            return str(step["peer"])
    if "expect_closed" in step:
        return str(step["expect_closed"]["peer"])
    if "expect_headers" in step:
        return str(step["expect_headers"]["peer"])
    for key in ("expect_result", "expect_error"):
        if key in step:
            ref = step[key]["ref"]
            owner = labels.get(ref)
            if owner is None:
                raise StepError(f"{key} names {ref!r}, which no step bound with 'as'")
            return owner
    raise StepError(f"no step kind in {step!r}")


async def run_fixture(control: Control, name: str, codec: Any) -> None:
    """One fixture: a fresh connection, the whole script, then the connection dropped."""
    fixture = load_fixture(name)
    reply = await control.rpc({"cmd": "begin", "fixture": name})

    connection = await websockets.connect(reply["url"], subprotocols=offer(codec.name))  # type: ignore[arg-type]
    verify_negotiated(getattr(connection, "subprotocol", None), codec.name)
    socket = WebsocketsSocket(connection)
    peer = muxws.Peer(socket, codec=codec, is_dialer=True, **peer_options(fixture))
    side = Side("dialer", peer, socket, codec)
    serving = asyncio.create_task(peer.serve())
    await control.rpc({"cmd": "ready"})

    ordinals: list[int] = []
    labels: dict[str, str] = {}
    try:
        for index, step in enumerate(fixture["steps"]):
            try:
                await run_step(control, side, step, index, ordinals, labels)
            except Exception as exc:
                # Every exception, not only `StepError`: a library exception (an iterator that saw a
                # reset, a send on a closed stream) is a fixture failure too, and is reported with
                # the fixture name and step index.
                raise SystemExit(f"interop FAILED: {name} step {index}: {type(exc).__name__}: {exc}") from exc
    finally:
        await side.teardown()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
        with contextlib.suppress(StepError, asyncio.TimeoutError):
            await control.rpc({"cmd": "end"})


async def run_step(
    control: Control,
    side: Side,
    step: dict[str, Any],
    index: int,
    ordinals: list[int],
    labels: dict[str, str],
) -> None:
    if "settle" in step:
        await settle_across(side, control)
        return

    who = owner_of(step, labels)
    if "call" in step and "as" in step:
        # Records which side holds the awaitable a later `expect_result` reads. Labels are separate
        # from ordinals: an ordinal identifies a stream on the wire, a label identifies a result, and
        # only the side that started the call holds it.
        labels[step["as"]] = who

    command = {"cmd": "step", "index": index, "ordinals": ordinals}
    if who == side.role:
        allocated = await side.execute(step, ordinals)
    elif step.get("call") == "close":
        # The corpus declares `close` started and not awaited; see `Control.dispatch`.
        await control.dispatch(command)
        allocated = None
    else:
        allocated = (await control.rpc(command))["stream"]

    if step.get("call") in ALLOCATING_CALLS:
        if allocated is None:
            raise StepError(f"{step['call']} allocated no ordinal")
        # The only place ordinals are appended; the other process receives a copy with each step
        # (WSM-TST-002).
        ordinals.append(int(allocated))


async def corpus_dial(endpoint: str, *, label: str) -> None:
    """Conduct the whole sequence corpus against the other language's acceptor (WSM-CDC-007)."""
    codec = muxws.resolve_codec()
    host, _, port = endpoint.rpartition(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    control = Control(reader, writer)

    names = sorted(path.stem for path in SEQUENCES_DIR.glob("*.json"))
    expected = pinned_fixture_count()
    check(
        len(names) == expected,
        f"{label}: {len(names)} fixtures on disk, {expected} pinned by muxws/conformance_test.py",
    )
    declared = {
        name: json.loads((SEQUENCES_DIR / f"{name}.json").read_text(encoding="utf-8"))["requires_codec"]
        for name in names
        if "requires_codec" in json.loads((SEQUENCES_DIR / f"{name}.json").read_text(encoding="utf-8"))
    }
    check(
        declared == CODEC_SPECIFIC_FIXTURES,
        f"{label}: the corpus declares {declared!r} codec-specific; this driver's skip list is "
        f"{CODEC_SPECIFIC_FIXTURES!r} and a fixture that declares a codec without being listed is skipped "
        f"by every pass that is not configured with that codec (WSM-CDC-007)",
    )

    ran: list[str] = []
    skipped: list[dict[str, str]] = []
    unsupported: list[dict[str, str]] = []
    for name in names:
        fixture = load_fixture(name)
        required = fixture.get("requires_codec")
        if required is not None and required != codec.name:
            skipped.append({"fixture": name, "reason": f"requires the {required} codec"})
            emit(role=label, event="fixture", fixture=name, outcome="skipped", reason=skipped[-1]["reason"])
            continue
        reason = unsupported_reason(fixture)
        if reason is not None:
            unsupported.append({"fixture": name, "reason": reason})
            emit(role=label, event="fixture", fixture=name, outcome="unsupported", reason=reason)
            continue
        await run_fixture(control, name, codec)
        ran.append(name)
        emit(role=label, event="fixture", fixture=name, outcome="ran")

    await control.close()

    # No fixture may be unsupported, at least one must run, and every fixture on disk must have run
    # or been skipped for its codec (WSM-CDC-007).
    check(not unsupported, f"{label}: this driver could not replay {unsupported!r}")
    check(bool(ran), f"{label}: the corpus run exercised no fixture at all")
    check(
        len(ran) + len(skipped) == len(names),
        f"{label}: {len(ran)} ran and {len(skipped)} were skipped, which is not the {len(names)} on disk",
    )
    emit(
        role=label,
        event="corpus",
        codec=codec.name,
        total=len(names),
        ran=len(ran),
        fixtures=ran,
        skipped=skipped,
        unsupported=unsupported,
    )
    emit(role=label, ok=True, ran=len(ran))


# --------------------------------------------------------------------------- entry points


async def dial(url: str) -> None:
    pushes: list[Any] = []
    journal = Journal()

    async def collect_push(payload: Any, _stream: Stream) -> None:
        pushes.append(payload)

    # `max_attempts=0`: the scenario ends with the acceptor's goaway, and the dialer must not reconnect
    # afterwards to the still-running acceptor.
    peer = await muxws.connect(
        url,
        reconnect=muxws.Reconnect(max_attempts=0),
        on_stream=collect_push,
    )
    peer.on_frame(journal.record)
    await run_tst_004(peer, journal, pushes, label="python-dialer")


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(
            "usage: runner.py accept <port> | accept-unix <path> | dial <url> | "
            "reconnect-dial <url> | corpus-accept <port> | corpus-dial <host:port>"
        )
    register_configured_codec()
    mode, argument = sys.argv[1], sys.argv[2]
    if mode == "accept":
        asyncio.run(accept_forever(int(argument)))
    elif mode == "accept-unix":
        asyncio.run(accept_unix_forever(argument))
    elif mode == "dial":
        asyncio.run(dial(argument))
    elif mode == "reconnect-dial":
        asyncio.run(run_tst_005(argument, label="python-dialer"))
    elif mode == "corpus-accept":
        asyncio.run(corpus_accept(int(argument)))
    elif mode == "corpus-dial":
        asyncio.run(corpus_dial(argument, label="python-corpus-dialer"))
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
