"""The Python half of the live cross-language interop matrix (M6 §7, tests 19-22).

Run as an acceptor:          python interop/runner.py accept <port>
Accept on a socket file:     python interop/runner.py accept-unix <path>
Run the WSM-TST-004 script:  python interop/runner.py dial ws://127.0.0.1:<port>
     ... over a socket file: python interop/runner.py dial ws+unix://<path>:/ws
Run the WSM-TST-005 script:  python interop/runner.py reconnect-dial ws://127.0.0.1:<port>
Serve the corpus:            python interop/runner.py corpus-accept <control-port>
Conduct the corpus:          python interop/runner.py corpus-dial 127.0.0.1:<control-port>

The same entry points exist in `interop/runner.ts`, and `interop/drive.sh` pairs them in both
role assignments, so a rule one port implements differently from the other shows up as a named
failure rather than as a hang.

`dial` takes a URL and nothing else, so the Unix pairing needs no dialling mode of its own: the same
script runs over a socket file with `ws+unix:///path/to.sock:/route` in place of the TCP URL. What
the pairing is here to witness is that the two ports split that URL identically - pathname and
search up to the **first** colon is the filesystem path, the rest is the HTTP request target.

`assert` is deliberately absent: this file is not a `*_test.py`, so ruff's S101 applies and, more to
the point, a driver that vanished under `python -O` would be worse than no driver.
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
#: A pause between export rows. Deliberate, not incidental: the interleaving assertion below has to
#: be a fact about the protocol rather than about how fast one handler happened to run. Without it an
#: acceptor could enqueue all five rows before any other handler was scheduled, and the assertion
#: would pass or fail on scheduling luck.
EXPORT_GAP = 0.01
#: The cancelled-mid-flight producer's cadence.
DRIP_GAP = 0.02
#: Rows the `slow` stream produces; it is the stream that must survive the goaway drain.
SLOW_ROWS = 3
SLOW_GAP = 0.05

#: The hello replayed on every connection the reconnect scenario's dialer makes (WSM-RCN-020/027).
#: Nested, so a replay that rebuilt the payload rather than replaying the captured bytes has
#: somewhere to differ.
HELLO_PAYLOAD: Any = {"who": "interop", "caps": ["a", "b"], "nested": {"n": 1}}
HELLO_HEADERS: dict[str, Any] = {"session": "interop-session"}

#: WSM-TST-005 bounds the reconnect job's wall clock: a jittered retry has to be observable inside a
#: CI job without a 30 s wait. The jitter fraction is left at its default, because the point of the
#: scenario is that the delay is *dispersed* - pinning it would be pinning the thing under test.
RECONNECT = muxws.Reconnect(initial_delay=0.05, max_delay=0.5)


def check(condition: bool, what: str) -> None:
    if not condition:
        raise SystemExit(f"interop FAILED: {what}")


def register_configured_codec() -> None:
    """Register whatever `MUXWS_CODEC` selected. The **application's** job, never the library's.

    WSM-CDC-013 forbids the library dynamic-importing or probing for a codec; it says nothing about
    an application choosing which codec module to import, which is exactly what a deployment does.
    An unknown name is a loud startup failure and never a silent JSON fallback (WSM-INV-015).
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
    """The bytes this frame goes out as, as hex.

    Hex rather than the value itself because a binary codec produces bytes and JSON produces text,
    and the byte-identity WSM-RCN-027 asks about is a question about neither one's spelling.
    """
    encoded = muxws.resolve_codec().encode(frame)
    return (encoded if isinstance(encoded, bytes) else encoded.encode("utf-8")).hex()


def emit(**fields: Any) -> None:
    """One JSON object per line on stdout - what `interop/drive.sh` greps."""
    print(json.dumps(fields), flush=True)


# --------------------------------------------------------------------------- the acceptor


def make_handler(peer: muxws.Peer, state: dict[str, Any]) -> Any:
    """Every shape the two scenarios exercise, chosen by the opening payload.

    The handler is built per connection so that it can reach `peer` - a server push is an ordinary
    `open()` from the acceptor - and so that `state` cannot leak from one connection to the next,
    which matters once the reconnect scenario gives this process two of them.
    """

    async def handler(payload: Any, stream: Stream) -> None:
        action = (payload or {}).get("action")

        if action is None:
            # The hello. Nothing marks it on the wire (WSM-RCN-021) and its acknowledgement is this
            # handler returning, which ends the stream implicitly (WSM-RCN-022/WSM-STM-035). An
            # acceptor that insisted on an `action` would reset the one stream the reconnect helper
            # needs accepted, and the reconnect scenario would never get past its first connection.
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
            # Comfortably over MAX_FRAME_BYTES once encoded, so fragmentation is exercised end to end
            # while other streams are in flight - which is WSM-FRG-019's round robin.
            await stream.reply({"blob": "š" * 40_000})

        elif action == "goaway":
            # Scheduled, never awaited here: `peer.close()` drains the streams still in flight and
            # this handler's own stream is one of them, so awaiting it would be the connection
            # waiting for itself. The task is parked in `state` because a bare `create_task` result
            # nobody holds may be collected mid-drain.
            state["shutdown"] = asyncio.create_task(peer.close(reason="interop goaway"))

        else:
            # Raised, not exited: an unknown action is one stream's problem, and killing the acceptor
            # process here would report it to the dialer as a socket death rather than as the
            # mismatch it is (WSM-ERR-006).
            raise ValueError(f"unknown action {action!r}")

    return handler


async def drip(stream: Stream, state: dict[str, Any]) -> None:
    """Produce until the consumer stops us - the remote half of the cancel-mid-flight assertion.

    Both exits are recorded, because WSM-ERR-013 makes an inbound `reset(CANCELLED)` cancel the
    handler task, while a reset that lands between two sends surfaces as `StreamClosed` from the
    send itself. A scenario asserting only one of them would pass or fail on which microsecond the
    reset arrived in.
    """
    try:
        while True:
            await stream.send({"row": state["drip_sent"]})
            state["drip_sent"] += 1
            await asyncio.sleep(DRIP_GAP)
    except asyncio.CancelledError:
        state["drip_stopped"] = True
        # Re-raised: swallowing a cancellation leaves a task the event loop believes it cancelled.
        raise
    except (muxws.StreamClosed, muxws.StreamReset):
        state["drip_stopped"] = True


async def serve_one_connection(connection: Any) -> None:
    """One accepted connection, whatever carried it - a TCP socket or a socket file.

    Module level rather than nested inside `accept_forever`: `accept_unix_forever` below hands the
    same coroutine to `unix_serve`, so the two acceptors differ in the line that binds and in nothing
    else. A UDS acceptor with its own copy of this handler could pass while the transport-agnostic
    path was broken (WSM-API-021).
    """
    # The HTTP request target this connection arrived on: the half of the `ws+unix://<path>:/route`
    # grammar that reaching the socket does not prove. Neither acceptor routes on the target, so a
    # dialer that sent `/` instead, or that dropped the query string, would connect and pass every
    # assertion in the script. The driver compares this line against the route it put in the URL.
    request = connection.request
    emit(event="accepted", target=None if request is None else request.path)
    peer = await muxws.accept(WebsocketsSocket(connection))
    state: dict[str, Any] = {"drip_sent": 0, "drip_stopped": False, "shutdown": None}
    seen_open = False

    def journal(direction: str, frame: Frame, _length: int) -> None:
        """Report the first inbound `open` of this connection, re-encoded.

        This is where WSM-TST-005's "byte-identical hello" is checked from the *other* language:
        the driver compares this line across the two acceptor processes, so the comparison is
        made by the port that had to accept the replay rather than by the one that sent it.
        Being the *first* open is itself WSM-RCN-023 - nothing may precede the hello.
        """
        nonlocal seen_open
        if direction == "rx" and frame.type == "open" and not seen_open:
            seen_open = True
            emit(event="first-open", encoding=encoding_of(frame))

    peer.on_frame(journal)
    peer.on_stream(make_handler(peer, state))
    # A dialer that walks away leaves the read loop reporting the close; that is this
    # connection ending, not this process failing.
    with contextlib.suppress(muxws.ConnectionClosed):
        await peer.serve()


async def accept_forever(port: int) -> None:
    """Serve until the driver kills us. It does, and in the reconnect scenario it means it."""
    async with websockets.serve(
        serve_one_connection, "127.0.0.1", port, select_subprotocol=muxws.select_subprotocol
    ) as service:
        emit(role="python-acceptor", port=service.sockets[0].getsockname()[1], codec=muxws.settings.codec)
        await asyncio.Future()


async def accept_unix_forever(path: str) -> None:
    """The same acceptor on a socket **file**, for the Unix half of WSM-TST-004.

    `select_subprotocol` is the same hook the TCP acceptor passes, so a dialer offering a codec this
    process does not speak still meets HTTP 400 here and still has to turn it into `CodecMismatch`
    (WSM-CDC-022/024): the only thing that changed is which kernel object the handshake travelled
    over, and the driver proves it by running the unmodified WSM-TST-004 script across it.

    The driver's readiness signal is the `path=` line below rather than a `port=` one - a socket file
    exists between `bind` and `listen`, so a driver that waited for the file would race the listen.
    """
    # Checked here rather than left to the bind: on Windows the bind failure is an event loop
    # reporting that it has no `create_unix_server`, which reads as a defect in this file.
    if not hasattr(socket, "AF_UNIX"):
        raise SystemExit("this platform has no AF_UNIX, so the unix scenario cannot run here")
    # Imported inside the guard for the same reason: on a platform this cannot run on, nothing about
    # the module should be touched before the line that explains why.
    from websockets.asyncio.server import unix_serve

    async with unix_serve(serve_one_connection, path, select_subprotocol=muxws.select_subprotocol):
        emit(role="python-acceptor", path=path, codec=muxws.settings.codec)
        await asyncio.Future()


# --------------------------------------------------------------------------- WSM-TST-004


class Journal:
    """Every frame the peer saw, in order, as `(direction, type, stream)`.

    The wire order is the only place "interleaved" is a fact rather than a hope: two calls that
    overlapped in Python could still have been serialised on the socket.
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

    # Six streams open at once. `gather` is what makes them concurrent rather than sequential, and
    # the export and the fragmented `big` reply are what make the concurrency visible on the wire.
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
    # Compared as a set: three pushed streams are three streams, and nothing in v1 orders the
    # delivery of one stream against another (WSM-STM-001). Asserting the arrival order would be
    # asserting an implementation detail of whichever port happened to be the acceptor.
    check(
        sorted(push["index"] for push in pushes) == list(range(PUSH_COUNT))
        and {push["event"] for push in pushes} == {"tick"},
        f"{label}: the server pushed {pushes!r}",
    )

    # Interleaving on the wire, and what that is worth: between the export's first and last
    # **inbound** frame there must be an inbound frame belonging to some other stream. Only inbound
    # frames count - the six opens leave together whatever the acceptor does with them, so counting
    # those would pass against a port that answered each stream to completion before starting the
    # next.
    #
    # What this does NOT catch, said plainly because the comment used to claim otherwise: removing
    # the round-robin writer entirely leaves this assertion passing. It is satisfied by the export
    # handler's own pauses, not by the writer's rotation. WSM-FRG-019's real witnesses are
    # `writer_test.py::test_round_robin_selects_across_streams_not_fifo` and the
    # `small-frame-overtakes-a-fragmented-payload` fixture, both of which do fail; this one proves
    # the weaker and still-useful thing that the two ports do not serialise streams end to end. The
    # sharp version - asserting the fragmented reply's own fragments are non-contiguous inbound -
    # depends on another lane having work at that instant, and a flaky cross-language assertion
    # would be worse than an honest weak one.
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
    # `message` is portable; `type` is NOT. WSM-ERR-006's default serializer reports the remote's own
    # exception class name, so a TypeScript acceptor says "Error" where a Python one says
    # "ValueError". Asserting equality on it would be asserting which language answered.
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

    # Polled rather than slept-then-asked once: the deadline is what gives the assertion its teeth,
    # and a fixed sleep is either a flake or a tax. A producer that never saw the reset never reports
    # `stopped` and this fails at the deadline.
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
    # The frame, not a sleep: everything below is a statement about what happens *after* the goaway
    # arrived, and timing it by sleeping would make the whole phase a race.
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
        # The second number is the ordering assertion: at the instant `on_reconnect` fires the
        # replayed hello must already have gone out and been acknowledged (WSM-RCN-023/030). A peer
        # that announced the identity first would record a zero here.
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

        Stream 1 is the hello and only the hello: the id space restarts at 1 on every socket, and
        WSM-RCN-023 forbids anything preceding it. The first connection's hello went out inside
        `connect()`, before any application code could register a handler - which is why the
        first-connection half of the byte comparison is made by the acceptor instead (see
        `accept_forever`). See GAPS.md.
        """
        if direction == "tx" and frame.type == "open" and frame.stream == 1:
            hellos.append(encoding_of(frame))

    peer.on_frame(journal)

    held = [peer.open({"action": "forever"}), peer.open({"action": "forever"})]
    # The driver kills the acceptor when it reads this, so the streams above must already exist.
    emit(role=label, event="streams-open", streams=[stream.id for stream in held])

    # Deadlined, not simply awaited. A peer that lost its socket and left these streams pending -
    # neither failed nor answered - is a defect WSM-RCN-041 exists to prevent, and without the
    # deadline it would surface as a job that hung rather than as a driver that said so.
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
    # `delays`, not `attempts`: the attempt counter resets the moment a connection is established
    # (WSM-RCN-004), so a peer that reconnected but never announced it would report zero here and
    # read as a peer that never tried.
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

    # The connection is usable again, which is the only proof that the acceptor accepted the replay
    # for more than the length of the handshake.
    echoed = await peer.request({"action": "echo", "value": 7})
    check(echoed == {"echo": 7}, f"{label}: the reconnected peer answered {echoed!r}")
    check(len(closes) >= 1, f"{label}: losing the socket fired no on_close")

    attempts = len(loop.delays)
    # Closed, not abandoned. A peer under test holds a heartbeat task and a supervisor task, and
    # `close()` is the only thing that stops the reconnect helper (WSM-RCN-040) - this peer is
    # configured to retry forever, so without it the process would go on dialing after the scenario.
    await peer.close(reason="interop reconnect scenario complete")
    emit(role=label, ok=True, attempts=attempts, reconnections=loop.reconnections)


def check_jitter_dispersed(delays: list[float], *, label: str) -> None:
    """WSM-RCN-002: jitter is applied to every delay, including the capped ones.

    Asserted as dispersion and never as a value. Pinning a jittered delay to a constant is an
    assertion that flakes by construction, so this looks at the delays whose *unjittered* schedule
    has already reached `max_delay`: those would all be exactly `max_delay` if jitter were missing,
    and that is the failure this catches.
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
# The cross-language half of WSM-CDC-007: "a Python peer and a TypeScript peer, both configured with
# that codec, running the sequence corpus". `muxws/conformance_test.py` and `ts/conformance.spec.ts`
# already replay `conformance/sequences/` with a real peer at each end - but both peers are in one
# process, so what they prove is that each port agrees with *itself*. This is the other half.
#
# The obstacle is that a sequence fixture scripts **both** peers, and here the two peers are two
# processes in two languages. The fixture's steps are one ordered script, so the two halves cannot
# simply be run side by side and hoped to line up. So one process conducts: the dialer reads the
# script, executes the steps whose `peer` is its own role, and ships every other step over a control
# channel to the acceptor process, which executes it through the same `Side` class and answers. The
# script therefore stays one totally ordered sequence, exactly as in the in-process runners, and the
# whole corpus runs rather than the subset one side happens to be able to drive alone.
#
# Two things move over the control channel and nothing else: a step **index**, and the ordinal ->
# stream id table. The fixture itself is loaded from disk by both processes, so a `$bytes`
# placeholder (`conformance/README.md`) is resolved twice from one file rather than being shipped as
# JSON - which would need a second spelling for bytes, and a second spelling is the divergence the
# corpus exists to prevent.

#: Read from disk by both processes. Not sent over the control channel; see above.
CONFORMANCE = Path(__file__).resolve().parent.parent / "conformance"
SEQUENCES_DIR = CONFORMANCE / "sequences"

#: The expected fixture count is **not** declared here. `muxws/conformance_test.py` pins it and
#: `ts/conformance.spec.ts` is pinned against that file, so this driver reads the literal rather than
#: adding a third number that could drift on its own and make a shrinking corpus look intentional.
FIXTURE_COUNT_SOURCE = Path(__file__).resolve().parent.parent / "muxws" / "conformance_test.py"

#: Mirrors `test_the_only_fixture_the_json_pass_skips_is_the_binary_codec_one`. Pinned rather than
#: derived from the corpus: "skip whatever declares a codec I am not configured with" is a rule that
#: empties itself as fixtures acquire declarations, and a corpus run that skipped everything would
#: report the same green line as one that ran everything.
CODEC_SPECIFIC_FIXTURES = {"bytes-payload-under-binary-codec": "msgpack"}

#: Every step kind and every `call` of `conformance/README.md`. A fixture reaching for something
#: outside these is reported as **unsupported by name** and fails the run, because the alternative -
#: skipping it - is the hollow coverage this whole exercise exists to prevent.
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

#: Wall-clock ceiling on one waiting step. Larger than the in-process runners' 2 s because every step
#: here crosses a socket and a process boundary; still a ceiling, so a stalled fixture fails by name
#: rather than hanging the job until its timeout.
CORPUS_STEP_TIMEOUT = 5.0

#: One quiescence window, and how many of them a `settle` may take before it gives up. See
#: `settle_across`.
QUIET = 0.03
QUIET_ROUNDS = 60

#: Ceiling on one control-channel round trip. A step the other process is stuck inside must surface
#: as this driver saying so, not as two processes waiting for each other.
CONTROL_TIMEOUT = 60.0

#: How long `stream_for` waits for an *inbound* stream to have been delivered. Zero in the in-process
#: runners, where a `settle` guarantees delivery; across a real socket the same `settle` guarantees it
#: too, and this exists only so that a fixture whose author forgot one fails by name after a bounded
#: wait instead of on a race.
STREAM_ARRIVAL_TURNS = 400


class StepError(Exception):
    """A step that did not hold. The fixture name and step index are attached where it is caught."""


class Error(Exception):
    """What a fixture's `{"handler": "raise"}` payload makes the receiving handler raise.

    The class name is load-bearing, which is why this is not a `RuntimeError`. WSM-ERR-006's default
    serializer sends `type(exc).__name__` here and `error.name` in TypeScript, so the
    `reset(APPLICATION_ERROR)` payload `handler-raises-produces-application-error` pins is one value
    in both languages only if the class is spelled the same in both. `Error` is the one name that
    already exists in JavaScript, and `muxws/conformance_test.py` declares its own for the same
    reason.
    """


def substitute_bytes(value: Any) -> Any:
    """Resolve every `{"$bytes": [...]}` placeholder into the byte string those integers spell.

    The same function as the one in `muxws/conformance_test.py`, and it has to run in **both**
    processes: JSON has no byte type, so a placeholder that survived loading would be sent as an
    ordinary map that msgpack carries happily, and `bytes-payload-under-binary-codec` would pass
    while asserting nothing about bytes (WSM-CDC-008).
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

    Both peers are constructed with them and neither may be encoded into any frame (WSM-CON-031),
    which is why they are read here and never looked at again.
    """
    return {key: fixture[key] for key in ("max_frame_bytes", "max_concurrent_streams") if key in fixture}


def unsupported_reason(fixture: dict[str, Any]) -> str | None:
    """Why this driver cannot replay the fixture, or None. Declared, never silently skipped."""
    for index, step in enumerate(fixture["steps"]):
        kind = next((key for key in STEP_KINDS if key in step), None)
        if kind is None:
            return f"step {index} has no step kind this driver recognises"
        if kind == "call" and step["call"] not in IMPLEMENTED_CALLS:
            return f"step {index} calls {step['call']!r}, which this driver does not implement"
    return None


class Side:
    """One process's half of a cross-language replay: one peer, one journal, one role in the script.

    Both processes run this class, and that is the point: a step means the same thing whichever end
    of the socket executes it, which is the property `conformance/README.md` exists to protect. The
    conductor runs the steps whose `peer` is its own role and ships the rest here over the control
    channel.
    """

    def __init__(self, role: str, peer: muxws.Peer, socket: WebsocketsSocket, codec: Any) -> None:
        self.role = role
        self.peer = peer
        self.socket = socket
        self.codec = codec
        #: Set from the conductor's table on every step; this side never keeps its own count, because
        #: two independently maintained ordinal lists is precisely the divergence to avoid.
        self.ordinals: list[int] = []
        self.by_id: dict[int, Stream] = {}
        self.refs: dict[str, Stream] = {}
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self.closing: asyncio.Task[None] | None = None
        #: This peer's own wire, in order. Taken from `on_frame`, which reports **wire** frames on the
        #: way out - after fragmentation - so `small-frame-overtakes-a-fragmented-payload` can assert
        #: on individual fragments (WSM-OBS-003, WSM-FRG-019).
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

        Holding matters: WSM-STM-035 ends a stream the moment its handler returns, so a handler that
        returned here would close every inbound stream before the script could `reply` on it.
        """
        self.by_id[stream.id] = stream
        if isinstance(payload, dict) and payload.get("handler") == "raise":
            raise Error(str(payload.get("message", "the handler raised")))
        await stream.closed.wait()

    # ------------------------------------------------------------------ the script

    async def execute(self, step: dict[str, Any], ordinals: list[int]) -> int | None:
        """Run one step **as this side**, and hand back the stream id it allocated, if any.

        Routing has already happened by the time a step arrives here, so `step["peer"]` is not read:
        the conductor decides who runs what, and for `inject` that decision is inverted (see
        `owner_of`).
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
        # `raises` says the *call itself* fails - a producer whose stream the consumer cancelled, say.
        # No frame matcher can express that: a peer that reset the stream and went on producing looks
        # identical on the wire (WSM-ERR-009).
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
        """`peer.request(...)`, started and not awaited (WSM-API-006) - the steps after it are the
        assertions about the frames it produced."""
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
        """The answering side's leading headers, on a frame with no payload at all (WSM-API-024)."""
        await (await self.stream_for(step["stream_ref"])).send_headers(step["headers"])

    async def corpus_end(self, step: dict[str, Any]) -> None:
        stream = await self.stream_for(step["stream_ref"])
        # An absent `payload` key is `ABSENT`, not `null`: they are different frames (D1).
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
        # Started rather than awaited: `close()` sends `goaway` and *then* drains, and the steps after
        # this one are what the drain window exists to let happen (WSM-CON-025).
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
        """The stream a call opened without handing it back, found through the public map.

        A bounded poll and not a single read: `request()` is a coroutine, so the `open()` inside it
        does not run until the task is first scheduled, while TypeScript reaches it synchronously -
        exactly the kind of difference a shared corpus must not be able to see.
        """
        for _ in range(STREAM_ARRIVAL_TURNS):
            fresh = [stream for stream_id, stream in self.peer.streams.items() if stream_id not in before]
            if fresh:
                return fresh[0]
            await asyncio.sleep(0)
        raise StepError("the call opened no stream")

    async def write_raw(self, envelope: dict[str, Any]) -> None:
        """Put one hand-written envelope on this side's socket, bypassing the writer.

        This is `inject` seen from the other end. In-process a runner hands the message straight to
        the receiving peer's socket; across two processes the only thing that can deliver a message
        *to* a peer is that peer's remote, so the step is executed here, by the other side, as an
        ordinary send. The envelope deliberately does not pass through `from_mapping`, which drops
        unknown keys (WSM-FRM-001) - a frame built through it could never carry the unknown field or
        the unknown type whose toleration is the thing being asserted.

        The writer is bypassed rather than used because there is no public API for "send exactly
        these keys", and there should not be. Nothing else is in flight for the two fixtures that
        need it, and a WebSocket message is atomic, so this cannot interleave with a fragment.
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
        """Not "not yet", but "not at all": the whole of this peer's wire is searched."""
        wanted = self.resolve(wanted_raw)
        offending = [frame for frame in self.sent if self.matches(frame, wanted)]
        if offending:
            raise StepError(f"{self.role} sent {offending}, and this fixture says it must send nothing like {wanted}")

    async def _value_of(self, ref: str) -> Any:
        task = self.tasks.get(ref)
        if task is not None:
            # Shielded: a step that times out must not cancel the call underneath it, or the failure
            # reported would be a reset this fixture never asked for.
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
        """What this peer holds on the attribute `of` names, once it can no longer change (WSM-API-025)."""
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
        """A bounded poll, not a single read: the other end learns of a close one turn later."""
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

        The socket is dropped rather than closed with a `goaway`: most fixtures deliberately end with
        streams still open, and a polite close would sit in its drain window for every one of them.
        """
        pending = [task for task in (*self.tasks.values(), self.closing) if task is not None]
        for task in pending:
            task.cancel()
        for task in pending:
            # Whatever a cancelled collector or close() raises on the way out is teardown noise: the
            # fixture's assertions have already run.
            await asyncio.gather(task, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.socket.close()


async def settle_across(side: Side, control: Control) -> None:
    """`settle: n` means n turns of one event loop; across two processes it means nothing.

    The corpus defines `settle` as "a lower bound on progress, never a duration"
    (`conformance/README.md`), and it is written for a transport where a message is delivered inside
    the same event loop that sent it. Over a real socket between two processes the count is not a
    lower bound on anything, so this driver reads a `settle` as what the fixtures actually want it to
    mean: **let both peers go quiet**. Both sides watch their own frame counter across one window,
    and the step returns only when neither saw a frame during it.

    That is strictly stronger than the in-process reading and cannot pass earlier than it: a fixture
    whose next step needs a reset to have arrived - `cancel-mid-stream-stops-the-producer` is the one
    that does - waits here until it has, instead of failing on a race the number 24 was never a
    promise about.
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

    A plain TCP socket beside the WebSocket under test, and deliberately not the WebSocket itself: a
    control message carried on the connection being asserted about would appear on the very wire the
    fixtures read frames off, and every `expect_no_frame` in the corpus would be asserting about this
    driver's own traffic.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        #: A command sent by `dispatch` whose acknowledgement nobody has read yet. Collected before
        #: the next command, so replies stay paired with the commands that asked for them.
        self._pending: asyncio.Task[dict[str, Any]] | None = None

    async def rpc(self, message: dict[str, Any]) -> dict[str, Any]:
        await self._collect()
        self._send(message)
        return await self._answer(message)

    async def dispatch(self, message: dict[str, Any]) -> None:
        """Send a command and go straight on to the next step, collecting the answer later.

        Exactly one step needs this, and the corpus asks for it in so many words: `close` is
        "**started and not awaited** - `close()` sends `goaway` and *then* drains, and the steps
        after it are what the drain window exists to let happen" (`conformance/README.md`).
        `goaway-drains-then-closes` then opens a stream "a moment too late", and the whole fixture
        turns on that stream leaving **before** its opener has heard the goaway (WSM-CON-021/023).

        In one process that is guaranteed by turn ordering. Across two it is a race, and waiting for
        the acknowledgement is the way to lose it: the goaway is written before the ack, so both are
        in flight towards the dialer and which arrives first is a coin toss - which is exactly how
        this fixture failed here first. Sending and moving on gives the local `open()` a head start
        of a full round trip, and gives the acceptor the control message one event-loop turn before
        the open it must exclude from `last_stream`.
        """
        await self._collect()
        self._send(message)
        self._pending = asyncio.ensure_future(self._answer(message))

    async def _collect(self) -> None:
        pending, self._pending = self._pending, None
        if pending is not None:
            await pending

    def _send(self, message: dict[str, Any]) -> None:
        # Written and not drained: these are a few hundred bytes on a loopback socket, and `drain()`
        # is a suspension point between "the command is out" and "the next step runs" that the one
        # command using `dispatch` is specifically trying not to have.
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
        # Belt to the handshake hook's braces, and a real cross-language assertion: the dialer must
        # have offered `muxws.v1.<codec>` first (WSM-CDC-020) and this is the acceptor saying so.
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
            except Exception as exc:  # the conductor is the only thing that can report a failure
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
    # The port `interop/drive.sh` greps and hands the conductor is the **control** port: the
    # WebSocket port is an implementation detail the conductor learns from `begin`, so a fixture that
    # one day needs a server of its own can have one without a change to the driver.
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
    """The count `muxws/conformance_test.py` already pins, read rather than restated.

    A fourth copy of "13" is a fourth thing that can drift, and the failure it would hide is silent:
    a corpus that stopped being collected reports the same green line as one that was.
    """
    source = FIXTURE_COUNT_SOURCE.read_text(encoding="utf-8")
    found = re.search(r"^EXPECTED_SEQUENCE_FIXTURES = (\d+)$", source, re.MULTILINE)
    if found is None:
        raise SystemExit(f"interop FAILED: {FIXTURE_COUNT_SOURCE} declares no EXPECTED_SEQUENCE_FIXTURES")
    return int(found.group(1))


def owner_of(step: dict[str, Any], labels: dict[str, str]) -> str:
    """Which side executes this step.

    `inject` is **inverted** and that is the one thing in here worth reading twice: the step says
    "deliver this message *to* peer X", and in a cross-process run the only thing that can deliver a
    message to X is X's remote. A driver that read the key as "X runs this" would have each peer send
    itself the unknown frame, and both extension-point fixtures would pass without a byte crossing
    the socket.
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
                # Every exception and not only `StepError`: a step that failed by raising out of the
                # library - an iterator that saw a reset, a send onto a dead handle - is a fixture
                # failure too, and reporting it as a bare traceback loses the one thing a
                # cross-language failure needs, which is the fixture and step that produced it.
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
        # Which side is holding the awaitable a later `expect_result` will ask for. Labels are
        # separate from ordinals: an ordinal identifies a stream on the wire, a label identifies a
        # result the script wants to await, and only the side that started the call has one.
        labels[step["as"]] = who

    command = {"cmd": "step", "index": index, "ordinals": ordinals}
    if who == side.role:
        allocated = await side.execute(step, ordinals)
    elif step.get("call") == "close":
        # The one call the corpus itself declares started-and-not-awaited; see `Control.dispatch`.
        await control.dispatch(command)
        allocated = None
    else:
        allocated = (await control.rpc(command))["stream"]

    if step.get("call") in ALLOCATING_CALLS:
        if allocated is None:
            raise StepError(f"{step['call']} allocated no ordinal")
        # Appended here and nowhere else, so there is one ordinal table and the other process is
        # handed a copy of it rather than keeping a second one (WSM-TST-002).
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
        f"{CODEC_SPECIFIC_FIXTURES!r} and a fixture that quietly acquired a declaration would shrink "
        f"every pass that is not configured with it (WSM-CDC-007)",
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

    # The three numbers, and every one of them has teeth. `unsupported` is empty on purpose: a
    # fixture this driver cannot replay across two processes has to be recorded here deliberately,
    # because the alternative - a driver that quietly runs three fixtures and reports success - is
    # exactly the hollow coverage WSM-CDC-007 exists to stop.
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

    # `max_attempts=0`: this scenario ends by having the *acceptor* go away, and a dialer that
    # re-dialled afterwards would keep reconnecting to a process the driver has not killed yet.
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
