"""The shape of a `demo.bench` run, asserted where a figure cannot be.

`run_matrix` measures a machine, so every number it produces is a property of that machine and of
whatever else is running on it. A shared CI runner reports a fraction of an idle laptop's throughput
and is not broken, so nothing here compares a rate, a ratio or a duration: the assertions are that
every mode produced a `Result`, that each one carries the cell it was asked for, and that
`format_report` turns them into text. That still covers the harness - a subprocess that no longer
starts, a dialer whose JSON no longer parses, a cell that silently yields no result - and every one
of those fails below without a number being compared to anything.

The budget is a fraction of `DEFAULT_SECONDS`: long enough that a cell runs, short enough that the
file costs seconds.
"""

from __future__ import annotations

import asyncio
import socket

from types import SimpleNamespace

import pytest

from demo.bench import format_report, MODES, MUXWS_SEND_WINDOW_BYTES, Result, RoundTrip, run_matrix
from demo.bench.dialer import SendWindow

#: The measured wall clock per cell here. The report's own default is `DEFAULT_SECONDS`.
SECONDS = 0.05

#: One payload, the smallest of the report's three.
PAYLOAD = 128


#: Whatever `on_progress` was told during the module's one run, in order.
PROGRESS: list[str] = []


@pytest.fixture(scope="module")
def smoke() -> tuple[list[Result], list[RoundTrip], list[str]]:
    """One tcp/json/128 B cell in each of the three modes.

    Module-scoped because it starts six subprocesses and every test below reads the same run.
    """
    return run_matrix(
        seconds=SECONDS,
        transports=("tcp",),
        codecs=("json",),
        payloads=(PAYLOAD,),
        streams=(1,),
        on_progress=PROGRESS.append,
    )


def test_every_mode_produces_a_result_for_the_cell_it_was_asked_for(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """Three modes in, three results out, each labelled with the transport, codec and stream count."""
    results, _round_trips, _notes = smoke

    by_mode = {result.mode: result for result in results}
    assert sorted(by_mode) == sorted(MODES), [result.mode for result in results]
    for mode, result in by_mode.items():
        assert (result.transport, result.codec, result.streams) == ("tcp", "json", 1), mode


def test_each_mode_moved_messages_and_reported_a_rate(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """Every result carries a positive message count and rates derived from it.

    A mode that never connected, or one whose row was assembled without the dialer's answer, arrives
    here as a zero and is otherwise indistinguishable from a slow machine.
    """
    results, _round_trips, _notes = smoke

    for result in results:
        assert result.messages > 0, result.mode
        assert result.messages_per_second > 0, result.mode
        assert result.megabytes_per_second > 0, result.mode


def test_the_payload_length_reported_is_the_one_all_three_modes_carried(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """`payload_bytes` is the encoded length actually sent, and it is the same length in every mode.

    The table decomposes the cost of the envelope, which it can only do if the three rows carry the
    same application payload. A mode that counted its own framing as payload would leave the MB/s
    columns incomparable while still reading as a measurement.
    """
    results, _round_trips, _notes = smoke

    lengths = {result.mode: result.payload_bytes for result in results}
    assert len(set(lengths.values())) == 1, lengths
    # Encoding cannot lose bytes, so the requested size is the floor and not the exact figure.
    for mode, length in lengths.items():
        assert length >= PAYLOAD, mode


def test_on_progress_is_told_about_every_cell_before_it_is_measured(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """The hook reports each cell and then the round trip, which is the whole of what the run does.

    A caller's only view of a run that takes minutes and prints nothing until it ends. What it says
    is not asserted beyond the mode names: it is a line for a human, not a record.
    """
    results, _round_trips, _notes = smoke

    assert len(PROGRESS) == len(results) + 1, PROGRESS
    for mode in MODES:
        assert any(line.startswith(mode) for line in PROGRESS), mode


def test_the_report_renders_the_results_as_text_naming_every_mode(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """`format_report` returns the whole report as a string and names each mode in it.

    The names are the only thing asserted: a row's numbers are the machine's, and a report checked
    against them would fail on a loaded runner.
    """
    results, round_trips, notes = smoke

    report = format_report(results, round_trips, notes)

    assert isinstance(report, str)
    for mode in MODES:
        assert mode in report, mode


class Hooks:
    """The whole of what a `SendWindow` uses a peer for: two hooks it registers at construction."""

    def __init__(self) -> None:
        self.frame = lambda *_arguments: None
        self.close = lambda *_arguments: None

    def on_frame(self, handler):
        self.frame = handler

    def on_close(self, handler):
        self.close = handler


def written(kind: str) -> SimpleNamespace:
    """One outgoing frame of `kind`, complete rather than a fragment."""
    return SimpleNamespace(type=kind, more=False)


async def test_the_send_window_raises_rather_than_waiting_on_a_connection_that_is_gone():
    """A muxws cell whose acceptor died ends as an error instead of spinning until something kills it.

    `Stream.send()` enqueues and returns, so a lost connection reaches this loop as frames that stop
    being written rather than as an exception - and the peer redials nothing, so nothing ever writes
    them. The wait needs an exit that is not the arrival of the frame it is waiting for.
    """
    peer = Hooks()
    window = SendWindow(peer, MUXWS_SEND_WINDOW_BYTES)

    await window.wait_for_room()
    waiting = asyncio.ensure_future(window.wait_for_room())
    await asyncio.sleep(0)
    peer.close("the socket died")

    with pytest.raises(ConnectionError):
        await waiting


async def test_the_send_window_counts_payloads_and_not_the_frames_that_carry_no_payload():
    """Only `data` frames free room, so the window is the same size whatever else the peer sent.

    An `open` frame carries a payload the window never counted as sent, and a ping carries none at
    all. Counting either as written would widen the window by one payload per stream, so a cell at
    twenty streams would run with a different allowance from the same cell at one.
    """
    peer = Hooks()
    window = SendWindow(peer, MUXWS_SEND_WINDOW_BYTES)

    await window.wait_for_room()
    peer.frame("tx", written("open"), 0)
    peer.frame("tx", written("ping"), 0)
    waiting = asyncio.ensure_future(window.wait_for_room())
    await asyncio.sleep(0)
    assert not waiting.done(), "a frame carrying no payload bought room for one"

    peer.frame("tx", written("data"), 0)
    await waiting


def test_the_unix_transport_without_af_unix_is_a_note_rather_than_an_exception(monkeypatch: pytest.MonkeyPatch):
    """Asking for a transport the platform does not have skips its cells and says so.

    The absence is simulated with monkeypatch rather than the test being skipped, so the branch is
    executed on the machines that do have `AF_UNIX` - which is every machine this suite normally runs
    on. `raising=False` so the removal reads the same on the platform the branch is about.
    """
    monkeypatch.delattr(socket, "AF_UNIX", raising=False)

    results, _round_trips, notes = run_matrix(
        seconds=SECONDS,
        transports=("unix",),
        codecs=("json",),
        payloads=(PAYLOAD,),
        streams=(1,),
    )

    assert [result for result in results if result.transport == "unix"] == []
    assert any("unix" in note.lower() for note in notes), notes
