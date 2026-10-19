"""Structure of a `demo.bench` run; no measured value is compared.

The numbers `run_matrix` produces depend on the machine and its load, so these tests do not compare
rates, ratios or durations. They assert that every mode produces a `Result`, that each result carries
the requested cell, and that `format_report` renders them as text. This covers subprocesses that do
not start, dialer output that does not parse, and cells that produce no result.

The time budget per cell is a fraction of `DEFAULT_SECONDS`.
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


#: The lines passed to `on_progress` during the module's run, in order.
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


def test_each_mode_reports_a_positive_message_count_and_rate(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """Every result carries a positive message count and rates derived from it.

    A mode that never connected, or a row built without the dialer's report, has a zero count.
    """
    results, _round_trips, _notes = smoke

    for result in results:
        assert result.messages > 0, result.mode
        assert result.messages_per_second > 0, result.mode
        assert result.megabytes_per_second > 0, result.mode


def test_the_payload_length_reported_is_the_one_all_three_modes_carried(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """`payload_bytes` is the encoded length sent, and it is the same length in every mode.

    The three rows are comparable only if they carry the same application payload. A mode that
    counted its own framing as payload would report a different length.
    """
    results, _round_trips, _notes = smoke

    lengths = {result.mode: result.payload_bytes for result in results}
    assert len(set(lengths.values())) == 1, lengths
    # Encoding cannot lose bytes, so the requested size is the floor and not the exact figure.
    for mode, length in lengths.items():
        assert length >= PAYLOAD, mode


def test_on_progress_is_called_for_every_cell_before_it_is_measured(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """The hook is called once per cell and once for the round trip.

    Only the mode names in the lines are asserted; the text is meant for a human reader.
    """
    results, _round_trips, _notes = smoke

    assert len(PROGRESS) == len(results) + 1, PROGRESS
    for mode in MODES:
        assert any(line.startswith(mode) for line in PROGRESS), mode


def test_the_report_renders_the_results_as_text_naming_every_mode(
    smoke: tuple[list[Result], list[RoundTrip], list[str]],
):
    """`format_report` returns the whole report as a string and names each mode in it.

    Only the names are asserted; the numbers depend on the machine.
    """
    results, round_trips, notes = smoke

    report = format_report(results, round_trips, notes)

    assert isinstance(report, str)
    for mode in MODES:
        assert mode in report, mode


class Hooks:
    """A peer stand-in with the two hooks a `SendWindow` registers at construction."""

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


async def test_a_pending_send_window_wait_raises_connection_error_on_close():
    """A close reported by the peer makes a pending wait raise `ConnectionError`.

    `Stream.send()` enqueues and returns, so after a lost connection frames are not written and
    nothing raises; the wait ends on the close notification instead.
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
    """Only `data` frames free room in the window; `open` and `ping` frames do not.

    The window does not count the `open` payload as sent, and a ping carries no payload. Counting
    either as written would enlarge the window by one payload per stream.
    """
    peer = Hooks()
    window = SendWindow(peer, MUXWS_SEND_WINDOW_BYTES)

    await window.wait_for_room()
    peer.frame("tx", written("open"), 0)
    peer.frame("tx", written("ping"), 0)
    waiting = asyncio.ensure_future(window.wait_for_room())
    await asyncio.sleep(0)
    assert not waiting.done(), "a frame without a payload enlarged the window"

    peer.frame("tx", written("data"), 0)
    await waiting


def test_the_unix_transport_without_af_unix_is_a_note_rather_than_an_exception(monkeypatch: pytest.MonkeyPatch):
    """Asking for a transport the platform does not have skips its cells and says so.

    The absence is simulated with monkeypatch, so the branch runs on machines that have `AF_UNIX`.
    `raising=False` makes the removal succeed on platforms without it.
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
