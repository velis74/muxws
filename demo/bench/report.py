"""The printable report: one table of cells, one of round trips, and what was skipped.

`format_report` returns the report as a string and prints nothing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from demo.bench import Result, RoundTrip

#: One row of the throughput table. Both header lines and every data line use it, so the columns
#: align.
_CELL = (
    "{transport:<11}{codec:<9}{payload:>9}{streams:>9}"
    "{raw_socket:>14}{raw_websocket:>15}{muxws:>13}{ratio:>11}{crossover:>11}"
)

#: One row of the round-trip table.
_TRIP = "{transport:<11}{codec:<9}{median:>13}{share:>18}"

#: Printed for a cell that was not measured, so it is not mistaken for zero.
_UNMEASURED = "-"

#: Round-trip time of the reference link the round trip is compared with: a typical intercontinental link.
_REFERENCE_LINK_MILLISECONDS = 40.0


def format_report(
    results: Sequence[Result],
    round_trips: Sequence[RoundTrip],
    notes: Sequence[str],
) -> str:
    """The whole report as one string."""
    lines = [*_preamble()]
    lines.extend(_throughput_table(results))
    lines.append("")
    lines.extend(_round_trip_table(round_trips))
    lines.append("")
    lines.extend(_notes(notes))
    return "\n".join(lines)


def _preamble() -> list[str]:
    """The description of the measurement: modes, conditions and units."""
    return [
        "muxws throughput: the same payload carried three ways",
        "",
        "  raw-socket     a socket and a 4-byte length prefix, no WebSocket at all. The receiving",
        "                 end finds the boundaries and counts the bytes; it reassembles nothing, so",
        "                 the figure is the transport's and not a buffer's.",
        "  raw-websocket  one WebSocket message per payload, no muxws",
        "  muxws          open() once, then send() per payload",
        "",
        "Regime: loopback and a unix domain socket, where the limit is the CPU and not the wire, so",
        "muxws/raw is what the envelope, the codec and the peer's bookkeeping cost against the",
        "transport underneath them. On a link slow enough to saturate, all three modes score the same",
        "and that ratio says nothing at all. raw-socket is the largest figure in every row it is",
        "measured in; a row where it is not is one whose denominator measured something other than",
        "the transport, and its ratio is not a cost.",
        "",
        "MB is 1,000,000 bytes. payload is the encoded length achieved, which is what each rate is",
        "computed from. streams is muxws streams over one connection; for the two raw modes, which",
        "have no multiplexing, it is that many connections. Each rate includes the drain: the clock",
        "stops when the acceptor has reported how many payloads reached it. Each cell is one",
        "measurement of one machine, so read the column and not the last digit.",
        "",
        "muxws/raw is the muxws rate over the raw-socket one. The two raw modes send a buffer the",
        "codec encoded once before the clock started; muxws is handed the payload object, encodes it",
        "per message, and fragments it above 65,536 bytes - all of that is inside its column. The",
        "trimmed matrix measures the twenty-stream cell for muxws alone, so on that row the two raw",
        "columns and the ratio built on them are empty; --full runs every mode at both stream counts.",
        "",
        "crossover is muxws's measured rate as a link speed. Below it the wire is the bottleneck and",
        "the envelope costs nothing a user could measure.",
        "",
    ]


def _throughput_table(results: Sequence[Result]) -> list[str]:
    """A line per cell: the three modes, muxws relative to the raw socket, and the crossover."""
    lines = [
        _CELL.format(
            transport="transport",
            codec="codec",
            payload="payload",
            streams="streams",
            raw_socket="raw-socket",
            raw_websocket="raw-websocket",
            muxws="muxws",
            ratio="muxws/raw",
            crossover="crossover",
        ),
        _CELL.format(
            transport="",
            codec="",
            payload="bytes",
            streams="",
            raw_socket="MB/s",
            raw_websocket="MB/s",
            muxws="MB/s",
            ratio="%",
            crossover="Gbit/s",
        ),
        "-" * 102,
    ]
    if not results:
        lines.append("nothing was measured")
        return lines

    for (transport, codec, payload, streams), modes in _cells(results).items():
        muxws = modes.get("muxws")
        denominator = modes.get("raw-socket")
        lines.append(
            _CELL.format(
                transport=transport,
                codec=codec,
                payload=payload,
                streams=streams,
                raw_socket=_rate(modes.get("raw-socket")),
                raw_websocket=_rate(modes.get("raw-websocket")),
                muxws=_rate(muxws),
                ratio=_ratio(muxws, denominator),
                crossover=_crossover(muxws),
            )
        )
    return lines


def _cells(results: Sequence[Result]) -> dict[tuple[str, str, int, int], dict[str, Result]]:
    """The results grouped by cell, in measurement order."""
    rows: dict[tuple[str, str, int, int], dict[str, Result]] = {}
    for result in results:
        key = (result.transport, result.codec, result.payload_bytes, result.streams)
        rows.setdefault(key, {})[result.mode] = result
    return rows


def _rate(result: Result | None) -> str:
    return _UNMEASURED if result is None else f"{result.megabytes_per_second:,.1f}"


def _ratio(muxws: Result | None, denominator: Result | None) -> str:
    """muxws as a percentage of the raw socket, when both were measured."""
    if muxws is None or denominator is None or denominator.megabytes_per_second == 0:
        return _UNMEASURED
    return f"{100 * muxws.megabytes_per_second / denominator.megabytes_per_second:.1f}"


def _crossover(muxws: Result | None) -> str:
    """muxws's measured rate as a link speed in gigabits per second: MB/s x 8 / 1000."""
    if muxws is None:
        return _UNMEASURED
    return f"{muxws.megabytes_per_second * 8 / 1_000:.2f}"


def _round_trip_table(round_trips: Sequence[RoundTrip]) -> list[str]:
    """The local latency of one `peer.request()`, and its share of the reference link's round trip."""
    lines = [
        f"round trip: muxws peer.request(), median, against a {_REFERENCE_LINK_MILLISECONDS:.0f} ms link",
        "",
        _TRIP.format(transport="transport", codec="codec", median="median", share="of the link"),
        _TRIP.format(transport="", codec="", median="us", share="%"),
        "-" * 51,
    ]
    if not round_trips:
        lines.append("nothing was measured")
        return lines

    reference = _REFERENCE_LINK_MILLISECONDS * 1_000
    for trip in round_trips:
        lines.append(
            _TRIP.format(
                transport=trip.transport,
                codec=trip.codec,
                median=f"{trip.microseconds:,.1f}",
                share=f"{100 * trip.microseconds / reference:.3f}",
            )
        )
    return lines


def _notes(notes: Sequence[str]) -> list[str]:
    """The skipped cells; when none were skipped, a line saying so."""
    if not notes:
        return ["notes: every transport and codec asked for was measured"]
    return ["notes:", *(f"  - {note}" for note in notes)]


__all__ = ["format_report"]
