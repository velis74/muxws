"""Throughput of muxws compared with the same payload sent without muxws.

`muxws/throughput_test.py` reports frames per second over the in-memory pair, without a baseline.
This package measures the baseline on the same machine. Every cell is measured three ways, all three
sending the same application payload bytes and, under a text codec, sending them as text:

    raw-socket      a socket and a 4-byte big-endian length prefix, no WebSocket at all
    raw-websocket   one WebSocket message per payload, no muxws
    muxws           `open()` once, then `send()` per payload

The difference between the first two rows is the WebSocket framing; the difference between the last
two is the muxws envelope and the peer's bookkeeping.

The two ends of a cell run in separate processes (`demo.bench.acceptor` and `demo.bench.dialer`), so
they can use two cores and neither shares the other's event loop. Per cell: a warmup burst, then the
clock starts, the dialer sends for a fixed wall-clock time, ends the run and waits for the acceptor
to report how many payloads arrived. The clock stops on that report, so the elapsed time includes
draining the data still in flight.

The acceptors refuse permessage-deflate. With it, the two WebSocket modes would compress a padded
payload that the raw-socket mode sends uncompressed.
"""

from __future__ import annotations

import contextlib
import functools
import importlib.util
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from demo.bench.report import format_report
from muxws.codecs import Codec, get_codec, register_codec

#: The three ways one cell is carried, in the order the report prints them.
MODES = ("raw-socket", "raw-websocket", "muxws")

#: The two transports. Both are local, so the table measures CPU cost, not network links.
TRANSPORTS = ("tcp", "unix")

#: Measured wall-clock seconds per cell; long enough for the rate to settle.
DEFAULT_SECONDS = 1.5

#: Small, medium, large: an envelope-dominated payload, an ordinary one, and one that muxws must
#: fragment (`MAX_FRAME_BYTES` is 65,536).
DEFAULT_PAYLOADS = (128, 4096, 262_144)

#: The codecs the default matrix and `--full` run.
DEFAULT_CODECS = ("json",)
FULL_CODECS = ("json", "msgpack")

#: The concurrency cell: this many muxws streams on one connection, and this many connections for the
#: raw modes, which cannot multiplex within one connection.
CONCURRENT_STREAMS = 20

#: The payload the default matrix runs that concurrency cell at.
CONCURRENT_PAYLOAD = 4096

#: Seconds the dialer sends before the clock starts. Measured in time, not payloads: a payload count
#: large enough to warm the smallest cell would take minutes in the largest. The acceptor counts
#: warmup payloads like any other, so the dialer subtracts what it sent and waits for the backlog to
#: drain before starting the clock; otherwise draining the warmup would count as measured time.
WARMUP_SECONDS = 0.2

#: `Stream.send()` enqueues and returns without waiting for the socket, so a loop that only sends
#: measures the queue, which at 256 KiB per payload grows without bound. The dialer keeps at most
#: this many bytes between `send()` and the socket; the other two modes get the same limit from
#: `drain()` and the kernel's send buffer. Counted in bytes, not payloads, so the amount left to
#: drain at the end of the measured time is the same for every payload size.
MUXWS_SEND_WINDOW_BYTES = 262_144

#: The length prefix of the raw-socket mode, and the width of the count it answers with.
LENGTH_PREFIX_BYTES = 4
COUNT_BYTES = 8

#: The message that ends a raw-websocket run. Sent as text under a text codec and as bytes under a
#: binary one, matching the other messages of the run.
SENTINEL_TEXT = "muxws-bench-end"
SENTINEL_BYTES = SENTINEL_TEXT.encode()

#: The HTTP request target both WebSocket modes ask for. Neither acceptor routes on it.
REQUEST_TARGET = "/bench"

#: The line the acceptor writes on stderr once its address is bound; on success it writes nothing
#: else. Waiting for this line, instead of probing the address, avoids mistaking another process that
#: took the port after `_address` released it for the acceptor.
ACCEPTOR_READY = "muxws-bench-acceptor-ready"

#: Round trips per transport, and warmup round trips before them.
ROUND_TRIP_SAMPLES = 400
ROUND_TRIP_WARMUP = 50

#: The payload of a round trip: minimal, so the figure is the library's latency and not the codec's
#: cost. The acceptor replies to it without iterating the stream.
ROUND_TRIP_PROBE = {"probe": True}

#: Seconds an acceptor has to bind before its cell fails, and seconds it has to exit and be read
#: before it is killed.
STARTUP_TIMEOUT_SECONDS = 20.0
TEARDOWN_TIMEOUT_SECONDS = 5.0

#: Seconds a dialer may run beyond its own time budget before the cell fails. A dialer whose acceptor
#: died mid-run does not end on its own; this limit makes the cell fail instead of hanging the matrix.
DIALER_MARGIN_SECONDS = 60.0


@dataclass(frozen=True)
class Result:
    """One mode of one cell: what was sent, and how fast."""

    mode: str
    transport: str
    codec: str
    #: The encoded length achieved; the rate is computed from it.
    payload_bytes: int
    streams: int
    messages: int
    seconds: float
    megabytes_per_second: float
    messages_per_second: float


@dataclass(frozen=True)
class RoundTrip:
    """One transport's `peer.request()` latency, measured locally."""

    transport: str
    codec: str
    #: The median of the round trips; unlike the mean, it is not moved by single scheduling delays.
    microseconds: float


def bench_codec(name: str) -> Codec:
    """The codec both halves of a cell use, registering msgpack when that is the one asked for.

    The library registers only JSON (WSM-CDC-014), so each process of each cell registers msgpack
    itself.
    """
    if name == "msgpack":
        from muxws.codecs.msgpack_ import MsgpackCodec

        register_codec("msgpack", MsgpackCodec())
    return get_codec(name)


def encoded_length(data: str | bytes) -> int:
    """Bytes on the wire: UTF-8 for text, the buffer's own length for bytes."""
    return len(data.encode("utf-8")) if isinstance(data, str) else len(data)


def build_payload(target_bytes: int, codec: Codec) -> tuple[Any, str | bytes]:
    """One object whose encoded form is `target_bytes` long, and that encoded form.

    A field is padded until the encoding has the requested length. The returned encoding has the
    length achieved, which can differ from the request: msgpack's string header grows at 32, 256 and
    65,536 bytes, so not every size is reachable. Rates are computed from the achieved length.
    """
    padding = max(target_bytes - encoded_length(codec.encode_payload({"n": 0, "pad": ""})), 0)
    payload = {"n": 0, "pad": "x" * padding}
    data = codec.encode_payload(payload)
    for _ in range(4):
        shortfall = target_bytes - encoded_length(data)
        if shortfall == 0 or padding + shortfall < 0:
            break
        padding += shortfall
        payload = {"n": 0, "pad": "x" * padding}
        data = codec.encode_payload(payload)
    return payload, data


def run_matrix(
    *,
    full: bool = False,
    seconds: float = DEFAULT_SECONDS,
    transports: Sequence[str] | None = None,
    codecs: Sequence[str] | None = None,
    payloads: Sequence[int] | None = None,
    streams: Sequence[int] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> tuple[list[Result], list[RoundTrip], list[str]]:
    """Measure every cell, then a round trip per transport. Third element: what was skipped and why.

    A cell whose transport or codec this machine cannot run is left out of the results and named in
    the notes.
    """
    notes: list[str] = []
    chosen_transports = _available_transports(transports or TRANSPORTS, notes)
    chosen_codecs = _available_codecs(codecs or (FULL_CODECS if full else DEFAULT_CODECS), notes)
    chosen_payloads = tuple(payloads or DEFAULT_PAYLOADS)

    results: list[Result] = []
    round_trips: list[RoundTrip] = []
    for transport in chosen_transports:
        for codec_name in chosen_codecs:
            environment = _cell_environment(codec_name)
            for payload in chosen_payloads:
                for mode in MODES:
                    for count in _stream_counts(mode, payload, full=full, override=streams):
                        if on_progress is not None:
                            on_progress(f"{mode} {transport} {codec_name} {payload}B x{count}")
                        results.append(
                            _measure_cell(
                                mode=mode,
                                transport=transport,
                                codec_name=codec_name,
                                payload=payload,
                                streams=count,
                                seconds=seconds,
                                environment=environment,
                            )
                        )
            if on_progress is not None:
                on_progress(f"round trip {transport} {codec_name}")
            round_trips.append(_measure_round_trip(transport, codec_name, environment))
    return results, round_trips, notes


def _available_transports(wanted: Sequence[str], notes: list[str]) -> tuple[str, ...]:
    """`wanted` minus the transports this interpreter has no way to open."""
    if "unix" in wanted and not hasattr(socket, "AF_UNIX"):
        notes.append("unix socket cells skipped: this platform has no socket.AF_UNIX")
        return tuple(name for name in wanted if name != "unix")
    return tuple(wanted)


def _available_codecs(wanted: Sequence[str], notes: list[str]) -> tuple[str, ...]:
    """`wanted` minus the codecs whose dependency is absent."""
    if "msgpack" in wanted and importlib.util.find_spec("msgpack") is None:
        notes.append('msgpack cells skipped: msgpack is not installed (pip install -e ".[msgpack]")')
        return tuple(name for name in wanted if name != "msgpack")
    return tuple(wanted)


def _stream_counts(mode: str, payload: int, *, full: bool, override: Sequence[int] | None) -> tuple[int, ...]:
    """How many concurrent streams this mode runs this payload at.

    The default matrix runs one concurrency cell: muxws at the middle payload. `--full` runs every
    mode at both counts, comparing the raw modes' 20 connections with muxws's 20 streams on one
    connection.
    """
    if override is not None:
        return tuple(override)
    if full:
        return (1, CONCURRENT_STREAMS)
    if mode == "muxws" and payload == CONCURRENT_PAYLOAD:
        return (1, CONCURRENT_STREAMS)
    return (1,)


def _measure_cell(
    *,
    mode: str,
    transport: str,
    codec_name: str,
    payload: int,
    streams: int,
    seconds: float,
    environment: dict[str, str],
) -> Result:
    """Start the acceptor, run the dialer against it, and turn its report into a `Result`."""
    with _address(transport) as address:
        acceptor = _start_acceptor(mode, transport, codec_name, address, environment)
        try:
            _wait_until_listening(address, acceptor)
            report = _run_dialer(
                mode=mode,
                transport=transport,
                codec_name=codec_name,
                address=address,
                payload=payload,
                streams=streams,
                seconds=seconds,
                environment=environment,
                acceptor=acceptor,
            )
        finally:
            _stop(acceptor)

    elapsed = report["seconds"]
    messages = report["messages"]
    return Result(
        mode=mode,
        transport=transport,
        codec=codec_name,
        payload_bytes=report["payload_bytes"],
        streams=streams,
        messages=messages,
        seconds=elapsed,
        megabytes_per_second=messages * report["payload_bytes"] / elapsed / 1_000_000,
        messages_per_second=messages / elapsed,
    )


def _measure_round_trip(transport: str, codec_name: str, environment: dict[str, str]) -> RoundTrip:
    """One muxws acceptor, a few hundred `peer.request()` calls against it, the median of them."""
    with _address(transport) as address:
        acceptor = _start_acceptor("muxws", transport, codec_name, address, environment)
        try:
            _wait_until_listening(address, acceptor)
            microseconds = _in_a_loop_of_its_own(lambda: _round_trip_median(transport, address, codec_name))
        finally:
            _stop(acceptor)
    return RoundTrip(transport=transport, codec=codec_name, microseconds=microseconds)


async def _round_trip_median(transport: str, address: str, codec_name: str) -> float:
    """`peer.request()` timed one call at a time, in microseconds."""
    from muxws import connect

    codec = bench_codec(codec_name)
    peer = await connect(muxws_url(transport, address), codec=codec)
    try:
        for _ in range(ROUND_TRIP_WARMUP):
            await peer.request(ROUND_TRIP_PROBE)
        samples = []
        for _ in range(ROUND_TRIP_SAMPLES):
            started = time.perf_counter()
            await peer.request(ROUND_TRIP_PROBE)
            samples.append(time.perf_counter() - started)
    finally:
        await peer.close()
    return statistics.median(samples) * 1_000_000


def _in_a_loop_of_its_own(factory: Callable[[], Any]) -> Any:
    """Run one coroutine to completion in a thread of its own.

    `run_matrix` is synchronous, and `asyncio.run()` raises when an event loop is already running in
    the calling thread. The separate thread makes the round trip callable with or without a running
    loop.
    """
    import asyncio

    outcome: dict[str, Any] = {}

    def target() -> None:
        try:
            outcome["value"] = asyncio.run(factory())
        except BaseException as exc:  # noqa: BLE001 - re-raised below, on the calling thread
            outcome["error"] = exc

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def muxws_url(transport: str, address: str) -> str:
    """The URL a muxws dialer opens for this cell."""
    if transport == "unix":
        return f"ws+unix://{address}:{REQUEST_TARGET}"
    return f"ws://{address}{REQUEST_TARGET}"


def split_host_and_port(address: str) -> tuple[str, int]:
    """`host:port` as its two halves."""
    host, _, port = address.rpartition(":")
    return host, int(port)


@contextlib.contextmanager
def _address(transport: str) -> Iterator[str]:
    """A free address: a free port, or a socket file in a new directory.

    The directory is created under `/tmp`, not `TMPDIR`: `sun_path` is limited to about 108 bytes, and
    a long `TMPDIR` would make every unix cell fail with `OSError: AF_UNIX path too long`.
    """
    if transport == "unix":
        directory = tempfile.mkdtemp(prefix="mux", dir="/tmp")  # noqa: S108 - see the docstring
        try:
            yield os.path.join(directory, "b.sock")
        finally:
            shutil.rmtree(directory, ignore_errors=True)
        return

    with contextlib.closing(socket.socket()) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    yield f"127.0.0.1:{port}"


def _start_acceptor(
    mode: str, transport: str, codec_name: str, address: str, environment: dict[str, str]
) -> subprocess.Popen[str]:
    """Start the counting end in its own process, so that each end can use a core."""
    return subprocess.Popen(  # noqa: S603 - a fixed argv, and every value in it is ours
        [
            sys.executable,
            "-m",
            "demo.bench.acceptor",
            "--mode",
            mode,
            "--transport",
            transport,
            "--codec",
            codec_name,
            "--address",
            address,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )


def _run_dialer(
    *,
    mode: str,
    transport: str,
    codec_name: str,
    address: str,
    payload: int,
    streams: int,
    seconds: float,
    environment: dict[str, str],
    acceptor: subprocess.Popen[str],
) -> dict[str, Any]:
    """Run the sending end to completion and return its stdout line, parsed."""
    try:
        completed = subprocess.run(  # noqa: S603 - a fixed argv, and every value in it is ours
            [
                sys.executable,
                "-m",
                "demo.bench.dialer",
                "--mode",
                mode,
                "--transport",
                transport,
                "--codec",
                codec_name,
                "--address",
                address,
                "--payload",
                str(payload),
                "--streams",
                str(streams),
                "--seconds",
                str(seconds),
            ],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
            # The time budget plus a margin. A dialer whose acceptor died does not end on its own,
            # and without a timeout `subprocess.run` would wait indefinitely.
            timeout=seconds + WARMUP_SECONDS + DIALER_MARGIN_SECONDS,
        )
    except subprocess.TimeoutExpired as expired:
        raise RuntimeError(
            f"the {mode} dialer over {transport} was killed after {expired.timeout:.1f} seconds\n"
            f"{_acceptor_output(acceptor)}"
        ) from expired
    if completed.returncode != 0:
        raise RuntimeError(
            f"the {mode} dialer over {transport} exited with {completed.returncode}\n"
            f"{completed.stderr}{_acceptor_output(acceptor)}"
        )
    return json.loads(completed.stdout)


def _wait_until_listening(address: str, acceptor: subprocess.Popen[str]) -> None:
    """Block until this acceptor reports that it has bound, or raise with the reason.

    Readiness is the acceptor's ready line, not a successful connection to the address: a port is
    free only when probed, and another process may bind it afterwards. An acceptor that exits at once
    (a missing dependency, a socket file it refuses to unlink) closes the pipe, and is reported as
    failed immediately instead of after the timeout.
    """
    first: list[str] = []
    # Read on a thread, because a pipe read has no timeout, and non-blocking pipe reads differ
    # between platforms.
    reader = threading.Thread(target=lambda: first.append(acceptor.stdout.readline()), daemon=True)
    reader.start()
    reader.join(STARTUP_TIMEOUT_SECONDS)

    if first and first[0].strip() == ACCEPTOR_READY:
        return
    if reader.is_alive():
        # Terminated first, so the pipe reaches EOF and the reader thread can be joined; nothing else
        # may read the pipe while that thread is blocked on it.
        acceptor.terminate()
        reader.join()
        raise RuntimeError(f"the acceptor did not bind {address} within {STARTUP_TIMEOUT_SECONDS} seconds")
    # Any other first line means the acceptor is failing and may still be writing its traceback; the
    # wait lets it finish before `_acceptor_output` terminates it.
    with contextlib.suppress(subprocess.TimeoutExpired):
        acceptor.wait(timeout=TEARDOWN_TIMEOUT_SECONDS)
    raise RuntimeError(f"the acceptor stopped before binding {address}\n{first[0]}{_acceptor_output(acceptor)}")


def _stop(acceptor: subprocess.Popen[str]) -> None:
    """End the acceptor, whatever state it is in.

    The acceptor serves until it is stopped: with more than one connection in a cell there is no
    single final handshake on which it could exit.
    """
    _acceptor_output(acceptor)


def _acceptor_output(acceptor: subprocess.Popen[str]) -> str:
    """Terminate the acceptor and return its output (empty on success).

    The acceptor is terminated first because reading a pipe the child still holds open blocks until
    EOF. The failure paths and `_stop` can both call this; a second call finds the pipe closed and
    returns an empty string.
    """
    if acceptor.poll() is None:
        acceptor.terminate()
    if acceptor.stdout is None or acceptor.stdout.closed:
        acceptor.wait()
        return ""
    try:
        output, _ = acceptor.communicate(timeout=TEARDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        acceptor.kill()
        output, _ = acceptor.communicate()
    return output or ""


def _cell_environment(codec_name: str) -> dict[str, str]:
    """The subprocess environment: this checkout on `PYTHONPATH`, and the codec both halves read."""
    return {**_example_environment(), "MUXWS_CODEC": codec_name}


@functools.lru_cache(maxsize=1)
def _example_environment() -> dict[str, str]:
    """`demo.py`'s environment builder, loaded from the script beside this package.

    `import demo` finds this package, not the script `demo.py` one directory up, so the script is
    loaded by path. Without this environment a child process imports an installed muxws, if any,
    instead of the checkout being measured.
    """
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    spec = importlib.util.spec_from_file_location("muxws_demo_script", os.path.join(root, "demo.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.example_environment()


__all__ = [
    "ACCEPTOR_READY",
    "CONCURRENT_STREAMS",
    "COUNT_BYTES",
    "DEFAULT_CODECS",
    "DEFAULT_PAYLOADS",
    "DEFAULT_SECONDS",
    "FULL_CODECS",
    "LENGTH_PREFIX_BYTES",
    "MODES",
    "MUXWS_SEND_WINDOW_BYTES",
    "REQUEST_TARGET",
    "SENTINEL_BYTES",
    "SENTINEL_TEXT",
    "TRANSPORTS",
    "WARMUP_SECONDS",
    "Result",
    "RoundTrip",
    "bench_codec",
    "build_payload",
    "encoded_length",
    "format_report",
    "muxws_url",
    "run_matrix",
    "split_host_and_port",
]
