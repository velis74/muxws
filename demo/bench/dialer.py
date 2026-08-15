"""The sending half of one cell. Prints one JSON object on stdout and nothing else.

    python -m demo.bench.dialer --mode M --transport T --codec C --address A --payload N
                                --streams K --seconds S

The object carries `mode`, `transport`, `codec`, `payload_bytes`, `streams`, `messages` and
`seconds`; `demo.bench.run_matrix` derives the rates from those and never re-measures anything here.

What is counted is what the **acceptor** reports, not what this process handed to a socket: those two
differ by everything still in flight, and the drain is part of the cost. The warmup is counted too -
the acceptor cannot tell it apart from the run - so it is subtracted here, where the number is known.

The three modes reach the same shape through three different channels: `send()` once per payload,
then a termination handshake whose answer is the count. The clock covers the sending and the
handshake both.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

from collections.abc import Callable
from typing import Any

from demo.bench import (
    bench_codec,
    build_payload,
    COUNT_BYTES,
    encoded_length,
    LENGTH_PREFIX_BYTES,
    MODES,
    MUXWS_SEND_WINDOW_BYTES,
    muxws_url,
    REQUEST_TARGET,
    SENTINEL_BYTES,
    SENTINEL_TEXT,
    split_host_and_port,
    TRANSPORTS,
    WARMUP_SECONDS,
)


class FramedChannel:
    """One raw-socket connection: a length prefix, the payload, and a drain that is the backpressure."""

    #: Payloads the acceptor has already counted before the warmup begins.
    presends = 0

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, body: bytes) -> None:
        self._reader = reader
        self._writer = writer
        #: Prefix and payload in one buffer, so a payload is one write rather than two.
        self._frame = len(body).to_bytes(LENGTH_PREFIX_BYTES, "big") + body

    async def send(self) -> None:
        self._writer.write(self._frame)
        await self._writer.drain()

    async def quiesce(self) -> None:
        """`send()` already waited on the socket for every payload; this is the last of them."""
        await self._writer.drain()

    async def finish(self) -> int:
        self._writer.write((0).to_bytes(LENGTH_PREFIX_BYTES, "big"))
        await self._writer.drain()
        count = int.from_bytes(await self._reader.readexactly(COUNT_BYTES), "big")
        self._writer.close()
        return count


class WebSocketChannel:
    """One raw-websocket connection: one message per payload, no muxws envelope around it."""

    presends = 0

    def __init__(self, connection: Any, data: str | bytes) -> None:
        self._connection = connection
        self._data = data
        #: Text under a text codec, bytes under a binary one - the terminator does not change medium.
        self._sentinel = SENTINEL_BYTES if isinstance(data, bytes) else SENTINEL_TEXT

    async def send(self) -> None:
        await self._connection.send(self._data)

    async def quiesce(self) -> None:
        """Nothing is outstanding: `websockets` drains inside `send()` before it returns."""

    async def finish(self) -> int:
        await self._connection.send(self._sentinel)
        return int(await self._connection.recv())


class MuxwsChannel:
    """One muxws stream: `open()` carried the first payload, `send()` carries the rest."""

    #: The `open` payload, which the acceptor counts like any other.
    presends = 1

    def __init__(self, stream: Any, payload: Any, window: SendWindow) -> None:
        self._stream = stream
        self._payload = payload
        self._window = window

    async def send(self) -> None:
        await self._stream.send(self._payload)
        await self._window.wait_for_room()

    async def quiesce(self) -> None:
        await self._window.wait_until_empty()

    async def finish(self) -> int:
        await self._stream.end()
        answer = await self._stream
        return int(answer["count"])


class SendWindow:
    """A cap on the payloads sitting between `send()` and the socket, one window for the whole peer.

    `Stream.send()` enqueues and returns, so a loop that only sends measures the writer's queue
    rather than the wire, and at 256 KiB a payload that queue is the machine's memory. Waiting here
    is what `drain()` gives the other two modes. The count comes from the peer's own frame hook, over
    `data` frames: a fragmented payload is many of them and exactly one carries `more=False`.

    The same enqueueing is why a lost connection has to be an exit of its own. Frames stop being
    written and nothing raises, so a wait keyed on the count alone would never end.
    """

    def __init__(self, peer: Any, payload_bytes: int) -> None:
        self._limit = max(1, MUXWS_SEND_WINDOW_BYTES // payload_bytes)
        self._sent = 0
        self._written = 0
        self._lost: BaseException | None = None
        peer.on_frame(self._note)
        peer.on_close(self._note_loss)

    def _note(self, direction: str, frame: Any, _length: int) -> None:
        # `data` frames alone. An `open` carries a payload that `_sent` never counted and a ping is
        # not a payload at all, so counting either would widen the window by however many of them the
        # run happened to send.
        if direction == "tx" and frame.type == "data" and not frame.more:
            self._written += 1

    def _note_loss(self, reason: Any) -> None:
        # `send()` enqueues, so a lost connection reaches this loop as frames that stop being
        # written rather than as an exception. Without this the wait below is unbounded.
        self._lost = ConnectionError(f"the connection was lost before the run ended: {reason}")

    async def wait_for_room(self) -> None:
        self._sent += 1
        await self._wait_while(lambda: self._sent - self._written > self._limit)

    async def wait_until_empty(self) -> None:
        """Block until nothing is between `send()` and the socket."""
        await self._wait_while(lambda: self._sent > self._written)

    async def _wait_while(self, waiting: Callable[[], bool]) -> None:
        while waiting():
            if self._lost is not None:
                raise self._lost
            await asyncio.sleep(0)


async def measure(channels: list[Any], seconds: float) -> tuple[int, float]:
    """Warm every channel, send for `seconds`, terminate, and wait to be told what arrived."""
    warmed = await asyncio.gather(*(warm(channel) for channel in channels))
    # Emptied before the clock starts. Draining is inside the measured interval by design, but a
    # backlog left by the warmup is time charged to payloads that are then subtracted from the count,
    # and the backlog is a different size in every mode.
    await asyncio.gather(*(channel.quiesce() for channel in channels))

    started = time.perf_counter()
    deadline = started + seconds
    counts = await asyncio.gather(*(pump(channel, deadline) for channel in channels))
    elapsed = time.perf_counter() - started

    before = sum(warmed) + sum(channel.presends for channel in channels)
    return sum(counts) - before, elapsed


async def warm(channel: Any) -> int:
    """Fill the pipe, and report how many payloads that took - the acceptor counts those too."""
    sent = 0
    deadline = time.perf_counter() + WARMUP_SECONDS
    while time.perf_counter() < deadline:
        await channel.send()
        sent += 1
    return sent


async def pump(channel: Any, deadline: float) -> int:
    """Send until the budget is spent, then terminate the run and return the acceptor's count."""
    while time.perf_counter() < deadline:
        await channel.send()
    return await channel.finish()


async def run_raw_socket(options: argparse.Namespace, data: str | bytes, _payload: Any) -> tuple[int, float]:
    """One connection per stream, carrying the codec's own bytes with a length prefix in front."""
    body = data.encode("utf-8") if isinstance(data, str) else data
    channels = []
    for _ in range(options.streams):
        if options.transport == "unix":
            reader, writer = await asyncio.open_unix_connection(options.address)
        else:
            host, port = split_host_and_port(options.address)
            reader, writer = await asyncio.open_connection(host, port)
        channels.append(FramedChannel(reader, writer, body))
    return await measure(channels, options.seconds)


async def run_raw_websocket(options: argparse.Namespace, data: str | bytes, _payload: Any) -> tuple[int, float]:
    """One connection per stream, one WebSocket message per payload. Text under a text codec.

    `compression=None` on this end as well as the acceptor's: the acceptor is what decides the
    extension, and offering one it will refuse only puts a line in the handshake.
    """
    from websockets.asyncio.client import connect, unix_connect

    settings = dict(compression=None, max_size=None)
    connections = []
    for _ in range(options.streams):
        if options.transport == "unix":
            connections.append(await unix_connect(options.address, uri=f"ws://localhost{REQUEST_TARGET}", **settings))
        else:
            connections.append(await connect(muxws_url(options.transport, options.address), **settings))
    try:
        return await measure([WebSocketChannel(connection, data) for connection in connections], options.seconds)
    finally:
        for connection in connections:
            await connection.close()


async def run_muxws(options: argparse.Namespace, data: str | bytes, payload: Any) -> tuple[int, float]:
    """One connection, one stream per stream, and the library's own `open()`/`send()`.

    The payload object, not its encoding: this is the one mode where the codec is muxws's to call.
    """
    from muxws import connect, Reconnect

    # `max_attempts=0`: a cell whose acceptor died is over. With the default schedule the peer
    # redials the dead address forever, `send()` goes on enqueueing, and the run has nothing left
    # that could end it.
    peer = await connect(
        muxws_url(options.transport, options.address),
        codec=bench_codec(options.codec),
        reconnect=Reconnect(max_attempts=0),
    )
    try:
        window = SendWindow(peer, encoded_length(data))
        channels = [MuxwsChannel(peer.open(payload), payload, window) for _ in range(options.streams)]
        return await measure(channels, options.seconds)
    finally:
        await peer.close()


#: One coroutine per mode, all three with the same signature so the caller has no branch of its own.
RUNNERS = {
    "raw-socket": run_raw_socket,
    "raw-websocket": run_raw_websocket,
    "muxws": run_muxws,
}


def main(argv: list[str] | None = None) -> None:
    options = parse_arguments(argv)
    codec = bench_codec(options.codec)
    payload, data = build_payload(options.payload, codec)

    messages, seconds = asyncio.run(RUNNERS[options.mode](options, data, payload))

    print(
        json.dumps(
            {
                "mode": options.mode,
                "transport": options.transport,
                "codec": options.codec,
                # The length achieved, not the one asked for: it is what every rate is computed from.
                "payload_bytes": encoded_length(data),
                "streams": options.streams,
                "messages": messages,
                "seconds": seconds,
            }
        )
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="The sending half of one throughput cell.")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--transport", choices=TRANSPORTS, required=True)
    parser.add_argument("--codec", required=True)
    parser.add_argument("--address", required=True, help="host:port, or the path of a socket file")
    parser.add_argument("--payload", type=int, required=True, help="the encoded payload length to aim for")
    parser.add_argument("--streams", type=int, required=True)
    parser.add_argument("--seconds", type=float, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
