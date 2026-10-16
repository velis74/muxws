"""The sending end of one cell. Prints one JSON object on stdout and nothing else.

    python -m demo.bench.dialer --mode M --transport T --codec C --address A --payload N
                                --streams K --seconds S

The object carries `mode`, `transport`, `codec`, `payload_bytes`, `streams`, `messages` and
`seconds`; `demo.bench.run_matrix` computes the rates from them.

The message count is the count the acceptor reports, not the number of payloads written to the
socket; the two differ by the data in flight, and draining it is part of the measured time. The
acceptor also counts warmup payloads, so this process subtracts them.

All three modes work the same way: `send()` once per payload, then a termination handshake whose
reply is the count. The clock covers both the sending and the handshake.
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
    """One raw-socket connection: a length prefix and the payload, with `drain()` as backpressure."""

    #: Payloads the acceptor has already counted before the warmup begins.
    presends = 0

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, body: bytes) -> None:
        self._reader = reader
        self._writer = writer
        #: Prefix and payload in one buffer, so each payload is a single write.
        self._frame = len(body).to_bytes(LENGTH_PREFIX_BYTES, "big") + body

    async def send(self) -> None:
        self._writer.write(self._frame)
        await self._writer.drain()

    async def quiesce(self) -> None:
        """Drain the writer; `send()` has already drained after every earlier payload."""
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
        #: Text under a text codec, bytes under a binary one, like the payload messages.
        self._sentinel = SENTINEL_BYTES if isinstance(data, bytes) else SENTINEL_TEXT

    async def send(self) -> None:
        await self._connection.send(self._data)

    async def quiesce(self) -> None:
        """No-op: `websockets` drains inside `send()` before it returns."""

    async def finish(self) -> int:
        await self._connection.send(self._sentinel)
        return int(await self._connection.recv())


class MuxwsChannel:
    """One muxws stream: `open()` sends the first payload, `send()` the rest."""

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
    """A limit on the payloads between `send()` and the socket, shared by all streams of the peer.

    `Stream.send()` enqueues and returns, so a loop that only sends measures the writer's queue, which
    at 256 KiB per payload grows without bound. Waiting here gives the same backpressure `drain()`
    gives the other two modes. Written payloads are counted from the peer's frame hook over `data`
    frames: a fragmented payload has many, and exactly one carries `more=False`.

    After a lost connection frames are not written and `send()` does not raise, so the wait
    also ends when the peer reports a close.
    """

    def __init__(self, peer: Any, payload_bytes: int) -> None:
        self._limit = max(1, MUXWS_SEND_WINDOW_BYTES // payload_bytes)
        self._sent = 0
        self._written = 0
        self._lost: BaseException | None = None
        peer.on_frame(self._note)
        peer.on_close(self._note_loss)

    def _note(self, direction: str, frame: Any, _length: int) -> None:
        # Only `data` frames: `_sent` does not count the `open` payload, and a ping is not a payload.
        if direction == "tx" and frame.type == "data" and not frame.more:
            self._written += 1

    def _note_loss(self, reason: Any) -> None:
        # `send()` only enqueues, so a lost connection raises nothing there; this ends the wait below.
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
    """Warm every channel, send for `seconds`, terminate, and return the acceptor's count and the time."""
    warmed = await asyncio.gather(*(warm(channel) for channel in channels))
    # Drained before the clock starts. The final drain is part of the measured time, but the warmup
    # backlog is not: its payloads are subtracted from the count, and its size differs between modes.
    await asyncio.gather(*(channel.quiesce() for channel in channels))

    started = time.perf_counter()
    deadline = started + seconds
    counts = await asyncio.gather(*(pump(channel, deadline) for channel in channels))
    elapsed = time.perf_counter() - started

    before = sum(warmed) + sum(channel.presends for channel in channels)
    return sum(counts) - before, elapsed


async def warm(channel: Any) -> int:
    """Send for `WARMUP_SECONDS` and return the number of payloads sent; the acceptor counts them too."""
    sent = 0
    deadline = time.perf_counter() + WARMUP_SECONDS
    while time.perf_counter() < deadline:
        await channel.send()
        sent += 1
    return sent


async def pump(channel: Any, deadline: float) -> int:
    """Send until `deadline`, then terminate the run and return the acceptor's count."""
    while time.perf_counter() < deadline:
        await channel.send()
    return await channel.finish()


async def run_raw_socket(options: argparse.Namespace, data: str | bytes, _payload: Any) -> tuple[int, float]:
    """One connection per stream, sending the codec's encoded bytes with a length prefix."""
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

    `compression=None` on this end as well: the acceptor refuses the extension, so offering it only
    adds a header to the handshake.
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
    """One connection with `streams` muxws streams, using `open()`/`send()`.

    Sends the payload object, not its encoding: in this mode muxws calls the codec.
    """
    from muxws import connect, Reconnect

    # `max_attempts=0`: a cell whose acceptor died ends. With the default schedule the peer redials
    # indefinitely, `send()` keeps enqueueing, and the run never ends.
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


#: One coroutine per mode, all with the same signature.
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
                # The length achieved, not the one requested; the rates are computed from it.
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
