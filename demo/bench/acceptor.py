"""The counting half of one cell. Answers the dialer with what arrived; stdout stays empty.

    python -m demo.bench.acceptor --mode M --transport T --codec C --address A

One process per cell, started by `demo.bench.run_matrix`, killed by it when the dialer has reported.
`ACCEPTOR_READY` goes to stderr the moment the address is bound, and it is the only thing this
process writes on a run that works. Its parent waits for that line rather than for the address to
answer: a port picked as free a moment earlier can be taken by anything, and a probe that accepts any
answer would hand the dialer a foreign server to measure.

Each mode carries its own termination handshake, because the clock the dialer keeps has to include
draining and can only be stopped by an answer from this end:

    raw-socket      a zero-length frame ends the run; the count goes back as 8 big-endian bytes
    raw-websocket   the sentinel message ends the run; the count goes back as text
    muxws           the dialer ends its stream; the handler, having iterated it to the end, sends
                    the count on that same stream and ends it

Every count is per connection, and per stream under muxws: the dialer adds them up. Nothing here is
shared between connections, so a cell with twenty of them needs no coordination and no lock.

`compression=None`: with permessage-deflate negotiated the two WebSocket modes would be compressing
padding that the raw-socket mode sends verbatim, and the table would report how compressible the
padding is as a property of muxws.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys

from typing import Any

from demo.bench import (
    ACCEPTOR_READY,
    bench_codec,
    COUNT_BYTES,
    LENGTH_PREFIX_BYTES,
    MODES,
    SENTINEL_BYTES,
    SENTINEL_TEXT,
    split_host_and_port,
    TRANSPORTS,
)

#: The parent reads the first line this process writes as the answer to whether it bound, and
#: everything after it as the reason a cell failed. Silenced so that a library log line can be
#: neither of those.
_QUIET = logging.CRITICAL


class FramedCounter(asyncio.Protocol):
    """One raw-socket connection: length-prefixed payloads counted as their bytes arrive.

    Only the boundaries are tracked; the payload bytes are counted and dropped. A reader that
    reassembled each payload - `StreamReader.readexactly`, the obvious way to write this - copies it
    out of a buffer that the transport keeps pausing, and above about 64 KiB that copy costs several
    times what the socket does. This mode is the denominator the other two are measured against, so a
    baseline spending its time in a buffer would measure the buffer and land under the WebSocket mode
    it exists to bound.

    A zero-length frame ends the run and the count goes back as `COUNT_BYTES` big-endian bytes. A
    dialer that goes away without terminating is answered by nobody, which is what the parent's
    teardown does to every acceptor it started.
    """

    def __init__(self) -> None:
        self._transport: asyncio.Transport | None = None
        self._count = 0
        #: Payload bytes still owed to the frame being read, and the prefix bytes of the next one.
        self._remaining = 0
        self._header = b""

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def data_received(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            if self._remaining:
                taken = min(self._remaining, len(view))
                self._remaining -= taken
                view = view[taken:]
                if not self._remaining:
                    self._count += 1
                continue
            # One `data_received` can split a prefix, so it is accumulated rather than assumed whole.
            wanted = LENGTH_PREFIX_BYTES - len(self._header)
            self._header += bytes(view[:wanted])
            view = view[wanted:]
            if len(self._header) < LENGTH_PREFIX_BYTES:
                return
            self._remaining = int.from_bytes(self._header, "big")
            self._header = b""
            if self._remaining == 0:
                self._transport.write(self._count.to_bytes(COUNT_BYTES, "big"))
                return


async def count_messages(connection: Any) -> None:
    """One raw-websocket connection: one message per payload until the sentinel ends it."""
    count = 0
    async for message in connection:
        if message in (SENTINEL_TEXT, SENTINEL_BYTES):
            await connection.send(str(count))
            return
        count += 1


async def count_stream(payload: Any, stream: Any) -> None:
    """One muxws stream: the opening payload, then every payload until the dialer ends it.

    The answer goes back on the same stream, which is what the dialer is awaiting: `open()` carried a
    payload, so the first thing this handler sees is already message number one.

    A probe is the round-trip shape. `peer.request()` puts its `end` on the open frame itself, so
    that stream's whole content is the payload already in hand and there is nothing to iterate.
    """
    if isinstance(payload, dict) and payload.get("probe"):
        await stream.reply({"count": 1})
        return

    count = 1
    async for _ in stream:
        count += 1
    await stream.send({"count": count}, end=True)


async def serve_framed(transport: str, address: str) -> None:
    """A socket and a length prefix, no WebSocket anywhere: the denominator for the other two."""
    loop = asyncio.get_running_loop()
    if transport == "unix":
        server = await loop.create_unix_server(FramedCounter, address)
    else:
        host, port = split_host_and_port(address)
        server = await loop.create_server(FramedCounter, host, port)
    async with server:
        await serve_until_stopped()


async def serve_websocket(transport: str, address: str, handler: Any, **options: Any) -> None:
    """One `websockets` acceptor, over a port or over a socket file."""
    from websockets.asyncio.server import serve, unix_serve

    settings = dict(compression=None, max_size=None, **options)
    if transport == "unix":
        server = unix_serve(handler, address, **settings)
    else:
        host, port = split_host_and_port(address)
        server = serve(handler, host, port, **settings)
    async with server:
        await serve_until_stopped()


async def serve_until_stopped() -> None:
    """Say the address is bound, then serve until the parent ends this process.

    The line is what `run_matrix` waits on, and it is written only from inside a bound server's
    context, so a parent that has read it knows the dialer's peer is this process and not whatever
    else the port was handed to.
    """
    print(ACCEPTOR_READY, file=sys.stderr, flush=True)
    await asyncio.Future()


async def serve_muxws(transport: str, address: str, codec_name: str) -> None:
    """The same acceptor as `raw-websocket`, with a muxws peer on top of every connection."""
    from muxws import select_subprotocol, serve

    codec = bench_codec(codec_name)

    async def handle(connection: Any) -> None:
        await serve(connection, handler=count_stream, codec=codec)

    await serve_websocket(transport, address, handle, select_subprotocol=select_subprotocol)


async def main(argv: list[str] | None = None) -> None:
    options = parse_arguments(argv)
    logging.getLogger("websockets").setLevel(_QUIET)
    bench_codec(options.codec)

    if options.transport == "unix" and not hasattr(socket, "AF_UNIX"):
        raise SystemExit("this cell needs AF_UNIX, which this platform does not have")

    if options.mode == "raw-socket":
        await serve_framed(options.transport, options.address)
    elif options.mode == "raw-websocket":
        await serve_websocket(options.transport, options.address, count_messages)
    else:
        await serve_muxws(options.transport, options.address, options.codec)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="The counting half of one throughput cell.")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--transport", choices=TRANSPORTS, required=True)
    parser.add_argument("--codec", required=True)
    parser.add_argument("--address", required=True, help="host:port, or the path of a socket file")
    return parser.parse_args(argv)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
