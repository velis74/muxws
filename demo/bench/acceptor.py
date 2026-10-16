"""The counting end of one cell. Reports to the dialer how many payloads arrived; writes nothing to stdout.

    python -m demo.bench.acceptor --mode M --transport T --codec C --address A

One process per cell, started by `demo.bench.run_matrix` and terminated by it after the dialer has
reported. `ACCEPTOR_READY` is written to stderr once the address is bound; on success nothing else is
written. The parent waits for that line instead of probing the address, because another process can
bind a port between its selection and this process binding it.

Each mode has its own termination handshake. The dialer's clock includes draining, so it stops only
on a reply from this end:

    raw-socket      a zero-length frame ends the run; the count goes back as 8 big-endian bytes
    raw-websocket   the sentinel message ends the run; the count goes back as text
    muxws           the dialer ends its stream; the handler, having iterated it to the end, sends
                    the count on that same stream and ends it

Every count is per connection, and per stream under muxws; the dialer sums them. No state is shared
between connections, so a cell with twenty connections needs no locking.

`compression=None`: with permessage-deflate the two WebSocket modes would compress padding that the
raw-socket mode sends uncompressed.
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

#: The parent reads this process's first output line as the ready signal and the rest as the failure
#: reason, so library log output is suppressed.
_QUIET = logging.CRITICAL


class FramedCounter(asyncio.Protocol):
    """One raw-socket connection: length-prefixed payloads counted as their bytes arrive.

    Only frame boundaries are tracked; payload bytes are counted and discarded. Reassembling each
    payload (for example with `StreamReader.readexactly`) copies it out of a buffer, and above about
    64 KiB that copy costs more than the socket transfer, so the baseline would be slower than the
    WebSocket mode.

    A zero-length frame ends the run and the count is sent back as `COUNT_BYTES` big-endian bytes. A
    dialer that disconnects without the terminating frame gets no reply; the parent terminates the
    acceptor in either case.
    """

    def __init__(self) -> None:
        self._transport: asyncio.Transport | None = None
        self._count = 0
        #: Payload bytes still to read for the current frame, and the prefix bytes read for the next one.
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
            # A prefix can be split across `data_received` calls, so it is accumulated.
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

    The count is sent back on the same stream, where the dialer awaits it. `open()` carries a
    payload, so the opening payload counts as message one.

    A probe is a round-trip request. `peer.request()` sets `end` on the open frame, so the opening
    payload is the stream's whole content and there is nothing to iterate.
    """
    if isinstance(payload, dict) and payload.get("probe"):
        await stream.reply({"count": 1})
        return

    count = 1
    async for _ in stream:
        count += 1
    await stream.send({"count": count}, end=True)


async def serve_framed(transport: str, address: str) -> None:
    """Serve a plain socket with a length prefix and no WebSocket; the baseline for the other modes."""
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
    """Report that the address is bound, then serve until the parent terminates this process.

    `run_matrix` waits for this line. It is written only inside a bound server's context, so after
    reading it the parent knows that this process holds the address.
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
