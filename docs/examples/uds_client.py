"""Unix-domain-socket dialer: the ordinary public `connect()`, pointed at a socket file.

Start `uds_server.py` first, then run `python uds_client.py`. The URL is
`ws+unix://<tempdir>/muxws.sock:/ws` unless the `MUXWS_URL` environment variable says otherwise.

`connect()` selects the transport from the URL scheme; no extra argument or adapter is needed. The
codec offer at the handshake, the `CodecMismatch` raised for HTTP 400, the heartbeat and the reconnect
loop use the same code as over TCP. The path is split at the first colon: `/tmp/muxws.sock` is the
file to open and `/ws` is the HTTP request target of the upgrade. There is no `wss+unix://`: a
filesystem socket is not reachable from another machine, and the socket file's permissions control
access.
"""

import asyncio
import os
import tempfile

from muxws import connect

DEFAULT_SOCKET = os.path.join(tempfile.gettempdir(), "muxws.sock")
URL = os.environ.get("MUXWS_URL", f"ws+unix://{DEFAULT_SOCKET}:/ws")


async def main() -> None:
    peer = await connect(URL)
    try:
        # The quick start's two call shapes, unary and streaming, over a Unix socket.
        answer = await peer.request({"say": "hello"})
        print(f"greeting: {answer['greeting']}")

        stream = peer.open({"count": 3}, end=True)
        async for chunk in stream:
            print(f"chunk {chunk['chunk']} of {chunk['of']}")

        # The heartbeat is a muxws `ping` frame, sent over the same socket. Only the arrival of the pong
        # is printed: the round-trip time varies between runs, and the guide compares this output
        # exactly.
        await peer.ping()
        print("ping: answered")
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
