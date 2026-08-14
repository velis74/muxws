"""Unix-domain-socket dialer: the ordinary public `connect()`, pointed at a socket file.

Start `uds_server.py` first, then run `python uds_client.py`. The URL is
`ws+unix://<tempdir>/muxws.sock:/ws` unless the `MUXWS_URL` environment variable says otherwise.

The point of this file is what is not in it. There is no new argument, no adapter to choose and no
second dialer: `connect()` reads the scheme off the URL and everything after it - the codec offered at
the handshake, the `CodecMismatch` an HTTP 400 turns into, the heartbeat, the reconnect loop - is the
code that already ran over TCP. Read the URL by splitting the path on the **first** colon:
`/tmp/muxws.sock` is the file to open and `/ws` is the HTTP request target inside the upgrade. There is
no `wss+unix://`: a filesystem socket is not reachable from another machine, so there is nothing for
TLS to protect against that the socket's own permissions do not already.
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
        # Unary, streaming: the two call shapes of the quick start, over a socket that is a file.
        answer = await peer.request({"say": "hello"})
        print(f"greeting: {answer['greeting']}")

        stream = peer.open({"count": 3}, end=True)
        async for chunk in stream:
            print(f"chunk {chunk['chunk']} of {chunk['of']}")

        # The heartbeat is a muxws `ping` frame and travels the same way here. Its round-trip time is
        # a measurement of the machine that ran it, so what gets printed is that a pong came back at
        # all - a line carrying milliseconds could not be the fixture the guide asserts byte for byte.
        await peer.ping()
        print("ping: answered")
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
