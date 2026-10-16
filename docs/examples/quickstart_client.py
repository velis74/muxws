"""Quick-start dialer: the `websockets` transport, two call shapes on one socket.

Start `quickstart_server.py` first, then run `python quickstart_client.py`. The URL is
`ws://127.0.0.1:8000/ws` unless the `MUXWS_URL` environment variable says otherwise.
"""

import asyncio
import os

from muxws import connect

URL = os.environ.get("MUXWS_URL", "ws://127.0.0.1:8000/ws")


async def main() -> None:
    peer = await connect(URL)
    try:
        # Unary. `request()` sends one payload, waits for exactly one back, and raises if the remote
        # sends a second; `await stream` does not raise in that case.
        answer = await peer.request({"say": "hello"})
        print(f"greeting: {answer['greeting']}")

        # Streaming response. `open()` is synchronous: it returns the Stream in the same turn in which
        # it allocates the id. `end=True` ends this side's sending, so the acceptor can start
        # streaming back immediately.
        stream = peer.open({"count": 3}, end=True)
        async for chunk in stream:
            print(f"chunk {chunk['chunk']} of {chunk['of']}")
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
