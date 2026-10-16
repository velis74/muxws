"""Server push, dialer side: a dialer with its own `on_stream` handler.

Start `push_server.py` first, then run `python push_client.py`. The URL is `ws://127.0.0.1:8001/ws`
unless `MUXWS_URL` says otherwise.

The handler is passed to `connect()` instead of being registered after it returns: the acceptor may
push a stream as soon as the connection is up, and a stream that arrives before a handler is
registered is answered with `reset(REFUSED)`.
"""

import asyncio
import os

from muxws import connect, Stream

URL = os.environ.get("MUXWS_URL", "ws://127.0.0.1:8001/ws")


async def main() -> None:
    finished = asyncio.Event()

    async def on_push(payload: object, stream: Stream) -> None:
        print(f"push: {payload['topic']}")
        async for tick in stream:
            print(f"tick {tick['tick']}")
        finished.set()

    peer = await connect(URL, on_stream=on_push)
    try:
        # One-shot: `notify()` sends a payload, ends the stream, and returns no handle; no reply is
        # expected.
        await peer.notify({"subscribe": "ticks"})
        # 10.0 seconds, so that a stuck example fails instead of hanging.
        await asyncio.wait_for(finished.wait(), timeout=10.0)
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
