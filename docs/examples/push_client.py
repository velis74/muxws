"""Server push, dialer half: a dialer with an `on_stream` handler of its own.

Start `push_server.py` first, then run `python push_client.py`. The URL is `ws://127.0.0.1:8001/ws`
unless `MUXWS_URL` says otherwise.

The handler is passed to `connect()` rather than registered after it returns, because the acceptor
may push a stream the instant it sees this dialer: a handler registered one `await` later would meet
that push with `reset(REFUSED)`.
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
        # One-shot: `notify()` sends a payload and ends the stream, and hands back no handle to
        # await, because there is nothing coming back on it.
        await peer.notify({"subscribe": "ticks"})
        # 10.0 seconds, and only so a stuck example fails rather than hangs.
        await asyncio.wait_for(finished.wait(), timeout=10.0)
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
