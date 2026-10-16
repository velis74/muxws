"""Surviving a disconnect: the same dialer with `hello=` and `reconnect=Reconnect()`.

Start `quickstart_server.py` first, then run `python reconnect_client.py`. The client sends one
request and exits, so `on_reconnect` does not fire unless the connection drops; it shows where a
long-running program is notified of a reconnection.
"""

import asyncio
import os

from muxws import connect, Peer, Reconnect

URL = os.environ.get("MUXWS_URL", "ws://127.0.0.1:8000/ws")


def on_reconnect(reconnection: int, _peer: Peer) -> None:
    """Called once per re-established connection: socket open, subprotocol accepted, hello answered.

    The number counts reconnections of this peer, starting at 1; it is not the number of dial
    attempts.
    """
    print(f"reconnected (reconnection {reconnection})")


async def main() -> None:
    peer = await connect(
        URL,
        # Captured once, by value, and replayed unchanged on every later connection. It is an
        # ordinary stream: the acceptor receives it in its on_stream handler and acknowledges it by
        # returning. Do not put credentials in it; authenticate at the upgrade.
        hello={"client": "quickstart"},
        # initial_delay 0.25 seconds, factor 2.0, max_delay 30.0 seconds, jitter 0.3, unlimited
        # attempts. Every duration in the Python implementation is in seconds, as a float.
        reconnect=Reconnect(),
        on_reconnect=on_reconnect,
    )
    try:
        answer = await peer.request({"say": "hello"})
        print(f"greeting: {answer['greeting']}")
    finally:
        await peer.close()


if __name__ == "__main__":
    asyncio.run(main())
