"""Surviving a disconnect: the same dialer with `hello=` and `reconnect=Reconnect()`.

Start `quickstart_server.py` first, then run `python reconnect_client.py`. This asks once and exits,
so `on_reconnect` does not fire on a healthy socket: it is here to show where a reconnection would
surface in a program that stays connected.
"""

import asyncio
import os

from muxws import connect, Peer, Reconnect

URL = os.environ.get("MUXWS_URL", "ws://127.0.0.1:8000/ws")


def on_reconnect(reconnection: int, _peer: Peer) -> None:
    """Fires once per **re-established** connection: socket open, subprotocol accepted, hello answered.

    The number is how many times this peer has reconnected - the first reconnection is 1 - and not
    how many dial attempts that reconnection cost.
    """
    print(f"reconnected (reconnection {reconnection})")


async def main() -> None:
    peer = await connect(
        URL,
        # Captured once, here, by value, and replayed verbatim on every later connection. It is an
        # ordinary stream: the acceptor sees it in its own on_stream handler and acknowledges it by
        # returning. Never put a credential in it - authentication belongs to the upgrade.
        hello={"client": "quickstart"},
        # initial_delay 0.25 seconds, factor 2.0, max_delay 30.0 seconds, jitter 0.3, unlimited
        # attempts. Every duration in the Python port is seconds as a float.
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
