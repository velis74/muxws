"""The Python half of the cross-language interop check (M3 §7 tests 22-24).

Run as an acceptor:  python interop/runner.py accept <port>
Run as a dialer:     python interop/runner.py dial ws://127.0.0.1:<port>

The scenario is the same in both roles and in both languages, so a mismatch shows up as a failed
assertion rather than as a hang. Exits non-zero with a named reason on any disagreement.

`assert` is deliberately absent: this file is not a `*_test.py`, so ruff's S101 applies and, more to
the point, a driver that vanished under `python -O` would be worse than no driver.
"""

from __future__ import annotations

import asyncio
import json
import sys

from typing import Any

import websockets

import muxws

from muxws.stream import Stream
from muxws.transports.websockets_ import WebsocketsSocket


def check(condition: bool, what: str) -> None:
    if not condition:
        raise SystemExit(f"interop FAILED: {what}")


async def handler(payload: Any, stream: Stream) -> None:
    """Every shape the scenario exercises, chosen by the opening payload."""
    action = (payload or {}).get("action")

    if action == "echo":
        await stream.reply({"echo": payload.get("value")})
    elif action == "export":
        for index in range(4):
            await stream.send({"row": index})
        await stream.end({"row": 4}, trailers={"rows": "5"})
    elif action == "raise":
        raise ValueError("interop handler said no")
    elif action == "forever":
        await stream.closed.wait()
    elif action == "big":
        # Comfortably over MAX_FRAME_BYTES once encoded, so fragmentation is exercised end to end.
        await stream.reply({"blob": "š" * 40_000})
    else:
        raise SystemExit(f"interop FAILED: unknown action {action!r}")


async def run_scenario(peer: muxws.Peer, *, label: str) -> None:
    """The dialer's side of the scenario. Every call shape, then a cancel, then a big payload."""
    echoed = await peer.request({"action": "echo", "value": 42})
    check(echoed == {"echo": 42}, f"{label}: unary request returned {echoed!r}")

    rows = [item async for item in peer.open({"action": "export"})]
    check(rows == [{"row": index} for index in range(5)], f"{label}: streaming export returned {rows!r}")

    await peer.notify({"action": "echo", "value": 1})

    try:
        await peer.request({"action": "raise"})
    except muxws.RemoteError as error:
        payload = error.payload or {}
        # `message` is portable; `type` is NOT. WSM-ERR-006's default serializer reports the remote's
        # own exception class name, so a TypeScript acceptor says "Error" where a Python one says
        # "ValueError". Asserting equality on it would be asserting which language answered.
        check(
            payload.get("message") == "interop handler said no",
            f"{label}: application error message was {payload!r}",
        )
        check(
            isinstance(payload.get("type"), str) and payload["type"],
            f"{label}: application error carried no type name: {payload!r}",
        )
    else:
        raise SystemExit(f"interop FAILED: {label}: a raising handler did not produce RemoteError")

    held = peer.open({"action": "forever"})
    await asyncio.sleep(0.1)
    await held.cancel("interop cancel")
    check(held.closed.is_set(), f"{label}: cancel did not close the stream locally")

    big = await peer.request({"action": "big"})
    check(len(big["blob"]) == 40_000, f"{label}: fragmented payload came back as {len(big['blob'])} chars")
    check(big["blob"][0] == "š", f"{label}: fragmented payload lost its non-ASCII content")

    print(json.dumps({"role": label, "ok": True}), flush=True)


async def accept_forever(port: int) -> None:
    """Serve until the dialer has finished; the driver kills us."""

    async def handle(connection: Any) -> None:
        peer = await muxws.accept(WebsocketsSocket(connection))
        peer.on_stream(handler)
        await peer.serve()

    async with websockets.serve(handle, "127.0.0.1", port, select_subprotocol=muxws.select_subprotocol) as service:
        print(json.dumps({"role": "python-acceptor", "port": service.sockets[0].getsockname()[1]}), flush=True)
        await asyncio.Future()


async def dial(url: str) -> None:
    peer = await muxws.connect(url)
    peer.on_stream(handler)
    try:
        await run_scenario(peer, label="python-dialer")
    finally:
        await peer._socket.close()


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: runner.py accept <port> | runner.py dial <url>")
    mode, argument = sys.argv[1], sys.argv[2]
    if mode == "accept":
        asyncio.run(accept_forever(int(argument)))
    elif mode == "dial":
        asyncio.run(dial(argument))
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
