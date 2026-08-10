"""The FastAPI app and its one WebSocket route.

One route, and nothing transport-specific after `accept()`. `accept()` performs the upgrade itself,
because it is the only party that knows which `muxws.v1.<codec>` subprotocol to select
(WSM-CDC-026) - which is also why there is no `await websocket.accept()` anywhere in this file.

`python demo.py` from the repository root starts this under uvicorn on :8020 and the Vite dev server
on :5173. The frontend dials `/ws` through Vite's proxy.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, WebSocket

from demo.backend_python.handlers import MarketService
from demo.backend_python.market import Market
from muxws import accept, PeerRegistry

HOST = "127.0.0.1"
PORT = int(os.environ.get("MUXWS_DEMO_PORT", "8020"))

app = FastAPI(title="muxws demo", docs_url=None, redoc_url=None)

#: Per process, and that is the whole of its scope. muxws ships no cross-process backplane
#: (WSM-REG-018): a deployment that ran two of these would publish on whatever bus it already has and
#: let each process fan out to the sockets it holds.
registry = PeerRegistry()
service = MarketService(Market(), registry)


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    """One socket, one peer, one handler.

    Where authentication would go: **here**, before `accept()`, out of the handshake - a header, a
    cookie or a ticket in the query string (WSM-AUT-001). It does not go in the hello, which is an
    ordinary application stream that has already been accepted by the time it is read (WSM-RCN-025).
    The demo dials without a credential and this comment is the whole of its auth story.
    """
    peer = await accept(websocket)
    # Registered before `serve()` starts reading, so a stream that arrives in the first frame off the
    # socket meets a handler rather than `reset(REFUSED, "no on_stream handler")` (WSM-STM-033).
    peer.on_stream(service.handler_for(peer))
    try:
        await peer.serve()
    finally:
        # `on_close` already does this - `_start_board` installs a hook, and `PeerRegistry.register`
        # installs its own (WSM-REG-016). This is here for the peer that never got as far as a hello:
        # it has no board and no index entry, and `forget` is idempotent, so the cost of being sure
        # is one dictionary lookup.
        service.forget(peer)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
