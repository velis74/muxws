"""The FastAPI app and its one WebSocket route.

Nothing after `accept()` is transport-specific. `accept()` performs the upgrade because it selects
the `muxws.v1.<codec>` subprotocol (WSM-CDC-026); this file therefore does not call
`await websocket.accept()`.

`python demo.py --browser` from the repository root starts this under uvicorn on :8020 and the Vite
dev server on :5173. The frontend dials `/ws` through Vite's proxy.
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

#: Scoped to this process. muxws has no cross-process backplane (WSM-REG-018); a deployment with
#: several processes publishes on its own message bus and each process fans out to its own sockets.
registry = PeerRegistry()
service = MarketService(Market(), registry)


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    """One socket, one peer, one handler.

    Authentication belongs here, before `accept()`, using the handshake: a header, a cookie or a
    ticket in the query string (WSM-AUT-001). It does not belong in the hello, which is an ordinary
    application stream that has already been accepted when it is read (WSM-RCN-025). The demo does
    not authenticate.
    """
    peer = await accept(websocket)
    # Registered before `serve()` starts reading, so a stream in the first frame reaches the handler
    # instead of being answered with `reset(REFUSED, "no on_stream handler")` (WSM-STM-033).
    peer.on_stream(service.handler_for(peer))
    try:
        await peer.serve()
    finally:
        # `on_close` hooks installed by `_start_board` and `PeerRegistry.register` (WSM-REG-016) also
        # do this. This call covers a peer that closed before sending a hello and so has no hooks;
        # `forget` is idempotent.
        service.forget(peer)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
