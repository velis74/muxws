"""Server push: the acceptor opens a stream of its own.

Run it with `python push_server.py`; it listens on 127.0.0.1:8001 unless `MUXWS_PORT` says
otherwise, and `push_client.py` (or `push-client.ts`) is the other half.

There is no separate push API. `peer.open()` here is the same call the dialer makes in
`quickstart_client.py`, on the same socket, with the same correlation and the same cancellation -
which is the whole of muxws's symmetry claim.
"""

import os

from fastapi import FastAPI, WebSocket

from muxws import accept, Stream

HOST = "127.0.0.1"
PORT = int(os.environ.get("MUXWS_PORT", "8001"))

app = FastAPI()


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    peer = await accept(websocket)

    async def on_stream(payload: object, _stream: Stream) -> None:
        """The dialer's one-shot notify. Nothing to answer here; the push is the interesting half."""
        topic = payload["subscribe"] if isinstance(payload, dict) else "ticks"
        # Opened by the acceptor, on its own initiative. The dialer sees it in *its* on_stream
        # handler, exactly as this peer saw the dialer's stream.
        ticks = peer.open({"topic": topic})
        for n in (1, 2, 3):
            await ticks.send({"tick": n})
        await ticks.end()

    peer.on_stream(on_stream)
    await peer.serve()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
