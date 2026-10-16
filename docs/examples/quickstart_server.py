"""Quick-start acceptor: FastAPI, one WebSocket route, one muxws peer.

Run it with `python quickstart_server.py`. It listens on 127.0.0.1:8000 unless the `MUXWS_PORT`
environment variable says otherwise - the documentation test starts it on a free port that way.

The route does three things: it hands the socket to `accept()`, registers the peer's `on_stream`
handler, and runs the read loop. Every stream the dialer opens on this connection arrives at that
handler.
"""

import os

from fastapi import FastAPI, WebSocket

from muxws import accept, Stream

#: 127.0.0.1: the quick start runs locally and is not a deployment.
HOST = "127.0.0.1"
PORT = int(os.environ.get("MUXWS_PORT", "8000"))

app = FastAPI()


async def on_stream(payload: object, stream: Stream) -> None:
    """The incoming-stream handler. `payload` is the value the dialer opened the stream with."""
    if isinstance(payload, dict) and payload.get("say") == "hello":
        # Unary: one payload and the end of the stream in a single call.
        await stream.reply({"greeting": "hello, muxws"})
        return

    if isinstance(payload, dict) and isinstance(payload.get("count"), int):
        # Streaming response: any number of payloads, then an end. `end` is a flag on the last frame,
        # not a separate frame, so three chunks take three frames.
        total = payload["count"]
        for n in range(1, total + 1):
            await stream.send({"chunk": n, "of": total})
        await stream.end()
        return

    # Any other stream, including the reconnect hello from the last section, is acknowledged by
    # returning: muxws ends a stream its handler left open.


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    """`accept()` performs the WebSocket upgrade, because it selects the subprotocol."""
    peer = await accept(websocket)
    peer.on_stream(on_stream)
    await peer.serve()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
