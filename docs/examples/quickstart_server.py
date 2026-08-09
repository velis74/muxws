"""Quick-start acceptor: FastAPI, one WebSocket route, one muxws peer.

Run it with `python quickstart_server.py`. It listens on 127.0.0.1:8000 unless the `MUXWS_PORT`
environment variable says otherwise - the documentation test starts it on a free port that way.

The route does three things and nothing else: hand the socket to `accept()`, register the one
`on_stream` handler this peer has, and run the read loop. Every stream the dialer opens - now or an
hour from now - arrives at that handler.
"""

import os

from fastapi import FastAPI, WebSocket

from muxws import accept, Stream

#: 127.0.0.1 on purpose: the quick start is a local demonstration, not a deployment.
HOST = "127.0.0.1"
PORT = int(os.environ.get("MUXWS_PORT", "8000"))

app = FastAPI()


async def on_stream(payload: object, stream: Stream) -> None:
    """The one incoming-stream handler. `payload` is the value the dialer opened the stream with."""
    if isinstance(payload, dict) and payload.get("say") == "hello":
        # Unary: one payload and the end of the stream in a single call.
        await stream.reply({"greeting": "hello, muxws"})
        return

    if isinstance(payload, dict) and isinstance(payload.get("count"), int):
        # Streaming response: as many payloads as we like, then an end. `end` is a flag on the last
        # frame, not a frame of its own, so this costs three frames rather than four.
        total = payload["count"]
        for n in range(1, total + 1):
            await stream.send({"chunk": n, "of": total})
        await stream.end()
        return

    # Anything else - the reconnect hello of the last section, for one - is acknowledged simply by
    # returning: muxws ends a stream its handler left open.


@app.websocket("/ws")
async def muxws_endpoint(websocket: WebSocket) -> None:
    """`accept()` performs the WebSocket upgrade itself, because it is what selects the subprotocol."""
    peer = await accept(websocket)
    peer.on_stream(on_stream)
    await peer.serve()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
