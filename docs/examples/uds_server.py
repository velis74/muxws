"""Unix-domain-socket acceptor: `unix_serve`, one muxws peer, and the caller named by the kernel.

Run it with `python uds_server.py`. It binds `<tempdir>/muxws.sock` unless the `MUXWS_SOCKET`
environment variable says otherwise. The documentation test sets it to a path in a short temporary
directory: a Unix socket path is limited to about 108 bytes, and `bind()` raises
`OSError: AF_UNIX path too long` for a longer one.

The muxws code is the same as over TCP. `unix_serve` performs the same HTTP GET + Upgrade as
`websockets.serve`, `select_subprotocol` selects the same `muxws.v1.<codec>` value, `accept()` wraps
the connection in the same `WebsocketsSocket`, and a dialer offering an unsupported codec is refused
with HTTP 400 before anything is accepted. Only the underlying socket differs.

The Unix socket adds the `accepted a dialer:` line. The socket file's permissions decide who may
connect, and `SO_PEERCRED` gives the acceptor the caller's pid, uid and gid from the kernel. The
connection is authenticated at the upgrade, before `accept()`, without a credential on the wire.
"""

import asyncio
import os
import socket
import stat
import struct
import tempfile

from contextlib import closing

from websockets.asyncio.server import unix_serve

from muxws import accept, select_subprotocol, Stream

#: The socket file this acceptor binds. Keep it short; see the 108-byte limit above.
SOCKET_PATH = os.environ.get("MUXWS_SOCKET", os.path.join(tempfile.gettempdir(), "muxws.sock"))


async def on_stream(payload: object, stream: Stream) -> None:
    """The incoming-stream handler; the same as the TCP quick start's, apart from the greeting."""
    if isinstance(payload, dict) and payload.get("say") == "hello":
        # Unary: one payload and the end of the stream in a single call.
        await stream.reply({"greeting": "hello over a unix socket"})
        return

    if isinstance(payload, dict) and isinstance(payload.get("count"), int):
        # Streaming response: any number of payloads, then an end.
        total = payload["count"]
        for n in range(1, total + 1):
            await stream.send({"chunk": n, "of": total})
        await stream.end()
        return

    # Any other stream is acknowledged by returning: muxws ends a stream its handler left open.


def peer_credentials(connection: object) -> str:
    """The dialer's pid, uid and gid as the kernel reports them, or why they are unavailable.

    `SO_PEERCRED` is a Linux socket option. macOS and the BSDs provide the uid and gid, but not the
    pid, through `getpeereid()`, which Python does not expose; no other muxws transport provides peer
    credentials. Where they are unavailable this returns a message instead of raising, so the example
    runs on those platforms.
    """
    transport = getattr(connection, "transport", None)
    raw = transport.get_extra_info("socket") if transport is not None else None
    if raw is None or not hasattr(socket, "SO_PEERCRED"):
        return "unavailable on this platform"

    layout = "3i"  # struct ucred: pid_t, uid_t, gid_t, three 32-bit integers.
    credentials = raw.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize(layout))
    pid, uid, gid = struct.unpack(layout, credentials)
    return f"pid={pid} uid={uid} gid={gid}"


async def handle(connection: object) -> None:
    """One connection, one peer. `unix_serve` calls this once per dialer, as `serve` does."""
    print(f"accepted a dialer: {peer_credentials(connection)}", flush=True)
    peer = await accept(connection)
    peer.on_stream(on_stream)
    await peer.serve()


def is_stale(path: str) -> bool:
    """True when `path` is a socket file with no listener, as left behind by a killed acceptor.

    A socket file remains after the process that bound it exits, and the next `bind()` fails with
    `EADDRINUSE` until it is removed. Removing it unconditionally would detach a running acceptor
    from its path. A connection attempt distinguishes the cases: it is refused when no process
    listens, and accepted by a running acceptor.

    The file type is checked before the connection attempt, because `connect()` on a regular file or
    a directory also fails with `ECONNREFUSED`. A path that is not a socket exits with an error and is
    never removed.
    """
    if not os.path.exists(path):
        return False
    if not stat.S_ISSOCK(os.lstat(path).st_mode):
        raise SystemExit(f"{path} exists and is not a socket file; refusing to remove it")
    with closing(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)) as probe:
        return probe.connect_ex(path) != 0


async def main() -> None:
    if not hasattr(socket, "AF_UNIX"):
        raise SystemExit("this example needs AF_UNIX, which Windows does not have; use the TCP quick start")

    if is_stale(SOCKET_PATH):
        os.unlink(SOCKET_PATH)

    async with unix_serve(handle, SOCKET_PATH, select_subprotocol=select_subprotocol):
        print(f"listening on {SOCKET_PATH}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
