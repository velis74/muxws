"""Unix-domain-socket acceptor: `unix_serve`, one muxws peer, and the caller named by the kernel.

Run it with `python uds_server.py`. It binds `<tempdir>/muxws.sock` unless the `MUXWS_SOCKET`
environment variable says otherwise - the documentation test binds it inside a short temporary
directory that way, because a Unix socket path is capped at about 108 bytes and a longer one raises
`OSError: AF_UNIX path too long` out of `bind()`, which names the reason but not the component to
shorten.

Nothing about muxws changes here, and that is the claim this file exists to execute. `unix_serve`
performs the same HTTP GET + Upgrade as `websockets.serve`, `select_subprotocol` is the same hook
selecting the same `muxws.v1.<codec>` value, `accept()` wraps the connection in the same
`WebsocketsSocket`, and a dialer offering a codec this acceptor does not speak is still refused with
HTTP 400 before anything is accepted. The socket underneath is a file; that is the entire difference,
and no adapter in the library can see it.

What the file buys is the `accepted a dialer:` line. The socket's own filesystem permissions decide
who may connect at all, and `SO_PEERCRED` hands the acceptor the caller's pid, uid and gid straight
from the kernel - so this connection is authenticated at the upgrade, before `accept()`, with no
credential anywhere on the wire and nothing for the dialer to forge.
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

#: The socket file this acceptor binds. Kept short on purpose - see the note about 108 bytes above.
SOCKET_PATH = os.environ.get("MUXWS_SOCKET", os.path.join(tempfile.gettempdir(), "muxws.sock"))


async def on_stream(payload: object, stream: Stream) -> None:
    """The one incoming-stream handler - the same one the TCP quick start registers, unchanged."""
    if isinstance(payload, dict) and payload.get("say") == "hello":
        # Unary: one payload and the end of the stream in a single call.
        await stream.reply({"greeting": "hello over a unix socket"})
        return

    if isinstance(payload, dict) and isinstance(payload.get("count"), int):
        # Streaming response: as many payloads as we like, then an end.
        total = payload["count"]
        for n in range(1, total + 1):
            await stream.send({"chunk": n, "of": total})
        await stream.end()
        return

    # Anything else is acknowledged simply by returning: muxws ends a stream its handler left open.


def peer_credentials(connection: object) -> str:
    """The dialer's pid, uid and gid as the kernel reports them, or why they are unavailable.

    `SO_PEERCRED` is a Linux socket option; macOS and the BSDs answer the same question through
    `getpeereid()`, which Python does not expose, and no other transport muxws ships can answer it at
    all. Degrading to a string rather than raising is deliberate: the credential is a bonus this
    transport happens to offer, and an acceptor that crashed on a platform without it would make the
    example unrunnable on machines where the rest of the file works perfectly.
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
    """One connection, one peer. `unix_serve` calls this once per dialer, exactly as `serve` would."""
    print(f"accepted a dialer: {peer_credentials(connection)}", flush=True)
    peer = await accept(connection)
    peer.on_stream(on_stream)
    await peer.serve()


def is_stale(path: str) -> bool:
    """True when `path` is a socket file nobody is listening on - the corpse a killed run leaves.

    A socket file outlives the process that bound it, so the next `bind()` fails with `EADDRINUSE`
    until someone removes it. Removing it unconditionally is the tempting one-liner and it is wrong:
    it would take the socket away from an acceptor that is alive and serving, and the two processes
    would then be reachable under one path with no way for a dialer to tell which it got. Connecting
    first answers the question the unlink needs answered - a corpse refuses, a live acceptor accepts.

    The inode type is checked before the probe, and that guard is not defensive tidiness: `connect()`
    on a regular file or a directory also fails with `ECONNREFUSED`, so a probe on its own reports
    `MUXWS_SOCKET=~/notes.txt` as a corpse and this example deletes it. Refusing outright is the only
    answer available - a path that is not a socket is never something this process may unlink.
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
