"""The `ws+unix:` URL grammar: one URL carrying both a filesystem socket path and a request target.

    ws+unix:///run/muxws/api.sock:/ws?tenant=42

Over a Unix domain socket the handshake is the same as over TCP: an HTTP GET with
`Upgrade: websocket` offering `muxws.v1.<codec>` first (WSM-CDC-020), answered 101 or 400
(WSM-CDC-022), and the socket adapter does not depend on the transport (WSM-API-021). An AF_UNIX URL
must name both the socket file and the HTTP request target; this module parses that URL. It opens no
socket.

The grammar follows the `ws` npm package, so one URL works in either port's configuration: take the
URL's pathname plus its query and split on `:`; the left part is the filesystem path and the right
part is the HTTP request target, which defaults to `/` when there is no `:`.

Both ports split on the **first** colon. `ws` splits on every colon (`opts.path.split(':')`) and
keeps `parts[1]`, so `/a.sock:/r:x` becomes the target `/r` and `/a.sock:/r?t=a:b` becomes `/r?t=a`,
with the remainder dropped. The two ports implement the rule, not `ws`'s behaviour.

Stdlib only, so the grammar is testable where AF_UNIX does not exist, and this module has no
optional dependency in `packaging_test.py` (WSM-PKG-002).
"""

from __future__ import annotations

import socket as socketlib

from dataclasses import dataclass
from urllib.parse import urlsplit

from muxws.errors import TransportUnsupportedError, TransportUrlError

#: The scheme this module owns.
SCHEME = "ws+unix"

#: The scheme this module refuses. Neither port supports `wss+unix:`: a filesystem socket is
#: protected by its directory's permissions and by `SO_PEERCRED`, and there is no host name to check a
#: certificate against. A `wss+unix:` URL raises `UnixUrlError` instead of being dialled without TLS.
_REFUSED_SCHEME = "wss+unix"

#: The `Host:` header value when the URL has no authority, which is the usual case. HTTP requires a
#: `Host` header; the acceptor does not route on it.
_DEFAULT_AUTHORITY = "localhost"


class UnixUrlError(TransportUrlError):
    """A `ws+unix:` URL that cannot be dialled: no socket path, a bad target, or `wss+unix:`.

    Defined in this module, not in `muxws/errors.py`; import it as
    `from muxws.transports.unix import UnixUrlError` (WSM-ERR-016). Its base `TransportUrlError` is
    both a `MuxwsError` and a `ValueError`, so catching any of the three catches it.
    """


class UnixSocketsUnsupportedError(TransportUnsupportedError):
    """This interpreter has no `AF_UNIX`, so a `ws+unix:` URL can never be dialled here.

    A `TransportUnsupportedError` (WSM-ERR-016), not a `TransportUrlError`: changing the URL does not
    help. The base is both a `RuntimeError` and a `MuxwsError`.

    Raised from the parse, so `connect()` raises it before dialling. `websockets`' `unix_connect`
    imports on Windows but fails during the dial with an `AttributeError` on
    `loop.create_unix_connection`.
    """


@dataclass(frozen=True, slots=True)
class UnixTarget:
    """Everything `unix_connect(path, uri=...)` needs, and nothing about muxws.

    `path` names the socket file to open; `uri` is the logical URL used for the handshake.
    `websockets` does not derive the second from the first: the handshake is an HTTP request and
    needs a `Host` and a request target.
    """

    #: The filesystem path of the listening socket. The kernel limits it to about 108 bytes.
    path: str

    #: `ws://<authority><target>`, sent as the request line and the `Host` header.
    uri: str


def parse_unix_url(url: str) -> UnixTarget | None:
    """Split a `ws+unix:` URL into a socket path and a logical `ws://` URI; `None` for `ws:`/`wss:`.

    `connect()` calls this for every URL. Any scheme other than `ws+unix:` and `wss+unix:` returns
    `None` and is left to `websockets.connect()` to accept or reject.

    Called once, outside the dial closure, so a malformed URL and a platform without AF_UNIX raise
    from `connect()` before any socket is opened, as `resolve_codec()` does for the codec
    (WSM-CDC-016), and every reconnect dials the same target (WSM-CDC-020, WSM-AUT-003).

    Raises `UnixUrlError` for `wss+unix:`, for a URL with no socket path, or for a request target not
    starting with `/`; raises `UnixSocketsUnsupportedError` when the platform has no AF_UNIX.
    """
    parts = urlsplit(url)

    if parts.scheme == _REFUSED_SCHEME:
        raise UnixUrlError(
            f"{_REFUSED_SCHEME}: is not a scheme muxws dials: there is no TLS over a Unix domain "
            f"socket. Use {SCHEME}: and let the socket's file permissions be the access control"
        )
    if parts.scheme != SCHEME:
        return None

    if not hasattr(socketlib, "AF_UNIX"):
        raise UnixSocketsUnsupportedError(
            f"{SCHEME}: URLs need Unix domain sockets, which this platform does not have "
            f"(socket.AF_UNIX is missing); dial a ws:// or wss:// URL instead"
        )

    # `ws` splits `pathname + search`, so the query is appended before the split; otherwise
    # `ws+unix:///p.sock:/r?a=1` would lose `?a=1`. `urlsplit` separates them and does not
    # percent-decode either; `ts/node.ts` reads the same path and query from the URL as written.
    target = parts.path + (f"?{parts.query}" if parts.query else "")
    path, _, route = target.partition(":")

    if not path:
        raise UnixUrlError(
            f"{url!r} names no socket file: a {SCHEME}: URL is the socket's filesystem path followed "
            f"by an optional ':' and the request target, as in {SCHEME}:///run/muxws/api.sock:/ws"
        )
    if route and not route.startswith("/"):
        # `ws://localhost` + `ws` is `ws://localhostws`, a valid URL with the target merged into the
        # authority: the dial opens the right socket and requests `/` with a wrong `Host`. Against
        # `unix_serve`, or a `WebSocketServer` with no `path`, that handshake succeeds because
        # neither routes on the target, so the error is raised before the dial.

        raise UnixUrlError(
            f"the request target in {url!r} must begin with '/': the part after the ':' is an HTTP "
            f"request target, not a path relative to anything"
        )

    return UnixTarget(path=path, uri=f"ws://{parts.netloc or _DEFAULT_AUTHORITY}{route or '/'}")


__all__ = ["SCHEME", "UnixSocketsUnsupportedError", "UnixTarget", "UnixUrlError", "parse_unix_url"]
