"""The `ws+unix:` URL grammar: one URL carrying both a filesystem socket path and a request target.

    ws+unix:///run/muxws/api.sock:/ws?tenant=42

Dialling a Unix domain socket changes nothing about muxws. The handshake is still an HTTP GET with
`Upgrade: websocket` offering `muxws.v1.<codec>` first (WSM-CDC-020), still answered 101 or 400
(WSM-CDC-022), and the socket adapter above it cannot tell what carried the bytes (WSM-API-021). The
one thing AF_UNIX needs that a TCP URL never does is a way to say *which file* to open as well as
*which request target* to ask for, and this module is the whole of that: a string transformation
with no socket anywhere in it.

The grammar is the `ws` npm package's, so that one URL pasted into either port's configuration gets
the same connection: take the URL's pathname plus its query, split on `:`, the left half is the
filesystem path and the right half is the HTTP request target, which defaults to `/` when there is
no `:` at all.

Both ports split on the **first** colon, where `ws` itself splits on every one: `ws` does
`opts.path.split(':')` and keeps `parts[1]`, so `/a.sock:/r:x` becomes the target `/r` and
`/a.sock:/r?t=a:b` becomes `/r?t=a`, the remainder silently dropped. Neither end can see that
happen - the acceptor routes on the target it was given and the dialer never learns it was
truncated - so the two ports must agree on the rule rather than on `ws`'s implementation of it.

Stdlib only: the grammar has to be testable where AF_UNIX does not exist at all, and keeping
`websockets` out keeps this file off the optional-dependency map in `packaging_test.py`
(WSM-PKG-002).
"""

from __future__ import annotations

import socket as socketlib

from dataclasses import dataclass
from urllib.parse import urlsplit

from muxws.errors import TransportUnsupportedError, TransportUrlError

#: The scheme this module owns.
SCHEME = "ws+unix"

#: The scheme it refuses. There is no `wss+unix:` in either port: a filesystem socket is protected by
#: its directory's permissions and by `SO_PEERCRED`, not by a certificate, and there is no host name
#: for a certificate to be checked against. A deployment that wrote `wss+unix:` believes it has
#: transport security, so the scheme is refused by name rather than dialled in plaintext.
_REFUSED_SCHEME = "wss+unix"

#: What goes in the `Host:` header when the URL has no authority, which is the normal shape. The
#: handshake is still HTTP and HTTP still requires a `Host`; nothing on the other side routes on it.
_DEFAULT_AUTHORITY = "localhost"


class UnixUrlError(TransportUrlError):
    """A `ws+unix:` URL that cannot be dialled: no socket path, a bad target, or `wss+unix:`.

    Defined beside the transport that owns the grammar rather than in `muxws/errors.py`, and imported
    as `from muxws.transports.unix import UnixUrlError` (WSM-ERR-016). The base is shared:
    `TransportUrlError` is a `MuxwsError` and a `ValueError` both, so a caller catching either one
    catches this, and an application that does not know which transport a configured URL names can
    write `except TransportUrlError` without importing this module at all.
    """


class UnixSocketsUnsupportedError(TransportUnsupportedError):
    """This interpreter has no `AF_UNIX`, so a `ws+unix:` URL can never be dialled here.

    A `TransportUnsupportedError` (WSM-ERR-016) rather than a `TransportUrlError`: the URL is not the
    problem and no rewriting of it will help. The base carries `RuntimeError` alongside `MuxwsError`.

    Raised from the parse. `websockets`' `unix_connect` imports perfectly well on Windows and fails
    deep inside the dial with a bare `AttributeError` on `loop.create_unix_connection`, out of a
    background reconnect attempt rather than out of the `connect()` call that caused it.
    """


@dataclass(frozen=True, slots=True)
class UnixTarget:
    """Everything `unix_connect(path, uri=...)` needs, and nothing about muxws.

    Two fields: `path` names a file to open, `uri` is the *logical* URL the handshake claims to be
    for. `websockets` will not invent the second from the first - a WebSocket handshake is an HTTP
    request and still needs a `Host` and a request target even when the connection is a file.
    """

    #: The filesystem path of the listening socket. Note ~108 bytes is the kernel's limit for it.
    path: str

    #: `ws://<authority><target>` - what the peer believes it dialled, and what goes on the wire as
    #: the request line and the `Host` header.
    uri: str


def parse_unix_url(url: str) -> UnixTarget | None:
    """Split a `ws+unix:` URL into a socket path and a logical `ws://` URI; `None` for `ws:`/`wss:`.

    `None` rather than an exception for an ordinary TCP URL, because this is called on **every** URL
    `connect()` is given: it is the branch, not a validator. Anything that is neither `ws+unix:` nor
    a scheme this module refuses is handed back untouched for `websockets.connect()` to accept or
    reject on its own terms, so a typo'd `wss:/` URL is answered by the transport that owns it.

    Called once, from outside the dial closure, so that a malformed URL and a platform without
    AF_UNIX both surface synchronously out of `connect()` before any socket is touched - the same
    ordering guarantee `resolve_codec()` gives the codec (WSM-CDC-016) - and so that every reconnect
    re-dials the identical target rather than re-deriving it (WSM-CDC-020, WSM-AUT-003).
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

    # `ws` operates on `pathname + search`, so the query has to be put back before the split or
    # `ws+unix:///p.sock:/r?a=1` loses its `?a=1` on the way to the request target. `urlsplit` is the
    # one that separates them; WHATWG `new URL()` agrees with it on everything else that matters here.
    target = parts.path + (f"?{parts.query}" if parts.query else "")
    path, _, route = target.partition(":")

    if not path:
        raise UnixUrlError(
            f"{url!r} names no socket file: a {SCHEME}: URL is the socket's filesystem path followed "
            f"by an optional ':' and the request target, as in {SCHEME}:///run/muxws/api.sock:/ws"
        )
    if route and not route.startswith("/"):
        # `ws://localhost` + `ws` is `ws://localhostws`, which is a *valid* URL naming a host that
        # does not exist, so the target is folded into the authority: the dial opens the right socket
        # and then asks for `/` with a `Host` nobody chose. Against either acceptor this
        # library documents - `unix_serve`, or a `WebSocketServer` with no `path` - that handshake
        # **succeeds**, 101 and all, because neither routes on the target. A misdial that
        # connects is worse than one that fails, and the only place to catch it is before the dial.
        raise UnixUrlError(
            f"the request target in {url!r} must begin with '/': the part after the ':' is an HTTP "
            f"request target, not a path relative to anything"
        )

    return UnixTarget(path=path, uri=f"ws://{parts.netloc or _DEFAULT_AUTHORITY}{route or '/'}")


__all__ = ["SCHEME", "UnixSocketsUnsupportedError", "UnixTarget", "UnixUrlError", "parse_unix_url"]
