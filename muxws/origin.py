"""The `Origin` check `accept()` runs when it is given `allowed_origins`.

A browser sends the cookies of the endpoint's site with a WebSocket handshake whatever page opened
it, and a WebSocket has no CSRF protection of its own. An endpoint whose session is a cookie answers
a handshake from another site's page with that session unless the `Origin` header is checked before
the socket is accepted.

`ts/origin.ts` implements the same parsing and the same decision.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Any

#: The default port of each scheme a serialized origin may carry.
DEFAULT_PORTS = {"http": 80, "https": 443}

#: The `allowed_origins` argument: exact origins, or a function of the `Origin` header value.
OriginPolicy = Iterable[str] | Callable[[str | None], bool]

#: A compiled policy: the `Origin` header values of one request in, the decision out.
OriginCheck = Callable[[Sequence[str]], bool]


def parse_origin(value: str) -> tuple[str, str, int] | None:
    """`(scheme, host, port)` of a serialized origin, or None when `value` is not one.

    An origin is `scheme://host[:port]` with the scheme `http` or `https`, surrounding whitespace and
    one trailing `/` ignored. Scheme and host are lower-cased, and a missing port is the scheme's
    default (80, 443). A path, a query, a fragment, userinfo, an empty or non-numeric port, a port
    above 65535, another scheme and `null` all return None.
    """
    text = value.strip()
    if text.endswith("/"):
        text = text[:-1]
    scheme, separator, authority = text.partition("://")
    scheme = scheme.lower()
    if not separator or scheme not in DEFAULT_PORTS:
        return None
    if not authority or any(char in authority for char in "/?#@") or any(char.isspace() for char in authority):
        return None

    if authority.startswith("["):
        end = authority.find("]")
        if end < 2:
            return None
        host, rest = authority[: end + 1], authority[end + 1 :]
        if rest and not rest.startswith(":"):
            return None
        port_text = rest[1:] if rest else None
    else:
        if authority.count(":") > 1:
            return None
        host, colon, port_text_or_empty = authority.partition(":")
        port_text = port_text_or_empty if colon else None
        if not host:
            return None

    if port_text is None:
        port = DEFAULT_PORTS[scheme]
    else:
        if not port_text or not (port_text.isascii() and port_text.isdigit()):
            return None
        port = int(port_text)
        if port > 65535:
            return None
    return scheme, host.lower(), port


def compile_policy(allowed_origins: OriginPolicy | None) -> OriginCheck | None:
    """The check `accept()` runs for `allowed_origins`, or None when there is nothing to check.

    - A callable is called with the request's single `Origin` value, or with None when the request
      has none; its truthiness is the decision. A request with more than one `Origin` header is
      refused without calling it.
    - An iterable of origins allows a request whose single `Origin` value parses to one of them
      under `parse_origin`. A request without `Origin`, with more than one, or with a value that is
      not an origin (`null` included) is refused.

    Raises `TypeError` for a `str` (an iterable of its characters) and `ValueError` for an entry that
    is not an origin.
    """
    if allowed_origins is None:
        return None
    if callable(allowed_origins):
        decide = allowed_origins

        def check_callable(values: Sequence[str]) -> bool:
            if len(values) > 1:
                return False
            return bool(decide(values[0] if values else None))

        return check_callable

    if isinstance(allowed_origins, (str, bytes)):
        raise TypeError("allowed_origins is an iterable of origins or a callable, not a single string")
    allowed = set()
    for entry in allowed_origins:
        parsed = parse_origin(entry) if isinstance(entry, str) else None
        if parsed is None:
            raise ValueError(f"{entry!r} in allowed_origins is not an origin: scheme://host[:port]")
        allowed.add(parsed)

    def check_list(values: Sequence[str]) -> bool:
        if len(values) != 1:
            return False
        return parse_origin(values[0]) in allowed

    return check_list


def origin_values(socket: Any) -> list[str] | None:
    """Every `Origin` header value of the handshake request behind `socket`, in order.

    Reads a Starlette `WebSocket`, a `websockets` server connection (`request.headers` in the asyncio
    implementation, `request_headers` in the legacy one), and a `WebsocketsSocket` wrapping either.
    None when `socket` exposes no request headers.
    """
    from muxws.transports.websockets_ import WebsocketsSocket

    if isinstance(socket, WebsocketsSocket):
        socket = socket._connection
    scope = getattr(socket, "scope", None)
    if isinstance(scope, dict) and "headers" in scope:
        return [value.decode("latin-1") for name, value in scope["headers"] if name.lower() == b"origin"]
    headers = getattr(getattr(socket, "request", None), "headers", None)
    if headers is None:
        headers = getattr(socket, "request_headers", None)
    if headers is None or not hasattr(headers, "get_all"):
        return None
    return list(headers.get_all("Origin"))
