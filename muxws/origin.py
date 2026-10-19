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

#: Every value of one request header, looked up by lower-case name, in order.
HeaderValues = Callable[[str], Sequence[str]]

#: A compiled policy: the request headers of one handshake in, the decision out.
OriginCheck = Callable[[HeaderValues], bool]


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
    parsed = _parse_authority(authority)
    if parsed is None:
        return None
    host, port = parsed
    return scheme, host, DEFAULT_PORTS[scheme] if port is None else port


def _parse_authority(authority: str) -> tuple[str, int | None] | None:
    """`(host, port)` of `host[:port]`, host lower-cased and port None when absent; None when invalid.

    The form of the authority in an origin and of a `Host` header value.
    """
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
        return host.lower(), None
    if not port_text or not (port_text.isascii() and port_text.isdigit()):
        return None
    port = int(port_text)
    if port > 65535:
        return None
    return host.lower(), port


def _parse_origins(origins: Iterable[str], argument: str) -> set[tuple[str, str, int]]:
    """Every entry of `origins` under `parse_origin`; `TypeError` for a `str`, `ValueError` for a non-origin."""
    if isinstance(origins, (str, bytes)):
        raise TypeError(f"{argument} is an iterable of origins, not a single string")
    parsed_origins = set()
    for entry in origins:
        parsed = parse_origin(entry) if isinstance(entry, str) else None
        if parsed is None:
            raise ValueError(f"{entry!r} in {argument} is not an origin: scheme://host[:port]")
        parsed_origins.add(parsed)
    return parsed_origins


def _single_origin(headers: HeaderValues) -> tuple[str, str, int] | None:
    """The request's `Origin` under `parse_origin`, or None when it is missing, repeated or not an origin."""
    values = headers("origin")
    if len(values) != 1:
        return None
    return parse_origin(values[0])


class SameOrigin:
    """The `allowed_origins` policy that allows a page served by the host the handshake is addressed to.

    A request is allowed when its single `Origin` has the host and port of the request's single `Host`
    header, or is one of the origins in `also`. A `Host` without a port stands for the default port
    of the origin's scheme. The scheme is not compared: `Host` carries none, and behind a proxy that
    terminates TLS the handshake reaching the acceptor is plain `ws`.

    With `trust_forwarded_host`, the first entry of `X-Forwarded-Host` takes the place of `Host` when
    the request has that header; a request without it is compared with `Host`. A browser cannot set
    either header on a WebSocket handshake, so either one names the host the page addressed. Without
    `trust_forwarded_host`, a proxy that rewrites `Host` to the upstream's name makes every browser
    request fail the comparison.

    A request without `Origin`, with more than one, with a value that is not an origin (`null`
    included), or with no single parsable `Host` (or `X-Forwarded-Host` entry) is refused unless its
    origin is in `also`.

    Raises `TypeError` when `also` is a single `str` and `ValueError` for an `also` entry that is not
    an origin.
    """

    def __init__(self, *, also: Iterable[str] = (), trust_forwarded_host: bool = False) -> None:
        self._also = frozenset(_parse_origins(also, "also"))
        self._trust_forwarded_host = trust_forwarded_host

    def __repr__(self) -> str:
        also = sorted(f"{scheme}://{host}:{port}" for scheme, host, port in self._also)
        return f"SameOrigin(also={also!r}, trust_forwarded_host={self._trust_forwarded_host!r})"

    def _request_host(self, headers: HeaderValues) -> tuple[str, int | None] | None:
        if self._trust_forwarded_host:
            forwarded = headers("x-forwarded-host")
            if forwarded:
                return _parse_authority(",".join(forwarded).split(",")[0].strip())
        hosts = headers("host")
        if len(hosts) != 1:
            return None
        return _parse_authority(hosts[0].strip())

    def _allows(self, headers: HeaderValues) -> bool:
        origin = _single_origin(headers)
        if origin is None:
            return False
        if origin in self._also:
            return True
        request_host = self._request_host(headers)
        if request_host is None:
            return False
        scheme, host, port = origin
        request_name, request_port = request_host
        return request_name == host and (DEFAULT_PORTS[scheme] if request_port is None else request_port) == port


#: The `allowed_origins` argument: exact origins, `SameOrigin`, or a function of the `Origin` header value.
OriginPolicy = Iterable[str] | SameOrigin | Callable[[str | None], bool]


def compile_policy(allowed_origins: OriginPolicy | None) -> OriginCheck | None:
    """The check `accept()` runs for `allowed_origins`, or None when there is nothing to check.

    - A `SameOrigin` compares `Origin` with the request's host; see `SameOrigin`.
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
    if isinstance(allowed_origins, SameOrigin):
        return allowed_origins._allows
    if callable(allowed_origins):
        decide = allowed_origins

        def check_callable(headers: HeaderValues) -> bool:
            values = headers("origin")
            if len(values) > 1:
                return False
            return bool(decide(values[0] if values else None))

        return check_callable

    if isinstance(allowed_origins, (str, bytes)):
        raise TypeError("allowed_origins is an iterable of origins or a callable, not a single string")
    allowed = _parse_origins(allowed_origins, "allowed_origins")

    def check_list(headers: HeaderValues) -> bool:
        origin = _single_origin(headers)
        return origin is not None and origin in allowed

    return check_list


def request_headers(socket: Any) -> HeaderValues | None:
    """The handshake request headers behind `socket`, as a lookup of every value of one header.

    Reads a Starlette `WebSocket`, a `websockets` server connection (`request.headers` in the asyncio
    implementation, `request_headers` in the legacy one), and a `WebsocketsSocket` wrapping either.
    None when `socket` exposes no request headers.
    """
    from muxws.transports.websockets_ import WebsocketsSocket

    if isinstance(socket, WebsocketsSocket):
        socket = socket._connection
    scope = getattr(socket, "scope", None)
    if isinstance(scope, dict) and "headers" in scope:
        raw = scope["headers"]

        def scope_values(name: str) -> list[str]:
            key = name.encode("latin-1")
            return [value.decode("latin-1") for header, value in raw if header.lower() == key]

        return scope_values
    headers = getattr(getattr(socket, "request", None), "headers", None)
    if headers is None:
        headers = getattr(socket, "request_headers", None)
    if headers is None or not hasattr(headers, "get_all"):
        return None
    found = headers

    def header_values(name: str) -> list[str]:
        return list(found.get_all(name))

    return header_values
