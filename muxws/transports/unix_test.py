"""The `ws+unix:` grammar, on every platform - including the ones that cannot dial one.

No test here opens a socket. CI runs on `ubuntu-latest` only, so the Windows behaviour is tested by a
table of strings that runs on every platform. A grammar inside the `websockets` adapter would need a
real acceptor, and its tests would be skipped where AF_UNIX is missing.
"""

from __future__ import annotations

import socket as socketlib

import pytest

from muxws.errors import MuxwsError, TransportUnsupportedError, TransportUrlError
from muxws.transports.unix import parse_unix_url, UnixSocketsUnsupportedError, UnixUrlError


@pytest.mark.parametrize(
    ("url", "path", "uri"),
    [
        ("ws+unix:///run/muxws/api.sock:/ws", "/run/muxws/api.sock", "ws://localhost/ws"),
        ("ws+unix:///run/muxws/api.sock", "/run/muxws/api.sock", "ws://localhost/"),
        ("ws+unix:///run/muxws/api.sock:", "/run/muxws/api.sock", "ws://localhost/"),
        ("ws+unix:///run/muxws/api.sock:/ws?tenant=42", "/run/muxws/api.sock", "ws://localhost/ws?tenant=42"),
        ("ws+unix://gateway/run/muxws/api.sock:/ws", "/run/muxws/api.sock", "ws://gateway/ws"),
        ("ws+unix:/run/muxws/api.sock:/ws", "/run/muxws/api.sock", "ws://localhost/ws"),
        ("WS+UNIX:///run/muxws/api.sock:/ws", "/run/muxws/api.sock", "ws://localhost/ws"),
        ("ws+unix:///run/muxws/api.sock:/ws:v2", "/run/muxws/api.sock", "ws://localhost/ws:v2"),
    ],
    ids=[
        "path-and-target",
        "no-colon-defaults-the-target",
        "an-empty-target-is-also-the-default",
        "the-query-belongs-to-the-target",
        "an-authority-becomes-the-host-header",
        "no-authority-at-all",
        "the-scheme-is-case-insensitive",
        "only-the-first-colon-splits",
    ],
)
def test_the_grammar_splits_a_socket_path_from_a_request_target(url: str, path: str, uri: str):
    """The `ws` package's grammar, case by case, because the two ports must agree on every row.

    A URL a deployment pastes into its configuration must reach the same socket with the same request
    target under both implementations, so the table is the contract. Notable rows: a missing colon
    defaults the target to `/`, not to the empty string that would put `GET  HTTP/1.1` on the wire;
    the query is re-attached before the split, because `urlsplit` returns it separately, so it
    belongs to the target when a colon precedes it and to the file name otherwise; and the authority
    is the `Host` header the acceptor sees.

    `only-the-first-colon-splits` is a row the `ws` package handles differently: it splits on every
    colon and keeps `parts[1]`, reading `/ws:v2` as `/ws`, so `ts/node.ts` performs the split before
    `ws` sees the URL. The TypeScript counterpart is `splits on the first colon only` in
    `ts/unix.spec.ts`, which asserts the target a real acceptor receives.
    """
    target = parse_unix_url(url)

    assert target is not None, f"{url!r} is a ws+unix: URL and must be recognised as one"
    assert (target.path, target.uri) == (path, uri)


@pytest.mark.parametrize("url", ["ws://127.0.0.1:8000/ws", "wss://example.test/ws", "ws://[::1]:9000/"])
def test_a_tcp_url_is_handed_back_untouched(url: str):
    """`None` for anything that is not `ws+unix:`, because this is the branch and not a validator.

    Every URL passed to `connect()` goes through the parser. If it raised on other URLs, a typo in an
    ordinary `ws://` host would produce an error from this module instead of the one `websockets`
    raises, and this module has no TCP-specific information to report.
    """
    assert parse_unix_url(url) is None


def test_tls_over_a_filesystem_socket_is_refused_by_name():
    """`wss+unix:` is not supported and raises rather than dialling plaintext.

    Treating it as `ws+unix:` would connect, since the socket is the same file, without the transport
    security the URL requests. The error names the scheme it was given.
    """
    with pytest.raises(UnixUrlError, match="wss\\+unix"):
        parse_unix_url("wss+unix:///run/muxws/api.sock:/ws")


@pytest.mark.parametrize(
    "url",
    ["ws+unix://", "ws+unix://gateway", "ws+unix::/ws"],
    ids=["nothing-after-the-scheme", "an-authority-and-nothing-else", "a-target-and-no-path-at-all"],
)
def test_a_url_naming_no_socket_file_is_refused(url: str):
    """An empty socket path is refused here, where the message can still say what the URL should be.

    `unix_connect(None)` raises `ValueError: no path and sock were specified` and `unix_connect("")`
    an `OSError`; neither names the URL that produced it, and both are raised from inside the dial.
    This check runs before the dial and raises from `connect()`.
    """
    with pytest.raises(UnixUrlError, match="socket file"):
        parse_unix_url(url)


def test_a_request_target_that_is_not_an_absolute_path_is_refused():
    """`…sock:ws` is refused rather than repaired, because repairing it dials the wrong thing.

    The target is inserted into `ws://<authority><target>`, so a target with no leading slash gives
    `ws://localhostws`, a valid URL whose authority includes the target. The dial succeeds, because
    the socket path is correct, and the acceptor sees a request for `/` with a wrong `Host`: against
    `unix_serve` the result is 101, `muxws.v1.json` negotiated and the handler run, since neither
    acceptor this library documents routes on the target. `ts/node.ts` refuses the same shape; there
    node answers `GET ws HTTP/1.1` with a 400 that the dialer reports as a codec mismatch.
    """
    with pytest.raises(UnixUrlError, match="must begin with"):
        parse_unix_url("ws+unix:///run/muxws/api.sock:ws")


def test_a_platform_without_af_unix_is_told_so_by_name(monkeypatch: pytest.MonkeyPatch):
    """Windows: a named error out of the parse, not an `AttributeError` out of the dial.

    `websockets`' `unix_connect` imports where AF_UNIX does not exist and fails at
    `loop.create_unix_connection`, which a Windows event loop does not define. The resulting
    `AttributeError` names neither the URL nor the platform, and because the reconnect helper calls
    the dial closure, it can be raised long after the `connect()` call. The test deletes the
    attribute to simulate this on Linux, where CI runs.
    """
    monkeypatch.delattr(socketlib, "AF_UNIX", raising=False)

    with pytest.raises(UnixSocketsUnsupportedError, match="AF_UNIX"):
        parse_unix_url("ws+unix:///run/muxws/api.sock:/ws")


def test_both_refusals_are_catchable_as_muxws_errors_and_as_what_they_are():
    """Both errors are catchable as `MuxwsError` and as their builtin base.

    An application that wraps its startup in `except MuxwsError` catches them. Code that does not
    know about muxws catches `ValueError` around a URL it built and `RuntimeError` around an
    unsupported platform; the documented `connect()` contract names these classes.
    """
    assert issubclass(UnixUrlError, MuxwsError)
    assert issubclass(UnixUrlError, ValueError)
    assert issubclass(UnixSocketsUnsupportedError, MuxwsError)
    assert issubclass(UnixSocketsUnsupportedError, RuntimeError)


def test_a_malformed_unix_url_is_a_transport_url_error_and_a_missing_af_unix_is_not():
    """WSM-ERR-016: the two refusals sit under the two shared bases, and not under the same one.

    An application that does not know which transport a configured URL names writes
    `except TransportUrlError` around `connect()` and catches a bad `ws+unix:` address and a bad `ws:`
    one with the same handler. The negatives are asserted too: a missing `AF_UNIX` is not a bad
    address, and reporting it as one would point at a correct configuration.
    """
    assert issubclass(UnixUrlError, TransportUrlError)
    assert not issubclass(UnixUrlError, TransportUnsupportedError)

    assert issubclass(UnixSocketsUnsupportedError, TransportUnsupportedError)
    assert not issubclass(UnixSocketsUnsupportedError, TransportUrlError)

    with pytest.raises(TransportUrlError):
        parse_unix_url("ws+unix://")
