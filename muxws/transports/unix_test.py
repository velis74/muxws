"""The `ws+unix:` grammar, on every platform - including the ones that cannot dial one.

Not one test here opens a socket, and that is the whole design of the module under test. CI runs on
`ubuntu-latest` only, so the Windows behaviour has exactly one witness: a table of strings that runs
wherever pytest does. A grammar that lived in the `websockets` adapter would need a real acceptor to
be exercised, would be skipped wholesale where AF_UNIX is missing, and would take the portability
guard down with it - the guard that exists precisely for that platform.
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

    A URL a deployment pastes into its configuration has to reach the same socket with the same
    request target under both implementations, so the table is the contract rather than the prose
    above it. Three of these rows are the ones an independent reimplementation gets wrong: the
    missing colon defaulting to `/` rather than to the empty string that would put `GET  HTTP/1.1` on
    the wire; the query travelling with the *target* and not with the path, since `urlsplit` hands it
    back separately and it is lost unless it is put back; and the authority - decorative for the
    dial, but the `Host` header the acceptor sees.

    The eighth row is the one `ws` itself gets wrong, which is why it is here twice over: `ws` splits
    on every colon and keeps `parts[1]`, reading `/ws:v2` as `/ws`, so `ts/node.ts` performs this
    split before `ws` sees the URL. The TypeScript half of the row is
    `splits on the first colon only` in `ts/unix.spec.ts`, which asserts the target a real acceptor
    was handed - the only place the agreement between the two ports is observable at all.
    """
    target = parse_unix_url(url)

    assert target is not None, f"{url!r} is a ws+unix: URL and must be recognised as one"
    assert (target.path, target.uri) == (path, uri)


@pytest.mark.parametrize("url", ["ws://127.0.0.1:8000/ws", "wss://example.test/ws", "ws://[::1]:9000/"])
def test_a_tcp_url_is_handed_back_untouched(url: str):
    """`None` for anything that is not `ws+unix:`, because this is the branch and not a validator.

    Every URL `connect()` is given passes through the parser. Were it to raise on a URL it does not
    own, the error a caller gets for a typo in an ordinary `ws://` host would stop being the one
    `websockets` has always given them and start being one from this module, which knows nothing
    about TCP and has nothing useful to say about it.
    """
    assert parse_unix_url(url) is None


def test_tls_over_a_filesystem_socket_is_refused_by_name():
    """`wss+unix:` does not exist, and it must fail loudly rather than dial plaintext.

    Silently treating it as `ws+unix:` would connect - the socket is the same file - and leave a
    deployment convinced it had transport security it never had. The error names the scheme it was
    given, which is the only way the author of the URL learns which of the two they wanted.
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
    an `OSError`; neither names the URL that produced it, and both arrive from inside the dial. The
    check is worth its three lines only because it happens before the dial, out of `connect()`.
    """
    with pytest.raises(UnixUrlError, match="socket file"):
        parse_unix_url(url)


def test_a_request_target_that_is_not_an_absolute_path_is_refused():
    """`…sock:ws` is refused rather than repaired, because repairing it dials the wrong thing.

    The target is pasted straight into `ws://<authority><target>`, so a target with no leading slash
    makes `ws://localhostws` - a syntactically perfect URL naming a host that does not exist, into
    whose authority the target has just been folded. The dial then succeeds, because the socket path
    was right, and what arrives at the acceptor is a request for `/` carrying a nonsense `Host`.
    Measured against `unix_serve`: 101, negotiated `muxws.v1.json`, handler run. Neither acceptor this
    library documents routes on the target, so nothing fails and nothing is logged - the connection is
    simply not the one the URL asked for. `ts/node.ts` refuses the same shape, where the alternative
    is worse still: node answers `GET ws HTTP/1.1` with a 400 that the dialer reads as a codec
    mismatch.
    """
    with pytest.raises(UnixUrlError, match="must begin with"):
        parse_unix_url("ws+unix:///run/muxws/api.sock:ws")


def test_a_platform_without_af_unix_is_told_so_by_name(monkeypatch: pytest.MonkeyPatch):
    """Windows: a named error out of the parse, not an `AttributeError` out of the dial.

    `websockets`' `unix_connect` imports perfectly well where AF_UNIX does not exist and fails at
    `loop.create_unix_connection`, which a Windows event loop simply does not define. The resulting
    `AttributeError` names neither the URL nor the platform nor the reason, and because the dial
    closure is what the reconnect helper calls, it can arrive minutes after the `connect()` that
    caused it. Deleting the attribute is how this is witnessed on Linux, where CI runs.
    """
    monkeypatch.delattr(socketlib, "AF_UNIX", raising=False)

    with pytest.raises(UnixSocketsUnsupportedError, match="AF_UNIX"):
        parse_unix_url("ws+unix:///run/muxws/api.sock:/ws")


def test_both_refusals_are_catchable_as_muxws_errors_and_as_what_they_are():
    """The double inheritance is a promise to two different callers, so it is asserted.

    An application that wraps its startup in `except MuxwsError` must catch these, or a bad URL
    escapes a handler that was written to be exhaustive. A caller who never heard of this library
    catches `ValueError` around a URL it built and `RuntimeError` around a platform it cannot help,
    and those are the classes the documented `connect()` contract points at.
    """
    assert issubclass(UnixUrlError, MuxwsError)
    assert issubclass(UnixUrlError, ValueError)
    assert issubclass(UnixSocketsUnsupportedError, MuxwsError)
    assert issubclass(UnixSocketsUnsupportedError, RuntimeError)


def test_a_malformed_unix_url_is_a_transport_url_error_and_a_missing_af_unix_is_not():
    """WSM-ERR-016: the two refusals sit under the two shared bases, and not under the same one.

    The test above holds the older contract - both builtins, both `MuxwsError`s - and this one holds
    what the shared bases added: an application that does not know which transport a configured URL
    names can write `except TransportUrlError` around `connect()` and catch a bad `ws+unix:` address
    and a bad `ws:` one with the same handler. That is only worth anything if the two bases stay
    distinct, so the negatives are asserted too. A missing `AF_UNIX` is not a bad address: no rewriting
    of the URL will help, and reporting it as one would send the reader to edit configuration that was
    already correct.
    """
    assert issubclass(UnixUrlError, TransportUrlError)
    assert not issubclass(UnixUrlError, TransportUnsupportedError)

    assert issubclass(UnixSocketsUnsupportedError, TransportUnsupportedError)
    assert not issubclass(UnixSocketsUnsupportedError, TransportUrlError)

    with pytest.raises(TransportUrlError):
        parse_unix_url("ws+unix://")
