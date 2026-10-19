"""`parse_origin`, `SameOrigin` and `compile_policy`, the `Origin` check behind `accept(allowed_origins=...)`."""

from __future__ import annotations

from collections.abc import Iterable

import pytest

from muxws.origin import compile_policy, HeaderValues, parse_origin, SameOrigin


def origins(*values: str) -> HeaderValues:
    """A request whose only headers are `values` as its `Origin` header lines."""
    return headers(("origin", value) for value in values)


def headers(pairs: Iterable[tuple[str, str]]) -> HeaderValues:
    """A request with the header lines `pairs`, looked up by lower-case name."""
    lines = list(pairs)
    return lambda name: [value for header, value in lines if header == name]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://example.com", ("https", "example.com", 443)),
        ("http://example.com", ("http", "example.com", 80)),
        ("HTTPS://Example.COM", ("https", "example.com", 443)),
        ("https://example.com:443", ("https", "example.com", 443)),
        ("https://example.com/", ("https", "example.com", 443)),
        (" https://example.com ", ("https", "example.com", 443)),
        ("http://localhost:5173", ("http", "localhost", 5173)),
        ("http://[::1]:8000", ("http", "[::1]", 8000)),
        ("http://[::1]", ("http", "[::1]", 80)),
    ],
)
def test_an_origin_parses_to_scheme_host_and_explicit_port(value: str, expected: tuple[str, str, int]):
    assert parse_origin(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "null",
        "",
        "example.com",
        "ftp://example.com",
        "ws://example.com",
        "https://example.com/path",
        "https://example.com?query",
        "https://example.com#fragment",
        "https://user@example.com",
        "https://example.com:",
        "https://example.com:port",
        "https://example.com:65536",
        "https://example.com:+80",
        "https://a:1:2",
        "https://",
        "https://:443",
        "http://[]",
        "http://[::1]x",
        "https://exa mple.com",
    ],
)
def test_anything_else_is_not_an_origin(value: str):
    assert parse_origin(value) is None


def test_no_policy_compiles_to_no_check():
    assert compile_policy(None) is None


def test_a_listed_origin_is_allowed_after_normalisation():
    check = compile_policy(["https://App.example.com", "http://localhost:5173/"])
    assert check is not None
    assert check(origins("https://app.example.com:443"))
    assert check(origins("http://LOCALHOST:5173"))
    assert not check(origins("http://app.example.com"))
    assert not check(origins("https://evil.example.com"))


def test_a_list_refuses_a_missing_a_repeated_and_an_unparsable_origin():
    check = compile_policy(["https://app.example.com"])
    assert check is not None
    assert not check(origins())
    assert not check(origins("https://app.example.com", "https://app.example.com"))
    assert not check(origins("null"))


def test_an_empty_list_refuses_everything():
    check = compile_policy([])
    assert check is not None
    assert not check(origins("https://app.example.com"))


def test_a_callable_receives_the_raw_value_or_none():
    seen: list[str | None] = []

    def decide(origin: str | None) -> bool:
        seen.append(origin)
        return origin is None or origin.endswith(".example.com")

    check = compile_policy(decide)
    assert check is not None
    assert check(origins())
    assert check(origins("https://a.example.com"))
    assert not check(origins("https://evil.test"))
    assert seen == [None, "https://a.example.com", "https://evil.test"]


def test_a_callable_is_not_called_for_a_repeated_origin():
    check = compile_policy(lambda _origin: True)
    assert check is not None
    assert not check(origins("https://a.example.com", "https://b.example.com"))


def test_a_single_string_is_a_type_error():
    with pytest.raises(TypeError, match="not a single string"):
        compile_policy("https://app.example.com")


@pytest.mark.parametrize("entry", ["app.example.com", "https://app.example.com/path", "null", 443])
def test_an_entry_that_is_not_an_origin_is_a_value_error(entry: object):
    with pytest.raises(ValueError, match="not an origin"):
        compile_policy([entry])  # type: ignore[list-item]


def same_origin(policy: SameOrigin, *lines: tuple[str, str]) -> bool:
    check = compile_policy(policy)
    assert check is not None
    return check(headers(lines))


@pytest.mark.parametrize(
    ("origin", "host"),
    [
        ("https://app.example.com", "app.example.com"),
        ("http://app.example.com", "app.example.com"),
        ("https://APP.example.com", "app.EXAMPLE.com"),
        ("https://app.example.com", "app.example.com:443"),
        ("http://app.example.com", "app.example.com:80"),
        ("http://localhost:5173", "localhost:5173"),
        ("http://[::1]:8000", "[::1]:8000"),
    ],
)
def test_same_origin_allows_the_host_and_port_of_the_host_header(origin: str, host: str):
    assert same_origin(SameOrigin(), ("origin", origin), ("host", host))


@pytest.mark.parametrize(
    ("origin", "host"),
    [
        ("https://evil.test", "app.example.com"),
        ("https://app.example.com", "app.example.com:8443"),
        ("https://app.example.com:8443", "app.example.com"),
        ("http://app.example.com", "app.example.com:443"),
        ("https://sub.app.example.com", "app.example.com"),
        ("null", "app.example.com"),
    ],
)
def test_same_origin_refuses_another_host_or_port(origin: str, host: str):
    assert not same_origin(SameOrigin(), ("origin", origin), ("host", host))


@pytest.mark.parametrize(
    "lines",
    [
        [("host", "app.example.com")],
        [("origin", "https://app.example.com"), ("origin", "https://app.example.com"), ("host", "app.example.com")],
        [("origin", "https://app.example.com")],
        [("origin", "https://app.example.com"), ("host", "app.example.com"), ("host", "app.example.com")],
        [("origin", "https://app.example.com"), ("host", "app.example.com/path")],
        [("origin", "https://app.example.com"), ("host", "")],
    ],
    ids=["no-origin", "repeated-origin", "no-host", "repeated-host", "invalid-host", "empty-host"],
)
def test_same_origin_refuses_a_missing_or_repeated_header(lines: list[tuple[str, str]]):
    assert not same_origin(SameOrigin(), *lines)


def test_same_origin_ignores_x_forwarded_host_unless_trusted():
    lines = [("origin", "https://app.example.com"), ("host", "backend:8000"), ("x-forwarded-host", "app.example.com")]
    assert not same_origin(SameOrigin(), *lines)
    assert same_origin(SameOrigin(trust_forwarded_host=True), *lines)


def test_a_trusted_x_forwarded_host_replaces_host():
    policy = SameOrigin(trust_forwarded_host=True)
    origin = ("origin", "https://app.example.com")
    assert not same_origin(policy, origin, ("host", "app.example.com"), ("x-forwarded-host", "other.example.com"))
    assert same_origin(policy, origin, ("host", "backend"), ("x-forwarded-host", "app.example.com, proxy.internal"))
    assert same_origin(
        policy, origin, ("host", "backend"), ("x-forwarded-host", "app.example.com"), ("x-forwarded-host", "b")
    )
    assert not same_origin(policy, origin, ("host", "backend"), ("x-forwarded-host", ", app.example.com"))


def test_a_trusted_x_forwarded_host_falls_back_to_host_when_absent():
    policy = SameOrigin(trust_forwarded_host=True)
    assert same_origin(policy, ("origin", "http://localhost:8000"), ("host", "localhost:8000"))


def test_same_origin_also_allows_the_listed_origins():
    policy = SameOrigin(also=["http://localhost:5173"])
    assert same_origin(policy, ("origin", "http://LOCALHOST:5173/"), ("host", "localhost:8000"))
    assert same_origin(policy, ("origin", "http://localhost:5173"))
    assert same_origin(policy, ("origin", "http://localhost:8000"), ("host", "localhost:8000"))
    assert not same_origin(policy, ("origin", "http://localhost:3000"), ("host", "localhost:8000"))


def test_same_origin_reads_also_once():
    policy = SameOrigin(also=(origin for origin in ["https://a.test"]))
    assert same_origin(policy, ("origin", "https://a.test"))
    assert same_origin(policy, ("origin", "https://a.test"))


def test_same_origin_validates_also_on_construction():
    with pytest.raises(TypeError, match="not a single string"):
        SameOrigin(also="https://app.example.com")
    with pytest.raises(ValueError, match="in also is not an origin"):
        SameOrigin(also=["app.example.com"])


def test_parse_origin_is_exported_from_the_package():
    import muxws

    assert muxws.parse_origin is parse_origin
    assert muxws.SameOrigin is SameOrigin
