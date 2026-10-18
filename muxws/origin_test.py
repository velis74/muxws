"""`parse_origin` and `compile_policy`, the `Origin` check behind `accept(allowed_origins=...)`."""

from __future__ import annotations

import pytest

from muxws.origin import compile_policy, parse_origin


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
    assert check(["https://app.example.com:443"])
    assert check(["http://LOCALHOST:5173"])
    assert not check(["http://app.example.com"])
    assert not check(["https://evil.example.com"])


def test_a_list_refuses_a_missing_a_repeated_and_an_unparsable_origin():
    check = compile_policy(["https://app.example.com"])
    assert check is not None
    assert not check([])
    assert not check(["https://app.example.com", "https://app.example.com"])
    assert not check(["null"])


def test_an_empty_list_refuses_everything():
    check = compile_policy([])
    assert check is not None
    assert not check(["https://app.example.com"])


def test_a_callable_receives_the_raw_value_or_none():
    seen: list[str | None] = []

    def decide(origin: str | None) -> bool:
        seen.append(origin)
        return origin is None or origin.endswith(".example.com")

    check = compile_policy(decide)
    assert check is not None
    assert check([])
    assert check(["https://a.example.com"])
    assert not check(["https://evil.test"])
    assert seen == [None, "https://a.example.com", "https://evil.test"]


def test_a_callable_is_not_called_for_a_repeated_origin():
    check = compile_policy(lambda _origin: True)
    assert check is not None
    assert not check(["https://a.example.com", "https://b.example.com"])


def test_a_single_string_is_a_type_error():
    with pytest.raises(TypeError, match="not a single string"):
        compile_policy("https://app.example.com")


@pytest.mark.parametrize("entry", ["app.example.com", "https://app.example.com/path", "null", 443])
def test_an_entry_that_is_not_an_origin_is_a_value_error(entry: object):
    with pytest.raises(ValueError, match="not an origin"):
        compile_policy([entry])  # type: ignore[list-item]
