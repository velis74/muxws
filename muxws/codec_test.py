"""Codec selection and the subprotocol assertion (§2.2, §2.3)."""

from __future__ import annotations

import logging

from typing import Any

import pytest

import muxws

from muxws.api import resolve_codec
from muxws.codecs import register_codec
from muxws.codecs.json_ import JsonCodec
from muxws.conf import Settings, settings
from muxws.errors import CodecMismatch, CodecNotRegistered
from muxws.subprotocol import find_offer, mismatch_error, offer, PREFIX, select
from muxws.transports.websockets_ import POLICY_VIOLATION, verify_negotiated


@pytest.fixture
def configured_codec(monkeypatch: pytest.MonkeyPatch):
    """Set `settings.codec` for one test and put it back afterwards."""

    def _set(name: str) -> None:
        monkeypatch.setattr(settings, "codec", name)

    return _set


class _DialRecorder:
    """Stands in for the socket layer, so 'no socket was opened' is a fact rather than a hope."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def connect(self, url: str, **_kwargs: Any) -> Any:
        self.calls.append(url)
        raise AssertionError("a socket must not be opened when the codec cannot be resolved")


async def test_unregistered_name_raises_before_socket(configured_codec, monkeypatch: pytest.MonkeyPatch):
    """WSM-CDC-016 **(spec)**: raise at startup, before any socket, naming variable/value/set."""
    recorder = _DialRecorder()
    import websockets

    monkeypatch.setattr(websockets, "connect", recorder.connect)
    configured_codec("msgpack")

    with pytest.raises(CodecNotRegistered) as info:
        await muxws.connect("ws://localhost:1/ws")

    assert recorder.calls == [], "no socket may be opened before the codec resolves"
    message = str(info.value)
    assert "MUXWS_CODEC" in message
    assert "msgpack" in message
    assert "json" in message


async def test_no_fallback_to_json_ever(configured_codec):
    """WSM-INV-015: a deployment that believes it runs msgpack must never silently run JSON."""
    configured_codec("msgpack")
    with pytest.raises(CodecNotRegistered):
        resolve_codec()


def test_env_default_and_runtime_override(monkeypatch: pytest.MonkeyPatch):
    """WSM-CDC-010/011: default `json`, reads MUXWS_CODEC, writable at runtime."""
    monkeypatch.delenv("MUXWS_CODEC", raising=False)
    assert Settings().codec == "json"

    monkeypatch.setenv("MUXWS_CODEC", "msgpack")
    assert Settings().codec == "msgpack"

    fresh = Settings()
    fresh.codec = "json"
    assert fresh.codec == "json"
    monkeypatch.setenv("MUXWS_CODEC", "other")
    fresh.reload()
    assert fresh.codec == "other"


def test_settings_is_read_at_call_time_not_import_time(configured_codec):
    """An application setting `settings.codec` during bootstrap must be obeyed."""
    marker = JsonCodec()
    register_codec("bootstrap-set", marker)
    try:
        configured_codec("bootstrap-set")
        assert resolve_codec() is marker
    finally:
        from muxws.codecs import _REGISTRY

        del _REGISTRY["bootstrap-set"]


def test_codec_argument_overrides_settings(configured_codec):
    """WSM-CDC-012: a test override and an escape hatch, documented as nothing else."""
    configured_codec("msgpack")  # not registered - resolving by name would raise
    override = JsonCodec()
    assert resolve_codec(override) is override


def test_dialer_offers_the_muxws_entry_first():
    """WSM-CDC-020/021: the muxws entry leads; the application's own entries follow untouched."""
    offered = offer("json", ["bearer.abc123", "x-app"])
    assert offered[0] == "muxws.v1.json"
    assert offered[1:] == ["bearer.abc123", "x-app"]
    assert find_offer(offered) == "muxws.v1.json"


def test_extra_subprotocols_are_ignored_by_the_acceptor():
    """WSM-CDC-021: everything but the muxws entry is left for the application's authentication."""
    assert select(["muxws.v1.json", "bearer.abc123"], "json") == "muxws.v1.json"
    assert select(["bearer.abc123", "muxws.v1.json"], "json") == "muxws.v1.json"


def test_mismatched_codecs_refuse_the_handshake(caplog):
    """WSM-CDC-022: no subprotocol selected, and the acceptor logs its half (WSM-CDC-029)."""
    with caplog.at_level(logging.ERROR, logger="muxws.codec"):
        assert select(["muxws.v1.msgpack"], "json") is None
    record = "\n".join(r.getMessage() for r in caplog.records)
    assert "msgpack" in record
    assert "json" in record
    assert "MUXWS_CODEC" in record


def test_v2_generation_is_rejected(caplog):
    """WSM-CDC-025: a different generation is refused at the handshake, not tolerated."""
    with caplog.at_level(logging.ERROR, logger="muxws.codec"):
        assert select(["muxws.v2.json"], "json") is None
    assert select(["muxws.v2.json"], "json") is None


def test_an_offer_with_no_muxws_entry_is_refused(caplog):
    with caplog.at_level(logging.ERROR, logger="muxws.codec"):
        assert select(["bearer.abc"], "json") is None
    assert "no muxws.v1.* subprotocol" in "\n".join(r.getMessage() for r in caplog.records)


def test_the_dialer_composes_its_own_mismatch_error():
    """WSM-CDC-024: a browser cannot read a rejection body, so the diagnostic cannot come from it."""
    error = mismatch_error("msgpack")
    assert isinstance(error, CodecMismatch)
    message = str(error)
    assert "msgpack" in message
    assert "VITE_MUXWS_CODEC" in message
    assert "MUXWS_CODEC" in message
    assert error.configured == "msgpack"


def test_post_open_verification_reports_the_mismatch():
    """WSM-CDC-028: the last resort, for a transport with neither hook."""
    verify_negotiated("muxws.v1.json", "json")
    with pytest.raises(CodecMismatch):
        verify_negotiated("muxws.v1.msgpack", "json")
    with pytest.raises(CodecMismatch):
        verify_negotiated(None, "json")
    assert POLICY_VIOLATION == 1008


def test_no_codec_branching_exists_in_the_peer():
    """WSM-CDC-023: an assertion, not a negotiation - so there is nothing to branch on."""
    import inspect

    from muxws import peer, stream

    for module in (peer, stream):
        source = inspect.getsource(module)
        assert "codec.name ==" not in source
        assert 'codec == "json"' not in source
        assert "isinstance(self._codec" not in source


def test_the_generation_prefix_is_the_only_version():
    """WSM-CON-009/WSM-PKG-005: one version on the wire, and it lives in the subprotocol name."""
    assert PREFIX == "muxws.v1."
    assert offer("json")[0].count(".") == 2


def test_a_different_generation_is_named_as_such(caplog):
    """WSM-CDC-025 is its own case: an offer from a future peer is not an absent muxws entry."""
    with caplog.at_level(logging.ERROR, logger="muxws.codec"):
        assert select(["muxws.v2.json"], "json") is None
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "generation 2" in logged
    assert "WSM-CDC-025" in logged


def test_generation_of_reads_the_only_version_on_the_wire():
    from muxws.subprotocol import generation_of

    assert generation_of("muxws.v1.json") == 1
    assert generation_of("muxws.v2.msgpack") == 2
    assert generation_of("bearer.abc") is None
    assert generation_of("muxws.json") is None
