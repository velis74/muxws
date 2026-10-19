"""The Starlette / FastAPI acceptor, against a real ASGI app."""

from __future__ import annotations

import json
import logging

from typing import Any

import pytest

from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.testclient import TestClient
from starlette.websockets import WebSocket

import muxws

from muxws.errors import ProtocolError
from muxws.stream import Stream


async def _echo(payload: Any, stream: Stream) -> None:
    if (payload or {}).get("action") == "stream":
        for index in range(2):
            await stream.send({"chunk": index})
        await stream.end({"chunk": 2})
    else:
        await stream.reply({"echo": payload})


def _app(*, accept_first: bool = False, codec: Any = None) -> Starlette:
    async def endpoint(websocket: WebSocket) -> None:
        if accept_first:
            # WSM-CDC-026: only muxws's accept() selects the subprotocol, so the endpoint must not accept.
            await websocket.accept()
        await muxws.serve(websocket, handler=_echo, codec=codec)

    return Starlette(routes=[WebSocketRoute("/ws", endpoint)])


def test_accept_performs_the_upgrade():
    """WSM-CDC-026: the endpoint never accepts; muxws selects the subprotocol and a request works."""
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws", subprotocols=["muxws.v1.json"]) as socket:
            socket.send_text(json.dumps({"type": "open", "stream": 1, "payload": {"n": 1}, "end": True}))
            frame = json.loads(socket.receive_text())
            assert frame["type"] == "data"
            assert frame["stream"] == 1
            assert frame["payload"] == {"echo": {"n": 1}}
            assert frame["end"] is True


def test_the_negotiated_subprotocol_is_the_muxws_entry():
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws", subprotocols=["muxws.v1.json", "bearer.abc"]) as socket:
            assert socket.accepted_subprotocol == "muxws.v1.json"


def test_extra_subprotocols_are_ignored():
    """WSM-CDC-021: the application's own entries are left entirely alone."""
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws", subprotocols=["bearer.abc123", "muxws.v1.json"]) as socket:
            assert socket.accepted_subprotocol == "muxws.v1.json"


def test_a_streaming_response_arrives_in_order():
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws", subprotocols=["muxws.v1.json"]) as socket:
            socket.send_text(json.dumps({"type": "open", "stream": 1, "payload": {"action": "stream"}, "end": True}))
            payloads = [json.loads(socket.receive_text())["payload"] for _ in range(3)]
            assert payloads == [{"chunk": 0}, {"chunk": 1}, {"chunk": 2}]


@pytest.mark.parametrize(
    "offered",
    [
        ["muxws.v1.msgpack"],
        ["muxws.v2.json"],
        ["bearer.abc123"],
        [],
    ],
    ids=["wrong-codec", "wrong-generation", "no-muxws-entry", "nothing-offered"],
)
def test_refusal_is_http_400_and_is_logged(offered: list[str], caplog):
    """WSM-CDC-022/025/029: deny the upgrade with 400 and no subprotocol, and log the reason.

    `websocket.close()` before accept renders 403, which is not what the rule asks for: the upgrade
    must be *refused*, not completed and then torn down.
    """
    from starlette.testclient import WebSocketDenialResponse

    with TestClient(_app()) as client, caplog.at_level(logging.ERROR, logger="muxws.codec"):
        with pytest.raises(WebSocketDenialResponse) as info:
            with client.websocket_connect("/ws", subprotocols=offered):
                pass

    # Denied, not accepted-then-closed: the upgrade never completed and no subprotocol was selected.
    assert info.value.status_code == 400

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "MUXWS_CODEC" in logged
    assert "json" in logged


def test_application_accepting_first_is_an_error():
    """WSM-CDC-026: a silent second accept would hide which subprotocol was selected."""
    with TestClient(_app(accept_first=True)) as client:
        with pytest.raises(ProtocolError, match="already accepted"):
            with client.websocket_connect("/ws", subprotocols=["muxws.v1.json"]) as socket:
                socket.receive_text()


def test_binary_flag_selects_the_send_method():
    """WSM-CDC-002/WSM-API-021: `codec.binary` picks the branch; nothing sniffs the value."""

    class BinaryJson(muxws.JsonCodec):
        name = "json"
        binary = True

        def encode(self, frame: Any) -> bytes:  # type: ignore[override]
            return super().encode(frame).encode("utf-8")

    with TestClient(_app(codec=BinaryJson())) as client:
        with client.websocket_connect("/ws", subprotocols=["muxws.v1.json"]) as socket:
            socket.send_bytes(json.dumps({"type": "open", "stream": 1, "payload": {"n": 1}, "end": True}).encode())
            message = socket.receive()
            assert message.get("bytes") is not None, "a binary codec must reach send_bytes"
            assert message.get("text") is None
            assert json.loads(message["bytes"])["payload"] == {"echo": {"n": 1}}


def test_a_disconnect_ends_serve_without_raising():
    """`serve()` returns on a clean socket close rather than propagating ConnectionClosed."""
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws", subprotocols=["muxws.v1.json"]) as socket:
            socket.send_text(json.dumps({"type": "open", "stream": 1, "payload": {}, "end": True}))
            socket.receive_text()
    # Leaving the context manager closes the socket; the app must shut down cleanly.


def _origin_app(allowed_origins: Any, accepted: list[bool]) -> Starlette:
    async def endpoint(websocket: WebSocket) -> None:
        await muxws.serve(websocket, handler=_echo, allowed_origins=allowed_origins)
        accepted.append(True)

    return Starlette(routes=[WebSocketRoute("/ws", endpoint)])


def _request(client: TestClient, headers: dict[str, str]) -> Any:
    return client.websocket_connect("/ws", subprotocols=["muxws.v1.json"], headers=headers)


@pytest.mark.parametrize(
    ("allowed", "headers"),
    [
        (None, {}),
        (None, {"origin": "https://evil.test"}),
        (["https://app.example.com"], {"origin": "https://app.example.com"}),
        (["https://app.example.com"], {"origin": "HTTPS://APP.example.com:443"}),
        (lambda origin: origin is None, {}),
    ],
    ids=["default-no-origin", "default-any-origin", "listed", "listed-normalised", "callable-no-origin"],
)
def test_an_allowed_origin_is_upgraded(allowed: Any, headers: dict[str, str]):
    accepted: list[bool] = []
    with TestClient(_origin_app(allowed, accepted)) as client:
        with _request(client, headers) as socket:
            assert socket.accepted_subprotocol == "muxws.v1.json"
            socket.send_text(json.dumps({"type": "open", "stream": 1, "payload": {"n": 1}, "end": True}))
            assert json.loads(socket.receive_text())["payload"] == {"echo": {"n": 1}}


@pytest.mark.parametrize(
    ("allowed", "headers"),
    [
        (["https://app.example.com"], {"origin": "https://evil.test"}),
        (["https://app.example.com"], {"origin": "http://app.example.com"}),
        (["https://app.example.com"], {}),
        (["https://app.example.com"], {"origin": "null"}),
        (lambda origin: origin == "https://app.example.com", {"origin": "https://evil.test"}),
    ],
    ids=["other-host", "other-scheme", "missing", "null", "callable-false"],
)
def test_a_refused_origin_is_answered_with_http_403(allowed: Any, headers: dict[str, str], caplog):
    from starlette.testclient import WebSocketDenialResponse

    accepted: list[bool] = []
    with TestClient(_origin_app(allowed, accepted)) as client, caplog.at_level(logging.INFO, logger="muxws.origin"):
        with pytest.raises(WebSocketDenialResponse) as info:
            with _request(client, headers):
                pass

    assert info.value.status_code == 403
    assert accepted == [True], "serve() raised after the refusal"
    assert "refused a muxws handshake" in caplog.text


def test_accept_raises_origin_refused_with_the_header_value():
    from starlette.testclient import WebSocketDenialResponse

    raised: list[BaseException] = []

    async def endpoint(websocket: WebSocket) -> None:
        try:
            await muxws.accept(websocket, allowed_origins=["https://app.example.com"])
        except muxws.OriginRefused as exc:
            raised.append(exc)

    with TestClient(Starlette(routes=[WebSocketRoute("/ws", endpoint)])) as client:
        with pytest.raises(WebSocketDenialResponse):
            with _request(client, {"origin": "https://evil.test"}):
                pass

    assert len(raised) == 1
    assert isinstance(raised[0], muxws.OriginRefused)
    assert raised[0].origin == "https://evil.test"


def test_an_invalid_allowed_origins_entry_raises_before_the_upgrade():
    raised: list[BaseException] = []

    async def endpoint(websocket: WebSocket) -> None:
        try:
            await muxws.accept(websocket, allowed_origins=["app.example.com"])
        except ValueError as exc:
            raised.append(exc)
            await websocket.close(code=1011)

    from starlette.websockets import WebSocketDisconnect

    with TestClient(Starlette(routes=[WebSocketRoute("/ws", endpoint)])) as client:
        with pytest.raises(WebSocketDisconnect):
            with _request(client, {"origin": "https://app.example.com"}):
                pass

    assert len(raised) == 1
    assert "not an origin" in str(raised[0])


def test_allowed_origins_on_an_already_accepted_socket_raises_protocol_error():
    async def endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await muxws.serve(websocket, handler=_echo, allowed_origins=["https://app.example.com"])

    with TestClient(Starlette(routes=[WebSocketRoute("/ws", endpoint)])) as client:
        with pytest.raises(ProtocolError, match="already accepted"):
            with _request(client, {"origin": "https://evil.test"}) as socket:
                socket.receive_text()
