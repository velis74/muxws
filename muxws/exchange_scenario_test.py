"""`peer.on_exchange` scenarios over a real WebSocket on 127.0.0.1: an application observes finished streams."""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import logging
import re

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import websockets

import muxws

from muxws import Exchange
from muxws.errors import RemoteError, ResetCode, StreamRefused, StreamReset
from muxws.frames import Frame
from muxws.peer import Peer
from muxws.stream import Stream
from muxws.subprotocol import offer
from muxws.transports.websockets_ import WebsocketsSocket

EXCHANGE_KEYS = [
    "ts",
    "conn",
    "stream",
    "opened_by",
    "oneway",
    "request",
    "response",
    "outcome",
    "reset_by",
    "code",
    "reason",
    "first_response_ms",
    "duration_ms",
]
SIDE_KEYS = ["headers", "payloads", "payload_count", "trailers", "ended"]
TS_FORMAT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
HAS_MSGPACK = importlib.util.find_spec("msgpack") is not None


class Recorder:
    """An `on_exchange` handler that keeps every exchange and lets a test wait for a count."""

    def __init__(self) -> None:
        self.exchanges: list[Exchange] = []
        self._changed = asyncio.Event()

    def __call__(self, exchange: Exchange) -> None:
        self.exchanges.append(exchange)
        self._changed.set()

    async def wait(self, count: int = 1, timeout: float = 3.0) -> list[Exchange]:
        async def until() -> None:
            while len(self.exchanges) < count:
                self._changed.clear()
                await self._changed.wait()

        await asyncio.wait_for(until(), timeout)
        return self.exchanges


class Link:
    """A dialer and an acceptor connected over a real `websockets` connection."""

    def __init__(self, client: Peer, server: Peer, connection: Any) -> None:
        self.client = client
        self.server = server
        self.connection = connection

    async def drop(self) -> None:
        """Abort the TCP connection without a close handshake."""
        self.connection.transport.abort()


Setup = Callable[[Peer], None]


def codec_named(name: str) -> Any:
    """The codec for `name`; msgpack is constructed directly, as it is not registered by default."""
    if name == "msgpack":
        from muxws.codecs.msgpack_ import MsgpackCodec

        return MsgpackCodec()
    return muxws.get_codec(name)


@pytest.fixture
async def make_link(monkeypatch) -> AsyncIterator[Callable[..., Awaitable[Link]]]:
    """Build a connected pair; `server` and `client` run on each peer before it serves."""
    async with AsyncExitStack() as stack:
        tasks: list[asyncio.Task[None]] = []

        async def build(*, server: Setup | None = None, client: Setup | None = None, codec: str = "json") -> Link:
            monkeypatch.setattr(muxws.settings, "codec", codec)
            codec_impl = codec_named(codec)
            accepted: asyncio.Future[Peer] = asyncio.get_running_loop().create_future()

            async def handle(connection: Any) -> None:
                peer = await muxws.accept(WebsocketsSocket(connection), codec=codec_impl)
                if server is not None:
                    server(peer)
                accepted.set_result(peer)
                await peer.serve()

            service = await stack.enter_async_context(
                websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol)
            )
            port = service.sockets[0].getsockname()[1]
            connection = await websockets.connect(f"ws://127.0.0.1:{port}", subprotocols=offer(codec))
            stack.push_async_callback(connection.close)
            dialer = Peer(WebsocketsSocket(connection), codec=codec_impl, is_dialer=True)
            if client is not None:
                client(dialer)
            tasks.append(asyncio.create_task(dialer.serve()))
            acceptor = await asyncio.wait_for(accepted, 3.0)
            return Link(dialer, acceptor, connection)

        try:
            yield build
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------------------------- handlers


async def unary_handler(payload: Any, stream: Stream) -> None:
    await stream.reply({"answer": payload}, headers={"status": "ok"}, trailers={"elapsed": 7})


async def streaming_handler(payload: Any, stream: Stream) -> None:
    await stream.send_headers({"kind": "rows"})
    for index in range(payload["count"]):
        await stream.send({"row": index})
    await stream.end(trailers={"rows": payload["count"]})


async def raising_handler(payload: Any, stream: Stream) -> None:
    _ = payload, stream
    raise ValueError("handler said no")


async def waiting_handler(payload: Any, stream: Stream) -> None:
    _ = payload
    await stream.closed.wait()


def recording(handler: Any, recorder: Recorder, **options: Any) -> Setup:
    def setup(peer: Peer) -> None:
        if handler is not None:
            peer.on_stream(handler)
        peer.on_exchange(recorder, **options)

    return setup


# ---------------------------------------------------------------------------------------------- unary


async def test_a_unary_request_is_reported_by_both_peers_with_both_sides(make_link):
    """The client sends a request with headers; each peer reports one exchange, request = the opener's side."""
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(unary_handler, on_server), client=recording(None, on_client))

    answer = await link.client.request({"q": 1}, headers={"trace": "abc"})
    assert answer == {"answer": {"q": 1}}
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    assert client_side.conn == link.client.id
    assert server_side.conn == link.server.id
    assert client_side.stream == server_side.stream
    assert (client_side.opened_by, server_side.opened_by) == ("local", "remote")
    for exchange in (client_side, server_side):
        assert exchange.oneway is False
        assert exchange.outcome == "ok"
        assert (exchange.reset_by, exchange.code, exchange.reason) == (None, None, None)
        assert exchange.request.headers == {"trace": "abc"}
        assert exchange.request.payloads == ({"q": 1},)
        assert exchange.request.payload_count == 1
        assert exchange.request.trailers is None
        assert exchange.request.ended is True
        assert exchange.response.headers == {"status": "ok"}
        assert exchange.response.payloads == ({"answer": {"q": 1}},)
        assert exchange.response.payload_count == 1
        assert exchange.response.trailers == {"elapsed": 7}
        assert exchange.response.ended is True


async def test_a_stream_the_acceptor_opens_reports_the_acceptor_as_the_requester(make_link):
    """The server pushes a request to the client; `request` is still the opener's side on both peers."""
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(None, on_server), client=recording(unary_handler, on_client))

    answer = await link.server.request({"push": True}, headers={"from": "server"})
    assert answer == {"answer": {"push": True}}
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    assert (server_side.opened_by, client_side.opened_by) == ("local", "remote")
    for exchange in (client_side, server_side):
        assert exchange.request.headers == {"from": "server"}
        assert exchange.request.payloads == ({"push": True},)
        assert exchange.response.payloads == ({"answer": {"push": True}},)
        assert exchange.response.headers == {"status": "ok"}


async def test_on_exchange_returns_the_handler_so_it_works_as_a_decorator(make_link):
    link = await make_link(server=lambda peer: peer.on_stream(unary_handler))
    seen: list[Exchange] = []

    def handler(exchange: Exchange) -> None:
        seen.append(exchange)

    assert link.client.on_exchange(handler) is handler
    await link.client.request({"q": 1})
    await asyncio.sleep(0.05)
    assert len(seen) == 1


# ---------------------------------------------------------------------------------------------- streamed responses


async def test_a_streamed_response_reports_every_payload_and_ignores_payload_less_frames(make_link):
    """`send_headers` and an `end()` without payload add no payload; the count is the payloads sent."""
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(streaming_handler, on_server), client=recording(None, on_client))

    received = [item async for item in link.client.open({"count": 3}, end=True)]
    assert received == [{"row": 0}, {"row": 1}, {"row": 2}]
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    for exchange in (client_side, server_side):
        assert exchange.outcome == "ok"
        assert exchange.response.headers == {"kind": "rows"}
        assert exchange.response.payloads == ({"row": 0}, {"row": 1}, {"row": 2})
        assert exchange.response.payload_count == 3
        assert exchange.response.trailers == {"rows": 3}
        assert exchange.response.ended is True


async def test_max_payloads_keeps_the_first_payloads_and_counts_all_of_them(make_link):
    on_client = Recorder()
    link = await make_link(
        server=lambda peer: peer.on_stream(streaming_handler), client=recording(None, on_client, max_payloads=2)
    )

    [_ async for _ in link.client.open({"count": 5}, end=True)]
    [exchange] = await on_client.wait()

    assert exchange.response.payloads == ({"row": 0}, {"row": 1})
    assert exchange.response.payload_count == 5
    assert exchange.request.payloads == ({"count": 5},)
    assert exchange.request.payload_count == 1


async def test_max_payloads_zero_reports_timing_and_outcome_without_payloads(make_link):
    on_client = Recorder()
    link = await make_link(
        server=lambda peer: peer.on_stream(streaming_handler), client=recording(None, on_client, max_payloads=0)
    )

    [_ async for _ in link.client.open({"count": 4}, end=True)]
    [exchange] = await on_client.wait()

    assert exchange.outcome == "ok"
    assert exchange.request.payloads == ()
    assert exchange.request.payload_count == 1
    assert exchange.response.payloads == ()
    assert exchange.response.payload_count == 4
    assert exchange.response.headers == {"kind": "rows"}
    assert exchange.response.trailers == {"rows": 4}
    assert exchange.duration >= 0


async def test_two_handlers_with_different_limits_each_receive_payloads_up_to_their_own_limit(make_link):
    """One stream, two handlers: each sees the first N of its own limit; `None` sees everything."""
    small, unlimited = Recorder(), Recorder()

    def setup(peer: Peer) -> None:
        peer.on_exchange(small, max_payloads=1)
        peer.on_exchange(unlimited, max_payloads=None)

    link = await make_link(server=lambda peer: peer.on_stream(streaming_handler), client=setup)

    [_ async for _ in link.client.open({"count": 150}, end=True)]
    [few] = await small.wait()
    [every] = await unlimited.wait()

    assert few.response.payloads == ({"row": 0},)
    assert every.response.payloads == tuple({"row": index} for index in range(150))
    assert few.response.payload_count == every.response.payload_count == 150
    assert few.stream == every.stream


async def test_the_default_limit_is_one_hundred_payloads(make_link):
    on_client = Recorder()
    link = await make_link(server=lambda peer: peer.on_stream(streaming_handler), client=recording(None, on_client))

    [_ async for _ in link.client.open({"count": 120}, end=True)]
    [exchange] = await on_client.wait()

    assert len(exchange.response.payloads) == 100
    assert exchange.response.payloads[-1] == {"row": 99}
    assert exchange.response.payload_count == 120


@pytest.mark.parametrize("limit", [-1, 1.5])
async def test_an_unusable_limit_is_refused_at_registration(make_link, limit):
    link = await make_link()
    with pytest.raises(ValueError, match="max_payloads"):
        link.client.on_exchange(lambda _exchange: None, max_payloads=limit)


async def test_a_payload_larger_than_one_frame_is_reported_once_and_decoded(make_link):
    """A payload over 64 KiB travels in fragments; both peers report it as one decoded value."""
    on_client, on_server = Recorder(), Recorder()

    async def echo(payload: Any, stream: Stream) -> None:
        await stream.reply({"echo": payload["blob"]})

    link = await make_link(server=recording(echo, on_server), client=recording(None, on_client))
    blob = "x" * 200_000

    answer = await link.client.request({"blob": blob})
    assert answer == {"echo": blob}
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    for exchange in (client_side, server_side):
        assert exchange.request.payloads == ({"blob": blob},)
        assert exchange.request.payload_count == 1
        assert exchange.response.payloads == ({"echo": blob},)
        assert exchange.response.payload_count == 1


# ---------------------------------------------------------------------------------------------- resets and loss


async def test_a_handler_that_raises_is_reported_as_an_application_error_reset_on_both_peers(make_link):
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(raising_handler, on_server), client=recording(None, on_client))

    with pytest.raises(RemoteError):
        await link.client.request({"q": 1})
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    assert (client_side.reset_by, server_side.reset_by) == ("remote", "local")
    for exchange in (client_side, server_side):
        assert exchange.outcome == "reset"
        assert exchange.code == ResetCode.APPLICATION_ERROR.value == 2
        assert exchange.reason == "handler said no"
        assert exchange.request.payloads == ({"q": 1},)
        assert exchange.response.payloads == ()
        assert exchange.response.ended is False


async def test_a_client_cancel_is_a_local_reset_on_the_client_and_a_remote_one_on_the_server(make_link):
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(waiting_handler, on_server), client=recording(None, on_client))

    stream = link.client.open({"q": 1})
    await asyncio.sleep(0.05)
    await stream.cancel("no longer needed")
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    assert client_side.reset_by == "local"
    assert server_side.reset_by == "remote"
    for exchange in (client_side, server_side):
        assert exchange.outcome == "reset"
        assert exchange.code == ResetCode.CANCELLED.value
        assert exchange.reason == "no longer needed"
        assert exchange.request.ended is False
        assert exchange.request.payloads == ({"q": 1},)


async def test_an_open_the_acceptor_refuses_is_a_remote_reset_on_the_client(make_link):
    """With no `on_stream` the acceptor answers with `reset(REFUSED)`."""
    on_client = Recorder()
    link = await make_link(client=recording(None, on_client))

    with pytest.raises(StreamReset):
        await link.client.request({"q": 1})
    [exchange] = await on_client.wait()

    assert exchange.outcome == "reset"
    assert exchange.reset_by == "remote"
    assert exchange.code == ResetCode.REFUSED.value


async def test_a_stream_a_received_goaway_refuses_is_a_remote_reset_on_the_opener(make_link):
    on_client = Recorder()
    link = await make_link(server=recording(waiting_handler, Recorder()), client=recording(None, on_client))

    stream = link.client.open({"q": 1})
    await asyncio.sleep(0.05)
    link.server._enqueue(Frame("goaway", code=1000, reason="shutting down", last_stream=0))
    with pytest.raises(StreamRefused):
        await stream.result(timeout=5.0)
    [exchange] = await on_client.wait()

    assert exchange.outcome == "reset"
    assert exchange.reset_by == "remote"
    assert exchange.code == ResetCode.REFUSED.value


async def test_a_dropped_connection_reports_live_streams_as_lost_on_both_peers(make_link):
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(waiting_handler, on_server), client=recording(None, on_client))

    link.client.open({"q": 1})
    await asyncio.sleep(0.05)
    await link.drop()
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    for exchange in (client_side, server_side):
        assert exchange.outcome == "lost"
        assert (exchange.reset_by, exchange.code, exchange.reason) == (None, None, None)
        assert exchange.request.payloads == ({"q": 1},)


# ---------------------------------------------------------------------------------------------- oneway


async def test_notify_is_reported_as_a_oneway_exchange_on_both_peers(make_link):
    on_client, on_server = Recorder(), Recorder()
    arrived = asyncio.Event()

    async def handler(payload: Any, stream: Stream) -> None:
        _ = payload, stream
        arrived.set()

    link = await make_link(server=recording(handler, on_server), client=recording(None, on_client))

    await link.client.notify({"event": "cache-invalidated"}, headers={"topic": "cache"})
    await asyncio.wait_for(arrived.wait(), 3.0)
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    assert (client_side.opened_by, server_side.opened_by) == ("local", "remote")
    for exchange in (client_side, server_side):
        assert exchange.oneway is True
        assert exchange.outcome == "ok"
        assert exchange.request.headers == {"topic": "cache"}
        assert exchange.request.payloads == ({"event": "cache-invalidated"},)
        assert exchange.request.ended is True
        assert exchange.response.payloads == ()
        assert exchange.response.payload_count == 0
        assert exchange.response.ended is False
        assert exchange.first_response is None


# ---------------------------------------------------------------------------------------------- timing


async def test_timing_is_in_seconds_and_first_response_precedes_the_end(make_link):
    on_client, on_server = Recorder(), Recorder()

    async def slow(payload: Any, stream: Stream) -> None:
        await asyncio.sleep(0.1)
        await stream.send({"first": True})
        await asyncio.sleep(0.1)
        await stream.end({"last": payload})

    link = await make_link(server=recording(slow, on_server), client=recording(None, on_client))
    before = datetime.now(timezone.utc)

    [_ async for _ in link.client.open({"q": 1}, end=True)]
    [client_side] = await on_client.wait()
    [server_side] = await on_server.wait()

    for exchange in (client_side, server_side):
        assert exchange.started_at.tzinfo is not None
        assert exchange.started_at.utcoffset() == timedelta(0)
        assert before - timedelta(seconds=1) <= exchange.started_at <= datetime.now(timezone.utc)
        assert exchange.first_response is not None
        assert 0 <= exchange.first_response <= exchange.duration
        assert 0.08 <= exchange.first_response < 5
        assert 0.18 <= exchange.duration < 5


async def test_first_response_is_none_when_the_answering_side_sent_nothing(make_link):
    on_client = Recorder()
    link = await make_link(server=lambda peer: peer.on_stream(waiting_handler), client=recording(None, on_client))

    stream = link.client.open({"q": 1})
    await asyncio.sleep(0.02)
    await stream.cancel()
    [exchange] = await on_client.wait()

    assert exchange.first_response is None
    assert exchange.duration >= 0


# ---------------------------------------------------------------------------------------------- as_dict


async def _unary_exchange(make_link, payload: Any, **request: Any) -> Exchange:
    on_client = Recorder()
    link = await make_link(server=lambda peer: peer.on_stream(unary_handler), client=recording(None, on_client))
    await link.client.request(payload, **request)
    [exchange] = await on_client.wait()
    return exchange


async def test_as_dict_has_a_fixed_key_order_and_json_ready_values(make_link):
    exchange = await _unary_exchange(make_link, {"q": 1}, headers={"trace": "abc"})

    record = exchange.as_dict()

    assert list(record) == EXCHANGE_KEYS
    assert list(record["request"]) == SIDE_KEYS
    assert list(record["response"]) == SIDE_KEYS
    assert TS_FORMAT.match(record["ts"])
    parsed = datetime.strptime(record["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    assert abs(parsed - exchange.started_at) < timedelta(milliseconds=1)
    assert record["conn"] == exchange.conn
    assert record["stream"] == exchange.stream
    assert (record["opened_by"], record["oneway"], record["outcome"]) == ("local", False, "ok")
    assert (record["reset_by"], record["code"], record["reason"]) == (None, None, None)
    assert record["request"] == {
        "headers": {"trace": "abc"},
        "payloads": [{"q": 1}],
        "payload_count": 1,
        "trailers": None,
        "ended": True,
    }
    assert record["response"]["payloads"] == [{"answer": {"q": 1}}]
    assert record["response"]["trailers"] == {"elapsed": 7}
    assert record["duration_ms"] == round(exchange.duration * 1000, 3)
    assert record["first_response_ms"] == round(exchange.first_response * 1000, 3)


async def test_as_dict_reports_a_reset_with_its_code_and_reason(make_link):
    on_client = Recorder()
    link = await make_link(server=lambda peer: peer.on_stream(raising_handler), client=recording(None, on_client))
    with pytest.raises(RemoteError):
        await link.client.request({"q": 1})
    [exchange] = await on_client.wait()

    record = exchange.as_dict()

    assert (record["outcome"], record["reset_by"], record["code"], record["reason"]) == (
        "reset",
        "remote",
        2,
        "handler said no",
    )
    assert record["first_response_ms"] is None or record["first_response_ms"] >= 0


async def test_as_dict_trims_long_payloads_and_records_the_number_of_bytes_cut(make_link):
    exchange = await _unary_exchange(make_link, {"text": "x" * 100}, headers={"long": "y" * 100})

    record = exchange.as_dict(trim_payload_size=20)

    # `{"text":"xxx…x"}` is 9 + 100 + 2 = 111 bytes of compact JSON.
    assert record["request"]["payloads"] == ['{"text":"xxxxxxxxxxx…(trimmed 91 bytes)…']
    assert record["request"]["headers"] == {"long": "y" * 100}
    assert record["response"]["trailers"] == {"elapsed": 7}


async def test_as_dict_cuts_back_to_a_character_boundary(make_link):
    """`"éééééééééé"` is 22 bytes; a 4-byte limit falls inside the second `é`, so 3 bytes stay."""
    exchange = await _unary_exchange(make_link, "é" * 10)

    record = exchange.as_dict(trim_payload_size=4)

    assert record["request"]["payloads"] == ['"é…(trimmed 19 bytes)…']


async def test_as_dict_leaves_payloads_within_the_limit_as_values(make_link):
    exchange = await _unary_exchange(make_link, {"q": 1})

    record = exchange.as_dict(trim_payload_size=1000)

    assert record["request"]["payloads"] == [{"q": 1}]
    assert record["response"]["payloads"] == [{"answer": {"q": 1}}]


async def test_as_dict_refuses_a_negative_trim_size(make_link):
    exchange = await _unary_exchange(make_link, {"q": 1})

    with pytest.raises(ValueError, match="trim_payload_size"):
        exchange.as_dict(trim_payload_size=-1)


async def test_mutating_the_dict_leaves_the_exchange_unchanged(make_link):
    exchange = await _unary_exchange(make_link, {"secret": "s3cr3t"}, headers={"token": "t0ken"})

    record = exchange.as_dict()
    redacted = "[redacted]"
    record["request"]["headers"]["token"] = redacted
    record["request"]["payloads"][0]["secret"] = redacted

    assert exchange.request.headers == {"token": "t0ken"}
    assert exchange.request.payloads == ({"secret": "s3cr3t"},)
    again = exchange.as_dict()
    assert again["request"]["headers"] == {"token": "t0ken"}
    assert again["request"]["payloads"] == [{"secret": "s3cr3t"}]


async def test_an_exchange_is_immutable(make_link):
    exchange = await _unary_exchange(make_link, {"q": 1})

    with pytest.raises(AttributeError):
        exchange.outcome = "reset"  # type: ignore[misc]


@pytest.mark.skipif(not HAS_MSGPACK, reason="the msgpack extra is not installed")
async def test_as_dict_turns_bytes_into_base64_under_a_binary_codec(make_link):
    on_client = Recorder()

    async def echo(payload: Any, stream: Stream) -> None:
        await stream.reply(payload)

    link = await make_link(server=lambda peer: peer.on_stream(echo), client=recording(None, on_client), codec="msgpack")
    payload = {"blob": b"\x00\x01\xff", "parts": [b"ab", "text"]}

    assert await link.client.request(payload) == payload
    [exchange] = await on_client.wait()
    assert exchange.request.payloads == (payload,)

    record = exchange.as_dict()
    expected = {
        "blob": base64.b64encode(b"\x00\x01\xff").decode("ascii"),
        "parts": [base64.b64encode(b"ab").decode("ascii"), "text"],
    }
    assert record["request"]["payloads"] == [expected]
    assert record["response"]["payloads"] == [expected]
    assert exchange.request.payloads == (payload,), "as_dict converted the recorded value in place"


# ---------------------------------------------------------------------------------------------- handler behaviour


async def test_a_raising_handler_is_logged_and_other_handlers_and_the_connection_are_unaffected(make_link, caplog):
    recorder = Recorder()

    def explode(exchange: Exchange) -> None:
        _ = exchange
        raise RuntimeError("the application's exchange log is broken")

    def setup(peer: Peer) -> None:
        peer.on_exchange(explode)
        peer.on_exchange(recorder)

    link = await make_link(server=lambda peer: peer.on_stream(unary_handler), client=setup)

    with caplog.at_level(logging.ERROR, logger="muxws"):
        assert await link.client.request({"q": 1}) == {"answer": {"q": 1}}
        assert await link.client.request({"q": 2}) == {"answer": {"q": 2}}
        await recorder.wait(2)

    assert [exchange.request.payloads for exchange in recorder.exchanges] == [({"q": 1},), ({"q": 2},)]
    assert all(exchange.outcome == "ok" for exchange in recorder.exchanges)
    assert link.client.is_open is True
    raised = [record for record in caplog.records if record.exc_info and record.exc_info[0] is RuntimeError]
    assert raised, "the handler's exception was not logged"
    assert all(record.name == "muxws.frames" and record.levelno == logging.ERROR for record in raised)


async def test_handlers_are_called_in_registration_order(make_link):
    order: list[str] = []

    def setup(peer: Peer) -> None:
        peer.on_exchange(lambda _exchange: order.append("first"))
        peer.on_exchange(lambda _exchange: order.append("second"))

    link = await make_link(server=lambda peer: peer.on_stream(unary_handler), client=setup)
    await link.client.request({"q": 1})
    await asyncio.sleep(0.05)

    assert order == ["first", "second"]


async def test_a_stream_created_before_any_handler_never_fires(make_link):
    release = asyncio.Event()

    async def gated(payload: Any, stream: Stream) -> None:
        await release.wait()
        await stream.reply(payload)

    link = await make_link(server=lambda peer: peer.on_stream(gated))
    recorder = Recorder()

    early = link.client.open({"n": "early"}, end=True)
    link.client.on_exchange(recorder)
    release.set()
    assert await early == {"n": "early"}
    await asyncio.sleep(0.05)
    assert recorder.exchanges == []

    assert await link.client.request({"n": "late"}) == {"n": "late"}
    [exchange] = await recorder.wait()
    assert exchange.request.payloads == ({"n": "late"},)


async def test_a_handler_registered_while_a_stream_is_live_gets_that_stream(make_link):
    """A stream recorded under one handler fires to every handler registered by the time it finishes."""
    release = asyncio.Event()

    async def gated(payload: Any, stream: Stream) -> None:
        await release.wait()
        await stream.reply(payload)

    first, second = Recorder(), Recorder()
    link = await make_link(server=lambda peer: peer.on_stream(gated), client=lambda peer: peer.on_exchange(first))

    live = link.client.open({"n": 1}, end=True)
    link.client.on_exchange(second)
    release.set()
    await live
    [one] = await first.wait()
    [two] = await second.wait()
    assert one.stream == two.stream


async def test_every_finished_stream_fires_exactly_once(make_link):
    on_client, on_server = Recorder(), Recorder()
    link = await make_link(server=recording(unary_handler, on_server), client=recording(None, on_client))

    await asyncio.gather(*(link.client.request({"n": index}) for index in range(10)))
    await on_client.wait(10)
    await on_server.wait(10)
    await asyncio.sleep(0.05)

    assert len(on_client.exchanges) == 10
    assert len(on_server.exchanges) == 10
    assert len({exchange.stream for exchange in on_client.exchanges}) == 10
