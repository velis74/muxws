"""Scenarios for the logging recipes in `docs/guide/observability.md`, over a real WebSocket.

A `websockets` acceptor on an ephemeral port and a `muxws.connect` dialer exchange streams; an
`ExchangeLog` from `exchange_log.py` is attached to one or both peers and its JSON lines are checked.
The last scenario applies the `LOGGING` dict from `django_logging.py` with `logging.config.dictConfig`.
"""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import logging
import logging.config
import re
import sys

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

import muxws

from muxws.errors import RemoteError, ResetCode
from muxws.frames import Frame
from muxws.stream import Stream

websockets = pytest.importorskip("websockets")

from muxws.transports.websockets_ import WebsocketsSocket  # noqa: E402 - needs websockets

EXAMPLES = Path(__file__).resolve().parent

RECORD_KEYS = [
    "ts",
    "conn",
    "stream",
    "opened_by",
    "oneway",
    "request",
    "response",
    "outcome",
    "code",
    "reason",
    "first_response_ms",
    "duration_ms",
]
SIDE_KEYS = ["headers", "payloads", "trailers", "dropped", "bytes"]
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
#: Larger than one 64 KiB frame, so the payload crosses the wire in fragments.
LARGE = "x" * 100_000
WAIT_SECONDS = 5.0


def load_example(name: str) -> ModuleType:
    """Import `docs/examples/<name>.py`; the directory is not a package."""
    spec = importlib.util.spec_from_file_location(f"muxws_docs_{name}", EXAMPLES / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ExchangeLog = load_example("exchange_log").ExchangeLog
CODEC = muxws.get_codec("json")


class FakeClock:
    """Monotonic seconds set by the test."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class Server:
    """The acceptor's side of the one connection a scenario makes."""

    clock: FakeClock = field(default_factory=FakeClock)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    holding: asyncio.Event = field(default_factory=asyncio.Event)
    peer: Any = None
    connection: Any = None


@dataclass
class Connection:
    dialer: Any
    server: Server


def acceptor_handler(server: Server) -> Callable[[Any, Stream], Any]:
    """The application on the acceptor, dispatching on `payload["action"]`."""

    async def handle(payload: Any, stream: Stream) -> None:
        if isinstance(payload, str):
            await stream.reply(len(payload))
            return
        action = (payload or {}).get("action")
        if action == "stream":
            for index in range(3):
                await stream.send({"chunk": index})
            await stream.end({"chunk": 3})
        elif action == "raise":
            raise ValueError("handler said no")
        elif action == "forever":
            server.holding.set()
            await stream.closed.wait()
        elif action == "timed":
            server.clock.now = 5.0
            await stream.send("first")
            await asyncio.sleep(0.05)
            server.clock.now = 12.0
            await stream.end("last")
        else:
            await stream.reply({"echo": payload})

    return handle


@asynccontextmanager
async def running(
    *,
    server_logs: tuple[Any, ...] = (),
    dialer_logs: tuple[Any, ...] = (),
    dialer_handler: Callable[[Any, Stream], Any] | None = None,
    server: Server | None = None,
) -> AsyncIterator[Connection]:
    """One real connection with the given logs attached to the acceptor and the dialer."""
    state = server or Server()

    async def handle(connection: Any) -> None:
        peer = await muxws.accept(WebsocketsSocket(connection), codec=CODEC)
        for log in server_logs:
            log.attach(peer)
        peer.on_stream(acceptor_handler(state))
        state.peer = peer
        state.connection = connection
        state.ready.set()
        await peer.serve()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        dialer = await muxws.connect(url, codec=CODEC, on_stream=dialer_handler)
        for log in dialer_logs:
            log.attach(dialer)
        await asyncio.wait_for(state.ready.wait(), WAIT_SECONDS)
        try:
            yield Connection(dialer, state)
        finally:
            with suppress(Exception):
                await dialer.close(drain=0.5)


def collector() -> tuple[list[str], Callable[[str], None]]:
    lines: list[str] = []
    return lines, lines.append


def records(lines: list[str]) -> list[dict[str, Any]]:
    for line in lines:
        assert "\n" not in line
    return [json.loads(line) for line in lines]


async def until(condition: Callable[[], bool]) -> None:
    """Wait until `condition()` holds, or fail after `WAIT_SECONDS`."""
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("the expected exchange records were not written in time")
        await asyncio.sleep(0.01)


def trimmed(value: str) -> tuple[str, int]:
    """Split a trimmed payload into the kept prefix and N of `…(trimmed N bytes)…`."""
    match = re.fullmatch(r"(?s)(?P<prefix>.*)…\(trimmed (?P<n>\d+) bytes\)…", value)
    assert match is not None, f"no trim marker at the end of {value[-60:]!r}"
    return match.group("prefix"), int(match.group("n"))


# ---------------------------------------------------------------- pairing and shape


async def test_unary_request_is_logged_on_both_sides_with_the_same_pairing() -> None:
    """A request logged by the dialer and by the acceptor: the same stream, request and response."""
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()
    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write),),
        server_logs=(ExchangeLog(CODEC, write=server_write),),
    ) as conn:
        reply = await conn.dialer.request({"action": "echo", "v": 1}, headers={"trace": "t-1"})
        assert reply == {"echo": {"action": "echo", "v": 1}}
        await until(lambda: len(dialer_lines) == 1 and len(server_lines) == 1)

        (mine,) = records(dialer_lines)
        (theirs,) = records(server_lines)
        assert mine["conn"] == conn.dialer.id
        assert theirs["conn"] == conn.server.peer.id
        assert mine["stream"] == theirs["stream"]
        assert mine["opened_by"] == "local"
        assert theirs["opened_by"] == "remote"
        for record in (mine, theirs):
            assert record["oneway"] is False
            assert record["request"]["headers"] == {"trace": "t-1"}
            assert record["request"]["payloads"] == [{"action": "echo", "v": 1}]
            assert record["request"]["trailers"] is None
            assert record["request"]["dropped"] == 0
            assert record["response"]["payloads"] == [{"echo": {"action": "echo", "v": 1}}]
            assert record["response"]["dropped"] == 0
            assert record["outcome"] == "ok"
            assert record["code"] is None
            assert record["reason"] is None
        assert mine["request"]["bytes"] == theirs["request"]["bytes"] > 0
        assert mine["response"]["bytes"] == theirs["response"]["bytes"] > 0


async def test_a_stream_the_acceptor_opens_is_its_request_on_both_sides() -> None:
    """A server push: the acceptor's payload is the request in both records, opened_by follows the logger."""
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()

    async def answer(payload: Any, stream: Stream) -> None:
        await stream.reply({"ack": payload["push"]})

    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write),),
        server_logs=(ExchangeLog(CODEC, write=server_write),),
        dialer_handler=answer,
    ) as conn:
        assert await conn.server.peer.request({"push": 7}) == {"ack": 7}
        await until(lambda: len(dialer_lines) == 1 and len(server_lines) == 1)

        (mine,) = records(dialer_lines)
        (theirs,) = records(server_lines)
        assert theirs["opened_by"] == "local"
        assert mine["opened_by"] == "remote"
        assert mine["stream"] == theirs["stream"]
        assert mine["stream"] % 2 == 0
        for record in (mine, theirs):
            assert record["request"]["payloads"] == [{"push": 7}]
            assert record["response"]["payloads"] == [{"ack": 7}]


async def test_record_keys_and_timestamp() -> None:
    """Keys come in the documented order; `ts` is UTC ISO 8601 with milliseconds and `Z`."""
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        before = datetime.now(timezone.utc)
        await conn.dialer.request({"action": "echo"})
        await until(lambda: len(lines) == 1)
        after = datetime.now(timezone.utc)

    (record,) = records(lines)
    assert list(record) == RECORD_KEYS
    assert list(record["request"]) == SIDE_KEYS
    assert list(record["response"]) == SIDE_KEYS
    assert TIMESTAMP.match(record["ts"])
    started = datetime.strptime(record["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    assert before - timedelta(seconds=1) <= started <= after + timedelta(seconds=1)


async def test_bytes_are_the_frame_lengths_of_each_side() -> None:
    """`bytes` is the sum of `on_frame` byte lengths of the frames each side sent on the stream."""
    lines, write = collector()
    seen: list[tuple[str, Frame, int]] = []
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        conn.dialer.on_frame(lambda direction, frame, length: seen.append((direction, frame, length)))
        [item async for item in conn.dialer.open({"action": "stream"}, end=True)]
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    on_stream = [(direction, length) for direction, frame, length in seen if frame.stream == record["stream"]]
    assert record["request"]["bytes"] == sum(length for direction, length in on_stream if direction == "tx")
    assert record["response"]["bytes"] == sum(length for direction, length in on_stream if direction == "rx")


# ---------------------------------------------------------------- streamed responses and limits


async def test_streamed_response_keeps_every_message_in_order() -> None:
    """A response of four messages is logged as four payloads in the order sent."""
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        chunks = [item async for item in conn.dialer.open({"action": "stream"}, end=True)]
        assert chunks == [{"chunk": index} for index in range(4)]
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert record["response"]["payloads"] == [{"chunk": index} for index in range(4)]
    assert record["response"]["dropped"] == 0
    assert record["outcome"] == "ok"


async def test_max_messages_keeps_the_first_and_counts_the_rest() -> None:
    """With `max_messages=2` a four-message response keeps two payloads and reports two dropped."""
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, max_messages=2),)) as conn:
        [item async for item in conn.dialer.open({"action": "stream"}, end=True)]
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert record["response"]["payloads"] == [{"chunk": 0}, {"chunk": 1}]
    assert record["response"]["dropped"] == 2
    assert record["request"]["payloads"] == [{"action": "stream"}]
    assert record["request"]["dropped"] == 0


async def test_max_messages_none_keeps_everything() -> None:
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, max_messages=None),)) as conn:
        [item async for item in conn.dialer.open({"action": "stream"}, end=True)]
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert len(record["response"]["payloads"]) == 4
    assert record["response"]["dropped"] == 0


# ---------------------------------------------------------------- trimming


async def test_a_large_fragmented_payload_is_logged_once_and_trimmed() -> None:
    """A 100 000-character payload crosses in fragments and is logged once, cut to 100 bytes plus the marker."""
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()
    fragments: list[Frame] = []
    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write, trim_payload_size=100),),
        server_logs=(ExchangeLog(CODEC, write=server_write, trim_payload_size=100),),
    ) as conn:
        conn.dialer.on_frame(lambda _direction, frame, _length: fragments.append(frame) if frame.fragment else None)
        assert await conn.dialer.request(LARGE) == len(LARGE)
        await until(lambda: len(dialer_lines) == 1 and len(server_lines) == 1)

    assert len(fragments) > 1, "the payload was expected to cross the wire in fragments"
    # The JSON text of the payload is the string in quotes: 100 002 bytes, of which 100 are kept.
    for record in records(dialer_lines) + records(server_lines):
        (logged,) = record["request"]["payloads"]
        prefix, cut = trimmed(logged)
        assert prefix == '"' + "x" * 99
        assert cut == len(LARGE) + 2 - 100
        assert record["request"]["bytes"] > len(LARGE)
        assert record["request"]["dropped"] == 0
        # A short payload is logged as the JSON value it is.
        assert record["response"]["payloads"] == [len(LARGE)]


async def test_trimming_cuts_on_a_character_boundary() -> None:
    """A multi-byte payload is cut before the character that would cross the limit."""
    lines, write = collector()
    text = "é€" * 500
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, trim_payload_size=101),)) as conn:
        await conn.dialer.request(text)
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    prefix, cut = trimmed(record["request"]["payloads"][0])
    kept = len(prefix.encode("utf-8"))
    assert 101 - 3 <= kept <= 101
    assert prefix.startswith('"')
    serialised = {len(json.dumps(text).encode("utf-8")), len(json.dumps(text, ensure_ascii=False).encode("utf-8"))}
    assert kept + cut in serialised


async def test_trim_none_logs_the_whole_payload() -> None:
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, trim_payload_size=None),)) as conn:
        await conn.dialer.request(LARGE)
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert record["request"]["payloads"] == [LARGE]


async def test_default_trim_keeps_short_payloads_and_trims_large_ones() -> None:
    """The default `trim_payload_size` is 4096 bytes."""
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        await conn.dialer.request({"action": "echo", "v": "y" * 1000})
        await conn.dialer.request(LARGE)
        await until(lambda: len(lines) == 2)

    short, large = records(lines)
    assert short["request"]["payloads"] == [{"action": "echo", "v": "y" * 1000}]
    prefix, cut = trimmed(large["request"]["payloads"][0])
    assert len(prefix.encode("utf-8")) == 4096
    assert cut == len(LARGE) + 2 - 4096


# ---------------------------------------------------------------- redact and include


async def test_redact_replaces_payloads_before_they_are_logged() -> None:
    """`redact(payload, side)` sees every payload with its side; the log holds what it returns."""
    lines, write = collector()
    calls: list[str] = []

    def redact(payload: Any, side: str) -> Any:
        calls.append(side)
        if isinstance(payload, dict) and "card" in payload:
            return {**payload, "card": "***"}
        if isinstance(payload, dict) and "echo" in payload:
            return {"echo": "<redacted>"}
        return payload

    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, redact=redact),)) as conn:
        reply = await conn.dialer.request({"action": "login", "user": "ana", "card": "4111 1111"})
        assert reply["echo"]["card"] == "4111 1111"
        await until(lambda: len(lines) == 1)

    assert "4111" not in lines[0]
    (record,) = records(lines)
    assert record["request"]["payloads"] == [{"action": "login", "user": "ana", "card": "***"}]
    assert record["response"]["payloads"] == [{"echo": "<redacted>"}]
    assert "request" in calls
    assert "response" in calls


async def test_include_false_leaves_the_stream_out_of_the_log() -> None:
    """`include(headers, payload)` returning False on the opening frame suppresses the whole record."""
    lines, write = collector()
    seen: list[tuple[Any, Any]] = []

    def include(headers: Any, payload: Any) -> bool:
        seen.append((headers, payload))
        return not (isinstance(payload, dict) and payload.get("action") == "secret")

    async with running(dialer_logs=(ExchangeLog(CODEC, write=write, include=include),)) as conn:
        await conn.dialer.request({"action": "secret"}, headers={"kind": "s"})
        await conn.dialer.request({"action": "echo"})
        await until(lambda: len(lines) >= 1)
        await asyncio.sleep(0.05)

    (record,) = records(lines)
    assert record["request"]["payloads"] == [{"action": "echo"}]
    assert ({"kind": "s"}, {"action": "secret"}) in seen


# ---------------------------------------------------------------- outcomes


async def test_a_handler_error_is_logged_as_reset_with_code_and_reason() -> None:
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()
    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write),),
        server_logs=(ExchangeLog(CODEC, write=server_write),),
    ) as conn:
        with pytest.raises(RemoteError):
            await conn.dialer.request({"action": "raise"})
        await until(lambda: len(dialer_lines) == 1 and len(server_lines) == 1)

    for record in records(dialer_lines) + records(server_lines):
        assert record["outcome"] == "reset"
        assert record["code"] == int(ResetCode.APPLICATION_ERROR)
        assert record["reason"] == "handler said no"
        assert record["request"]["payloads"] == [{"action": "raise"}]


async def test_a_cancelled_stream_is_logged_as_reset_by_the_opener() -> None:
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        held = conn.dialer.open({"action": "forever"}, end=True)
        await asyncio.wait_for(conn.server.holding.wait(), WAIT_SECONDS)
        await held.cancel()
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert record["outcome"] == "reset"
    assert record["code"] == int(ResetCode.CANCELLED)


async def test_notify_is_logged_as_oneway_at_once() -> None:
    """A notify is written when its open is seen: oneway, empty response, outcome ok."""
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()
    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write),),
        server_logs=(ExchangeLog(CODEC, write=server_write),),
    ) as conn:
        await conn.dialer.notify({"action": "echo", "note": 1})
        await until(lambda: len(dialer_lines) == 1 and len(server_lines) == 1)

    for record in records(dialer_lines) + records(server_lines):
        assert record["oneway"] is True
        assert record["request"]["payloads"] == [{"action": "echo", "note": 1}]
        assert record["response"]["payloads"] == []
        assert record["outcome"] == "ok"
        assert record["first_response_ms"] is None


async def test_connection_drop_writes_open_streams_as_lost_once() -> None:
    """A dropped socket writes each unfinished stream as lost; finished streams are not written again."""
    dialer_lines, dialer_write = collector()
    server_lines, server_write = collector()
    async with running(
        dialer_logs=(ExchangeLog(CODEC, write=dialer_write),),
        server_logs=(ExchangeLog(CODEC, write=server_write),),
    ) as conn:
        await conn.dialer.request({"action": "echo"})
        held = conn.dialer.open({"action": "forever"}, end=True)
        await asyncio.wait_for(conn.server.holding.wait(), WAIT_SECONDS)
        conn.server.connection.transport.abort()
        await until(lambda: len(dialer_lines) == 2 and len(server_lines) == 2)
        await asyncio.sleep(0.05)

    for lines in (dialer_lines, server_lines):
        finished, lost = records(lines)
        assert finished["outcome"] == "ok"
        assert finished["request"]["payloads"] == [{"action": "echo"}]
        assert lost["outcome"] == "lost"
        assert lost["stream"] == held.id
        assert lost["request"]["payloads"] == [{"action": "forever"}]
        assert lost["response"]["payloads"] == []
        assert lost["first_response_ms"] is None
        assert lost["duration_ms"] >= 0


async def test_a_raising_write_redact_or_include_drops_only_its_stream(caplog: pytest.LogCaptureFixture) -> None:
    """Each callback raises for one request: that stream is not logged, the others are and every reply arrives."""
    lines: list[str] = []

    def write(line: str) -> None:
        if '"fail-write"' in line:
            raise OSError("disk full")
        lines.append(line)

    def redact(payload: Any, _side: str) -> Any:
        if payload == "fail-redact":
            raise ValueError("cannot redact")
        return payload

    def include(_headers: dict[str, Any] | None, payload: Any) -> bool:
        if payload == "fail-include":
            raise ValueError("cannot decide")
        return True

    caplog.set_level(logging.ERROR, logger="muxws_docs_exchange_log")
    async with running(server_logs=(ExchangeLog(CODEC, write=write, redact=redact, include=include),)) as conn:
        for payload in ("fail-write", "fail-redact", "fail-include", "fine"):
            assert await conn.dialer.request(payload) == len(payload)
        await until(lambda: len(lines) == 1)
        await asyncio.sleep(0.05)
        assert conn.dialer.is_open

    (record,) = records(lines)
    assert record["request"]["payloads"] == ["fine"]
    assert caplog.text.count("exchange log dropped stream") == 3


# ---------------------------------------------------------------- timing


async def test_durations_are_non_negative_and_ordered() -> None:
    lines, write = collector()
    async with running(dialer_logs=(ExchangeLog(CODEC, write=write),)) as conn:
        await conn.dialer.request({"action": "echo"})
        [item async for item in conn.dialer.open({"action": "stream"}, end=True)]
        await until(lambda: len(lines) == 2)

    for record in records(lines):
        assert record["first_response_ms"] is not None
        assert 0 <= record["first_response_ms"] <= record["duration_ms"]


async def test_timing_follows_the_injected_clock() -> None:
    """With a test clock: first response 5 s after the open, end 12 s after it."""
    server = Server()
    lines, write = collector()
    async with running(server_logs=(ExchangeLog(CODEC, write=write, clock=server.clock),), server=server) as conn:
        chunks = [item async for item in conn.dialer.open({"action": "timed"}, end=True)]
        assert chunks == ["first", "last"]
        await until(lambda: len(lines) == 1)

    (record,) = records(lines)
    assert record["first_response_ms"] == pytest.approx(5000.0)
    assert record["duration_ms"] == pytest.approx(12000.0)


# ---------------------------------------------------------------- logging integration


async def test_without_write_each_line_goes_to_the_named_logger(caplog: pytest.LogCaptureFixture) -> None:
    """`write=None` sends the JSON line to `logging.getLogger("app.muxws.exchanges").info`."""
    caplog.set_level(logging.INFO, logger="app.muxws.exchanges")
    async with running(dialer_logs=(ExchangeLog(CODEC),)) as conn:
        await conn.dialer.request({"action": "echo"})
        await until(lambda: any(r.name == "app.muxws.exchanges" for r in caplog.records))

    (entry,) = [r for r in caplog.records if r.name == "app.muxws.exchanges"]
    assert entry.levelno == logging.INFO
    assert json.loads(entry.getMessage())["request"]["payloads"] == [{"action": "echo"}]


async def test_a_custom_logger_name_is_used(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="audit.ws")
    async with running(dialer_logs=(ExchangeLog(CODEC, logger="audit.ws"),)) as conn:
        await conn.dialer.request({"action": "echo"})
        await until(lambda: any(r.name == "audit.ws" for r in caplog.records))


@pytest.fixture
def django_logging(tmp_path: Path) -> Any:
    """The recipe's `LOGGING`, every file handler pointed into `tmp_path`, applied as Django applies it."""
    config = copy.deepcopy(load_example("django_logging").LOGGING)
    files: dict[str, Path] = {}
    for name, handler in config.get("handlers", {}).items():
        if "filename" in handler:
            files[name] = tmp_path / f"{name}.log"
            handler["filename"] = str(files[name])
    assert files, "the recipe's LOGGING has no handler writing to a file"
    logging.config.dictConfig(config)
    yield config, files
    for name in config.get("loggers", {}):
        configured = logging.getLogger(name)
        for handler in list(configured.handlers):
            configured.removeHandler(handler)
            handler.close()
        configured.setLevel(logging.NOTSET)
        configured.propagate = True
    for existing in logging.Logger.manager.loggerDict.values():
        if isinstance(existing, logging.Logger):
            existing.disabled = False


def lines_in(files: dict[str, Path]) -> list[str]:
    return [line for path in files.values() if path.exists() for line in path.read_text("utf-8").splitlines()]


async def test_django_logging_routes_muxws_frames_to_the_file(django_logging: Any) -> None:
    """With the recipe's `LOGGING` applied, `muxws.frames` lines of a real exchange land in the file."""
    config, files = django_logging
    assert "muxws.frames" in config["loggers"] or "muxws" in config["loggers"]
    async with running() as conn:
        await conn.dialer.request({"action": "echo"})
    for handler in logging.getLogger("muxws.frames").handlers + logging.getLogger("muxws").handlers:
        handler.flush()

    frame_lines = [line for line in lines_in(files) if f"conn={conn.dialer.id}" in line]
    assert any("dir=tx" in line and "type=open" in line for line in frame_lines)
    assert any("dir=rx" in line and "type=data" in line for line in frame_lines)
    assert all('"action"' not in line for line in frame_lines)


async def test_django_logging_writes_exchange_lines_as_bare_json(django_logging: Any) -> None:
    """The recipe's `app.muxws.exchanges` entry writes each record as the bare JSON line."""
    config, files = django_logging
    assert "app.muxws.exchanges" in config["loggers"]
    async with running(dialer_logs=(ExchangeLog(CODEC),)) as conn:
        await conn.dialer.request({"action": "echo"})
        await asyncio.sleep(0.05)
    for handler in logging.getLogger("app.muxws.exchanges").handlers:
        handler.flush()

    exchanges = [json.loads(line) for line in lines_in(files) if line.startswith("{")]
    assert [record["conn"] for record in exchanges] == [conn.dialer.id]
    assert exchanges[0]["request"]["payloads"] == [{"action": "echo"}]
