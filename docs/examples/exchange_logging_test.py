"""Runs `exchange_logging.log_exchanges` with `django_logging.LOGGING` applied and reads the JSON lines it wrote."""

import asyncio
import importlib
import json
import logging
import logging.config

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from exchange_logging import log_exchanges

from muxws import JsonCodec, Peer, RemoteError, Stream
from muxws.transports.memory import memory_pair

LOGGERS = ("muxws", "muxws.frames", "app.muxws.exchanges")


@pytest.fixture
def log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    saved = {
        name: (logger.level, logger.handlers[:], logger.propagate)
        for name in LOGGERS
        for logger in [logging.getLogger(name)]
    }
    monkeypatch.setenv("MYAPP_LOG_DIR", str(tmp_path))
    import django_logging

    logging.config.dictConfig(importlib.reload(django_logging).LOGGING)
    yield tmp_path
    for name, (level, handlers, propagate) in saved.items():
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            if handler not in handlers:
                handler.close()
        logger.setLevel(level)
        logger.handlers[:] = handlers
        logger.propagate = propagate


@pytest.fixture
async def peers() -> AsyncIterator[tuple[Peer, Peer]]:
    left, right = memory_pair()
    dialer = Peer(left, codec=JsonCodec(), is_dialer=True)
    acceptor = Peer(right, codec=JsonCodec(), is_dialer=False)
    loops = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
    yield dialer, acceptor
    await dialer.close(drain=0.1)
    for loop in loops:
        loop.cancel()
    await asyncio.gather(*loops, return_exceptions=True)


def written_records(log_dir: Path) -> list[dict[str, Any]]:
    for handler in logging.getLogger("app.muxws.exchanges").handlers:
        handler.flush()
    lines = (log_dir / "exchanges.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


async def test_a_request_is_written_as_one_redacted_json_line(log_dir: Path, peers: tuple[Peer, Peer]) -> None:
    dialer, acceptor = peers

    async def handler(payload: Any, stream: Stream) -> None:
        await stream.reply({"user": payload["user"], "ok": True}, headers={"status": 200})

    acceptor.on_stream(handler)
    log_exchanges(dialer)

    answer = await dialer.request({"user": "ana", "password": "hunter2"}, headers={"cookie": "sid=1"}, timeout=5.0)

    assert answer == {"user": "ana", "ok": True}
    [record] = written_records(log_dir)
    assert record["opened_by"] == "local"
    assert record["outcome"] == "ok"
    assert record["request"]["payloads"] == [{"user": "ana", "password": "***"}]
    assert record["request"]["headers"] == {"cookie": "***"}
    assert record["response"]["payloads"] == [{"user": "ana", "ok": True}]
    assert record["response"]["headers"] == {"status": 200}
    assert record["duration_ms"] >= 0
    text = (log_dir / "exchanges.jsonl").read_text(encoding="utf-8")
    assert "hunter2" not in text
    assert "sid=1" not in text
    assert "hunter2" not in (log_dir / "muxws.log").read_text(encoding="utf-8")


async def test_a_long_payload_is_trimmed_and_still_redacted(log_dir: Path, peers: tuple[Peer, Peer]) -> None:
    dialer, acceptor = peers

    async def handler(payload: Any, stream: Stream) -> None:
        await stream.reply({"size": len(payload["blob"])})

    acceptor.on_stream(handler)
    log_exchanges(dialer)

    await dialer.request({"password": "hunter2", "blob": "z" * 10_000}, timeout=5.0)

    [record] = written_records(log_dir)
    assert record["request"]["payloads"] == ["***"]
    assert record["response"]["payloads"] == [{"size": 10_000}]


async def test_a_reset_stream_is_written_with_its_code(log_dir: Path, peers: tuple[Peer, Peer]) -> None:
    dialer, acceptor = peers

    async def handler(_payload: Any, _stream: Stream) -> None:
        raise ValueError("no")

    acceptor.on_stream(handler)
    log_exchanges(acceptor, redact=None)

    with pytest.raises(RemoteError):
        await dialer.request("hi", timeout=5.0)
    await asyncio.sleep(0.05)

    [record] = written_records(log_dir)
    assert record["opened_by"] == "remote"
    assert record["outcome"] == "reset"
    assert record["reset_by"] == "local"
    assert record["code"] == 2
    assert record["request"]["payloads"] == ["hi"]
