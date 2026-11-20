"""Applies `django_logging.LOGGING` the way Django does and checks which file each log line is written to."""

import importlib
import logging
import logging.config

from collections.abc import Iterator
from pathlib import Path

import pytest

from muxws import Frame
from muxws.observability import log_frame

LOGGERS = ("muxws", "muxws.frames", "app.muxws.exchanges")


@pytest.fixture
def configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
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


def test_frame_lines_go_to_the_muxws_file(configured: Path) -> None:
    log_frame("t-0", "tx", Frame("open", stream=1, payload="secret", end=True), 30)
    logging.getLogger("muxws.reconnect").warning("dial failed")
    for handler in logging.getLogger("muxws").handlers:
        handler.flush()

    text = (configured / "muxws.log").read_text(encoding="utf-8")
    assert "DEBUG muxws.frames muxws conn=t-0 dir=tx type=open" in text
    assert "WARNING muxws.reconnect dial failed" in text
    assert "secret" not in text


def test_exchange_lines_go_to_their_own_file_as_bare_messages(configured: Path) -> None:
    logging.getLogger("app.muxws.exchanges").info('{"stream":1}')
    for handler in logging.getLogger("app.muxws.exchanges").handlers:
        handler.close()

    assert (configured / "exchanges.jsonl").read_text(encoding="utf-8") == '{"stream":1}\n'
    assert not (configured / "muxws.log").read_text(encoding="utf-8")
