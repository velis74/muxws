"""Django `LOGGING` that writes the muxws loggers and the exchange log (`exchange_logging.py`) to their own files."""

import os

from pathlib import Path

LOG_DIR = Path(os.environ.get("MYAPP_LOG_DIR", "/var/log/myapp"))

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "timestamped": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
        "message": {"format": "%(message)s"},
    },
    "handlers": {
        "muxws_file": {
            "class": "logging.FileHandler",
            "filename": str(LOG_DIR / "muxws.log"),
            "formatter": "timestamped",
        },
        # One JSON object per line: the formatter writes the message alone.
        "exchanges_file": {
            "class": "logging.FileHandler",
            "filename": str(LOG_DIR / "exchanges.jsonl"),
            "formatter": "message",
        },
    },
    "loggers": {
        # "muxws" is the parent of muxws.frames, muxws.reconnect, muxws.registry, muxws.origin and muxws.codec.
        "muxws": {"handlers": ["muxws_file"], "level": "INFO", "propagate": False},
        # One line per frame; "INFO" or higher turns it off.
        "muxws.frames": {"level": "DEBUG"},
        "app.muxws.exchanges": {"handlers": ["exchanges_file"], "level": "INFO", "propagate": False},
    },
}
