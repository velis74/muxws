"""Deployment configuration (§2.2, WSM-CDC-010/011).

The codec name is read from the environment, not from a call argument, because both ends of a
connection must agree on it and an argument is decided per call site rather than per deployment.
"""

from __future__ import annotations

import os


class Settings:
    """The one settings singleton. Writable, so an application may set it during bootstrap."""

    def __init__(self) -> None:
        self.codec: str = os.environ.get("MUXWS_CODEC", "json")

    def reload(self) -> None:
        """Re-read the environment. For tests; an application sets `codec` directly."""
        self.codec = os.environ.get("MUXWS_CODEC", "json")

    def __repr__(self) -> str:
        return f"Settings(codec={self.codec!r})"


#: Read at connection time, never at import time - or WSM-CDC-011's "an application may set it
#: during bootstrap" stops being true for anything that imports muxws early.
settings = Settings()
