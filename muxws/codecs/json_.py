"""The JSON codec - the interoperability baseline (WSM-CDC-004..008).

The module name carries a trailing underscore: a top-level `json.py` inside the package would shadow
the stdlib module this file imports.

This module does not register itself (WSM-CDC-014). `muxws/__init__.py` registers it, because the
library ships with JSON registered (WSM-CDC-004).
"""

from __future__ import annotations

import json

from typing import Any

from muxws.errors import ProtocolError
from muxws.frames import Frame, from_mapping, to_mapping

#: Compact separators and no ASCII escaping, so that the output matches `JSON.stringify` except for
#: floats and integers beyond 2**53.
_DUMP_KWARGS: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False, "allow_nan": False}


def _reject(value: Any) -> Any:
    """`json.dumps` default hook: raises `TypeError` for every value it receives.

    Bytes are a first-class payload type under a binary codec and are *not* one under JSON, and muxws
    MUST NOT base64-encode them on the application's behalf (WSM-CDC-008).
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(
            f"the json codec cannot carry {type(value).__name__}: bytes are a payload type only under "
            f"a binary codec, and muxws does not base64-encode them (WSM-CDC-008). Either "
            f"encode them in the application or configure the msgpack codec."
        )
    raise TypeError(f"the json codec cannot encode {type(value).__name__}")


class JsonCodec:
    """`Codec` implementation over the standard library's JSON."""

    name = "json"
    binary = False

    def encode(self, frame: Frame) -> str:
        return json.dumps(to_mapping(frame), default=_reject, **_DUMP_KWARGS)

    def decode(self, message: str | bytes) -> Frame:
        try:
            mapping = json.loads(message)
        except (ValueError, TypeError) as exc:
            raise ProtocolError(f"the json codec could not decode the message: {exc} (WSM-FRM-005)") from exc
        if not isinstance(mapping, dict):
            raise ProtocolError(f"a frame must decode to an object, got {type(mapping).__name__} (WSM-FRM-005)")
        return from_mapping(mapping)

    def encode_payload(self, payload: Any) -> str:
        return json.dumps(payload, default=_reject, **_DUMP_KWARGS)

    def decode_payload(self, data: str | bytes) -> Any:
        try:
            return json.loads(data)
        except (ValueError, TypeError) as exc:
            raise ProtocolError(f"the json codec could not decode a reassembled payload: {exc}") from exc
