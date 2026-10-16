"""The msgpack codec, a binary implementation of `Codec` (WSM-CDC-006/007/008).

The module name carries a trailing underscore for the same reason `json_.py` does: a top-level
`msgpack.py` inside the package would shadow the third-party module this very file imports.

`msgpack` is an optional extra (WSM-PKG-002). No other muxws module imports this one, so `msgpack`
is imported only by an application that imports this module. The module does not register itself
(WSM-CDC-014); the application calls `register_codec("msgpack", MsgpackCodec())` during bootstrap
(WSM-CDC-013).

Test fixtures do not pin msgpack bytes (WSM-CDC-006): this library and `@msgpack/msgpack` choose
integer widths and map formats differently and both are valid. Tests assert round-trips only.
"""

from __future__ import annotations

from typing import Any, Final

import msgpack

from muxws.errors import ProtocolError
from muxws.frames import Frame, from_mapping, to_mapping

#: `use_bin_type=True` keeps `bytes` and `str` distinct on the wire; bytes are a payload type under
#: a binary codec (WSM-CDC-008). It is the msgpack 1.x default and is stated explicitly: with the
#: two types merged, round-trips within one language still succeed but the other port decodes
#: bytes as strings.
_PACK_KWARGS: Final[dict[str, Any]] = {"use_bin_type": True}

#: - `raw=False` decodes msgpack `str` to `str` rather than to `bytes`. Without it every envelope key
#:   and every `type` value would decode as `bytes`, `from_mapping` would find no `"type"` key, and
#:   every inbound frame would be a protocol error. It is the 1.x default and is stated explicitly
#:   because msgpack 0.x defaulted to `raw=True`.
#: - `strict_map_key=False` allows map keys other than `str`/`bytes`. With the default (True) an
#:   integer key raises `ValueError`, and a payload containing one would fail to decode and end the
#:   connection. `@msgpack/msgpack` accepts integer keys by default, so both ports decode the same
#:   set of payloads. The results differ: JavaScript object keys are strings, so `{1: "a"}` decodes
#:   as `{1: "a"}` here and as `{"1": "a"}` in TypeScript. The shared corpus contains no such payload.
#: - `use_list=True` decodes a msgpack array to a `list`, not a `tuple`. It is the default. With
#:   `use_list=False`, `decode(encode(frame)) == frame` fails for every payload containing an array,
#:   and WSM-CDC-006 requires that equality.
_UNPACK_KWARGS: Final[dict[str, Any]] = {"raw": False, "strict_map_key": False, "use_list": True}


def _unpack(data: str | bytes, what: str) -> Any:
    """Decode one msgpack value, turning every library failure into a `ProtocolError`.

    A correct peer never sends a text message under a binary codec, because it picks `send_bytes`
    from the declared `binary` (WSM-CDC-002). A `str` arriving here means the remote sent a text
    message on a binary connection; the original bytes cannot be recovered from it, so it raises
    `ProtocolError` (WSM-FRM-005).
    """
    if isinstance(data, str):
        raise ProtocolError(
            f"the msgpack codec was handed a text {what}: msgpack is a binary codec and a text "
            f"WebSocket message cannot carry it (WSM-CDC-002, WSM-FRM-005)"
        )
    try:
        return msgpack.unpackb(data, **_UNPACK_KWARGS)
    except (msgpack.UnpackException, ValueError, TypeError) as exc:
        raise ProtocolError(f"the msgpack codec could not decode the {what}: {exc} (WSM-FRM-005)") from exc


class MsgpackCodec:
    """`Codec` implementation over the `msgpack` package."""

    name = "msgpack"
    #: Declared, not inferred (WSM-CDC-002). The peer reads this to choose `send_bytes`, not the type
    #: `encode` returned.

    binary = True

    def encode(self, frame: Frame) -> bytes:
        return msgpack.packb(to_mapping(frame), **_PACK_KWARGS)

    def decode(self, message: str | bytes) -> Frame:
        mapping = _unpack(message, "message")
        if not isinstance(mapping, dict):
            raise ProtocolError(f"a frame must decode to a map, got {type(mapping).__name__} (WSM-FRM-005)")
        return from_mapping(mapping)

    def encode_payload(self, payload: Any) -> bytes:
        return msgpack.packb(payload, **_PACK_KWARGS)

    def decode_payload(self, data: str | bytes) -> Any:
        return _unpack(data, "reassembled payload")
