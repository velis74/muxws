"""The msgpack codec - the honest proof that the codec seam is a seam (WSM-CDC-006/007/008).

The module name carries a trailing underscore for the same reason `json_.py` does: a top-level
`msgpack.py` inside the package would shadow the third-party module this very file imports.

`msgpack` is an **optional extra** (WSM-PKG-002). Importing this module is the whole of the
selection: nothing inside muxws imports it, so a deployment that never asks for msgpack never pays
for it. And this module does **not** register itself (WSM-CDC-014) - the application calls
`register_codec("msgpack", MsgpackCodec())` during bootstrap. There is no entry-point scan and no
"is it installed" probe anywhere (WSM-CDC-013).

Nothing here may be pinned as bytes in a fixture (WSM-CDC-006): this library and
`@msgpack/msgpack` make different but equally valid choices about integer width and map format, and
a pinned-bytes fixture would make a legal encoder fail. Round-trip is the only assertion.
"""

from __future__ import annotations

from typing import Any, Final

import msgpack

from muxws.errors import ProtocolError
from muxws.frames import Frame, from_mapping, to_mapping

#: `use_bin_type=True` is what keeps `bytes` and `str` distinguishable on the wire, and bytes are a
#: first-class payload type under a binary codec (WSM-CDC-008). It has been the default since
#: msgpack 1.0; it is written out because a codec whose byte type collapsed into its string type
#: would still round-trip within one language and fail only against the other port.
_PACK_KWARGS: Final[dict[str, Any]] = {"use_bin_type": True}

#: - `raw=False` decodes msgpack `str` to `str` rather than to `bytes`. Without it every envelope key
#:   and every `type` value would come back as `bytes`, `from_mapping` would see no `"type"` key, and
#:   every inbound frame would be a protocol error. It is the 1.x default, and it is stated because
#:   it was *not* the default in 0.x and a codec that silently changed shape on a downgrade is worse
#:   than one that fails.
#: - `strict_map_key=False` lets a map key be something other than `str`/`bytes`. The default (True)
#:   raises `ValueError` on an integer key, which would turn an application's choice of payload shape
#:   into a decode failure that kills the connection - muxws does not police what a payload contains.
#:   `@msgpack/msgpack` accepts integer keys by default, so this is also what makes the two ports
#:   agree about which payloads are decodable at all. They do *not* agree on the result: JavaScript
#:   object keys are strings, so `{1: "a"}` comes back as `{1: "a"}` here and as `{"1": "a"}` there.
#:   That divergence is reported in GAPS.md and is why no such payload is in the shared corpus.
#: - `use_list=True` decodes a msgpack array to a `list`, not a `tuple`. It is the default, and it is
#:   load-bearing: with `use_list=False`, `decode(encode(frame)) == frame` fails for every payload
#:   containing an array, which is exactly the assertion WSM-CDC-006 rests on.
_UNPACK_KWARGS: Final[dict[str, Any]] = {"raw": False, "strict_map_key": False, "use_list": True}


def _unpack(data: str | bytes, what: str) -> Any:
    """Decode one msgpack value, turning every library failure into a `ProtocolError`.

    A `str` never reaches a binary codec from a correct peer: `binary` is declared, and a peer picks
    `send_bytes` from it rather than sniffing the encoded value (WSM-CDC-002). A text message
    arriving here therefore means the remote sent one on a binary connection, and there is no
    recoding that could recover the bytes - the transport has already lost them. Saying so is the
    only honest answer (WSM-FRM-005).
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
    #: Declared, never inferred (WSM-CDC-002). The peer reads this to choose `send_bytes`; it does
    #: not look at what `encode` returned.
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
