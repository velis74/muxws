"""The `Codec` protocol and the codec registry (§2.1, WSM-CDC-001..016)."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from muxws.errors import CodecNotRegistered
from muxws.frames import Frame


@runtime_checkable
class Codec(Protocol):
    """Turns a logical frame into a WebSocket message and back.

    `binary` is declared, not inferred (WSM-CDC-002): the peer reads it to choose the socket's text
    or binary send method. The peer does not inspect a message to choose.

    `encode_payload` / `decode_payload` encode and decode a payload without an envelope.
    Fragmentation slices the encoded form of a logical payload (WSM-FRG-011), and the receiver
    decodes the reassembled bytes (WSM-FRG-030); neither can be expressed through `encode`/`decode`.
    """

    name: str
    binary: bool

    def encode(self, frame: Frame) -> str | bytes:
        """One whole frame - envelope and payload - as it goes on the wire.

        `str` under a text codec, `bytes` under a binary one; the peer picks the socket method from
        `binary`, not from this return type. An absent field MUST NOT be emitted: an omitted
        `payload` key and `"payload": null` are different frames.
        """
        ...

    def decode(self, message: str | bytes) -> Frame:
        """The inverse. Unknown envelope fields MUST be dropped rather than rejected (WSM-FRM-001).

        Raises `ProtocolError` when this codec cannot decode the message. The peer treats that as a
        connection-level error and answers it with `goaway`.
        """
        ...

    def encode_payload(self, payload: Any) -> str | bytes:
        """A payload on its own, with no envelope around it.

        Fragmentation slices this output (WSM-FRG-011), so it must equal the bytes that appear in a
        frame's payload position; otherwise the receiver cannot decode the reassembled fragments.
        """
        ...

    def decode_payload(self, data: str | bytes) -> Any:
        """Decode concatenated fragments into a payload (WSM-FRG-030). The inverse of `encode_payload`.

        Raises `ProtocolError` when the data cannot be decoded.
        """
        ...


_REGISTRY: dict[str, Codec] = {}


def register_codec(name: str, codec: Codec) -> None:
    """Register `codec` under `name`.

    Registration is explicit (WSM-CDC-013): muxws does not import codec modules dynamically, scan
    entry points, or check which modules are installed. A codec module does not register itself at
    import time (WSM-CDC-014). Registering a name again replaces the previous codec.
    """
    _REGISTRY[name] = codec


def get_codec(name: str) -> Codec:
    """Return the codec registered under `name`.

    Raises `CodecNotRegistered` naming the environment variable, the value found and the registered
    names (WSM-CDC-016). There is no fallback to JSON (WSM-INV-015).

    """
    try:
        return _REGISTRY[name]
    except KeyError:
        available = registered_codecs()
        raise CodecNotRegistered(
            f"codec {name!r} is not registered (MUXWS_CODEC={name!r}); "
            f"registered codecs are {available!r}. Call register_codec({name!r}, ...) during "
            f"bootstrap, before connecting.",
            configured=name,
            available=available,
        ) from None


def registered_codecs() -> list[str]:
    """Every registered codec name, sorted."""
    return sorted(_REGISTRY)
