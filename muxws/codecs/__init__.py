"""The codec seam: the port, and the explicit registry behind it (§2.1, WSM-CDC-001..016)."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from muxws.errors import CodecNotRegistered
from muxws.frames import Frame


@runtime_checkable
class Codec(Protocol):
    """Turns a logical frame into a WebSocket message and back.

    `binary` is **declared, not inferred** (WSM-CDC-002): the peer reads it to choose the socket's
    text or binary send method and the inbound message type it expects. A peer never sniffs a message
    to decide which branch to take.

    `encode_payload` / `decode_payload` are the payload-level half of the port. WSM-CDC-001 names
    only the frame-level pair, but fragmentation is defined in terms of the encoded form of a
    *logical payload* (WSM-FRG-011) which the receiver hands back to the codec once reassembled
    (WSM-FRG-030), and neither operation can be expressed through `encode`/`decode` alone.
    See GAPS.md.
    """

    name: str
    binary: bool

    def encode(self, frame: Frame) -> str | bytes: ...

    def decode(self, message: str | bytes) -> Frame: ...

    def encode_payload(self, payload: Any) -> str | bytes: ...

    def decode_payload(self, data: str | bytes) -> Any: ...


_REGISTRY: dict[str, Codec] = {}


def register_codec(name: str, codec: Codec) -> None:
    """Register `codec` under `name`.

    Registration is explicit and eager (WSM-CDC-013): there is no dynamic import, no lazy
    auto-registration, no entry-point scan, and no probing of whether a module happens to be
    installed. A codec module never registers itself at import time (WSM-CDC-014).
    """
    _REGISTRY[name] = codec


def get_codec(name: str) -> Codec:
    """Return the codec registered under `name`.

    Raises `CodecNotRegistered` naming the environment variable, the value found and the registered
    set (WSM-CDC-016). There is no fallback to JSON, ever - a deployment that believes it is running
    msgpack and silently is not may never find out (WSM-INV-015).
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
