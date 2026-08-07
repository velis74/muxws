"""The logical frame model and its envelope mapping (§3).

One frame per WebSocket message. Field names are spelled out, never abbreviated, and stay snake_case
in both languages.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any, Final

from muxws.errors import ProtocolError


class _Absent:
    """The sentinel distinguishing "no payload" from an explicit `null` (D1).

    Falsy, so `if frame.payload:` behaves the way a reader expects, and with a `repr` that names
    itself rather than leaking a memory address into a test failure.
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "ABSENT"

    def __copy__(self) -> _Absent:
        return self

    def __deepcopy__(self, _memo: dict) -> _Absent:
        return self


#: Single module-level sentinel. `payload=ABSENT` emits no key; `payload=None` emits `"payload": null`.
ABSENT: Any = _Absent()

#: The frame types a v1 peer sends. `window_update` is reserved and unimplemented (WSM-BPR-001), and
#: there is no `settings` frame (WSM-CON-031). A receiver tolerates anything not in this set.
V1_FRAME_TYPES: Final[frozenset[str]] = frozenset({"open", "data", "reset", "ping", "pong", "goaway"})

#: Envelope keys carrying a value other than the field default are emitted in this order: `type`,
#: then `stream`, then the rest alphabetically (WSM-CDC-005).
_LEADING_KEYS: Final[tuple[str, ...]] = ("type", "stream")


@dataclass(frozen=True, slots=True)
class Frame:
    """One logical protocol unit. Frozen and compared by value (D3)."""

    type: str
    stream: int | None = None
    payload: Any = ABSENT
    fragment: str | bytes | None = None
    more: bool = False
    headers: dict[str, Any] | None = None
    end: bool = False
    trailers: dict[str, Any] | None = None
    code: int | None = None
    reason: str | None = None
    nonce: str | None = None
    last_stream: int | None = None


_FIELD_NAMES: Final[frozenset[str]] = frozenset(f.name for f in fields(Frame))
_FIELD_DEFAULTS: Final[dict[str, Any]] = {f.name: f.default for f in fields(Frame)}


def to_mapping(frame: Frame) -> dict[str, Any]:
    """Render `frame` as an envelope, omitting every field still at its default.

    `payload` is the one field whose default is not `None`: `ABSENT` omits the key entirely, while
    `None` emits `"payload": null` (D1).
    """
    present: dict[str, Any] = {}
    for name in _FIELD_NAMES:
        value = getattr(frame, name)
        if name == "payload":
            if value is not ABSENT:
                present[name] = value
            continue
        if value != _FIELD_DEFAULTS[name] or name == "type":
            present[name] = value

    ordered: dict[str, Any] = {}
    for key in _LEADING_KEYS:
        if key in present:
            ordered[key] = present.pop(key)
    for key in sorted(present):
        ordered[key] = present[key]
    return ordered


def from_mapping(mapping: Mapping[str, Any]) -> Frame:
    """Build a `Frame` from a decoded envelope.

    Unknown keys are dropped rather than preserved (WSM-FRM-001, D2) - a decoder that round-tripped
    them would make `decode(encode(frame)) == frame` pass on garbage. An unrecognised `type` survives
    as an ordinary `Frame` (D4); it is the peer, not the codec, that ignores it (WSM-FRM-002).
    """
    if "type" not in mapping:
        raise ProtocolError("frame is missing the required 'type' field (WSM-FRM-005)")
    if "payload" in mapping and "fragment" in mapping:
        raise ProtocolError("frame carries both 'payload' and 'fragment' (WSM-FRM-004)")

    known = {key: value for key, value in mapping.items() if key in _FIELD_NAMES}
    if "payload" not in known:
        known["payload"] = ABSENT
    return Frame(**known)
