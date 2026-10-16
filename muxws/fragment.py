"""Fragmentation: the splitter and the assembler (§4).

Nothing here touches a socket; the splitter is a pure function of its arguments (WSM-FRG-015).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Final

from muxws.codecs import Codec
from muxws.errors import ProtocolError
from muxws.frames import ABSENT, Frame

#: The largest encoded message a sender may emit. A **protocol constant**, not a setting: it is never
#: negotiated, never announced, and never read from configuration (WSM-FRG-004). A receiver accepts
#: anything up to it and may accept more; a sender always fragments at it regardless of what the
#: remote appears willing to accept.
MAX_FRAME_BYTES: Final[int] = 65_536


def encoded_length(message: str | bytes) -> int:
    """Byte length of an encoded message: UTF-8 for text, buffer length for bytes.

    A JavaScript string's `.length` counts UTF-16 code units and differs from this for any non-ASCII
    text, so WSM-FRG-002 defines the measurement for both ports.
    """
    if isinstance(message, str):
        return len(message.encode("utf-8"))
    return len(message)


def _reservation(cap: int) -> int:
    """The envelope budget the sender sets aside before slicing (WSM-FRG-013)."""
    return min(512, cap // 2)


def _floor_boundary(data: bytes, cut: int, *, text: bool) -> int:
    """Move `cut` back to the nearest offset in `data` a fragment may end on.

    Under a text codec that is a UTF-8 codepoint boundary (WSM-FRG-012). The slice is carried inside
    the fragment's envelope as a string value of the codec, and a JSON string cannot hold part of a
    codepoint. muxws does not use WebSocket continuation frames, because a continuation sequence
    occupies the socket until the message ends (WSM-INV-004); every fragment is a whole message and
    every slice consists of whole codepoints, in both ports (WSM-FRG-016).

    The loop moves back at most three bytes, because UTF-8 continuation bytes have the form
    `0b10xxxxxx`. Under a binary codec the unit is the byte and every offset is a boundary.
    """
    if text:
        while 0 < cut < len(data) and data[cut] & 0xC0 == 0x80:
            cut -= 1
    return cut


def _next_boundary(data: bytes, start: int, *, text: bool) -> int:
    """The first offset above `start` a fragment may end on: one codepoint on, or one byte on."""
    cut = start + 1
    if text:
        while cut < len(data) and data[cut] & 0xC0 == 0x80:
            cut += 1
    return cut


def _chunk(data: bytes, start: int, stop: int, *, text: bool) -> str | bytes:
    """The fragment payload for a byte range. A text codec's `fragment` field carries `str`."""
    piece = data[start:stop]
    return piece.decode("utf-8") if text else piece


def _fragment_frame(
    source: Frame,
    chunk: str | bytes,
    *,
    first: bool,
    last: bool,
) -> Frame:
    """Build one fragment frame.

    `headers` ride only the first fragment and are never split (WSM-FRG-021); `end` and `trailers`
    ride only the last (WSM-FRG-020); `more` is true on every fragment but the last.
    """
    return Frame(
        type=source.type,
        stream=source.stream,
        payload=ABSENT,
        fragment=chunk,
        more=not last,
        headers=source.headers if first else None,
        end=source.end if last else False,
        trailers=source.trailers if last else None,
        code=source.code,
        reason=source.reason,
        nonce=source.nonce,
        last_stream=source.last_stream,
    )


def _floor_error(cap: int) -> ProtocolError:
    """WSM-FRG-034: the cap cannot hold the envelope plus one indivisible unit of payload."""
    return ProtocolError(
        f"a frame cap of {cap} bytes cannot hold this frame's envelope plus one indivisible unit "
        f"of payload; raise the cap (WSM-FRG-034)"
    )


def _closing_floor_error(cap: int) -> ProtocolError:
    """The closing fragment does not fit even carrying no payload at all.

    `end` and `trailers` travel on the final fragment and are not fragmented (WSM-FRG-020), as
    `headers` are not (WSM-FRG-021). If they do not fit, no valid split exists, and the splitter
    raises instead of emitting fragments without end.
    """
    return ProtocolError(
        f"a frame cap of {cap} bytes cannot hold this frame's closing envelope; `trailers` ride the "
        f"final fragment whole and are never fragmented (WSM-FRG-020/034)"
    )


def _largest_fitting_count(
    source: Frame,
    data: bytes,
    position: int,
    ceiling: int,
    *,
    text: bool,
    first: bool,
    cap: int,
    codec: Codec,
) -> int:
    """Largest slice at `position` whose non-final fragment frame still fits under `cap`, in bytes.

    The verify-and-re-split step of WSM-FRG-014, as a binary search. It needs at most log2(ceiling)
    encodes, independent of how much the codec's escaping expands the payload (control characters
    under JSON expand up to sixfold). Both ports run the same search over the same encoded form in the
    same order and cut at the same boundary (WSM-FRG-016). Each probe's byte limit is floored onto a
    boundary the codec can represent, so the counted and the cut quantities are the same.

    Returns 0 when not even one unit fits: every probe below the first unit's width floors to an empty
    slice, which is not recorded.
    """
    low, high, best = 1, ceiling, 0
    while low <= high:
        middle = (low + high) // 2
        count = _floor_boundary(data, min(position + middle, len(data)), text=text) - position
        probe = _fragment_frame(source, _chunk(data, position, position + count, text=text), first=first, last=False)
        if encoded_length(codec.encode(probe)) <= cap:
            best = count
            low = middle + 1
        else:
            high = middle - 1
    return best


def split_frame(frame: Frame, cap: int = MAX_FRAME_BYTES, codec: Codec | None = None) -> list[Frame]:
    """All fragments of `frame` as a list. `iter_fragments` performs the same computation lazily."""
    return list(iter_fragments(frame, cap, codec))


def iter_fragments(frame: Frame, cap: int = MAX_FRAME_BYTES, codec: Codec | None = None) -> Iterator[Frame]:
    """Yield `frame` when it already fits, else the fragment frames that replace it, **lazily**.

    The writer consumes this one slice at a time, because a stream holds at most one unsent fragment
    (WSM-FRG-018): fragment *n+1* is sliced after fragment *n* has been handed to the socket, so the
    writer can interleave other streams' frames between them (WSM-FRG-019).

    `split_frame` and the writer both use this function, so they cut at the same boundaries.

    A pure function of `(frame, cap, codec)` (WSM-FRG-015): it reads nothing else and mutates neither
    argument. `cap` defaults to the protocol constant; a smaller value comes only from a test or the
    conformance runner (WSM-FRG-005) and is never read off the wire.

    The sender encodes the logical payload with the codec, slices the encoded form, and puts each
    slice into a frame the codec encodes again (WSM-FRG-011). Each slice is sized to leave room for
    the envelope (WSM-FRG-013), then the encoded fragment is measured and re-split if it exceeds the
    cap (WSM-FRG-014).

    Raises `ProtocolError` when no codec is given, when `cap` is below 2, when a frame over the cap
    has no payload, or when the cap cannot hold an envelope plus one unit of payload (WSM-FRG-034).
    """
    if codec is None:
        raise ProtocolError("split_frame requires a codec: fragment boundaries are defined over its output")
    if cap < 2:
        raise ProtocolError(f"a frame cap of {cap} bytes is too small to hold any frame (WSM-FRG-034)")

    if encoded_length(codec.encode(frame)) <= cap:
        yield frame
        return

    if frame.payload is ABSENT:
        raise ProtocolError(
            f"frame {frame.type!r} exceeds the {cap}-byte cap but carries no payload to fragment; "
            f"headers are never fragmented (WSM-FRG-021)"
        )

    # Encoded to UTF-8 once, then sliced as bytes: every offset below is a byte offset and every
    # budget a byte budget, so the loop measures the quantity it is budgeting for directly. A text
    # payload goes back into the frame as `str`, which is what `_chunk` decodes each slice for.
    encoded = codec.encode_payload(frame.payload)
    text = isinstance(encoded, str)
    data = encoded.encode("utf-8") if text else encoded

    budget = max(1, cap - _reservation(cap))
    position = 0
    emitted = 0
    total = len(data)

    while True:
        # Try the whole remainder as the closing fragment. `end` and `trailers` travel only on this
        # fragment, so it is measured separately from a middle fragment. Checked first on every pass,
        # including when no payload is left: the sequence MUST end with a fragment carrying
        # `more: false`, even an empty one. The encode is skipped when the raw remainder alone
        # exceeds the cap, since the encoded frame is never shorter than the remainder; encoding it on
        # every pass would make splitting quadratic in payload size.
        if total - position <= cap:
            tail = _fragment_frame(frame, _chunk(data, position, total, text=text), first=emitted == 0, last=True)
            if encoded_length(codec.encode(tail)) <= cap:
                yield tail
                return

        if position >= total:
            raise _closing_floor_error(cap)

        # Otherwise a middle fragment: the slice is sized to leave room for the envelope
        # (WSM-FRG-013) and floored onto a boundary. A slice is never empty: when the first unit is
        # wider than the budget it is taken whole, and the check below shrinks it if needed.
        end = _floor_boundary(data, min(position + budget, total), text=text)
        if end == position:
            end = _next_boundary(data, position, text=text)
        count = end - position
        candidate = _fragment_frame(frame, _chunk(data, position, end, text=text), first=emitted == 0, last=False)

        # Measure the encoded fragment and re-split if it exceeds the cap (WSM-FRG-014).
        if encoded_length(codec.encode(candidate)) > cap:
            count = _largest_fitting_count(
                frame, data, position, count, text=text, first=emitted == 0, cap=cap, codec=codec
            )
            if count == 0:
                raise _floor_error(cap)
            candidate = _fragment_frame(
                frame, _chunk(data, position, position + count, text=text), first=emitted == 0, last=False
            )

        yield candidate
        emitted += 1
        position += count


class Assembler:
    """Receive side: concatenates `fragment` values and decodes once the last one lands.

    `feed()` returns `ABSENT` while `more` is true, and the decoded payload on the frame that closes
    the sequence (WSM-FRG-030).
    """

    __slots__ = ("_parts", "_bytes")

    def __init__(self) -> None:
        self._parts: list[str | bytes] = []
        self._bytes = 0

    @property
    def in_progress(self) -> bool:
        """True between the first fragment and the one that arrives without `more`."""
        return bool(self._parts)

    @property
    def byte_length(self) -> int:
        """Bytes accumulated so far.

        The peer checks `max_payload_bytes` against this as fragments arrive, before reassembly
        (WSM-FRG-032).
        """
        return self._bytes

    def reset(self) -> None:
        """Drop the partial buffer and its byte count."""

        self._parts.clear()
        self._bytes = 0

    def feed(self, frame: Frame, codec: Codec) -> Any:
        """Absorb one fragment frame; return `ABSENT` while more are expected, else the payload."""
        if frame.fragment is None:
            raise ProtocolError("Assembler.feed was given a frame carrying no fragment")
        self._parts.append(frame.fragment)
        self._bytes += encoded_length(frame.fragment)
        if frame.more:
            return ABSENT

        parts = self._parts
        joined: str | bytes = b"".join(parts) if isinstance(parts[0], bytes) else "".join(parts)  # type: ignore[arg-type]
        self.reset()
        return codec.decode_payload(joined)
