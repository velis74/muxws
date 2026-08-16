"""Fragmentation: the splitter and the assembler, as pure functions (§4).

Nothing here touches a socket: the splitter and the assembler are pure functions of their arguments,
which is what WSM-FRG-015 asks for.
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

    A JavaScript string's `.length` counts UTF-16 code units and disagrees with this on every
    non-BMP character, which is why WSM-FRG-002 spells the measurement out for both ports.
    """
    if isinstance(message, str):
        return len(message.encode("utf-8"))
    return len(message)


def _reservation(cap: int) -> int:
    """The envelope budget the sender sets aside before slicing (WSM-FRG-013)."""
    return min(512, cap // 2)


def _floor_boundary(data: bytes, cut: int, *, text: bool) -> int:
    """Move `cut` back to the nearest offset in `data` a fragment may end on.

    Under a text codec that is a UTF-8 codepoint boundary (WSM-FRG-012). The slice does not travel as
    bytes: it sits inside the fragment's own envelope as a value of the codec's type system, and half
    a codepoint has no representation in a JSON string. WebSocket's continuation frames would carry a
    split codepoint happily, but muxws does not use them - a continuation sequence holds the socket
    until the message ends, which is the head-of-line blocking the library exists to prevent
    (WSM-INV-004). So every fragment is a whole message and every slice is whole codepoints, in both
    ports (WSM-FRG-016).

    At most three steps back: UTF-8 is self-synchronising, a continuation byte being exactly
    `0b10xxxxxx`. Under a binary codec the unit is the byte and every offset is already a boundary.
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

    `end` and `trailers` ride the final fragment and cannot themselves be fragmented (WSM-FRG-020),
    exactly as `headers` cannot (WSM-FRG-021). If they do not fit, no valid split exists and saying
    so is the only honest answer - the alternative is a sequence that never terminates.
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

    This is the verify-and-re-split half of WSM-FRG-014, and it is a **binary search** rather than a
    guess-and-shrink loop for two reasons. It terminates in log2(ceiling) encodes instead of however
    many rounds a multiplier happens to need - a payload of control characters under JSON expands
    enough that a proportional guess converges too slowly to be bounded honestly. And it is exactly
    reproducible: both ports run the same search over the same encoded form, in the same order, and
    therefore cut at the same boundary, which is what WSM-FRG-016 requires. The search bisects byte
    limits and floors each probe onto a boundary the codec can represent, so what it counts and what
    it cuts are the same quantity.

    Returns 0 when not even one unit fits - the probes below the first unit's width all floor back to
    an empty slice, which fits and records nothing.
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
    """Every fragment at once. `iter_fragments` is the same computation, one slice at a time.

    The pure-function tests and the conformance corpus want the whole list, and a caller with a small
    payload should not have to think about generators.
    """
    return list(iter_fragments(frame, cap, codec))


def iter_fragments(frame: Frame, cap: int = MAX_FRAME_BYTES, codec: Codec | None = None) -> Iterator[Frame]:
    """Yield `frame` when it already fits, else the fragment frames that replace it, **lazily**.

    The writer consumes this one slice at a time, because WSM-FRG-018 says a stream holds at most one
    unsent fragment: fragment *n+1* is sliced only once fragment *n* has been handed to the socket.
    Computing them all up front would commit the wire order in advance, and interleaving - the whole
    point of WSM-FRG-019 - becomes impossible once the order is already decided.

    Every boundary decision lives here, so `split_frame` and the writer cut in exactly the same
    places by construction rather than by two implementations agreeing.

    A pure function of `(frame, cap, codec)` (WSM-FRG-015): it reads nothing else and mutates neither
    argument. `cap` defaults to the protocol constant; a smaller value comes only from a test or the
    conformance runner (WSM-FRG-005) and is never a value read off the wire.

    The sender encodes the logical payload with the codec, slices *that* encoded form, and puts each
    slice into a frame the codec then encodes again (WSM-FRG-011). Slicing budgets for the envelope
    (WSM-FRG-013) and then **verifies and re-splits** (WSM-FRG-014): the reservation is a per-codec
    hint, the loop is the guarantee.
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
        # Everything left, as the closing fragment? `end` and `trailers` ride only this one, so it is
        # a different size from a middle fragment and has to be measured as itself. This is checked
        # first on every pass, including the one where the payload is already spent: the sequence
        # MUST end with a fragment carrying `more: false`, even if that fragment carries no bytes.
        # Skipped when the answer is already known: the encoded frame carries the remaining payload
        # plus an envelope plus whatever the codec's escaping adds, so it is never *shorter* than the
        # remainder itself. Encoding a remainder that is already over the cap renders the whole rest
        # of the payload for nothing, and does it again on every pass, which is quadratic in payload
        # size.
        if total - position <= cap:
            tail = _fragment_frame(frame, _chunk(data, position, total, text=text), first=emitted == 0, last=True)
            if encoded_length(codec.encode(tail)) <= cap:
                yield tail
                return

        if position >= total:
            raise _closing_floor_error(cap)

        # Otherwise a middle fragment: budget for the envelope first (WSM-FRG-013), floored onto a
        # boundary, and never onto `position` itself - a fragment carrying nothing leaves the loop no
        # way forward, so a first unit wider than the budget is taken whole and cut back below.
        end = _floor_boundary(data, min(position + budget, total), text=text)
        if end == position:
            end = _next_boundary(data, position, text=text)
        count = end - position
        candidate = _fragment_frame(frame, _chunk(data, position, end, text=text), first=emitted == 0, last=False)

        # ...then verify, and re-split if the reservation guessed low (WSM-FRG-014). The reservation
        # is a per-codec hint; this is the guarantee.
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

        `max_payload_bytes` is enforced against this **as fragments arrive** rather than after
        reassembly (WSM-FRG-032): a receiver that assembles a payload in order to measure it has
        already spent what the limit was protecting.
        """
        return self._bytes

    def reset(self) -> None:
        """Drop the partial buffer. Called when the stream is reset, releasing the bytes with it."""
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
