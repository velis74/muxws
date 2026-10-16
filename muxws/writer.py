"""The send path: per-stream queues and a round-robin writer (§4.2).

This module implements WSM-FRG-017, WSM-FRG-018 and WSM-FRG-019:

  a stream holds at most **one** unsent fragment (WSM-FRG-018),
  fragments of one payload stay contiguous **on their own stream** (WSM-FRG-017),
  and the next frame to go out is chosen by **round-robin across streams** (WSM-FRG-019).

Together they keep a large payload on one stream from delaying a small frame on another stream by
more than one fragment (WSM-INV-004). A single FIFO of frames would fix the order at enqueue time,
and frames of different streams could not interleave.
"""

from __future__ import annotations

import asyncio

from collections import deque
from collections.abc import Iterator

from muxws.codecs import Codec
from muxws.fragment import iter_fragments, MAX_FRAME_BYTES
from muxws.frames import Frame

#: The lane for connection-level frames (`ping`, `pong`, `goaway`), which have no stream. It takes
#: its turn in the rotation like a stream lane.
CONNECTION_LANE = 0


class LaneEncodingError(Exception):
    """A frame on one lane could not be encoded.

    Fragmentation encodes inside the writer, so a codec that refuses a payload (for example bytes
    under JSON) raises here. The exception carries the lane, so the write loop can fail that stream
    and keep the connection open instead of ending the write loop.
    """

    def __init__(self, lane: int, cause: BaseException) -> None:
        super().__init__(f"could not encode a frame on lane {lane}: {cause}")
        self.lane = lane
        self.cause = cause


class StreamQueue:
    """One stream's outbound work: whole frames waiting, and **at most one** prepared fragment.

    Within one stream the order is FIFO, so fragments of one payload are contiguous on their stream
    (WSM-FRG-017). The choice between streams is made by `Writer`'s rotation.
    """

    __slots__ = ("stream_id", "_waiting", "_prepared", "_fragments")

    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self._waiting: deque[Frame] = deque()
        #: The fragment already sliced and not yet handed to the socket; at most one (WSM-FRG-018).
        self._prepared: Frame | None = None
        #: The lazy remainder of the payload in flight, if this stream is mid-fragmentation.
        self._fragments: Iterator[Frame] | None = None

    def __len__(self) -> int:
        """How many frames are queued but unsent. Never more than one **prepared** (WSM-FRG-018)."""
        return len(self._waiting) + (1 if self._prepared is not None else 0)

    @property
    def has_work(self) -> bool:
        return self._prepared is not None or bool(self._waiting) or self._fragments is not None

    @property
    def prepared_depth(self) -> int:
        """The number of prepared fragments, 0 or 1 (WSM-FRG-018)."""
        return 1 if self._prepared is not None else 0

    def put(self, frame: Frame) -> None:
        self._waiting.append(frame)

    def prepare(self, cap: int, codec: Codec) -> None:
        """Slice the next fragment, and only that one.

        Called after the previous fragment has reached the socket. Slicing further ahead would fix the
        order of frames before the writer chooses it (WSM-FRG-018).
        """
        if self._prepared is not None:
            return

        if self._fragments is not None:
            self._prepared = next(self._fragments, None)
            if self._prepared is not None:
                return
            self._fragments = None

        if not self._waiting:
            return
        self._fragments = iter_fragments(self._waiting.popleft(), cap, codec)
        self._prepared = next(self._fragments, None)

    def take(self) -> Frame | None:
        """Hand over the prepared fragment. The next one is not sliced until `prepare` is called."""
        frame, self._prepared = self._prepared, None
        return frame

    def discard(self) -> None:
        self._waiting.clear()
        self._prepared = None
        self._fragments = None

    def discard_except_unsent_open(self) -> None:
        """Drop every unsent frame except an `open` of which no fragment has been sliced."""
        unsent_open = None
        if self._prepared is None and self._fragments is None and self._waiting and self._waiting[0].type == "open":
            unsent_open = self._waiting.popleft()
        self.discard()
        if unsent_open is not None:
            self._waiting.append(unsent_open)


class Writer:
    """Chooses the next frame across streams by round-robin, and never by arrival order."""

    def __init__(self, codec: Codec, *, max_frame_bytes: int = MAX_FRAME_BYTES) -> None:
        self._codec = codec
        self._cap = max_frame_bytes
        self._queues: dict[int, StreamQueue] = {}
        #: The rotation order of lanes; the lane at the front is asked next. A deque rather than an
        #: index into a list, so removing a lane does not shift which lane is asked next.
        self._order: deque[int] = deque()
        self._wake = asyncio.Event()
        #: Set while the write loop has sent every frame handed to `enqueue`; cleared by `enqueue`.
        self._idle = asyncio.Event()
        self._idle.set()
        self._stopped = False

    def __len__(self) -> int:
        return sum(len(queue) for queue in self._queues.values())

    @property
    def lanes(self) -> int:
        return len(self._queues)

    def depth_of(self, stream_id: int) -> int:
        queue = self._queues.get(stream_id)
        return len(queue) if queue else 0

    def prepared_depth_of(self, stream_id: int) -> int:
        queue = self._queues.get(stream_id)
        return queue.prepared_depth if queue else 0

    def enqueue(self, frame: Frame) -> None:
        """Synchronous by contract: `open()` must not suspend between allocating and enqueuing."""
        lane = frame.stream if frame.stream is not None else CONNECTION_LANE
        queue = self._queues.get(lane)
        if queue is None:
            queue = StreamQueue(lane)
            self._queues[lane] = queue
            self._order.append(lane)
        queue.put(frame)
        self._idle.clear()
        self._wake.set()

    async def next_frame(self) -> Frame | None:
        """The next frame to put on the wire, waiting if there is nothing to send.

        Returns None once `stop()` has been called.
        """
        while True:
            if self._stopped:
                return None
            frame = self._rotate()
            if frame is not None:
                return frame
            self._wake.clear()
            if not any(queue.has_work for queue in self._queues.values()):
                # The caller asks for the next frame only after the previous one reached the socket,
                # so no work here means every enqueued frame has been sent.
                self._idle.set()
                await self._wake.wait()
            if self._stopped:
                return None

    def _rotate(self) -> Frame | None:
        """One full turn of the rotation, at most.

        Every lane is asked once before any lane is asked twice, so a stream sending a large payload
        holds the wire for one fragment at a time.
        """
        for _ in range(len(self._order)):
            lane = self._order[0]
            self._order.rotate(-1)
            queue = self._queues.get(lane)
            if queue is None:
                continue

            try:
                queue.prepare(self._cap, self._codec)
            except LaneEncodingError:
                raise
            except Exception as exc:  # noqa: BLE001
                queue.discard()
                self._retire(lane)
                raise LaneEncodingError(lane, exc) from exc
            frame = queue.take()
            if frame is not None:
                return frame

            if not queue.has_work:
                self._retire(lane)
        return None

    def _retire(self, lane: int) -> None:
        """Remove a lane with no work left. No state is kept per closed stream (WSM-STM-001)."""
        if lane == CONNECTION_LANE:
            return
        self._queues.pop(lane, None)
        try:
            self._order.remove(lane)
        except ValueError:
            pass

    async def wait_idle(self) -> None:
        """Return once every enqueued frame, including every fragment of a fragmented one, has been sent.

        Also returns after `stop()` and `discard_all()`, which leave nothing to send.
        """
        await self._idle.wait()

    def advance(self, stream_id: int) -> None:
        """Slice that stream's next fragment, now that its previous one has reached the socket."""
        queue = self._queues.get(stream_id if stream_id else CONNECTION_LANE)
        if queue is not None:
            queue.prepare(self._cap, self._codec)
            if queue.has_work:
                self._wake.set()

    def discard(self, stream_id: int) -> None:
        """Drop one stream's queued frames, for a reset of that stream (WSM-FRM-014).

        The remainder of a payload whose fragmentation has started is dropped; the `reset` that follows
        ends the fragment sequence on the receiver. An `open` of which no fragment has been sliced is
        kept: a `reset` for an id above the remote's high-water mark is a connection-level error
        (WSM-STM-003).
        """
        queue = self._queues.get(stream_id)
        if queue is None:
            return
        queue.discard_except_unsent_open()
        if not queue.has_work:
            self._retire(stream_id)

    def stop(self) -> None:
        """Stop the writer. Every later `next_frame` call returns None."""
        self._stopped = True
        self._idle.set()
        self._wake.set()

    def discard_all(self) -> None:
        """Socket death: drop every queued frame; nothing is kept for a next socket (WSM-RCN-042)."""

        for queue in self._queues.values():
            queue.discard()
        self._queues.clear()
        self._order.clear()
        self._idle.set()
        self._wake.set()
