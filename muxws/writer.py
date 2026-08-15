"""The send path: per-stream queues and a round-robin writer (§4.2).

This module is the whole of WSM-FRG-017, WSM-FRG-018 and WSM-FRG-019, and it is one object on
purpose. The three rules are really one requirement seen from three sides:

  a stream holds at most **one** unsent fragment (WSM-FRG-018),
  fragments of one payload stay contiguous **on their own stream** (WSM-FRG-017),
  and the next frame to go out is chosen by **round-robin across streams** (WSM-FRG-019).

Together they are what stops a 1 MB export adding a full second of latency to a 200-byte progress
update on another stream (WSM-INV-004). A single FIFO of frames anywhere in this path breaks all
three at once: the ordering decision is then made at enqueue time, and interleaving stops being
possible no matter what the loop does afterwards.
"""

from __future__ import annotations

import asyncio

from collections import deque
from collections.abc import Iterator

from muxws.codecs import Codec
from muxws.fragment import iter_fragments, MAX_FRAME_BYTES
from muxws.frames import Frame

#: Connection-level frames - `ping`, `pong`, `goaway` - have no stream of their own. They get one
#: lane keyed here, so they take their turn in the rotation rather than jumping it or waiting on it.
CONNECTION_LANE = 0


class LaneEncodingError(Exception):
    """A frame on one lane could not be encoded.

    Named rather than allowed to propagate: the encode happens inside the writer, so a codec that
    refuses a payload - bytes under JSON, say - would otherwise take the write loop down with it, and
    a peer whose writer is dead while it still reports itself open is the worst possible state. The
    lane is carried so the caller can fail exactly that stream and leave the connection working.
    """

    def __init__(self, lane: int, cause: BaseException) -> None:
        super().__init__(f"could not encode a frame on lane {lane}: {cause}")
        self.lane = lane
        self.cause = cause


class StreamQueue:
    """One stream's outbound work: whole frames waiting, and **at most one** prepared fragment.

    The per-stream order is a queue and has to be - fragments of one payload are contiguous on their
    stream (WSM-FRG-017). What must never be a queue is the choice *between* streams.
    """

    __slots__ = ("stream_id", "_waiting", "_prepared", "_fragments")

    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self._waiting: deque[Frame] = deque()
        #: The one fragment already sliced and not yet handed to the socket. WSM-FRG-018 in a field.
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
        """Instrumentation for the rule: this must never exceed 1."""
        return 1 if self._prepared is not None else 0

    def put(self, frame: Frame) -> None:
        self._waiting.append(frame)

    def prepare(self, cap: int, codec: Codec) -> None:
        """Slice the next fragment - and only the next one.

        Called *after* the previous fragment has reached the socket, never before. Slicing ahead is
        the bug WSM-FRG-018 forbids: it is not wrong on the wire, it is wrong in the queue, because
        it decides an order the writer has not been asked to commit to yet.
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


class Writer:
    """Chooses the next frame across streams by round-robin, and never by arrival order."""

    def __init__(self, codec: Codec, *, max_frame_bytes: int = MAX_FRAME_BYTES) -> None:
        self._codec = codec
        self._cap = max_frame_bytes
        self._queues: dict[int, StreamQueue] = {}
        #: Where the rotation resumes. Not an index into a list: streams come and go, and an index
        #: would silently start favouring whoever happened to land in the vacated slot.
        self._order: deque[int] = deque()
        self._wake = asyncio.Event()
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
        self._wake.set()

    async def next_frame(self) -> Frame | None:
        """The next frame to put on the wire, waiting if there is nothing to send.

        Returns None when `discard_all` emptied the writer while we were waiting - the caller checks
        for that rather than being handed a frame that no longer means anything.
        """
        while True:
            if self._stopped:
                return None
            frame = self._rotate()
            if frame is not None:
                return frame
            self._wake.clear()
            if not any(queue.has_work for queue in self._queues.values()):
                await self._wake.wait()
            if self._stopped:
                return None

    def _rotate(self) -> Frame | None:
        """One full turn of the rotation, at most.

        Every lane gets asked once before any lane is asked twice. That is the entire mechanism: a
        stream mid-way through a megabyte holds the wire for exactly one fragment at a time.
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
        """Forget a lane with nothing left. Nothing is retained per closed stream (WSM-STM-001)."""
        if lane == CONNECTION_LANE:
            return
        self._queues.pop(lane, None)
        try:
            self._order.remove(lane)
        except ValueError:
            pass

    def advance(self, stream_id: int) -> None:
        """Slice that stream's next fragment, now that its previous one has reached the socket."""
        queue = self._queues.get(stream_id if stream_id else CONNECTION_LANE)
        if queue is not None:
            queue.prepare(self._cap, self._codec)
            if queue.has_work:
                self._wake.set()

    def discard(self, stream_id: int) -> None:
        """Drop one stream's queued work - it was reset, and none of it means anything now."""
        queue = self._queues.pop(stream_id, None)
        if queue is not None:
            queue.discard()
            try:
                self._order.remove(stream_id)
            except ValueError:
                pass

    def stop(self) -> None:
        """Retire the writer. `next_frame` returns None from here on, once and for good."""
        self._stopped = True
        self._wake.set()

    def discard_all(self) -> None:
        """Socket death: everything queued is dropped and nothing is held for a next socket (WSM-RCN-042)."""
        for queue in self._queues.values():
            queue.discard()
        self._queues.clear()
        self._order.clear()
        self._wake.set()
