"""`peer.tags` and `PeerRegistry`: finding a peer by keys the application chose (§9.5).

The registry indexes; it does not watch. That distinction is the whole design, and WSM-REG-013 and
WSM-REG-014 exist to make a plain dict provably the right choice for `tags`: writing a tag is free
and costs no bookkeeping, and the index only moves when someone says so.

The documented usage rule follows from that: **look up on keys you do not mutate, and mutate keys you
do not look up.** A consumer needing both on one key calls `register(peer)` after each write.
"""

from __future__ import annotations

import logging

from collections.abc import Hashable, Iterator
from contextlib import contextmanager
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.peer import Peer

logger = logging.getLogger("muxws.registry")


class PeerRegistry:
    """Per-process. muxws does not ship a cross-process backplane (WSM-REG-018)."""

    def __init__(self) -> None:
        #: (key, value) -> the peers registered under it. A value that cannot be hashed never gets
        #: an entry, so a dict-valued tag costs nothing at all.
        self._index: dict[tuple[str, Hashable], set[Peer]] = {}
        #: The reverse map, so `register` can replace a peer's entries **wholesale** in one pass
        #: (WSM-REG-012) rather than scanning the index for it.
        self._entries: dict[Peer, set[tuple[str, Hashable]]] = {}

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def index_size(self) -> int:
        """How many (key, value) buckets exist. WSM-REG-014 asserts this does not grow."""
        return len(self._index)

    def register(self, peer: Peer) -> None:
        """Index `peer` under **every key its `tags` holds at this moment** (WSM-REG-010).

        It has no notion of which keys matter, and re-registering replaces the previous entries
        rather than adding to them: found under the new values, no longer under the old ones.

        Removal on close is automatic (WSM-REG-016) - a consumer never has to prune the index.
        """
        self.deregister(peer)

        entries: set[tuple[str, Hashable]] = set()
        for key, value in peer.tags.items():
            try:
                entry = (key, value)
                hash(entry)
            except TypeError:
                # A dict or a list cannot serve as a lookup key. Passing over it is deliberate: the
                # peer is simply not findable by that key, and raising would make an ordinary tag
                # write into an error (WSM-REG-011).
                continue
            entries.add(entry)
            self._index.setdefault(entry, set()).add(peer)

        self._entries[peer] = entries
        peer.on_close(lambda _reason, target=peer: self.deregister(target))

    def deregister(self, peer: Peer) -> None:
        """Forget `peer` entirely. Idempotent."""
        for entry in self._entries.pop(peer, set()):
            holders = self._index.get(entry)
            if holders is None:
                continue
            holders.discard(peer)
            if not holders:
                del self._index[entry]

    @contextmanager
    def registered(self, peer: Peer) -> Iterator[Peer]:
        """`register` plus an explicit deregister, for a consumer that wants the scope visible."""
        self.register(peer)
        try:
            yield peer
        finally:
            self.deregister(peer)

    def peers_for(self, **tags: Any) -> list[Peer]:
        """Every live peer whose `tags` match **all** the given keys.

        A **list** in a stable order, never a set (WSM-REG-015): callers iterate it, and a set's
        order would vary between runs for no reason anyone can see. Treat it as a snapshot - a peer
        in it may already be closing.
        """
        if not tags:
            return sorted(self._entries, key=lambda peer: peer.id)

        found: set[Peer] | None = None
        for key, value in tags.items():
            try:
                holders = self._index.get((key, value), set())
            except TypeError:
                return []
            found = set(holders) if found is None else found & holders
            if not found:
                return []

        return sorted(found or (), key=lambda peer: peer.id)
