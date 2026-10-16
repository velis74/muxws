"""`peer.tags` and `PeerRegistry`: finding a peer by keys the application chose (§9.5).

The registry does not observe `tags`. `tags` is a plain dict; writing a tag does no registry work, and
the index changes only when `register` or `deregister` is called (WSM-REG-013, WSM-REG-014).

Usage rule: **look up on keys you do not mutate, and mutate keys you do not look up.** A consumer that
needs both on one key calls `register(peer)` after each write.
"""

from __future__ import annotations

import logging
import weakref

from collections.abc import Hashable, Iterator
from contextlib import contextmanager
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to a type checker
    from muxws.peer import Peer

logger = logging.getLogger("muxws.registry")


class PeerRegistry:
    """Per-process. muxws does not ship a cross-process backplane (WSM-REG-018)."""

    def __init__(self) -> None:
        #: (key, value) -> the peers registered under it. A tag whose value cannot be hashed gets no
        #: entry.
        self._index: dict[tuple[str, Hashable], set[Peer]] = {}
        #: The reverse map, so `register` can replace all of a peer's entries without scanning the
        #: index (WSM-REG-012).
        self._entries: dict[Peer, set[tuple[str, Hashable]]] = {}
        #: Peers whose close hook is installed. A consumer may call `register(peer)` after every tag
        #: write (WSM-REG-017); the hook is installed once per peer, not once per call (WSM-REG-001).
        #: A weak set, so the registry does not keep a peer alive.
        self._hooked: weakref.WeakSet[Peer] = weakref.WeakSet()

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def index_size(self) -> int:
        """How many (key, value) buckets exist. WSM-REG-014 asserts this does not grow."""
        return len(self._index)

    def register(self, peer: Peer) -> None:
        """Index `peer` under **every key its `tags` holds at this moment** (WSM-REG-010).

        Every key is indexed. Registering again replaces the previous entries: the peer is found under
        the new values and not under the old ones.

        The peer is removed automatically when its connection closes (WSM-REG-016).
        """
        self.deregister(peer)

        entries: set[tuple[str, Hashable]] = set()
        for key, value in peer.tags.items():
            try:
                entry = (key, value)
                hash(entry)
            except TypeError:
                # A dict or a list cannot be a lookup key. The tag is skipped and the peer cannot be
                # found by it; no error is raised (WSM-REG-011).
                continue
            entries.add(entry)
            self._index.setdefault(entry, set()).add(peer)

        self._entries[peer] = entries
        if peer not in self._hooked:
            self._hooked.add(peer)
            peer.on_close(lambda _reason, target=peer: self._forget_closed(target))

    def _forget_closed(self, peer: Peer) -> None:
        """The close hook: deregister `peer` and mark its hook as no longer installed.

        A `Peer` that survives a reconnect (WSM-RCN-032) and is registered again on its next
        connection gets its hook installed again.
        """
        self._hooked.discard(peer)
        self.deregister(peer)

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

        A list sorted by `peer.id`, not a set (WSM-REG-015), so iteration order is reproducible. It is
        a snapshot: a peer in it may already be closing. With no arguments, every registered peer.

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
