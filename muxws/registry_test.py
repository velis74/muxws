"""`peer.tags` and `PeerRegistry` (§9.5)."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from muxws.conftest import DialableServer
from muxws.observability import CloseReason
from muxws.reconnect import Hello
from muxws.registry import PeerRegistry
from muxws.stream import Stream


@pytest.fixture
def registry() -> PeerRegistry:
    return PeerRegistry()


async def test_tags_are_an_ordinary_dict(make_pair):
    """WSM-REG-001/002/003: any key, last write wins, no bookkeeping, and muxws never reads it."""
    pair = make_pair()
    peer = pair.dialer

    assert peer.tags == {}
    peer.tags["session"] = "abc"
    peer.tags["session"] = "def"
    assert peer.tags["session"] == "def", "last write wins, with no copy in the read path"
    peer.tags["anything"] = {"nested": True}
    assert isinstance(peer.tags, dict)


async def test_register_indexes_every_key_present_at_call_time(registry: PeerRegistry, make_pair):
    """WSM-REG-010: it has no notion of which keys matter."""
    pair = make_pair()
    pair.dialer.tags.update({"session": "abc", "tenant": 7, "role": "admin"})
    registry.register(pair.dialer)

    assert registry.peers_for(session="abc") == [pair.dialer]
    assert registry.peers_for(tenant=7) == [pair.dialer]
    assert registry.peers_for(role="admin") == [pair.dialer]


async def test_register_skips_an_unhashable_tag_value(registry: PeerRegistry, make_pair):
    """WSM-REG-011 **(spec)**: the peer is not findable by that key, and `register()` does not raise."""
    pair = make_pair()
    pair.dialer.tags.update({"session": "abc", "profile": {"deep": True}, "seen": [1, 2]})
    registry.register(pair.dialer)  # must not raise

    assert registry.peers_for(session="abc") == [pair.dialer]
    assert registry.index_size == 1, "an unhashable value must not take an index entry"


async def test_reregister_replaces_entries_wholesale(registry: PeerRegistry, make_pair):
    """WSM-REG-012 **(spec)**: found under the new values and not under the old ones."""
    pair = make_pair()
    pair.dialer.tags["session"] = "old"
    registry.register(pair.dialer)
    assert registry.peers_for(session="old") == [pair.dialer]

    pair.dialer.tags["session"] = "new"
    registry.register(pair.dialer)
    assert registry.peers_for(session="new") == [pair.dialer]
    assert registry.peers_for(session="old") == []


async def test_tag_written_after_register_is_not_found_until_reregister(registry: PeerRegistry, make_pair):
    """WSM-REG-013 **(spec)**: the registry does not observe writes to the dict."""
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)

    pair.dialer.tags["session"] = "changed"
    assert pair.dialer.tags["session"] == "changed", "a direct read sees the newest value at once"
    assert registry.peers_for(session="changed") == [], "the index still holds the value from register()"
    assert registry.peers_for(session="abc") == [pair.dialer]

    registry.register(pair.dialer)
    assert registry.peers_for(session="changed") == [pair.dialer]


async def test_overwriting_an_unindexed_key_leaves_the_index_unchanged(registry: PeerRegistry, make_pair):
    """WSM-REG-014 **(spec)**: writing a key that is not indexed does not touch the registry.

    Register under `session`, then overwrite a different, never-looked-up, map-valued key a hundred
    times without re-registering. Every read sees the newest value, the lookup is unchanged
    throughout, and the index does not grow by a single entry.
    """
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)
    size_before = registry.index_size

    for generation in range(100):
        pair.dialer.tags["watch"] = {"generation": generation}
        assert pair.dialer.tags["watch"] == {"generation": generation}
        assert registry.peers_for(session="abc") == [pair.dialer]

    assert registry.index_size == size_before, "writing an unindexed key must not change the index"


async def test_reconnect_starts_with_empty_tags(registry: PeerRegistry, dialable_server: DialableServer):
    """WSM-RCN-033/WSM-INV-014 **(spec)**: a reconnect is a new acceptor-side peer, tagless.

    Uses the reconnect driver rather than two hand-built peers, because the assertion concerns the
    state after a reconnect. The acceptor cannot tell that a newly accepted socket belongs to a client
    that was connected before, so the old peer's tags reach the new peer only if an implementation
    copies them. Copied tags would apply settings such as `muted` to a connection that never set them.
    """

    async def index_the_tab(payload: Any, stream: Stream) -> None:
        acceptor = stream._peer
        acceptor.tags["tab"] = payload["tab"]
        registry.register(acceptor)

    dialable_server.handler = index_the_tab
    peer, loop = await dialable_server.driver(hello=Hello(payload={"tab": "abc"}))
    again = asyncio.Event()
    peer.on_reconnect(lambda _attempt, _peer: again.set())
    await loop.establish()
    loop.start()
    try:
        # A tag the application writes on this connection; the hello does not replay it.
        first = dialable_server.acceptors[0]
        first.tags["muted"] = True
        registry.register(first)
        assert registry.peers_for(muted=True) == [first]

        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)
        successor = dialable_server.acceptors[-1]

        assert successor is not first, "the acceptor side of a reconnect is a new peer object"
        assert successor.tags == {"tab": "abc"}, "only the tags this connection's hello set"
        assert "muted" not in successor.tags, "tags set on the previous connection must not carry over"

        # The closed connection's index entries are removed, so no lookup finds it.
        await asyncio.wait_for(_until(lambda: registry.peers_for(tab="abc") == [successor]), 5.0)
        assert registry.peers_for(muted=True) == []
    finally:
        await loop.stop()


async def test_peers_for_returns_a_list_in_stable_order(registry: PeerRegistry, make_pair):
    """WSM-REG-015: a list, never a set, so two runs agree."""
    peers = []
    for _ in range(5):
        pair = make_pair()
        pair.dialer.tags["room"] = "lobby"
        registry.register(pair.dialer)
        peers.append(pair.dialer)

    first = registry.peers_for(room="lobby")
    assert isinstance(first, list)
    assert len(first) == 5
    assert first == registry.peers_for(room="lobby"), "the order must not vary between calls"


async def test_peers_for_matches_all_given_keys(registry: PeerRegistry, make_pair):
    """A partial match is not a match."""
    both = make_pair().dialer
    both.tags.update({"room": "lobby", "role": "admin"})
    one = make_pair().dialer
    one.tags.update({"room": "lobby", "role": "guest"})
    registry.register(both)
    registry.register(one)

    assert registry.peers_for(room="lobby", role="admin") == [both]
    assert len(registry.peers_for(room="lobby")) == 2
    assert registry.peers_for(room="lobby", role="nobody") == []


async def test_close_removes_the_peer_from_the_index_automatically(registry: PeerRegistry, make_pair):
    """WSM-REG-016: via the peer's own close hook. The consumer does not remove entries."""
    pair = make_pair()
    pair.acceptor.on_stream(_hold)
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)
    pair.start()
    try:
        assert registry.peers_for(session="abc") == [pair.dialer]
        await pair.dialer_socket.drop()
        await pair.settle()
        assert registry.peers_for(session="abc") == [], "a closed peer must stop being findable"
    finally:
        await pair.stop()


async def test_reregistering_installs_one_close_hook_per_peer(registry: PeerRegistry, make_pair):
    """WSM-REG-001/012/017: re-registering adds no state to the peer, and replaces the index entries.

    WSM-REG-017 tells a consumer that both looks up and mutates one key to call `register(peer)`
    after every write. If each call appended a close hook, a long-lived peer would hold one
    deregistration handler per write.
    """
    peer = make_pair().dialer
    for generation in range(50):
        peer.tags["session"] = f"s{generation}"
        registry.register(peer)

    assert len(peer._close_handlers) == 1, "one hook per peer, however often it is re-registered"
    assert registry.peers_for(session="s49") == [peer]
    assert registry.peers_for(session="s0") == []


async def test_reregistering_after_each_reconnect_installs_one_close_hook(registry: PeerRegistry, make_pair):
    """WSM-REG-001/016/RCN-032: a peer registered again after each of 20 socket losses holds one close hook.

    Each loss deregisters the peer, and the hook stays installed for the next loss.
    """
    peer = make_pair().dialer
    for generation in range(20):
        peer.tags["session"] = f"s{generation}"
        registry.register(peer)
        assert registry.peers_for(session=f"s{generation}") == [peer]
        peer._notify_close(CloseReason(code=1006, reason="", was_clean=False, will_retry=True))
        assert registry.peers_for(session=f"s{generation}") == [], "every socket loss deregisters the peer"

    assert len(peer._close_handlers) == 1, "one hook per peer, however often it reconnects and is re-registered"


async def test_registered_indexes_the_peer_only_inside_the_block(registry: PeerRegistry, make_pair):
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    with registry.registered(pair.dialer):
        assert registry.peers_for(session="abc") == [pair.dialer]
    assert registry.peers_for(session="abc") == []


async def test_deregister_is_idempotent(registry: PeerRegistry, make_pair):
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)
    registry.deregister(pair.dialer)
    registry.deregister(pair.dialer)
    assert len(registry) == 0
    assert registry.index_size == 0


async def test_an_unhashable_lookup_finds_nothing_rather_than_raising(registry: PeerRegistry, make_pair):
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)
    assert registry.peers_for(session=["not", "hashable"]) == []


def test_the_registry_ships_no_cross_process_backplane():
    """WSM-REG-018: the registry is per-process; the module contains no cross-process mechanism."""
    import inspect

    from muxws import registry as module

    source = inspect.getsource(module)
    for forbidden in ("redis", "socket", "multiprocessing", "pickle", "requests"):
        assert forbidden not in source


async def _hold(payload: Any, stream: Any) -> None:
    _ = payload
    await stream.closed.wait()


async def _until(condition: Any) -> None:
    """Poll until `condition` holds. A reconnect has no fixed duration, so tests poll instead of sleeping."""
    while not condition():
        await asyncio.sleep(0.005)
