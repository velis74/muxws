"""`peer.tags` and `PeerRegistry` (§9.5)."""

from __future__ import annotations

from typing import Any

import pytest

from muxws.registry import PeerRegistry


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


async def test_register_passes_over_unhashable_tag_value(registry: PeerRegistry, make_pair):
    """WSM-REG-011 **(spec)**: the peer is simply not findable by that key - raising would be worse."""
    pair = make_pair()
    pair.dialer.tags.update({"session": "abc", "profile": {"deep": True}, "seen": [1, 2]})
    registry.register(pair.dialer)  # must not raise

    assert registry.peers_for(session="abc") == [pair.dialer]
    assert registry.index_size == 1, "an unhashable value must not take an index entry"


async def test_reregister_replaces_entries_wholesale(registry: PeerRegistry, make_pair):
    """WSM-REG-012 **(spec)**: found under the new values, no longer under the old ones."""
    pair = make_pair()
    pair.dialer.tags["session"] = "old"
    registry.register(pair.dialer)
    assert registry.peers_for(session="old") == [pair.dialer]

    pair.dialer.tags["session"] = "new"
    registry.register(pair.dialer)
    assert registry.peers_for(session="new") == [pair.dialer]
    assert registry.peers_for(session="old") == []


async def test_tag_written_after_register_is_not_found_until_reregister(registry: PeerRegistry, make_pair):
    """WSM-REG-013 **(spec)**: the registry does not watch the dict, and must not."""
    pair = make_pair()
    pair.dialer.tags["session"] = "abc"
    registry.register(pair.dialer)

    pair.dialer.tags["session"] = "changed"
    assert pair.dialer.tags["session"] == "changed", "a direct read sees the newest value at once"
    assert registry.peers_for(session="changed") == [], "but the index has not been told"
    assert registry.peers_for(session="abc") == [pair.dialer]

    registry.register(pair.dialer)
    assert registry.peers_for(session="changed") == [pair.dialer]


async def test_overwriting_a_never_indexed_key_is_free(registry: PeerRegistry, make_pair):
    """WSM-REG-014 **(spec)**: this is what makes a plain dict provably the right choice.

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

    assert registry.index_size == size_before, "a rewrite must cost the registry nothing"


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
    """WSM-REG-016: via the peer's own close hook. A consumer never prunes."""
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


async def test_registered_is_a_scope(registry: PeerRegistry, make_pair):
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
    """WSM-REG-018: per-process, and nothing in this module reaches further."""
    import inspect

    from muxws import registry as module

    source = inspect.getsource(module)
    for forbidden in ("redis", "socket", "multiprocessing", "pickle", "requests"):
        assert forbidden not in source


async def _hold(payload: Any, stream: Any) -> None:
    _ = payload
    await stream.closed.wait()
