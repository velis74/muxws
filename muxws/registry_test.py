"""`peer.tags` and `PeerRegistry` (§9.5)."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from muxws.conftest import DialableServer
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


async def test_reconnect_starts_with_empty_tags(registry: PeerRegistry, dialable_server: DialableServer):
    """WSM-RCN-033/WSM-INV-014 **(spec)**: a reconnect is a new acceptor-side peer, tagless.

    Driven through the real reconnect driver rather than by building two peers by hand, because the
    claim is about what a *reconnect* leaves behind: the acceptor never learns that the socket it
    just accepted belongs to the client that was here a moment ago, so there is nowhere for the old
    peer's tags to come from unless an implementation deliberately carries them - and a tab that
    silenced something and then died would keep a successor silent that never asked to be.
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
        # Something the *application* wrote on this connection, which no hello ever replays.
        first = dialable_server.acceptors[0]
        first.tags["muted"] = True
        registry.register(first)
        assert registry.peers_for(muted=True) == [first]

        await dialable_server.drop()
        await asyncio.wait_for(again.wait(), 5.0)
        successor = dialable_server.acceptors[-1]

        assert successor is not first, "the acceptor side of a reconnect is a new peer object"
        assert successor.tags == {"tab": "abc"}, "carrying only what this connection itself set"
        assert "muted" not in successor.tags, "a tab that silenced something must not silence its successor"

        # And the dead connection's index entries went with it, so nothing finds it either.
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


async def test_reregistering_costs_the_peer_no_bookkeeping_either(registry: PeerRegistry, make_pair):
    """WSM-REG-001/012/017: a rewrite pays nothing - on the peer as well as in the index.

    WSM-REG-017 tells a consumer that both looks up and mutates one key to call `register(peer)`
    after every write, so this is the documented pattern rather than a pathological one. A hook
    appended per call would leave a long-lived peer carrying one close handler per write, all of them
    doing the same already-idempotent deregistration, and nothing anywhere would ever look wrong.
    """
    peer = make_pair().dialer
    for generation in range(50):
        peer.tags["session"] = f"s{generation}"
        registry.register(peer)

    assert len(peer._close_handlers) == 1, "one hook per peer, however often it is re-registered"
    assert registry.peers_for(session="s49") == [peer]
    assert registry.peers_for(session="s0") == []


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


async def _until(condition: Any) -> None:
    """Poll until `condition` holds. A reconnect finishes when it finishes, not on a fixed delay."""
    while not condition():
        await asyncio.sleep(0.005)
