"""The transport itself, so a failure in it is never misread as a peer bug."""

from __future__ import annotations

import asyncio

import pytest

from muxws.errors import ConnectionClosed
from muxws.transports import SocketAdapter
from muxws.transports.memory import memory_pair


async def test_pair_delivers_and_drop_kills_both_directions():
    left, right = memory_pair()
    assert isinstance(left, SocketAdapter)

    await left.send_text("hello")
    assert await right.receive() == "hello"
    await right.send_bytes(b"\x00\xff")
    assert await left.receive() == b"\x00\xff"

    assert left.sent == ["hello"]
    assert right.sent == [b"\x00\xff"]

    await left.drop()
    assert left.is_closed
    assert right.is_closed
    with pytest.raises(ConnectionClosed):
        await right.receive()
    with pytest.raises(ConnectionClosed):
        await left.send_text("too late")


async def test_receive_blocks_until_something_arrives():
    left, right = memory_pair()
    task = asyncio.create_task(right.receive())
    await asyncio.sleep(0)
    assert not task.done()
    await left.send_text("now")
    assert await task == "now"


async def test_inject_bypasses_the_other_peer():
    """How the conformance runner delivers frames no correct implementation would send."""
    _, right = memory_pair()
    right.inject('{"type":"widget"}')
    assert await right.receive() == '{"type":"widget"}'


async def test_close_is_idempotent():
    left, _ = memory_pair()
    await left.close()
    await left.close()
    assert left.is_closed
