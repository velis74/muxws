"""The msgpack codec: round-trip only, no pinned bytes, and no registration on import.

Every assertion here is `decode(encode(frame)) == frame` (WSM-CDC-006). None compares against a byte
string: this library and `@msgpack/msgpack` make different but valid choices about integer width and
map format, so a pinned wire would fail a conforming encoder. The logical corpus is shared with the
JSON tests, so the same frames pass through a second codec.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json

from pathlib import Path
from typing import Any

import pytest

# `msgpack` is an optional extra (WSM-PKG-002) and the suite must pass without it, so the skip runs
# before anything that imports it. The remaining imports follow this line and carry E402.
msgpack = pytest.importorskip("msgpack", reason="msgpack is an optional extra (WSM-PKG-002)")

from muxws.codecs import get_codec, register_codec, registered_codecs  # noqa: E402
from muxws.codecs.json_ import JsonCodec  # noqa: E402
from muxws.codecs.msgpack_ import MsgpackCodec  # noqa: E402
from muxws.errors import ProtocolError  # noqa: E402
from muxws.fragment import Assembler, split_frame  # noqa: E402
from muxws.frames import ABSENT, Frame, from_mapping  # noqa: E402
from muxws.peer import Peer  # noqa: E402
from muxws.stream import Stream  # noqa: E402
from muxws.transports.memory import memory_pair, MemorySocket  # noqa: E402

_CONFORMANCE = Path(__file__).parent.parent.parent / "conformance"
CORPUS: list[dict[str, Any]] = json.loads((_CONFORMANCE / "frames" / "v1-frames.json").read_text(encoding="utf-8"))


@pytest.fixture
def codec() -> MsgpackCodec:
    return MsgpackCodec()


@pytest.fixture
def registered() -> Any:
    """Register msgpack for the duration of one test, and unregister it afterwards.

    The registry is process state (WSM-CDC-013), and `codecs/registry_test.py` asserts that
    `get_codec("msgpack")` raises. A registration left behind would make that test depend on
    collection order.
    """
    from muxws.codecs import _REGISTRY

    register_codec("msgpack", MsgpackCodec())
    try:
        yield get_codec("msgpack")
    finally:
        _REGISTRY.pop("msgpack", None)


# ---------------------------------------------------------------------- the port


def test_binary_is_declared_true(codec: MsgpackCodec):
    """WSM-CDC-002: `binary` is declared. The peer reads it; it never sniffs what `encode` returned."""
    assert codec.name == "msgpack"
    assert codec.binary is True
    assert isinstance(MsgpackCodec.binary, bool)


@pytest.mark.parametrize("case", CORPUS, ids=[case["name"] for case in CORPUS])
def test_round_trip_over_the_whole_frame_corpus(case: dict[str, Any], codec: MsgpackCodec):
    """WSM-CDC-006: the only conformance a non-JSON codec gets is `decode(encode(frame)) == frame`.

    `case["json_wire"]` is not read. It is the JSON codec's contract, and asserting a second codec
    against it, or against a pinned msgpack wire, would pin bytes that a conforming encoder may
    encode differently.
    """
    frame = from_mapping(case["frame"])
    encoded = codec.encode(frame)
    assert isinstance(encoded, bytes), "a binary codec's encode() must produce bytes"
    assert codec.decode(encoded) == frame


def test_the_corpus_covers_every_v1_frame_type():
    """Every v1 frame type is in the corpus, so the parametrized test above covers all of them.

    Asserted here as well as in the JSON tests, because this file runs the corpus through the second
    codec, and the round-trip test above passes for a corpus of any size.
    """
    assert len(CORPUS) > 10
    assert len({case["name"] for case in CORPUS}) == len(CORPUS)
    assert {case["frame"]["type"] for case in CORPUS} == {"open", "data", "reset", "ping", "pong", "goaway"}


def test_no_msgpack_wire_is_pinned_anywhere_in_the_corpus():
    """WSM-CDC-006: the corpus may pin a JSON wire and nothing else.

    Searches for keys rather than byte patterns: a pinned msgpack wire would be a key such as
    `msgpack_wire` or `wire_hex` next to `json_wire`. `max_frame_bytes` is not a wire but the
    runner's construction argument (WSM-FRG-005), so the match is on the key suffix and not on the
    word "bytes".
    """
    offenders: list[str] = []
    for path in sorted(_CONFORMANCE.rglob("*.json")):
        for key in _every_key(json.loads(path.read_text(encoding="utf-8"))):
            if key != "json_wire" and ("msgpack" in key or key.endswith(("_wire", "_hex"))):
                offenders.append(f"{path.name}:{key}")
    assert offenders == [], f"a codec wire other than json_wire is pinned in the corpus: {offenders}"


def _every_key(node: Any) -> list[str]:
    if isinstance(node, dict):
        return list(node) + [name for value in node.values() for name in _every_key(value)]
    if isinstance(node, list):
        return [name for item in node for name in _every_key(item)]
    return []


# ---------------------------------------------------------------------- packer and unpacker options


def test_decoded_strings_are_text_not_bytes(codec: MsgpackCodec):
    """`raw=False`. With `raw=True` every envelope key arrives as bytes and no frame has a `type`."""
    frame = codec.decode(codec.encode(Frame("data", stream=1, payload={"label": "č"})))
    assert frame.type == "data"
    assert frame.payload == {"label": "č"}
    assert isinstance(frame.payload["label"], str)


def test_a_payload_with_an_integer_map_key_decodes_rather_than_killing_the_connection(codec: MsgpackCodec):
    """`strict_map_key=False`. The msgpack default raises on a non-string map key.

    muxws does not restrict the content of an application payload; a `ValueError` from the unpacker
    would be a decode failure and close the connection (WSM-FRM-005) for a valid map.
    `@msgpack/msgpack` accepts such a key by default, so with this setting both ports decode the same
    payloads, though with different results, since a JavaScript object key is a string.
    """
    assert codec.decode_payload(codec.encode_payload({1: "a"})) == {1: "a"}


def test_a_payload_containing_an_array_round_trips_as_a_list(codec: MsgpackCodec):
    """`use_list=True`. With `use_list=False` every array decodes to a tuple and WSM-CDC-006 fails."""
    payload = {"rows": [1, 2, [3, 4]]}
    decoded = codec.decode_payload(codec.encode_payload(payload))
    assert decoded == payload
    assert isinstance(decoded["rows"], list)


# ---------------------------------------------------------------------- bytes are a payload type


def test_bytes_payload_survives_msgpack_round_trip(codec: MsgpackCodec):
    """WSM-CDC-008: under a binary codec raw bytes are a first-class payload type."""
    frame = Frame("data", stream=1, payload=b"\x00\xff", end=True)
    assert codec.decode(codec.encode(frame)) == frame

    nested = {"blob": b"\x00\xff", "name": "x", "parts": [b"\x01", b""]}
    decoded = codec.decode_payload(codec.encode_payload(nested))
    assert decoded == nested
    assert isinstance(decoded["blob"], bytes)


def test_bytes_and_text_stay_distinguishable(codec: MsgpackCodec):
    """`use_bin_type=True`. Collapsing bin into str would round-trip within one language and fail
    only against the other port."""
    assert codec.decode_payload(codec.encode_payload(b"ab")) == b"ab"
    assert codec.decode_payload(codec.encode_payload("ab")) == "ab"


def test_json_codec_refuses_bytes_rather_than_base64_encoding_them():
    """WSM-CDC-008's other half: under JSON, bytes are not a payload type and muxws does not invent one.

    The base64 of `b"\\x00\\xff"` is `AP8=`; asserting its absence makes this test fail if the codec
    encodes bytes instead of refusing them.
    """
    json_codec = JsonCodec()
    with pytest.raises(TypeError, match="WSM-CDC-008") as info:
        json_codec.encode(Frame("data", stream=1, payload=b"\x00\xff"))
    assert "AP8=" not in str(info.value)

    with pytest.raises(TypeError, match="WSM-CDC-008"):
        json_codec.encode_payload({"blob": b"\x00\xff"})

    # The same bytes that JSON refuses are an ordinary payload under msgpack.
    assert MsgpackCodec().decode_payload(MsgpackCodec().encode_payload(b"\x00\xff")) == b"\x00\xff"


# ---------------------------------------------------------------------- decode failures


def test_an_undecodable_message_is_a_protocol_error(codec: MsgpackCodec):
    """WSM-FRM-005: what the configured codec refuses to decode is a connection-level error."""
    with pytest.raises(ProtocolError):
        codec.decode(b"\xc1")
    with pytest.raises(ProtocolError):
        codec.decode(b"")


def test_trailing_bytes_after_one_frame_are_refused(codec: MsgpackCodec):
    """One frame per WebSocket message (§3). A second value in the same message is not a frame."""
    with pytest.raises(ProtocolError):
        codec.decode(codec.encode(Frame("ping")) + codec.encode(Frame("ping")))


def test_a_non_map_message_is_a_protocol_error(codec: MsgpackCodec):
    with pytest.raises(ProtocolError, match="map"):
        codec.decode(msgpack.packb([1, 2, 3]))
    with pytest.raises(ProtocolError, match="map"):
        codec.decode(msgpack.packb(7))
    with pytest.raises(ProtocolError, match="map"):
        codec.decode(msgpack.packb(b"\x00"))


def test_a_text_message_is_a_protocol_error_not_a_recoding(codec: MsgpackCodec):
    """WSM-CDC-002: a binary codec is chosen by declaration, so text on the wire is the remote's bug.

    No recoding can recover the bytes, because the transport has already lost them, so a recoding
    would turn a detectable violation into silent corruption.
    """
    with pytest.raises(ProtocolError, match="text"):
        codec.decode('{"type":"ping"}')
    with pytest.raises(ProtocolError, match="text"):
        codec.decode_payload("plain")


# ---------------------------------------------------------------------- registration


def test_msgpack_codec_does_not_register_itself_on_import():
    """WSM-CDC-013/014: importing the module registers nothing; the application does, explicitly."""
    import muxws.codecs.msgpack_ as module

    assert "msgpack" not in registered_codecs(), "importing the module registered it"

    register_codec("msgpack", MsgpackCodec())
    try:
        assert get_codec("msgpack").name == "msgpack"
    finally:
        from muxws.codecs import _REGISTRY

        _REGISTRY.pop("msgpack", None)

    assert "msgpack" not in registered_codecs()

    # A repeated import is a no-op in Python, so the registry check above cannot detect a
    # module-scope `register_codec` call. The source is read instead, as `registry_test.py` does for
    # the JSON codec.
    tree = ast.parse(inspect.getsource(module))
    module_scope_calls = [
        node
        for node in tree.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "register_codec"
    ]
    assert not module_scope_calls, f"{module.__name__} registers itself at import time"
    assert "register_codec" not in inspect.getsource(module).split('"""', 2)[-1]


def test_no_dynamic_import_machinery_in_the_codec_module():
    """WSM-CDC-013: no entry-point scan, no importlib probing, no 'is msgpack installed' check."""
    import muxws.codecs.msgpack_ as module

    source = inspect.getsource(module)
    for forbidden in ("import_module", "entry_points", "find_spec", "__import__", "pkgutil"):
        assert forbidden not in source

    # The names above detect explicit import machinery. The check below detects a guarded import
    # (`import` under `try:`) and a lazy import (inside a function), both of which have a non-zero
    # `col_offset`; no forbidden literal matches either. Both would replace WSM-INV-015's startup
    # failure with a deployment configured for msgpack that does not run it.
    nested = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node.col_offset
    ]
    assert not nested, f"a guarded or lazy import at line {[node.lineno for node in nested]} (WSM-CDC-013)"


# ---------------------------------------------------------------------- fragmentation


def test_msgpack_fragment_boundaries_are_byte_boundaries(codec: MsgpackCodec):
    """WSM-FRG-003: under a binary codec the length that matters is the produced buffer's.

    Uses `len()` on the encoded bytes rather than `encoded_length`, so the assertion does not depend
    on the helper under test. The two bounds detect two errors. Under-counting (decoding the buffer
    to text and counting characters) puts a fragment over the cap. Over-counting (measuring `repr()`
    or UTF-16 units) keeps every fragment under the cap but makes it smaller, so the constant slice
    size check fails. The cap check alone detects neither.
    """
    cap = 512
    blob = bytes(range(256)) * 12
    parts = split_frame(Frame("data", stream=1, payload=blob, end=True), cap=cap, codec=codec)

    assert len(parts) > 1, "the payload was not fragmented"
    sizes = [len(codec.encode(part)) for part in parts]
    assert max(sizes) <= cap

    # The indivisible unit under a binary codec is one byte (WSM-FRG-012), so every fragment except
    # the last carries a full budget.
    carried = [len(part.fragment) for part in parts]
    assert len(set(carried[:-1])) == 1, f"fragments carry different amounts of payload: {carried}"
    assert carried[0] >= cap // 4, f"fragments are far under the cap, so something over-counts: {carried}"
    assert sum(carried) == len(codec.encode_payload(blob))
    assert all(isinstance(part.fragment, bytes) for part in parts)

    assembler = Assembler()
    result: Any = ABSENT
    for part in parts:
        result = assembler.feed(part, codec)
    assert result == blob


# ---------------------------------------------------------------------- through the real peer


class _SpySocket:
    """A `SocketAdapter` that records which send method the peer called.

    It delegates to a real `MemorySocket`, so the peer on the other end answers and the same test
    checks both the send method and a working connection.
    """

    def __init__(self, inner: MemorySocket) -> None:
        self._inner = inner
        self.text_calls: list[Any] = []
        self.byte_calls: list[Any] = []

    @property
    def is_closed(self) -> bool:
        return self._inner.is_closed

    async def send_text(self, text: str) -> None:
        self.text_calls.append(text)
        await self._inner.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        self.byte_calls.append(data)
        await self._inner.send_bytes(data)

    async def receive(self) -> str | bytes:
        return await self._inner.receive()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._inner.close(code, reason)


class _MisdeclaredCodec(MsgpackCodec):
    """Declares `binary = True` and returns `str` anyway.

    Test-only. WSM-CDC-002 says the peer MUST NOT inspect the encoded value's type, and the
    difference between "declared" and "inferred" is observable only with a codec where the two
    disagree.
    """

    def encode(self, frame: Frame) -> Any:
        return super().encode(frame).decode("latin-1")


async def _echo(payload: Any, stream: Stream) -> None:
    await stream.end(payload)


async def test_binary_codec_uses_send_bytes_never_send_text(registered: MsgpackCodec):
    """WSM-CDC-002 through the real `Peer`: `binary` selects the send method, and nothing else does.

    Builds the peers the way an application does (`register_codec`, then `get_codec`, then `Peer`)
    and asserts on the `SocketAdapter` spy, because the in-memory transport accepts a `str` on
    `send_bytes` and would not detect a peer that chose the wrong method.
    """
    left, right = memory_pair()
    spy = _SpySocket(left)
    dialer = Peer(spy, codec=registered, is_dialer=True)
    acceptor = Peer(right, codec=registered, is_dialer=False)
    acceptor.on_stream(_echo)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    try:
        assert await dialer.request({"action": "ping", "blob": b"\x00\xff"}) == {"action": "ping", "blob": b"\x00\xff"}
        assert spy.text_calls == [], "a binary codec called send_text"
        assert spy.byte_calls, "nothing called send_bytes"
        assert all(isinstance(message, bytes) for message in spy.byte_calls)
    finally:
        await _stop(tasks, left, right)


async def test_a_binary_codec_that_returns_text_still_goes_out_over_send_bytes():
    """WSM-CDC-002: the declaration selects the send method, not the value's type."""
    left, right = memory_pair()
    spy = _SpySocket(left)
    peer = Peer(spy, codec=_MisdeclaredCodec(), is_dialer=True)
    task = asyncio.create_task(peer.serve())

    try:
        peer.open({"a": 1}, end=True)
        for _ in range(12):
            await asyncio.sleep(0)
        assert spy.text_calls == []
        assert [type(message) for message in spy.byte_calls] == [str]
    finally:
        await _stop([task], left, right)


async def test_a_large_bytes_payload_survives_fragmentation_over_a_live_peer_pair(registered: MsgpackCodec):
    """The end-to-end path: codec, splitter, writer, socket, assembler, codec again.

    A lowered cap is a runner/test construction argument (WSM-FRG-005), never a wire value. Every
    message is measured as its buffer length (WSM-FRG-003), which is the same measurement the
    splitter had to make for the payload to arrive intact.
    """
    left, right = memory_pair()
    dialer = Peer(left, codec=registered, is_dialer=True, max_frame_bytes=1024)
    acceptor = Peer(right, codec=registered, is_dialer=False, max_frame_bytes=1024)
    acceptor.on_stream(_echo)
    tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]

    blob = bytes(range(256)) * 40
    try:
        assert await dialer.request({"blob": blob}) == {"blob": blob}
        assert len(left.sent) > 1, "a 10 KiB payload under a 1 KiB cap must have been fragmented"
        assert all(isinstance(message, bytes) for message in left.sent)
        assert max(len(message) for message in left.sent) <= 1024
    finally:
        await _stop(tasks, left, right)


async def _stop(tasks: list[asyncio.Task[None]], *sockets: MemorySocket) -> None:
    for socket in sockets:
        await socket.drop()
    for task in tasks:
        task.cancel()
    for task in tasks:
        # serve() is cancelled at the end of a test; exceptions it raises while cancelled are ignored.
        await asyncio.gather(task, return_exceptions=True)
