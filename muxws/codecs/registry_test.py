import ast
import inspect
import subprocess
import sys

import pytest

from muxws.codecs import Codec, get_codec, register_codec, registered_codecs
from muxws.errors import CodecNotRegistered
from muxws.frames import Frame


class _FakeCodec:
    name = "fake"
    binary = True

    def encode(self, _frame: Frame) -> bytes:
        return b""

    def decode(self, _message: str | bytes) -> Frame:
        return Frame("data")

    def encode_payload(self, _payload: object) -> bytes:
        return b""

    def decode_payload(self, _data: str | bytes) -> object:
        return None


def test_json_is_registered_by_the_library():
    """WSM-CDC-004: the library ships JSON registered, and JSON is the default."""
    import muxws

    assert "json" in registered_codecs()
    assert get_codec("json").name == "json"
    assert isinstance(get_codec("json"), muxws.JsonCodec)


def test_codec_module_does_not_register_itself_on_import():
    """WSM-CDC-014: a side-effecting import can never be tree-shaken out, so there must not be one.

    The rule is about a *codec module* carrying a registration call at module scope. It cannot be
    observed by importing one and inspecting the registry, because in Python importing
    `muxws.codecs.json_` necessarily executes `muxws/__init__.py` first - and that file registers
    JSON deliberately, as WSM-CDC-004 requires it to. The two rules are only separable by looking at
    where the call is written, which is what this reads the source for. See GAPS.md.
    """
    import muxws.codecs.json_ as json_module

    for module in (json_module,):
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
        assert "register_codec" not in inspect.getsource(module)


def test_the_library_is_what_registers_json_not_the_codec_module():
    """WSM-CDC-004 and WSM-CDC-014 pull in opposite directions; `muxws/__init__.py` is where they meet."""
    import muxws

    assert 'register_codec("json"' in inspect.getsource(muxws)


def test_a_fresh_interpreter_that_never_imports_muxws_has_no_registry():
    """The registry is process state built by explicit calls, not by anything ambient."""
    program = "import sys;print('muxws' in sys.modules)"
    # S603: the argument vector is this interpreter plus a literal program - nothing untrusted.
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "False"


def test_unregistered_name_raises_codec_not_registered():
    """WSM-CDC-016: the message names the variable, the value found and the registered set."""
    with pytest.raises(CodecNotRegistered) as info:
        get_codec("msgpack")
    message = str(info.value)
    assert "MUXWS_CODEC" in message
    assert "msgpack" in message
    assert "json" in message
    assert info.value.configured == "msgpack"
    assert "json" in info.value.available


def test_registration_is_explicit_and_replaces():
    """WSM-CDC-013: explicit registration, no probing and no auto-discovery."""
    register_codec("fake", _FakeCodec())
    try:
        assert get_codec("fake").binary is True
        assert "fake" in registered_codecs()
        assert registered_codecs() == sorted(registered_codecs())
    finally:
        from muxws.codecs import _REGISTRY

        del _REGISTRY["fake"]


def test_codec_protocol_is_structural():
    """The port is a Protocol: anything with the right shape is a codec, no base class required."""
    assert isinstance(_FakeCodec(), Codec)


def test_no_dynamic_import_machinery_exists():
    """WSM-CDC-013: no entry-point scan, no importlib probing, no 'is it installed' check."""
    import inspect

    import muxws.codecs as module

    source = inspect.getsource(module)
    for forbidden in ("import_module", "entry_points", "find_spec", "__import__", "pkgutil"):
        assert forbidden not in source
