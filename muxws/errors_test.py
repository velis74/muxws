from __future__ import annotations

import importlib

from pathlib import Path

import pytest

from muxws.errors import (
    CodecError,
    CodecMismatch,
    CodecNotRegistered,
    ConnectionClosed,
    ConnectionGoingAway,
    ConnectionLost,
    exception_for_reset,
    MuxwsError,
    ProtocolError,
    RemoteError,
    ResetCode,
    StreamAlreadyConsumed,
    StreamClosed,
    StreamRefused,
    StreamReset,
    StreamTimeout,
    TransportUnsupportedError,
    TransportUrlError,
)

ROOT = Path(__file__).resolve().parent.parent

#: The three packages an extra exists for. A module that fails to import for one of these reasons is
#: skipped by the walk below; any other import failure propagates, because a skipped module removes
#: its exception classes from the walk and the guard would pass without checking them.
_OPTIONAL_DEPENDENCIES = frozenset({"msgpack", "starlette", "websockets"})

#: The transport errors the walk must find, by name; without them the guard would pass in an
#: environment where no transport module imported. Each lives in a module that imports without its
#: optional dependency, so the check passes in an environment without extras.
_TRANSPORT_ERRORS_THAT_MUST_EXIST = frozenset(
    {
        "UnixSocketsUnsupportedError",
        "UnixUrlError",
        "WebsocketUrlError",
        "WebsocketsNotInstalledError",
    }
)


def test_hierarchy():
    """WSM-ERR-002/005/009: the shape of the tree is itself normative."""
    assert issubclass(ConnectionLost, StreamReset)
    assert not issubclass(ConnectionClosed, StreamReset)

    # A send() after a normal close raises StreamClosed, which is neither a reset nor a protocol error.
    assert not issubclass(StreamClosed, StreamReset)
    assert not issubclass(StreamClosed, ProtocolError)

    # Neither codec error is a stream failure and neither is retryable.
    assert not issubclass(CodecError, StreamReset)
    assert issubclass(CodecNotRegistered, CodecError)
    assert issubclass(CodecMismatch, CodecError)

    for cls in (RemoteError, StreamTimeout, StreamRefused, ConnectionLost):
        assert issubclass(cls, StreamReset)

    for cls in (
        ProtocolError,
        ConnectionClosed,
        ConnectionGoingAway,
        StreamAlreadyConsumed,
        StreamClosed,
        CodecError,
        StreamReset,
    ):
        assert issubclass(cls, MuxwsError)


def test_the_two_transport_bases_carry_their_builtin_as_well_as_muxwserror():
    """WSM-ERR-016: each base is catchable as its builtin and as `MuxwsError`.

    No behavioural test detects the removal of one of the two bases, so the inheritance is asserted
    directly. A caller handling a malformed URL catches `ValueError`; a caller on a platform without
    the transport catches `RuntimeError`; an application with a single `except MuxwsError` catches
    both. The two bases separate a malformed address from an unsupported platform, so neither is a
    subclass of the other.
    """
    assert issubclass(TransportUrlError, MuxwsError)
    assert issubclass(TransportUrlError, ValueError)
    assert issubclass(TransportUnsupportedError, MuxwsError)
    assert issubclass(TransportUnsupportedError, RuntimeError)

    assert not issubclass(TransportUrlError, TransportUnsupportedError)
    assert not issubclass(TransportUnsupportedError, TransportUrlError)

    # Neither is a stream failure or a codec failure: a URL or a platform is refused before any stream
    # or codec exists.
    for base in (TransportUrlError, TransportUnsupportedError):
        assert not issubclass(base, StreamReset)
        assert not issubclass(base, CodecError)


def _library_modules() -> list[str]:
    """The dotted name of every shipped module under `muxws/`. Tests and fixtures are not shipped."""
    names: list[str] = []
    for path in sorted((ROOT / "muxws").rglob("*.py")):
        if path.name.endswith("_test.py") or path.name == "conftest.py":
            continue
        parts = path.relative_to(ROOT).with_suffix("").parts
        names.append(".".join(parts[:-1] if parts[-1] == "__init__" else parts))
    return names


def _import_the_whole_library() -> tuple[list[str], dict[str, str]]:
    """Import every shipped module, reporting the ones an absent optional extra made unimportable.

    A module is skipped only when the import failed on one of the three packages an extra is named
    for, and the caller asserts that; anything else propagates. A bare `except ImportError: pass`
    would also skip a module that fails for another reason, such as a typo or a circular import, and
    the guard below would pass without checking that module's exception classes.
    """
    imported: list[str] = []
    skipped: dict[str, str] = {}
    for name in _library_modules():
        try:
            importlib.import_module(name)
        except ImportError as exc:  # pragma: no cover - depends on which extras are installed
            missing = (exc.name or "").split(".")[0]
            if missing not in _OPTIONAL_DEPENDENCIES:
                raise
            skipped[name] = missing
        else:
            imported.append(name)
    return imported, skipped


def _descendants(cls: type[BaseException]) -> set[type[BaseException]]:
    """Every subclass of `cls`, transitively.

    Uses the subclass tree instead of reading `class X(Y)` from the source: a name-based scan misses a
    class whose base is an alias or an attribute expression, and a module that is not listed.
    """
    found: set[type[BaseException]] = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _descendants(sub)
    return found


def test_every_muxws_error_defined_outside_errors_py_derives_from_a_transport_base():
    """WSM-ERR-016: a transport's own exception is a `TransportUrlError` or a `TransportUnsupportedError`.

    The rule makes `except MuxwsError` around `connect()` catch every transport's errors. A transport
    that raised a bare `ImportError` or its own library's `InvalidURI` would escape that handler.

    The test names no transport. The walk imports every shipped module and reads the class tree, so a
    new transport module is covered without changes here, and a failure names `module.QualName`.
    """
    imported, skipped = _import_the_whole_library()
    assert len(imported) > 15, imported
    assert set(skipped.values()) <= _OPTIONAL_DEPENDENCIES, skipped

    shipped = set(imported)
    offenders = sorted(
        f"{cls.__module__}.{cls.__qualname__}"
        for cls in _descendants(MuxwsError)
        if cls.__module__ in shipped
        and cls.__module__ != "muxws.errors"
        and not issubclass(cls, (TransportUrlError, TransportUnsupportedError))
    )
    assert offenders == [], "defined outside muxws/errors.py under neither transport base (WSM-ERR-016)"

    # Control: the walk reached classes outside the shared module, so the assertion above does not
    # compare two empty lists. The `ws+unix:` grammar is stdlib-only and imports in every environment.
    found = {cls.__name__ for cls in _descendants(MuxwsError) if cls.__module__ != "muxws.errors"}
    assert _TRANSPORT_ERRORS_THAT_MUST_EXIST <= found, sorted(found)


def test_the_transport_bases_are_root_exported_and_their_subclasses_are_not():
    """WSM-ERR-016: the bases are exported from `muxws`; the concrete classes are not.

    The bases are root-exported because an application that does not know in advance which transport
    a configured URL names must be able to write `except TransportUrlError` without importing a
    transport module. The concrete classes are not root-exported: the adapter interface is public
    (WSM-API-021), a third-party adapter cannot add a name to `muxws/__init__.py`, and the in-tree
    transports follow the same rule.
    """
    import muxws

    for base in (TransportUrlError, TransportUnsupportedError):
        assert base.__name__ in muxws.__all__
        assert getattr(muxws, base.__name__) is base

    _import_the_whole_library()
    concrete = {cls.__name__ for cls in _descendants(MuxwsError) if cls.__module__ != "muxws.errors"}
    assert concrete, "nothing to check - see the control in the test above"
    assert concrete.isdisjoint(muxws.__all__), sorted(concrete & set(muxws.__all__))
    assert [name for name in concrete if hasattr(muxws, name)] == []


def test_reset_codes_are_the_pinned_integers():
    """Nine members; 5 is unassigned (STREAM_LIMIT is retired) and is never reused."""
    assert [(c.name, c.value) for c in ResetCode] == [
        ("NO_ERROR", 0),
        ("CANCELLED", 1),
        ("APPLICATION_ERROR", 2),
        ("PROTOCOL_ERROR", 3),
        ("REFUSED", 4),
        ("TIMEOUT", 6),
        ("PAYLOAD_TOO_LARGE", 7),
        ("INTERNAL_ERROR", 8),
        ("CONNECTION_CLOSED", 9),
    ]
    assert len(ResetCode) == 9
    assert 5 not in {c.value for c in ResetCode}
    with pytest.raises(ValueError, match="5"):
        ResetCode(5)


def test_no_stream_limit_class_exists():
    """WSM-ERR-001 is retired: StreamRefused covers every refusal and has no sibling class."""
    import muxws.errors as errors

    assert not hasattr(errors, "StreamLimit")


@pytest.mark.parametrize(
    ("cls", "code"),
    [
        (RemoteError, ResetCode.APPLICATION_ERROR),
        (StreamTimeout, ResetCode.TIMEOUT),
        (StreamRefused, ResetCode.REFUSED),
        (ConnectionLost, ResetCode.CONNECTION_CLOSED),
    ],
)
def test_each_subclass_pins_its_code(cls: type[StreamReset], code: ResetCode):
    assert cls().code == code
    assert cls("why", stream_id=7).stream_id == 7


def test_exception_for_reset_maps_the_wire_code():
    assert isinstance(exception_for_reset(ResetCode.APPLICATION_ERROR), RemoteError)
    assert isinstance(exception_for_reset(4), StreamRefused)
    # A code with no dedicated class still produces a StreamReset carrying it.
    plain = exception_for_reset(ResetCode.PAYLOAD_TOO_LARGE, "too big", stream_id=3)
    assert type(plain) is StreamReset
    assert plain.code == ResetCode.PAYLOAD_TOO_LARGE
    assert plain.stream_id == 3


def test_remote_error_carries_the_structured_payload():
    """WSM-ERR-006: the reset's payload is what the remote's error_serializer produced."""
    err = RemoteError("handler raised", stream_id=1, payload={"type": "ValueError", "message": "nope"})
    assert err.payload == {"type": "ValueError", "message": "nope"}


def test_codec_error_carries_configured_and_available():
    err = CodecError("bad", configured="msgpack", available=["json"])
    assert err.configured == "msgpack"
    assert err.available == ["json"]


def test_connection_closed_carries_the_socket_close_details():
    err = ConnectionClosed(code=1001, reason="going away", was_clean=True)
    assert (err.code, err.reason, err.was_clean) == (1001, "going away", True)


def test_connection_closed_message_falls_back_to_reason_then_default():
    """An empty message is replaced by `reason`, and an empty reason by "connection closed"."""
    assert str(ConnectionClosed()) == "connection closed"
    assert str(ConnectionClosed(reason="going away")) == "going away"
    assert str(ConnectionClosed("bye", reason="going away")) == "bye"


def test_nothing_in_the_error_hierarchy_carries_a_status_code():
    """WSM-ERR-007: muxws maps exceptions to no status code. Reset codes are not status codes.

    No error class has a `status_code` attribute such as `RemoteError.status_code = 500`. The HTTP
    status numbers below are not `ResetCode` values: the two sets do not overlap, and 5
    (WSM-STM-022) is the only unassigned value in the reset table.

    The error serializer turns a local exception into a structured wire value, so it is also checked:
    WSM-ERR-006 fixes its two keys.
    """
    import muxws

    from muxws.peer import default_error_serializer

    carriers = {
        name: sorted(attribute for attribute in dir(cls) if "status" in attribute.lower())
        for name in muxws.__all__
        if isinstance(cls := getattr(muxws, name), type) and issubclass(cls, BaseException)
    }
    assert len(carriers) >= 10, "the walker found almost no exception classes"
    assert {name: found for name, found in carriers.items() if found} == {}

    assert default_error_serializer(ValueError("nope")) == {"type": "ValueError", "message": "nope"}

    http_statuses = {400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504}
    assert http_statuses.isdisjoint({code.value for code in ResetCode})
