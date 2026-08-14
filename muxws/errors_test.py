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
#: skipped by the walk below; a module that fails for any other reason is a bug the walk must not
#: swallow, because a module that silently disappears takes its exception classes with it and the
#: guard goes green by seeing nothing.
_OPTIONAL_DEPENDENCIES = frozenset({"msgpack", "starlette", "websockets"})

#: The transport errors the walk must actually find, by name; without them the guard would pass in an
#: environment where no transport module imported at all. Each lives in a module that imports with
#: its optional dependency absent, so naming them cannot make the leanest supported environment red.
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

    # A normal close racing a last send() is an expected outcome, not a failure and not a caller bug.
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
    """WSM-ERR-016: each base is catchable by a handler written before muxws was ever heard of.

    The dual inheritance is asserted rather than described, because dropping half of it breaks
    nothing a behavioural test would notice. A caller who typed a URL wrong is already catching
    `ValueError`; one running where the transport cannot exist is already catching `RuntimeError`;
    and an application with a single `except MuxwsError` must not have either leak through. The split
    between the two bases carries the only distinction the caller acts on - retype the address, or
    change where you run - so neither may be a subclass of the other.
    """
    assert issubclass(TransportUrlError, MuxwsError)
    assert issubclass(TransportUrlError, ValueError)
    assert issubclass(TransportUnsupportedError, MuxwsError)
    assert issubclass(TransportUnsupportedError, RuntimeError)

    assert not issubclass(TransportUrlError, TransportUnsupportedError)
    assert not issubclass(TransportUnsupportedError, TransportUrlError)

    # Neither is a stream failure, and neither is a codec failure: a URL is refused, and a platform is
    # refused, before there is a stream or a negotiated codec to fail.
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
    for, and the caller asserts that; anything else propagates. The distinction is the whole point of
    doing this by hand rather than with a bare `except ImportError: pass`: a module that fails for a
    real reason - a typo, a circular import - would otherwise vanish from the walk with its exception
    classes, and the guard below would go green precisely when it should be loudest.
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
    """Every subclass of `cls`, transitively - the classes that exist, not the ones a scan can name.

    By subclass walk and not by reading `class X(Y)` out of the source: a name-based scan cannot see a
    class whose base is an alias or an attribute expression, and, worse, cannot see a transport module
    nobody thought to list. Whatever was defined is here, however it was spelled.
    """
    found: set[type[BaseException]] = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _descendants(sub)
    return found


def test_every_muxws_error_defined_outside_errors_py_derives_from_a_transport_base():
    """WSM-ERR-016: a transport's own exception is a `TransportUrlError` or a `TransportUnsupportedError`.

    The rule this enforces is what makes `except MuxwsError` exhaustive across transports: without
    it a transport is free to raise a bare `ImportError` or its own library's `InvalidURI`, and an
    application catching `MuxwsError` around `connect()` finds out which scheme it configured only in
    production.

    Nothing here names a transport. The walk imports every shipped module and then asks the class
    tree what exists, so a transport added later is covered on the day its module is written, and the
    failure names `module.QualName` so it also says which file to fix.
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

    # The control, without which the assertion above is a comparison of two empty lists: the walk
    # really did reach the classes that live outside the shared module. A stripped environment cannot
    # turn this test into a no-op, because the `ws+unix:` grammar is stdlib-only and always imports.
    found = {cls.__name__ for cls in _descendants(MuxwsError) if cls.__module__ != "muxws.errors"}
    assert _TRANSPORT_ERRORS_THAT_MUST_EXIST <= found, sorted(found)


def test_the_transport_bases_are_root_exported_and_their_subclasses_are_not():
    """WSM-ERR-016's export half: the bases come from `muxws`, the concrete classes never do.

    Both directions matter and neither implies the other. The bases must be root-exported, because an
    application configured with a URL it has not read yet cannot know which transport module to
    import and `except TransportUrlError` has to be writable without one. The concrete classes must
    *not* be: the adapter seam is public (WSM-API-021) and a third-party adapter cannot add a name to
    `muxws/__init__.py`, so a root-exported concrete class here would set an example nobody outside
    this repository can follow.
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
    """Nine members, and 5 is a hole: STREAM_LIMIT is retired and its number is never reused."""
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
    """WSM-ERR-001 is retired: StreamRefused covers every refusal and there is no sibling."""
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


def test_nothing_in_the_error_hierarchy_carries_a_status_code():
    """WSM-ERR-007: muxws maps exceptions to nothing. Reset codes are not status codes.

    The rule reads like a truism until you notice what it is aimed at: a library sitting under a web
    framework is one convenience away from `RemoteError.status_code = 500`, and once one exists every
    caller writes against it and the reset table becomes a second, worse HTTP. The three numbers
    below are the ones anybody would reach for first, and none of them is a `ResetCode` - the
    numbering was chosen so that the two vocabularies cannot be confused, and 5 being a hole in it
    (WSM-STM-022) is the only gap there is.

    The serializer is the other place a mapping would appear, because it is the one thing that turns
    a local exception into something structured for the wire. WSM-ERR-006 fixes its two keys.
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
