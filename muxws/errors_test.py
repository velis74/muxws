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
