"""muxws - multiplexed, cancellable, bidirectional streams over one WebSocket."""

from muxws.codecs import Codec, get_codec, register_codec, registered_codecs
from muxws.codecs.json_ import JsonCodec
from muxws.errors import (
    CodecError,
    CodecMismatch,
    CodecNotRegistered,
    ConnectionClosed,
    ConnectionGoingAway,
    ConnectionLost,
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
from muxws.fragment import Assembler, encoded_length, MAX_FRAME_BYTES, split_frame
from muxws.frames import ABSENT, Frame, from_mapping, to_mapping

__version__ = "0.1.0"

# The library registers JSON itself (WSM-CDC-004); the codec module must not (WSM-CDC-014).
register_codec("json", JsonCodec())

__all__ = [
    "ABSENT",
    "MAX_FRAME_BYTES",
    "Assembler",
    "CloseReason",
    "Codec",
    "ErrorSerializer",
    "CodecError",
    "CodecMismatch",
    "CodecNotRegistered",
    "ConnectionClosed",
    "ConnectionGoingAway",
    "ConnectionLost",
    "Frame",
    "JsonCodec",
    "MuxwsError",
    "Peer",
    "ProtocolError",
    "RemoteError",
    "ResetCode",
    "SocketAdapter",
    "Stream",
    "StreamAlreadyConsumed",
    "StreamClosed",
    "StreamHandler",
    "StreamRefused",
    "StreamState",
    "StreamReset",
    "StreamTimeout",
    "__version__",
    "default_error_serializer",
    "encoded_length",
    "from_mapping",
    "get_codec",
    "register_codec",
    "registered_codecs",
    "split_frame",
    "to_mapping",
]
