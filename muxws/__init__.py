"""muxws - multiplexed, cancellable, bidirectional streams over one WebSocket."""

from muxws.api import accept, connect, resolve_codec, select_subprotocol, serve
from muxws.codecs import Codec, get_codec, register_codec, registered_codecs
from muxws.codecs.json_ import JsonCodec
from muxws.conf import Settings, settings
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
from muxws.observability import CloseReason
from muxws.peer import default_error_serializer, ErrorSerializer, Peer, StreamHandler
from muxws.reconnect import backoff_delay, Hello, Reconnect
from muxws.registry import PeerRegistry
from muxws.stream import Stream, StreamState
from muxws.transports import SocketAdapter

__version__ = "0.1.0"

# The library registers JSON itself (WSM-CDC-004); the codec module must not (WSM-CDC-014).
register_codec("json", JsonCodec())

__all__ = [
    "ABSENT",
    "Assembler",
    "CloseReason",
    "Codec",
    "CodecError",
    "CodecMismatch",
    "CodecNotRegistered",
    "ConnectionClosed",
    "ConnectionGoingAway",
    "ConnectionLost",
    "ErrorSerializer",
    "Frame",
    "Hello",
    "JsonCodec",
    "MAX_FRAME_BYTES",
    "MuxwsError",
    "Peer",
    "PeerRegistry",
    "ProtocolError",
    "Reconnect",
    "RemoteError",
    "ResetCode",
    "Settings",
    "SocketAdapter",
    "Stream",
    "StreamAlreadyConsumed",
    "StreamClosed",
    "StreamHandler",
    "StreamRefused",
    "StreamReset",
    "StreamState",
    "StreamTimeout",
    "__version__",
    "accept",
    "backoff_delay",
    "connect",
    "default_error_serializer",
    "encoded_length",
    "from_mapping",
    "get_codec",
    "register_codec",
    "registered_codecs",
    "resolve_codec",
    "select_subprotocol",
    "serve",
    "settings",
    "split_frame",
    "to_mapping",
]
