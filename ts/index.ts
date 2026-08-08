// Browser entry point. Nothing reachable from here may import `ws` (WSM-API-022).

import { JsonCodec, registerCodec } from './codec';

export { type Codec, JsonCodec, clearCodecs, getCodec, registerCodec, registeredCodecs } from './codec';
export { Settings, settings } from './conf';
export {
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
  exceptionForReset,
} from './errors';
export { ABSENT, type Absent, type Frame, V1_FRAME_TYPES, framesEqual, fromMapping, toMapping } from './frames';
export { Assembler, MAX_FRAME_BYTES, encodedLength, splitFrame } from './fragment';
export { GoawayState, MAX_STREAM_ID, PingRegistry, newNonce } from './lifecycle';
export { type CloseReason, type FrameDirection, type LogLevel, logFrame, logger } from './observability';
export {
  type CloseOptions,
  DEFAULT_DRAIN_MS,
  DEFAULT_MAX_CONCURRENT_STREAMS,
  DEFAULT_MAX_PAYLOAD_BYTES,
  DEFAULT_PING_TIMEOUT_MS,
  type ErrorSerializer,
  type OpenOptions,
  Peer,
  type PeerOptions,
  type RequestOptions,
  type StreamHandler,
  defaultErrorSerializer,
} from './peer';
export {
  type EndOptions,
  type ReplyOptions,
  type ResultOptions,
  type SendOptions,
  type SendOptions as StreamSendOptions,
  Stream,
  StreamState,
} from './stream';
export { PREFIX, findOffer, generationOf, mismatchError, offer, select } from './subprotocol';
export { BrowserSocket, type BrowserSocketOptions } from './transports/browser-socket';
export { type SocketAdapter } from './transports/index';
export { MemorySocket, memoryPair } from './transports/memory';
export { VERSION } from './version';

// The library registers JSON itself (WSM-CDC-004); the codec module must not (WSM-CDC-014).
registerCodec('json', new JsonCodec());
