// Browser entry point. Nothing reachable from here may import `ws` (WSM-API-022).

import { type Codec, getCodec, JsonCodec, registerCodec } from './codec';
import { settings } from './conf';
import type { Peer } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { BrowserSocket } from './transports/browser-socket';

/**
 * Dial `url` and return a serving peer (m3 §4.5).
 *
 * Throws if the **first** attempt fails, with the underlying error, whatever `reconnect` says
 * (WSM-RCN-006/WSM-INV-018). Reconnection applies to connections that were established and then
 * lost; a peer that retried its first dial forever would turn a typo in the URL into silence, and
 * the caller would hold something that looks alive and never will be.
 *
 * The codec is resolved **before** any socket is touched. Putting the lookup after the upgrade is
 * the single most likely wrong implementation of WSM-CDC-016, and it is what makes a misconfigured
 * deployment fail as a puzzling decode error on the tenth frame rather than as a named startup
 * error.
 */
export async function connect(url: string, options: ConnectOptions = {}): Promise<Peer> {
  const codec: Codec = options.codec ?? getCodec(settings.codec);
  // The one closure the reconnect driver holds. It captures nothing the first dial did not, so the
  // hundredth attempt offers the same subprotocol to the same url as the first (WSM-RCN-020).
  const dial: Dial = async () => BrowserSocket.connect(url, codec.name, { subprotocols: options.subprotocols });
  return dialAndEstablish(await dial(), dial, options, codec);
}

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
// What an application configures, and the schedule it may want to reason about - and nothing else.
// `ConnectionLoop`, `Heartbeat`, `AttemptCounter` and `dialAndEstablish` are how the helper is built,
// not what a consumer touches: `connect()` constructs them, `peer.close()` stops them, and a name
// exported from the package root is one this library owes compatibility to for the whole of v1. The
// defaults are spelled as parameter defaults, the way Python spells them.
export {
  type ConnectOptions,
  Hello,
  type HelloOptions,
  Reconnect,
  type ReconnectOptions,
  backoffDelay,
  shouldRetry,
  unjitteredDelay,
} from './reconnect';
export { PeerRegistry, type TagValue } from './registry';
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
