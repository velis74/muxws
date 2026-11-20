// Browser entry point. Nothing reachable from here may import `ws` (WSM-API-022).

import { type Codec, getCodec, JsonCodec, registerCodec } from './codec';
import { settings } from './conf';
import type { Peer } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { BrowserSocket, UnixSocketsUnsupportedError } from './transports/browser-socket';

/**
 * Dial `url` and return a serving peer. The codec is resolved before any socket opens (WSM-CDC-016).
 * If the first attempt fails it throws the underlying error, whatever `reconnect` says
 * (WSM-RCN-006/WSM-INV-018); only an established connection that is lost is reconnected.
 */
export async function connect(url: string, options: ConnectOptions = {}): Promise<Peer> {
  // Checked before the codec lookup: a url this entry point cannot dial raises
  // `UnixSocketsUnsupportedError` even when the codec is also misconfigured. No socket is opened, so
  // WSM-CDC-016 still holds.
  refuseUnixScheme(url);
  const codec: Codec = options.codec ?? getCodec(settings.codec);
  // The reconnect driver calls this closure for every attempt; each attempt offers the same
  // subprotocol to the same url as the first (WSM-RCN-020).
  const dial: Dial = async () => BrowserSocket.connect(url, codec.name, { subprotocols: options.subprotocols });
  return dialAndEstablish(dial, options, codec);
}

/**
 * Throws `UnixSocketsUnsupportedError` (WSM-ERR-016), naming `muxws/node`, for a `ws+unix:` url; every
 * other url is left to the platform's `WebSocket` (WSM-API-022). The scheme is matched
 * case-insensitively on the trimmed url, without `new URL()`, so a malformed url keeps the platform's
 * own error.
 */
function refuseUnixScheme(url: string): void {
  if (!url.trim().toLowerCase().startsWith('ws+unix:')) return;
  throw new UnixSocketsUnsupportedError(
    `cannot dial '${url}' from this entry point: a ws+unix: url names a filesystem socket, which ` +
      "neither a browser nor node's global WebSocket can open. Import connect from 'muxws/node', " +
      'which dials it through the `ws` package.',
  );
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
  OriginRefused,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamAlreadyConsumed,
  StreamClosed,
  StreamRefused,
  StreamReset,
  StreamTimeout,
  // The two shared transport base classes (WSM-ERR-016), exported here so a browser build can test
  // `instanceof TransportUrlError` without importing `muxws/node`, where the subclasses are defined.
  TransportUnsupportedError,
  TransportUrlError,
  exceptionForReset,
} from './errors';
export {
  Exchange,
  type ExchangeDict,
  type ExchangeDictOptions,
  type ExchangeOptions,
  type ExchangeOutcome,
  type ExchangeParty,
  type ExchangeSide,
  type ExchangeSideDict,
} from './exchange';
export { ABSENT, type Absent, type Frame, V1_FRAME_TYPES, framesEqual, fromMapping, toMapping } from './frames';
export { Assembler, MAX_FRAME_BYTES, encodedLength, splitFrame } from './fragment';
// `GoawayState`, `MAX_STREAM_ID`, `PingRegistry`, `newNonce` and `logFrame` are not exported: they
// are internal to the peer, and Python excludes them from `muxws.__all__`. `logger` is exported; it
// replaces Python's standard-library logging, and setting its level enables frame logging
// (WSM-OBS-001).
export { type CloseReason, type FrameDirection, type LogLevel, logger } from './observability';
export { SameOrigin, type SameOriginOptions, parseOrigin } from './origin';
export {
  type CloseOptions,
  DEFAULT_DRAIN_MS,
  DEFAULT_MAX_CONCURRENT_STREAMS,
  DEFAULT_MAX_PAYLOAD_BYTES,
  DEFAULT_PING_TIMEOUT_MS,
  type ErrorSerializer,
  type IncomingHeadersInterceptor,
  type OpenInterceptor,
  type OpenOptions,
  Peer,
  type PeerOptions,
  type ReplyHeadersInterceptor,
  type RequestOptions,
  type StreamHandler,
  defaultErrorSerializer,
} from './peer';
// The reconnect configuration types and the backoff schedule functions. `ConnectionLoop`,
// `Heartbeat`, `AttemptCounter` and `dialAndEstablish` are internal. Every name exported from the package
// root is covered by v1 compatibility. Defaults are parameter defaults, as in Python.
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
// `PREFIX` and `select` only. `offer`, `findOffer`, `generationOf` and `mismatchError` are internal;
// Python does not export them either.
export { PREFIX, select } from './subprotocol';
// `UnixSocketsUnsupportedError` is defined in the transport module that throws it and re-exported
// only here (WSM-ERR-016: a concrete transport error is reachable only from the entry point that
// ships its transport). `muxws/node` does not export it, because `muxws/node` can dial unix sockets.
export { BrowserSocket, type BrowserSocketOptions, UnixSocketsUnsupportedError } from './transports/browser-socket';
export { type SocketAdapter } from './transports/index';
export { MemorySocket, memoryPair } from './transports/memory';
export { VERSION } from './version';

// The library registers JSON itself (WSM-CDC-004); the codec module must not (WSM-CDC-014).
registerCodec('json', new JsonCodec());
