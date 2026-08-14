// Browser entry point. Nothing reachable from here may import `ws` (WSM-API-022).

import { type Codec, getCodec, JsonCodec, registerCodec } from './codec';
import { settings } from './conf';
import type { Peer } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { BrowserSocket, UnixSocketsUnsupportedError } from './transports/browser-socket';

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
  // Before the codec, and therefore before everything: a url this entry point can never dial is not
  // a socket, so refusing it here does not weaken WSM-CDC-016 - it keeps the codec error for the
  // case where the url *was* dialable, which is the diagnosis a reader of that error wants.
  refuseUnixScheme(url);
  const codec: Codec = options.codec ?? getCodec(settings.codec);
  // The one closure the reconnect driver holds. It captures nothing the first dial did not, so the
  // hundredth attempt offers the same subprotocol to the same url as the first (WSM-RCN-020).
  const dial: Dial = async () => BrowserSocket.connect(url, codec.name, { subprotocols: options.subprotocols });
  return dialAndEstablish(await dial(), dial, options, codec);
}

/**
 * The one url this entry point inspects, and the only reason it inspects any (WSM-API-022).
 *
 * `ws+unix:///run/app.sock:/route` dials a filesystem socket. That is a `ws` capability and nothing
 * else's: the platform's global `WebSocket` - a browser's, and node's own undici one - rejects the
 * scheme in its own parser, with prose that names neither muxws nor the entry point that would have
 * worked (`expected a ws: or wss: url` under node, `The URL's scheme must be either 'ws' or 'wss'`
 * under jsdom). Both are true and neither is actionable, and the reader has typed a url this library
 * documents as supported, so the fix - import `connect` from `muxws/node` - has to be in the message.
 * This is the same reasoning as `unopenedError` in `ts/transports/browser-socket.ts`: when the
 * platform cannot say why, the dialer composes the diagnostic itself.
 *
 * A `UnixSocketsUnsupportedError` and therefore a `MuxwsError` (WSM-ERR-016): a refusal muxws itself
 * composed must not leak through an application's one `instanceof MuxwsError` handler. `CodecMismatch`
 * stays reserved for a handshake that was actually refused (WSM-CDC-024), and none happens here.
 *
 * Matched as a lowercased string rather than through `new URL()` deliberately: the parser throws
 * `ERR_INVALID_URL` for anything malformed, which would swap the platform's own diagnostic for ours
 * on every mistyped url, not just this one. A scheme is ASCII-case-insensitive (`new URL()` folds
 * `WS+UNIX:` to `ws+unix:`), hence the fold; leading whitespace is what the parser would strip,
 * hence the trim.
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
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamAlreadyConsumed,
  StreamClosed,
  StreamRefused,
  StreamReset,
  StreamTimeout,
  // The two shared transport bases (WSM-ERR-016). A browser build must be able to write
  // `instanceof TransportUrlError` without importing `muxws/node`, where the classes that extend them
  // live.
  TransportUnsupportedError,
  TransportUrlError,
  exceptionForReset,
} from './errors';
export { ABSENT, type Absent, type Frame, V1_FRAME_TYPES, framesEqual, fromMapping, toMapping } from './frames';
export { Assembler, MAX_FRAME_BYTES, encodedLength, splitFrame } from './fragment';
// `GoawayState`, `MAX_STREAM_ID`, `PingRegistry` and `newNonce` are deliberately NOT re-exported:
// they are the connection's own bookkeeping, Python keeps every one of them out of `muxws.__all__`,
// and a consumer who reaches for them is reimplementing the peer. `logFrame` goes with them - the
// peer calls it, an application does not. `logger` stays, because it is this port's stand-in for the
// logging module Python gets from its standard library, and setting its level is the only way to
// turn frame logging on (WSM-OBS-001).
export { type CloseReason, type FrameDirection, type LogLevel, logger } from './observability';
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
// `PREFIX` and `select` only. `offer`, `findOffer`, `generationOf` and `mismatchError` are how the
// dialer and the acceptor build and read the handshake between themselves; Python exports none of
// them either, and an application that needs the prefix needs the constant, not the machinery.
export { PREFIX, select } from './subprotocol';
// `UnixSocketsUnsupportedError` is defined beside the transport that refuses the url and re-exported
// here, which is WSM-ERR-016's placement clause: a concrete transport error lives in its own
// transport's module and is reachable only from the entry point that ships that transport.
// `muxws/node` must not carry it - that subpath's unix dial works.
export { BrowserSocket, type BrowserSocketOptions, UnixSocketsUnsupportedError } from './transports/browser-socket';
export { type SocketAdapter } from './transports/index';
export { MemorySocket, memoryPair } from './transports/memory';
export { VERSION } from './version';

// The library registers JSON itself (WSM-CDC-004); the codec module must not (WSM-CDC-014).
registerCodec('json', new JsonCodec());
