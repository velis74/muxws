/**
 * Subpath export `muxws/node`: the acceptor side, over the `ws` package.
 *
 * The peer implementation is shared with the browser entry point; only the socket adapter differs
 * (WSM-API-022). `ws` is an optional peer dependency and is reached only from here.
 */

import type { WebSocket as NodeWebSocket } from 'ws';

import { type Codec, getCodec } from './codec';
import { settings } from './conf';
import { type ErrorSerializer, Peer, type StreamHandler } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { mismatchError, offer, PREFIX, select } from './subprotocol';
import { WsSocket } from './transports/ws-socket';

// The same surface the browser entry point offers, for the same reason: see `ts/index.ts`.
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
export { WsSocket } from './transports/ws-socket';
export { VERSION } from './version';

/** `ConnectOptions` plus the one field only node can honour: a browser cannot set handshake headers. */
export interface NodeConnectOptions extends ConnectOptions {
  headers?: Record<string, string>;
}

/**
 * Dial `url` over the `ws` package and return a serving peer (m3 §4.5).
 *
 * The node twin of `connect()` in `ts/index.ts`, and the same contract: it throws if the **first**
 * attempt fails, with the underlying error, whatever `reconnect` says (WSM-RCN-006/WSM-INV-018).
 * Only the dial closure differs, which is what `SocketAdapter` exists for (WSM-API-021).
 *
 * `ws` is imported inside the closure and not at module scope: it is an optional peer dependency, and
 * `accept()` / `handleProtocols()` must keep working for a server whose application never dials.
 */
export async function connect(url: string, options: NodeConnectOptions = {}): Promise<Peer> {
  // Before any socket is touched (WSM-CDC-016) - see the same note on the browser entry point.
  const codec: Codec = options.codec ?? getCodec(settings.codec);
  const dial: Dial = async () => dialWs(url, codec.name, options);
  return dialAndEstablish(await dial(), dial, options, codec);
}

async function dialWs(url: string, codecName: string, options: NodeConnectOptions): Promise<WsSocket> {
  const { WebSocket } = await import('ws');
  const socket = new WebSocket(url, offer(codecName, options.subprotocols), { headers: options.headers });

  const adapter = await new Promise<WsSocket>((resolve, reject) => {
    const onError = (error: Error) => {
      socket.off('open', onOpen);
      // A 400 on the upgrade is what an acceptor answers a codec it does not speak (WSM-CDC-022);
      // `ws` surfaces it as an ordinary error carrying the status in its message.
      reject(/\b400\b/.test(error.message) ? mismatchError(codecName) : error);
    };
    const onOpen = () => {
      socket.off('error', onError);
      // Constructed inside the handler rather than after the await: `WsSocket`'s constructor is what
      // attaches the lasting `error` listener, and a gap between the two would let a socket failing
      // in that microtask reach node as an unhandled `error` event.
      resolve(new WsSocket(socket));
    };
    socket.once('open', onOpen);
    socket.once('error', onError);
  });

  const wanted = `${PREFIX}${codecName}`;
  if (socket.protocol !== wanted) {
    // A server that completed the handshake having negotiated something else - or nothing - is the
    // same failure one step later, and the socket is closed with the policy-violation code
    // (WSM-CDC-028).
    adapter.close(1008, 'muxws subprotocol mismatch');
    throw mismatchError(codecName);
  }
  return adapter;
}

export interface AcceptOptions {
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
  errorSerializer?: ErrorSerializer;
  codec?: Codec;
  maxFrameBytes?: number;
}

/**
 * The handshake hook for a `ws` server.
 *
 * `ws` decides the subprotocol from `handleProtocols(protocols, request)`, where `protocols` is a
 * `Set`. Returning `false` refuses the subprotocol; the assertion itself lives in `select`, so this
 * is only the shape adaptation (WSM-CDC-022/027).
 */
export function handleProtocols(protocols: Set<string>): string | false {
  const selected = select([...protocols], settings.codec);
  return selected ?? false;
}

/**
 * Wrap an accepted `ws` connection in a peer.
 *
 * The connection has already handshaken by the time `ws` hands it over, so the subprotocol is
 * verified on the open socket and the socket is closed with the policy-violation code if it
 * disagrees (WSM-CDC-028).
 */
export async function accept(socket: NodeWebSocket, options: AcceptOptions = {}): Promise<Peer> {
  const codec = options.codec ?? getCodec(settings.codec);
  const negotiated = socket.protocol;

  if (negotiated !== `${PREFIX}${codec.name}`) {
    socket.close(1008, 'codec mismatch');
    throw mismatchError(codec.name);
  }

  return new Peer(new WsSocket(socket), {
    codec,
    isDialer: false,
    errorSerializer: options.errorSerializer,
    maxFrameBytes: options.maxFrameBytes,
    // The two local caps (WSM-FRG-035, WSM-STM-036). Neither is ever encoded into a frame and
    // neither has a remote counterpart to consult.
    maxPayloadBytes: options.maxPayloadBytes,
    maxConcurrentStreams: options.maxConcurrentStreams,
  });
}

/** Accept, register `handler`, and run the read loop until the socket closes. */
export async function serve(socket: NodeWebSocket, options: AcceptOptions & { handler: StreamHandler }): Promise<void> {
  const { handler, ...rest } = options;
  const peer = await accept(socket, rest);
  peer.onStream(handler);
  await peer.serve();
}
