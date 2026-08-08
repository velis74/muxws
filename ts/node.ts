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
import { select } from './subprotocol';
import { WsSocket } from './transports/ws-socket';

export { WsSocket } from './transports/ws-socket';
export { VERSION } from './version';

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

  if (negotiated !== `muxws.v1.${codec.name}`) {
    socket.close(1008, 'codec mismatch');
    const { mismatchError } = await import('./subprotocol');
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
