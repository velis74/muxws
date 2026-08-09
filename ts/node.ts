/**
 * Subpath export `muxws/node`: the acceptor side, over the `ws` package.
 *
 * The peer implementation is shared with the browser entry point; only the socket adapter differs
 * (WSM-API-022). `ws` is an optional peer dependency and is reached only from here.
 */

import type { ClientRequest, IncomingMessage } from 'node:http';

import type { WebSocket as NodeWebSocket, WebSocketServer } from 'ws';

import { type Codec, getCodec } from './codec';
import { settings } from './conf';
import { CodecMismatch } from './errors';
import { type ErrorSerializer, Peer, type StreamHandler } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { mismatchError, offer, PREFIX, select } from './subprotocol';
import { WsSocket } from './transports/ws-socket';

/**
 * What an acceptor answers a codec it does not speak (WSM-CDC-022), and the only status either half
 * of this module reads as a refused muxws handshake.
 */
const REFUSED = 400;

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

  const wanted = `${PREFIX}${codecName}`;
  const adapter = await new Promise<WsSocket>((resolve, reject) => {
    // What the acceptor selected, read off the 101 itself. `ws` emits `upgrade` with the response
    // and only then checks the subprotocol, aborting with prose and never opening a socket - so this
    // is the one moment the negotiated value exists as data rather than as an error message.
    let upgraded = false;
    let negotiated: string | undefined;
    const onUpgrade = (response: IncomingMessage) => {
      upgraded = true;
      const header = response.headers['sec-websocket-protocol'];
      negotiated = Array.isArray(header) ? header.join(',') : header;
    };
    const onUnexpectedResponse = (request: ClientRequest, response: IncomingMessage) => {
      socket.off('open', onOpen);
      // `ws` emits this instead of `error` as soon as a listener exists, so this branch owns the
      // cleanup as well: without it the aborted upgrade's socket is never released.
      request.destroy();
      response.destroy();
      // `res.statusCode` is the only place `ws` hands the refusal over as a number. The message the
      // `error` path carries is prose, and reading a status out of prose is what let a
      // cross-language dial - where the wording differs, and once did not contain "400" at all -
      // miss a real refusal and surface a bare connection failure (WSM-CDC-024).
      reject(
        response.statusCode === REFUSED
          ? mismatchError(codecName)
          : new Error(`unexpected server response: ${response.statusCode ?? 'none'}`),
      );
    };
    const onError = (error: Error) => {
      socket.off('open', onOpen);
      socket.off('upgrade', onUpgrade);
      socket.off('unexpected-response', onUnexpectedResponse);
      // An acceptor that answered 101 without echoing our entry is the same refusal one step later,
      // and it is the shape a **cross-language** dial actually meets: `ws` aborts it as `Server sent
      // no subprotocol`, a message with no status in it at all, so the fallback below cannot see it
      // and the WSM-CDC-028 check after this promise never runs because no socket ever opens. This
      // is the only place it can be caught, and leaving it uncaught is a bare connection failure
      // where WSM-CDC-024 requires `CodecMismatch`.
      const refusedAtUpgrade = upgraded && negotiated !== wanted;
      // The fallback only: a `ws` release that reports the status without emitting the event above.
      // Matched against `ws`'s whole sentence and not against the number, because a bare `400` also
      // appears in `connect ECONNREFUSED 127.0.0.1:400` - a port nothing is listening on, reported
      // as a codec mismatch, which is the same "read a status out of prose" defect one address over.
      const refusedByStatus = new RegExp(`unexpected server response: ${REFUSED}\\b`, 'i').test(error.message);
      reject(refusedAtUpgrade || refusedByStatus ? mismatchError(codecName) : error);
    };
    const onOpen = () => {
      socket.off('error', onError);
      socket.off('upgrade', onUpgrade);
      socket.off('unexpected-response', onUnexpectedResponse);
      // Constructed inside the handler rather than after the await: `WsSocket`'s constructor is what
      // attaches the lasting `error` listener, and a gap between the two would let a socket failing
      // in that microtask reach node as an unhandled `error` event.
      resolve(new WsSocket(socket));
    };
    socket.once('open', onOpen);
    socket.once('upgrade', onUpgrade);
    // Left attached after `onUnexpectedResponse` has already rejected: destroying the request makes
    // `ws` emit one more `error`, and with no listener node would take the process down for it.
    socket.once('error', onError);
    socket.once('unexpected-response', onUnexpectedResponse);
  });

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
 * The **selection** hook for a `ws` server: which subprotocol to echo back (WSM-CDC-022/027).
 *
 * `ws` decides the subprotocol from `handleProtocols(protocols, request)`, where `protocols` is a
 * `Set`. Returning `false` selects nothing - it does **not** refuse: `ws` still answers 101, just
 * without a `Sec-WebSocket-Protocol` header. Refusing is `refuseMismatchedUpgrade`'s job, and a
 * server that installs only this hook violates WSM-CDC-022.
 */
export function handleProtocols(protocols: Set<string>): string | false {
  const selected = select([...protocols], settings.codec);
  return selected ?? false;
}

/** The `Sec-WebSocket-Protocol` request header as the list `select` reads (WSM-CDC-020/021). */
function offeredProtocols(request: IncomingMessage): string[] {
  const header = request.headers['sec-websocket-protocol'];
  if (header === undefined) return [];
  return (Array.isArray(header) ? header.join(',') : header).split(',').map((entry) => entry.trim());
}

/**
 * Install the **refusal** on a `ws` server, so a codec it does not speak gets HTTP 400.
 *
 * This exists because `handleProtocols` cannot do it. That hook only picks a value; whatever it
 * returns, `ws` completes the handshake with 101, leaving the mismatch to be found on an open socket
 * - the "complete the handshake and close afterwards" WSM-CDC-022 forbids wherever the transport
 * gives a choice. `shouldHandle` is the hook that aborts an upgrade with a status, and `ws` 8
 * deprecated the only other one (`verifyClient`), so the acceptor needs both hooks and this one is
 * not redundant with the line above it:
 *
 * ```ts
 * const server = refuseMismatchedUpgrade(new WebSocketServer({ port, handleProtocols }));
 * ```
 *
 * The inherited `shouldHandle` runs first, so a server constructed with `path` keeps that check.
 */
export function refuseMismatchedUpgrade(server: WebSocketServer): WebSocketServer {
  const inherited = server.shouldHandle.bind(server);
  server.shouldHandle = (request: IncomingMessage): boolean =>
    inherited(request) === true && select(offeredProtocols(request), settings.codec) !== null;
  return server;
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

/**
 * Accept, register `handler`, and run the read loop until the socket closes.
 *
 * A `CodecMismatch` returns quietly rather than rejecting, which is what `muxws.serve()` does in
 * Python and the reason the two ports agree here. By the time it is raised the refusal has already
 * been answered on the wire with HTTP 400 (WSM-CDC-022) and already logged with both codec names
 * (WSM-CDC-029), so rejecting again reports nothing new - and in Node it reports it as an unhandled
 * rejection out of a `ws` connection handler, which takes the whole process down. That is not
 * hypothetical: it is how `interop/runner.ts`'s acceptor died during M6.
 */
export async function serve(socket: NodeWebSocket, options: AcceptOptions & { handler: StreamHandler }): Promise<void> {
  const { handler, ...rest } = options;
  let peer;
  try {
    peer = await accept(socket, rest);
  } catch (error) {
    if (error instanceof CodecMismatch) return;
    throw error;
  }
  peer.onStream(handler);
  await peer.serve();
}
