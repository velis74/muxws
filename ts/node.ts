/**
 * Subpath export `muxws/node`: the acceptor side, over the `ws` package.
 *
 * The peer implementation is shared with the browser entry point; only the socket adapter differs
 * (WSM-API-022). `ws` is an optional peer dependency and is reached only from here.
 *
 * It is also the only entry point that can dial a unix-domain socket; see `unixTarget` for the url
 * grammar and `dialWs` for the dial. `ts/index.ts` rejects `ws+unix:` and names this module in the
 * error, because neither a browser nor node's global `WebSocket` can open a socket file.
 */

import type { ClientRequest, IncomingMessage } from 'node:http';
import type { NetConnectOpts, Socket } from 'node:net';

import type { ClientOptions, WebSocket as NodeWebSocket, WebSocketServer } from 'ws';

import { type Codec, getCodec } from './codec';
import { settings } from './conf';
import { CodecMismatch, OriginRefused, ProtocolError, TransportUnsupportedError, TransportUrlError } from './errors';
import { type SameOrigin, compilePolicy } from './origin';
import {
  type ErrorSerializer,
  type IncomingHeadersInterceptor,
  type OpenInterceptor,
  Peer,
  registerInterceptors,
  type ReplyHeadersInterceptor,
  type StreamHandler,
} from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { logNegotiatedMismatch, mismatchError, offer, PREFIX, select } from './subprotocol';
import { WsSocket } from './transports/ws-socket';

/**
 * The HTTP status an acceptor returns for an unsupported codec (WSM-CDC-022), and the only status
 * this module treats as a refused muxws handshake on the dialer side.
 */
const REFUSED = 400;

/** The WebSocket policy-violation close code, sent for a codec mismatch on an open socket (WSM-CDC-028). */
const POLICY_VIOLATION = 1008;

/** The scheme `unixTarget` parses, as `new URL()` normalises it: lowercase, colon included. */
const UNIX_SCHEME = 'ws+unix:';

/** A scheme muxws does not support; `unixTarget` rejects it with a `UnixUrlError` that names it. */
const TLS_UNIX_SCHEME = 'wss+unix:';

/** The `Host:` header value when the url has no authority (the usual `ws+unix:` form). */
const DEFAULT_AUTHORITY = 'localhost';

/**
 * The schemes the `ws` constructor accepts. Used only to distinguish a url failure from other
 * constructor failures.
 *
 * `ws` throws a `SyntaxError` both for an unparseable url and for an invalid subprotocol token, with
 * no field distinguishing them. `translateUrlThrow` checks the url against this list so an invalid
 * subprotocol is not reported as an invalid address.
 */
const WS_SCHEMES = ['ws:', 'wss:', 'http:', 'https:', UNIX_SCHEME];

/**
 * An address `muxws/node` cannot open because `ws` cannot parse it (WSM-ERR-016).
 *
 * Named after the package that performs the dial, as the Python counterpart is named after
 * `websockets`. The message includes `ws`'s original message, and the original error is `cause`.
 *
 * Dial failures are not covered: `ENOTFOUND`, `ECONNREFUSED` and a 503 from a proxy are not url
 * errors and reach the caller unchanged.
 */
export class WsUrlError extends TransportUrlError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'WsUrlError';
  }
}

/**
 * An invalid `ws+unix:` url (WSM-ERR-016).
 *
 * Corresponds to `muxws.transports.unix.UnixUrlError` and is thrown for the same three cases: a
 * request target that does not start with `/`, a url that names no socket file, and the `wss+unix:`
 * scheme. They are checked here, not by `ws`, because `unixTarget` parses the `ws+unix:` grammar
 * itself (differently from `ws`).
 *
 * A `TransportUrlError`, not a `TransportUnsupportedError`: in each case the url is wrong and the
 * transport is available.
 */
export class UnixUrlError extends TransportUrlError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'UnixUrlError';
  }
}

/**
 * `muxws/node` was asked to dial and the optional `ws` peer dependency is not installed.
 *
 * The underlying failure is `Error: Cannot find package 'ws' imported from …/node_modules/muxws/dist/
 * node.js` with `code: 'ERR_MODULE_NOT_FOUND'`. WSM-ERR-016 requires the error to state the install
 * command; this class's message does.
 *
 * Named `NotInstalled`, not `Unavailable`, as Python's `WebsocketsNotInstalledError`: "unavailable"
 * suggests an unreachable endpoint, which is transient and retryable; a missing package is not.
 *
 * Only `ERR_MODULE_NOT_FOUND` is converted to this class. Any other import failure, such as a broken
 * install or an error while evaluating `ws`, is rethrown unchanged.
 */
export class WsNotInstalledError extends TransportUnsupportedError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'WsNotInstalledError';
  }
}

/** A parsed `ws+unix:` url: the socket file to open, and the `ws://` url used for the handshake. */
interface UnixTarget {
  /** The filesystem path of the listening socket. `sun_path` limits it to about 108 bytes on Linux. */
  socketPath: string;
  /** `ws://<authority><target>`: provides the request target and the `Host` header. */
  uri: string;
}

/**
 * The `ws+unix:` grammar: the path is split on the first colon into the socket file (before) and the
 * request target (after).
 *
 * `ws` also parses this scheme, but uses `opts.path.split(':')` and keeps `parts[1]`, splitting on
 * every colon: `ws+unix:///a.sock:/ws:v2` requests `/ws`, and
 * `.../a.sock:/ws?since=2026-08-14T10:00:00Z` requests `/ws?since=2026-08-14T10`, without an error.
 * `muxws/transports/unix.py` splits on the first colon and keeps the remainder whole; splitting here,
 * before `ws` sees the url, makes both implementations request the same target. `ws` receives an
 * ordinary `ws:` url plus a `createConnection` that opens the file, so its `isIpcUrl` branch is not
 * used and the rest of the dial (the offer, the events, the 400) is the same as over TCP.
 *
 * Returns `null` for every other url, including one `new URL()` cannot parse. This runs on every
 * dial and only selects the unix branch; other urls are passed to `ws`, whose error is reported as a
 * `WsUrlError`.
 *
 * It throws `UnixUrlError` in three cases, the same three `parse_unix_url` rejects in Python
 * (WSM-ERR-016 requires a `MuxwsError`): a request target that does not start with `/`, a url naming
 * no socket file, and `wss+unix:`. These checks run before `ws` is imported and before anything is
 * opened, so an invalid `ws+unix:` url raises `UnixUrlError` even when `ws` is not installed.
 */
function unixTarget(url: string): UnixTarget | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (parsed.protocol === TLS_UNIX_SCHEME) {
    // A socket file has no TLS layer; filesystem permissions control access. Neither implementation
    // supports the scheme. `ws` would report only the list of schemes it accepts, so the scheme is
    // rejected here with a message that names `ws+unix:`.
    throw new UnixUrlError(
      `cannot dial '${url}': wss+unix: is not a scheme. A filesystem socket has no TLS to negotiate - ` +
        'the permissions on the socket file are the access control - so use ws+unix: instead.',
    );
  }
  if (parsed.protocol !== UNIX_SCHEME) return null;

  // The query belongs to the request target, so it is appended before the split:
  // `ws+unix:///p.sock:/r?a=1` opens `/p.sock` and requests `/r?a=1`.
  const whole = rawPathAndQuery(url);
  const colon = whole.indexOf(':');
  const socketPath = colon === -1 ? whole : whole.slice(0, colon);
  const route = colon === -1 ? '' : whole.slice(colon + 1);

  // A url without a path names no socket file. `ws` would report "The URL's pathname is empty"; this
  // message shows the expected `ws+unix:` form instead, e.g. for `ws+unix://run` (an authority and no
  // path).
  if (socketPath === '') {
    throw new UnixUrlError(
      `the url '${url}' names no socket file: a ws+unix: url is ` +
        "'ws+unix:///absolute/path.sock:/request-target', and everything before the first ':' of the " +
        'path is the file to open. An authority with no path names nothing that can be opened.',
    );
  }
  if (route !== '' && !route.startsWith('/')) {
    // `ws://localhost` + `ws` is `ws://localhostws`, a valid url: the target becomes part of the
    // authority, and the dial reaches the socket file with target `/` and a wrong `Host`. An
    // acceptor that does not route (`unix_serve`, or a `WebSocketServer` without `path`) accepts
    // that handshake. A malformed request line can also make node answer 400, which would surface as
    // a `CodecMismatch`. `muxws/transports/unix.py` rejects the same url.
    throw new UnixUrlError(
      `the request target in '${url}' must begin with '/': the part after the ':' is an HTTP ` +
        'request target, not a path relative to anything',
    );
  }

  const authority = parsed.host === '' ? DEFAULT_AUTHORITY : parsed.host;
  return { socketPath, uri: `ws://${authority}${route === '' ? '/' : route}` };
}

/**
 * The path and query of a `ws+unix:` url as they are written in it, joined by `?` when the query is
 * not empty: the string Python's `urlsplit` returns as `path + "?" + query`.
 *
 * `URL.pathname` and `URL.search` are percent-encoded and have dot segments removed, so
 * `ws+unix:///tmp/my dir/a.sock` would name `/tmp/my%20dir/a.sock`. This function applies only the
 * preprocessing of `urlsplit`: leading C0 controls and spaces are removed, and so are tabs and
 * newlines anywhere. Trailing C0 controls and spaces are kept, as `urlsplit` keeps them, although
 * `new URL()` removes them. A space, a non-ASCII character and a `%XX` sequence are kept as written.
 * The caller has already parsed `url` with `new URL()`, so the scheme is present.
 */
function rawPathAndQuery(url: string): string {
  // eslint-disable-next-line no-control-regex
  let rest = url.replace(/^[\u0000-\u0020]+/, '').replace(/[\t\n\r]/g, '');
  rest = rest.slice(rest.indexOf(':') + 1);
  const hash = rest.indexOf('#');
  if (hash !== -1) rest = rest.slice(0, hash);
  if (rest.startsWith('//')) {
    const end = rest.slice(2).search(/[/?]/);
    rest = end === -1 ? '' : rest.slice(2 + end);
  }
  const question = rest.indexOf('?');
  if (question === -1) return rest;
  const query = rest.slice(question + 1);
  return query === '' ? rest.slice(0, question) : rest;
}

/**
 * A `createConnection` that opens the socket file instead of a TCP port, replacing `ws`'s `isIpcUrl`
 * branch.
 *
 * `node:net` is imported inside the function, as `ws` is, so acceptors and TCP dials do not load it.
 * The http agent's options object is passed through unchanged except for `path`, so `timeout` and
 * other options still apply; `net.connect` treats `path` as an IPC endpoint and ignores host and port.
 */
async function unixConnector(socketPath: string): Promise<(options: NetConnectOpts) => Socket> {
  const { connect } = await import('node:net');
  return (options) => connect({ ...options, path: socketPath });
}

/**
 * `import('ws')`, converting a missing package into `WsNotInstalledError`.
 *
 * WSM-ERR-016: a missing optional dependency must not reach the caller as a module-resolution error,
 * and the message must name the install command. The import is dynamic, so `accept()`, `serve()`,
 * `handleProtocols()` and `refuseMismatchedUpgrade()` work in a process that never dials and does not
 * have `ws` installed. A module-scope import would make importing `muxws/node` itself fail, before
 * any `instanceof WsNotInstalledError` check could run.
 */
// The return type is inferred, not written as `Promise<typeof import('ws')>`: under this project's
// `module: commonjs` the written type is `ws`'s `export =` shape, while the inferred one is the
// namespace a dynamic import produces.
async function requireWs() {
  try {
    return await import('ws');
  } catch (error) {
    // Only a resolution failure is converted. Other failures (a corrupt install, an error while
    // evaluating `ws`) are rethrown, because the package is installed.
    if ((error as { code?: unknown } | null)?.code !== 'ERR_MODULE_NOT_FOUND') throw error;
    throw new WsNotInstalledError(
      "muxws/node dials through the 'ws' package, which is not installed: run `npm install ws`. It is " +
        'an optional peer dependency because an application that only accepts connections never needs ' +
        `it. The resolver said: ${(error as Error).message}`,
      { cause: error },
    );
  }
}

/**
 * Rethrow a `ws` constructor error as a `WsUrlError` if it concerns the url, otherwise unchanged.
 *
 * `ws` throws a `SyntaxError` both for `Invalid URL: nonsense` and for `An invalid or duplicated
 * subprotocol was specified`, and the two errors cannot be told apart, so the url is parsed again: if
 * `new URL()` cannot parse `dialed`, or its scheme is not one `ws` accepts, the error concerns the
 * url; otherwise it is rethrown unchanged, so an invalid subprotocol is not reported as an invalid
 * address.
 *
 * `dialed` is the url passed to `ws` and `url` is the caller's url; they differ for `ws+unix:`, and
 * the message quotes the caller's url.
 */
function translateUrlThrow(url: string, dialed: string, error: unknown): never {
  let parsed: URL | null = null;
  try {
    parsed = new URL(dialed);
  } catch {
    parsed = null;
  }
  if (parsed !== null && WS_SCHEMES.includes(parsed.protocol)) throw error;
  // `ws`'s message is included unchanged (WSM-ERR-016); it names the invalid text.
  throw new WsUrlError(`cannot dial '${url}': ${(error as Error).message}`, { cause: error });
}

// The same reconnect exports as the browser entry point; see `ts/index.ts`.
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

// `UnixUrlError`, `WsUrlError` and `WsNotInstalledError` are exported where they are defined, and
// only from this module: WSM-ERR-016 keeps concrete transport errors out of the package root. The
// two base classes are exported from `muxws`, not from here, so a browser build can test
// `instanceof TransportUrlError` without importing a module that loads `ws` (WSM-API-022).

/** `ConnectOptions` plus `headers`, which only node supports: a browser cannot set handshake headers. */
export interface NodeConnectOptions extends ConnectOptions {
  headers?: Record<string, string>;
}

/**
 * Dial `url` over the `ws` package and return a serving peer.
 *
 * The node counterpart of `connect()` in `ts/index.ts`, with the same contract: it throws the
 * underlying error if the first attempt fails, regardless of `reconnect` (WSM-RCN-006/WSM-INV-018).
 * Only the dial closure differs (WSM-API-021).
 *
 * `ws` is imported inside the closure, not at module scope: it is an optional peer dependency, and
 * `accept()` / `handleProtocols()` work in a server that never dials.
 */
export async function connect(url: string, options: NodeConnectOptions = {}): Promise<Peer> {
  // Resolved before any socket is opened (WSM-CDC-016), as in the browser entry point.
  const codec: Codec = options.codec ?? getCodec(settings.codec);
  const dial: Dial = async () => dialWs(url, codec.name, options);
  return dialAndEstablish(dial, options, codec);
}

/**
 * Dial `url` with `ws` and return the adapter once the handshake is verified.
 *
 * `url` may be `ws:`, `wss:` or `ws+unix:///absolute/path.sock:/route`. `unixTarget` parses the unix
 * form, and `ws` receives a plain `ws://<authority><target>` url plus a `createConnection` that opens
 * the file; from there the code path is shared. The handshake bytes are the same as over TCP (an HTTP
 * GET with `Upgrade: websocket` and `Sec-WebSocket-Protocol: muxws.v1.<codec>`, answered 101 or 400),
 * so every branch below, including WSM-CDC-022/024/028, applies to unix sockets as to TCP.
 *
 * `unixTarget` splits on the first colon, as `muxws/transports/unix.py` does, so both languages derive
 * the same socket path and request target from a url. The authority is optional (`ws+unix://` is
 * usually followed directly by an absolute path); if present it becomes the `Host` header, and if
 * absent `Host: localhost` is sent, as in Python. `wss+unix:` is rejected by `unixTarget` with a
 * `UnixUrlError` before `ws` is imported.
 *
 * `createConnection` is used instead of `ws`'s `socketPath` option: `ws` (`initAsClient`) sets
 * `socketPath` to `undefined` after copying the caller's options, so passing it has no effect and the
 * dial goes to TCP port 80. `ws` keeps `createConnection` and uses it for every dial.
 *
 * `sun_path` limits a unix socket path to about 108 bytes on Linux (104 on macOS); exceeding it gives
 * a bind or connect error naming the path, not the length, so socket files belong in a short
 * directory. There is no `process.platform` check: on Windows `net.connect({ path })` opens a named
 * pipe, but no `ws+unix:` url can address one, because `new URL()` rejects the backslashes in
 * `\\.\pipe\name` on every platform. A Windows dial of a POSIX path fails with an error naming the
 * path, so this module has no equivalent of Python's `UnixSocketsUnsupportedError`; the browser entry
 * point has one for a different reason (see `UnixSocketsUnsupportedError` in `ts/index.ts`).
 *
 * Before a socket is opened, three checks run in this order (WSM-ERR-016): the `ws+unix:` grammar
 * (`unixTarget`, which does not use `ws`, so an invalid `ws+unix:` url raises `UnixUrlError` even
 * when `ws` is missing); the dependency (`requireWs`), because `ws` parses all other urls; and the
 * address, reported as a `WsUrlError`. All three precede the refusal handling below, which detects a
 * 400 in `ws`'s error message; otherwise a url containing a status number could be reported as a
 * `CodecMismatch`.
 */
async function dialWs(url: string, codecName: string, options: NodeConnectOptions): Promise<WsSocket> {
  const unix = unixTarget(url);
  const { WebSocket } = await requireWs();
  // For a `ws+unix:` url, the derived `ws://<authority><target>`; otherwise the caller's url,
  // unchanged.
  const dialed = unix === null ? url : unix.uri;
  const wsOptions: ClientOptions =
    unix === null
      ? { headers: options.headers }
      : {
          headers: options.headers,
          // `createConnection` is typed as `net.connect`'s overload set (options, port+host, path); an
          // http agent calls only the options form, and a single-signature function needs a cast to
          // be assignable to the overload set.
          createConnection: (await unixConnector(unix.socketPath)) as ClientOptions['createConnection'],
        };
  let socket: NodeWebSocket;
  try {
    socket = new WebSocket(dialed, offer(codecName, options.subprotocols), wsOptions);
  } catch (error) {
    translateUrlThrow(url, dialed, error);
  }

  const wanted = `${PREFIX}${codecName}`;
  const adapter = await new Promise<WsSocket>((resolve, reject) => {
    // The subprotocol the acceptor selected, read from the 101 response. `ws` emits `upgrade` before
    // checking the subprotocol; on a mismatch it aborts with an error message and never opens the
    // socket, so `upgrade` is the only event that carries the negotiated value.
    let upgraded = false;
    let negotiated: string | undefined;
    const onUpgrade = (response: IncomingMessage) => {
      upgraded = true;
      const header = response.headers['sec-websocket-protocol'];
      negotiated = Array.isArray(header) ? header.join(',') : header;
    };
    const onUnexpectedResponse = (request: ClientRequest, response: IncomingMessage) => {
      socket.off('open', onOpen);
      // When a listener exists, `ws` emits this instead of `error` and leaves cleanup to the
      // listener; without these calls the aborted upgrade's socket is not released.
      request.destroy();
      response.destroy();
      // `response.statusCode` is the only place `ws` provides the status as a number. The `error`
      // message is text whose wording varies and may not contain "400", so a refusal from another
      // implementation would otherwise surface as a generic connection failure instead of the
      // `CodecMismatch` WSM-CDC-024 requires.
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
      // An acceptor that answered 101 without selecting our entry also refused the codec. `ws` aborts
      // this case with `Server sent no subprotocol`, which contains no status, so the fallback below
      // does not match it, and the WSM-CDC-028 check after this promise does not run because no
      // socket opens. It is detected here so it surfaces as `CodecMismatch` (WSM-CDC-024).
      const refusedAtUpgrade = upgraded && negotiated !== wanted;
      // Fallback for a `ws` version that reports the status without emitting `unexpected-response`.
      // The match is on `ws`'s full sentence, not the bare number, because `400` also appears in
      // `connect ECONNREFUSED 127.0.0.1:400` (no listener on port 400), which is not a refusal.
      const refusedByStatus = new RegExp(`unexpected server response: ${REFUSED}\\b`, 'i').test(error.message);
      reject(refusedAtUpgrade || refusedByStatus ? mismatchError(codecName) : error);
    };
    const onOpen = () => {
      socket.off('error', onError);
      socket.off('upgrade', onUpgrade);
      socket.off('unexpected-response', onUnexpectedResponse);
      // Constructed inside the handler, not after the await: the `WsSocket` constructor attaches the
      // permanent `error` listener, and an `error` emitted between this handler and the await
      // continuation would otherwise be unhandled.
      resolve(new WsSocket(socket));
    };
    socket.once('open', onOpen);
    socket.once('upgrade', onUpgrade);
    // Stays attached after `onUnexpectedResponse` rejects: destroying the request makes `ws` emit
    // another `error`, and an `error` event without a listener terminates the node process.
    socket.once('error', onError);
    socket.once('unexpected-response', onUnexpectedResponse);
  });

  if (socket.protocol !== wanted) {
    // The handshake completed with a different subprotocol or none: close with the
    // policy-violation code and throw `CodecMismatch` (WSM-CDC-028).
    logNegotiatedMismatch(socket.protocol, wanted, POLICY_VIOLATION);
    adapter.close(POLICY_VIOLATION, 'muxws subprotocol mismatch');
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
  /** Registered on the peer, in order, as `peer.interceptOpen()` would (WSM-API-026). */
  openInterceptors?: readonly OpenInterceptor[];
  /** Registered on the peer, in order, as `peer.interceptReplyHeaders()` would (WSM-API-026). */
  replyHeadersInterceptors?: readonly ReplyHeadersInterceptor[];
  /** Registered on the peer, in order, as `peer.interceptIncomingHeaders()` would (WSM-API-026). */
  incomingHeadersInterceptors?: readonly IncomingHeadersInterceptor[];
  /** Exact origins, `SameOrigin`, or a function of the `Origin` header value. Requires `request`. */
  allowedOrigins?: Iterable<string> | SameOrigin | ((origin: string | undefined) => boolean);
  /** The upgrade request `ws` passed with the connection; read for its `Origin` and host headers. */
  request?: IncomingMessage;
}

/**
 * The subprotocol selection hook for a `ws` server: returns the subprotocol to echo back
 * (WSM-CDC-022/027).
 *
 * `ws` calls `handleProtocols(protocols, request)` with `protocols` as a `Set`. Returning `false`
 * selects nothing but does not refuse the upgrade: `ws` still answers 101, without a
 * `Sec-WebSocket-Protocol` header. `refuseMismatchedUpgrade` performs the refusal; a server that
 * installs only this hook violates WSM-CDC-022.
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
 * Make a `ws` server answer HTTP 400 to an upgrade offering an unsupported codec.
 *
 * `handleProtocols` only selects a value; whatever it returns, `ws` completes the handshake with
 * 101, and the mismatch would be detected only on the open socket, which WSM-CDC-022 forbids where
 * the transport allows a refusal. `shouldHandle` can abort an upgrade with a status (`ws` 8
 * deprecates the alternative, `verifyClient`), so an acceptor installs both hooks:
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
 * `ws` passes the connection after the handshake, so the subprotocol is verified on the open socket;
 * on a mismatch one error line naming the negotiated and the expected subprotocol is logged
 * (WSM-CDC-029), the socket is closed with the policy-violation code and `CodecMismatch` is thrown
 * (WSM-CDC-028).
 *
 * With `allowedOrigins`, the `Origin` header of `request` is checked first; a refused origin closes
 * the socket with the policy-violation code and throws `OriginRefused`.
 */
export async function accept(socket: NodeWebSocket, options: AcceptOptions = {}): Promise<Peer> {
  const codec = options.codec ?? getCodec(settings.codec);
  const check = compilePolicy(options.allowedOrigins);
  if (check !== null) {
    if (options.request === undefined) {
      // The check cannot run, and the socket is not used unchecked.
      socket.close(POLICY_VIOLATION, 'origin unknown');
      throw new ProtocolError(
        'allowedOrigins needs the upgrade request: pass the `request` ws emitted with the connection',
      );
    }
    const distinct = options.request.headersDistinct;
    if (!check((name) => distinct[name] ?? [])) {
      const values = distinct.origin ?? [];
      const origin = values.length > 0 ? values.join(', ') : undefined;
      socket.close(POLICY_VIOLATION, 'origin not allowed');
      throw new OriginRefused(`origin ${JSON.stringify(origin ?? null)} is not allowed to open this endpoint`, {
        origin,
      });
    }
  }
  const negotiated = socket.protocol;
  const wanted = `${PREFIX}${codec.name}`;

  if (negotiated !== wanted) {
    logNegotiatedMismatch(negotiated, wanted, POLICY_VIOLATION);
    socket.close(POLICY_VIOLATION, 'codec mismatch');
    throw mismatchError(codec.name);
  }

  const peer = new Peer(new WsSocket(socket), {
    codec,
    isDialer: false,
    errorSerializer: options.errorSerializer,
    maxFrameBytes: options.maxFrameBytes,
    // The two local limits (WSM-FRG-035, WSM-STM-036). Neither is sent to the remote.
    maxPayloadBytes: options.maxPayloadBytes,
    maxConcurrentStreams: options.maxConcurrentStreams,
  });
  registerInterceptors(peer, options);
  return peer;
}

/**
 * Accept, register `handler`, and run the read loop until the socket closes.
 *
 * On `CodecMismatch` and `OriginRefused` it resolves instead of rejecting, as `muxws.serve()` does in
 * Python. `accept()` has closed a refused origin's socket with 1008. A server with
 * `refuseMismatchedUpgrade` has already answered such an upgrade with HTTP 400 (WSM-CDC-022), and
 * `handleProtocols` has logged both codec names (WSM-CDC-029); on a server with neither hook,
 * `accept()` has logged the negotiated and the expected subprotocol. A rejection from a `ws` connection
 * handler would be an unhandled rejection, which terminates a Node process by default.
 */
export async function serve(socket: NodeWebSocket, options: AcceptOptions & { handler: StreamHandler }): Promise<void> {
  const { handler, ...rest } = options;
  let peer;
  try {
    peer = await accept(socket, rest);
  } catch (error) {
    if (error instanceof CodecMismatch || error instanceof OriginRefused) return;
    throw error;
  }
  peer.onStream(handler);
  await peer.serve();
}
