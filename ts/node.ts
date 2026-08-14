/**
 * Subpath export `muxws/node`: the acceptor side, over the `ws` package.
 *
 * The peer implementation is shared with the browser entry point; only the socket adapter differs
 * (WSM-API-022). `ws` is an optional peer dependency and is reached only from here.
 *
 * It is also the only entry point that can dial a unix-domain socket - see `unixTarget` for the url
 * grammar and `dialWs` for what is done with it. `ts/index.ts` refuses `ws+unix:` outright and names
 * this module in the error, because a browser and node's global `WebSocket` both parse the scheme and
 * neither can open a file.
 */

import type { ClientRequest, IncomingMessage } from 'node:http';
import type { NetConnectOpts, Socket } from 'node:net';

import type { ClientOptions, WebSocket as NodeWebSocket, WebSocketServer } from 'ws';

import { type Codec, getCodec } from './codec';
import { settings } from './conf';
import { CodecMismatch, TransportUnsupportedError, TransportUrlError } from './errors';
import { type ErrorSerializer, Peer, type StreamHandler } from './peer';
import { type ConnectOptions, dialAndEstablish, type Dial } from './reconnect';
import { mismatchError, offer, PREFIX, select } from './subprotocol';
import { WsSocket } from './transports/ws-socket';

/**
 * What an acceptor answers a codec it does not speak (WSM-CDC-022), and the only status either half
 * of this module reads as a refused muxws handshake.
 */
const REFUSED = 400;

/** The scheme `unixTarget` owns, as `new URL()` normalises it - lowercased, colon included. */
const UNIX_SCHEME = 'ws+unix:';

/** The scheme that looks like it should exist and does not; refused by name rather than by silence. */
const TLS_UNIX_SCHEME = 'wss+unix:';

/** What goes in the `Host:` header when the url has no authority, which is the normal shape. */
const DEFAULT_AUTHORITY = 'localhost';

/**
 * The schemes `ws`'s own constructor accepts. Read only to tell a url failure from any other failure.
 *
 * `ws` throws a `SyntaxError` for a url it cannot parse *and* for a subprotocol token it does not
 * like, with no field distinguishing them - measured: `new WebSocket('ws://h/x', ['bad protocol'])`
 * fails with `SyntaxError: An invalid or duplicated subprotocol was specified`, the same class a
 * malformed url produces. A blanket `catch` around the constructor would therefore report a bad
 * subprotocol as a bad address, so the translation asks this list first.
 */
const WS_SCHEMES = ['ws:', 'wss:', 'http:', 'https:', UNIX_SCHEME];

/**
 * An address `muxws/node` cannot open because `ws` cannot parse it (WSM-ERR-016).
 *
 * Named after the third-party package that owns this dial, the way Python's twin is named after
 * `websockets`. It frames `ws`'s own wording rather than replacing it - `SyntaxError: Invalid URL:
 * nonsense` is a better sentence than anything this module could compose about a string it also
 * failed to read - and chains the original as `cause` so a debugger keeps the stack that produced it.
 *
 * What it deliberately does not cover is a dial that failed: `ENOTFOUND`, `ECONNREFUSED` and a 503
 * from a proxy are not url errors and must reach the caller untouched, because the remedy for each of
 * them is somewhere other than the address bar.
 */
export class WsUrlError extends TransportUrlError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'WsUrlError';
  }
}

/**
 * A `ws+unix:` url this port refuses to turn into a request (WSM-ERR-016).
 *
 * The twin of `muxws.transports.unix.UnixUrlError`, refusal for refusal: a request target that is not
 * an absolute path, a url naming no socket file, and `wss+unix:`. The three are checked here rather
 * than left to `ws` because the `ws+unix:` grammar is this library's - `unixTarget` already splits it
 * differently from `ws` for exactly that reason - and because two of the three would otherwise arrive
 * as a `ws`-shaped `SyntaxError` for a scheme muxws documents and `ws` merely tolerates.
 *
 * A `TransportUrlError` and not a `TransportUnsupportedError`: every one of these is fixed by
 * retyping the url, and the transport itself is present and working.
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
 * Without it the failure is `Error: Cannot find package 'ws' imported from …/node_modules/muxws/dist/
 * node.js` with `code: 'ERR_MODULE_NOT_FOUND'` - measured. That names a file the reader did not write
 * and does not name the one command that fixes it, which is why WSM-ERR-016 requires the class to
 * state the install. GAPS.md records the same shape one layer up: a uvicorn install with no WebSocket
 * implementation answered 404 to every upgrade and was read as a muxws defect.
 *
 * Named `NotInstalled` rather than `Unavailable` deliberately, and matching Python's
 * `WebsocketsNotInstalledError`: "unavailable" in a traceback out of a dial reads as *the endpoint was
 * unreachable*, a transient condition a caller may retry. This one is permanent until somebody runs
 * `npm install`, and the name has to say which of the two it is.
 *
 * Only `ERR_MODULE_NOT_FOUND` becomes this class. A broken install, or a syntax error inside `ws`
 * itself, is rethrown untouched - telling that reader to install a package they already have would be
 * a confident wrong answer, and the original message is the only thing that names the real fault.
 */
export class WsNotInstalledError extends TransportUnsupportedError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'WsNotInstalledError';
  }
}

/** A `ws+unix:` url taken apart: the file to open, and the `ws://` url the handshake claims to be for. */
interface UnixTarget {
  /** The filesystem path of the listening socket. `sun_path` caps it at about 108 bytes on Linux. */
  socketPath: string;
  /** `ws://<authority><target>` - what goes on the wire as the request line and the `Host` header. */
  uri: string;
}

/**
 * The `ws+unix:` grammar: split the path on the **first** colon, file in front, request target behind.
 *
 * `ws` has parsed this scheme for years and this function exists anyway, which needs justifying. `ws`
 * runs `opts.path.split(':')` and keeps `parts[1]`, so it splits on **every** colon:
 * `ws+unix:///a.sock:/ws:v2` asks for `/ws`, and `.../a.sock:/ws?since=2026-08-14T10:00:00Z` asks for
 * `/ws?since=2026-08-14T10`, both silently. `muxws/transports/unix.py` splits on the first colon and
 * keeps the remainder whole. Leaving that alone would have meant one url naming two different request
 * targets depending on which port read it, with no error on either side and nothing for a deployment
 * to notice - the acceptor routes on the target and the dialer never learns it was truncated. So the
 * split happens here, before `ws` sees anything, and `ws` is handed an ordinary `ws:` url plus a
 * `createConnection` that opens the file. It is not a second url parser: `ws`'s own `isIpcUrl` branch
 * is simply never reached, and everything else about the dial - the offer, the events, the 400 - is
 * `ws`'s exactly as it is over TCP.
 *
 * `null` for every other url, and for anything `new URL()` cannot read at all: this runs on **every**
 * dial, so it is the branch and not a validator. A url that is not `ws+unix:`-shaped must keep
 * reaching `ws`, whose own diagnostic - framed as a `WsUrlError` - is better than "not a ws+unix: url"
 * would be.
 *
 * The three refusals below are the exception, and they are the three `parse_unix_url` refuses in
 * Python (WSM-ERR-016 requires each to be a `MuxwsError`, and this grammar's owner is this module):
 * a request target that is not absolute, a url naming no socket file, and `wss+unix:`. Two of them
 * were previously delegated to `ws` and came back as a `SyntaxError` about *its* scheme list, which
 * left the two ports disagreeing about who owns a scheme muxws documents. The refusals happen before
 * `ws` is imported and before anything is opened, so a `ws+unix:` typo is still a `UnixUrlError` on a
 * machine where `ws` is not installed - which is correct, since installing it would not make the url
 * dialable.
 */
function unixTarget(url: string): UnixTarget | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (parsed.protocol === TLS_UNIX_SCHEME) {
    // There is no TLS to negotiate over a socket file - the filesystem permissions on the file are
    // the access control - so the scheme does not exist in either port. `ws` would refuse it too, by
    // listing the schemes it takes, which tells the reader nothing about muxws having a unix dial at
    // all and reads as though `ws+unix:` were `ws`'s feature rather than this library's grammar.
    throw new UnixUrlError(
      `cannot dial '${url}': wss+unix: is not a scheme. A filesystem socket has no TLS to negotiate - ` +
        'the permissions on the socket file are the access control - so use ws+unix: instead.',
    );
  }
  if (parsed.protocol !== UNIX_SCHEME) return null;

  // The query travels with the request target, not with the file, so it has to be put back before the
  // split: `ws+unix:///p.sock:/r?a=1` opens `/p.sock` and asks for `/r?a=1`. This is the same
  // `pathname + search` `ws` splits, and the same string `urlsplit` hands the Python port.
  const whole = parsed.pathname + parsed.search;
  const colon = whole.indexOf(':');
  const socketPath = colon === -1 ? whole : whole.slice(0, colon);
  const route = colon === -1 ? '' : whole.slice(colon + 1);

  // A url with no path at all names no file, and there is nothing to dial. `ws` would answer "The
  // URL's pathname is empty", which is true of the url and silent about what a ws+unix: url is
  // supposed to contain; the reader who typed `ws+unix://run/app.sock` - an authority, no path -
  // needs the shape, not the parser's verdict.
  if (socketPath === '') {
    throw new UnixUrlError(
      `the url '${url}' names no socket file: a ws+unix: url is ` +
        "'ws+unix:///absolute/path.sock:/request-target', and everything before the first ':' of the " +
        'path is the file to open. An authority with no path names nothing that can be opened.',
    );
  }
  if (route !== '' && !route.startsWith('/')) {
    // `ws://localhost` + `ws` is `ws://localhostws`: a *valid* url naming a host nothing resolves,
    // so the target would be folded into the authority and the dial would reach the right file
    // carrying `/` as its target and a `Host` nobody asked for. Against an acceptor that does not
    // route - `unix_serve`, or a `WebSocketServer` with no `path` - that handshake **succeeds**,
    // which is the worst of the available outcomes. Refused here for the reason
    // `muxws/transports/unix.py` refuses it: a typo in a url must not become a working connection to
    // the wrong request, nor - via node's own 400 for a malformed request line - a `CodecMismatch`
    // sending the reader off to check `MUXWS_CODEC` on two ends that agree.
    throw new UnixUrlError(
      `the request target in '${url}' must begin with '/': the part after the ':' is an HTTP ` +
        'request target, not a path relative to anything',
    );
  }

  const authority = parsed.host === '' ? DEFAULT_AUTHORITY : parsed.host;
  return { socketPath, uri: `ws://${authority}${route === '' ? '/' : route}` };
}

/**
 * The one line of `ws`'s `isIpcUrl` branch that is still wanted: open the file instead of a port.
 *
 * `node:net` is imported inside the function rather than at module scope, for the reason `ws` is: an
 * acceptor that never dials, and every TCP dial, should pay nothing for a module only a unix dial
 * needs. The options object is the http agent's own, spread through untouched apart from `path`, so
 * `timeout` and anything else node put in still applies - `net.connect` reads `path` as an IPC
 * endpoint and ignores the host and port beside it.
 */
async function unixConnector(socketPath: string): Promise<(options: NetConnectOpts) => Socket> {
  const { connect } = await import('node:net');
  return (options) => connect({ ...options, path: socketPath });
}

/**
 * `import('ws')`, with the one failure that has a remedy translated into a class that states it.
 *
 * WSM-ERR-016: a missing optional dependency must not reach the caller as a module-resolution error,
 * and the message must name the install. The check is here rather than at module scope because the
 * import is dynamic on purpose - `accept()`, `serve()`, `handleProtocols()` and
 * `refuseMismatchedUpgrade()` all keep working in a process that never dials, which is what lets a
 * pure acceptor run without the package at all. Guarding the import at module scope would break that
 * and would also make `catch (e) { e instanceof WsNotInstalledError }` throw the very error it exists
 * to replace, because importing `muxws/node` would already have failed.
 */
// The return type is inferred rather than written as `Promise<typeof import('ws')>`: under this
// project's `module: commonjs` the two are genuinely different types - the written one is `ws`'s
// `export =` shape, the inferred one is the namespace a dynamic import produces - and spelling it
// wrong is a type error about `EventEmitter` statics that says nothing about this function.
async function requireWs() {
  try {
    return await import('ws');
  } catch (error) {
    // Narrowed to the resolution failure. Anything else - a corrupt install, a `ws` that throws while
    // evaluating - keeps its own diagnostic, because "run npm install ws" would be a confident wrong
    // answer to a reader who has it installed already.
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
 * Frame a `ws` constructor throw as a `WsUrlError`, or leave it alone.
 *
 * The hybrid is forced by measurement: `ws` throws a `SyntaxError` both for `Invalid URL: nonsense`
 * and for `An invalid or duplicated subprotocol was specified`, and nothing on either object tells
 * them apart. So the address is re-read here - if `new URL()` cannot parse `dialed`, or parses it to
 * a scheme `ws` does not take, the throw was about the url; otherwise it was about something else and
 * is rethrown untouched. A blanket translation would label a bad subprotocol a bad address and send
 * the reader to fix a url that was already correct.
 *
 * `dialed` is what `ws` was handed and `url` is what the caller typed; for a `ws+unix:` url the two
 * differ, and the message must quote the one the reader can find in their own source.
 */
function translateUrlThrow(url: string, dialed: string, error: unknown): never {
  let parsed: URL | null = null;
  try {
    parsed = new URL(dialed);
  } catch {
    parsed = null;
  }
  if (parsed !== null && WS_SCHEMES.includes(parsed.protocol)) throw error;
  // `ws`'s own sentence is kept whole rather than paraphrased (WSM-ERR-016): it names the offending
  // text, and this frame only says who was asked to dial it.
  throw new WsUrlError(`cannot dial '${url}': ${(error as Error).message}`, { cause: error });
}

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

// `UnixUrlError`, `WsUrlError` and `WsNotInstalledError` are exported above, where they are defined,
// and from **here only**. They are the concrete transport errors of the two transports this subpath
// ships, and WSM-ERR-016 keeps them off the package root: `ts/errors.ts` holds the two bases so that a
// browser build can write `instanceof TransportUrlError` without importing a module that reaches for
// `ws` (WSM-API-022), and a third party writing an adapter - who cannot add a class to `ts/errors.ts`
// - follows the same convention from its own package. Both bases come from `muxws`, not from here.

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

/**
 * The dial itself, and the whole of what `ws` is imported for.
 *
 * `url` may be `ws:`, `wss:` or **`ws+unix:///absolute/path.sock:/route`**, and this function does
 * not branch on which: `ws` reads the scheme, and for the unix form sets `socketPath` on an otherwise
 * ordinary `http.ClientRequest` instead of a host and a port. The handshake on the wire is byte for
 * byte the one TCP carries - an HTTP GET with `Upgrade: websocket` and `Sec-WebSocket-Protocol:
 * muxws.v1.<codec>`, answered 101 or 400 - so every branch below is reached over a unix socket
 * exactly as it is over TCP, WSM-CDC-022/024/028 included. `ts/unix.spec.ts` is the witness; nothing
 * here was written for it.
 *
 * The url grammar is `unixTarget`'s - the same first-colon split `muxws/transports/unix.py` performs,
 * so a socket path and a request target are spelled the same in both languages down to the byte. What
 * `ws` is given for the unix form is therefore a plain `ws://<authority><target>` url plus a
 * `createConnection` that opens the file, which is what `ws`'s own `isIpcUrl` branch would have built
 * one colon-split earlier. The authority is decorative - `ws+unix://` is normally followed straight by
 * an absolute path - but if one is given it becomes the `Host` header, and an empty one sends `Host:
 * localhost`, which is the value Python synthesises for the same url. `wss+unix:` does not exist and
 * never reaches `createConnection`: `unixTarget` refuses it as a `UnixUrlError` before `ws` is even
 * imported, which it did not always do - `ws` used to answer it by listing the schemes *it* takes,
 * a sentence that reads as though `ws+unix:` were `ws`'s feature rather than this library's grammar.
 *
 * `createConnection` rather than `socketPath` for one reason worth recording, because the option
 * exists and looks like the obvious way: `ws` overwrites `socketPath` with `undefined` immediately
 * after spreading the caller's options (`initAsClient`), so passing it does nothing and the dial
 * quietly goes to TCP port 80 instead - which on a developer's machine may well answer. `ws` leaves
 * `createConnection` alone, and uses it for every dial it makes, TCP included.
 *
 * One portability note that costs an afternoon when it bites: `sun_path` caps a unix socket path at
 * about 108 bytes on Linux (104 on macOS), and the failure is a bind or a connect error naming the
 * path rather than the length. Keep socket files in a short directory. There is no `process.platform`
 * guard here: `net.connect({ path })` is a named pipe on Windows rather than an error, so the dial is
 * meaningful there in a way it is not in Python - but no `ws+unix:` url can address a pipe, because
 * `new URL()` rejects the backslashes in `\\.\pipe\name` on every platform (`ts/unix.spec.ts` pins
 * it). A Windows dial of a POSIX-looking path therefore fails naming the path it tried, which is why
 * this port has nothing like Python's `UnixSocketsUnsupportedError` to raise. The browser entry point
 * does have one, for a different reason - see `UnixSocketsUnsupportedError` in `ts/index.ts`.
 *
 * Three things happen before a socket is opened, in this order, and the order is the rule
 * (WSM-ERR-016). The grammar first: `unixTarget` is `new URL()` and nothing else, so a malformed
 * `ws+unix:` url is a `UnixUrlError` even where `ws` is missing - installing it would not help. The
 * dependency second, because the url parser that judges the rest of the addresses is `ws`'s own, and a
 * reader with neither should be told about the one that blocks the other. The address last, framed as
 * a `WsUrlError`. All three are ahead of the refusal handling below, which is not stylistic: that
 * handler reads a 400 out of `ws`'s prose, and a url whose own text contains a status would otherwise
 * be reported as a `CodecMismatch` - a real, measured mistranslation in the Python port that sent a
 * reader off to compare `MUXWS_CODEC` across a connection that was never made.
 */
async function dialWs(url: string, codecName: string, options: NodeConnectOptions): Promise<WsSocket> {
  const unix = unixTarget(url);
  const { WebSocket } = await requireWs();
  // For a `ws+unix:` url this is the synthesised `ws://<authority><target>`; for everything else it
  // is the caller's own string, handed to `ws` unchanged.
  const dialed = unix === null ? url : unix.uri;
  const wsOptions: ClientOptions =
    unix === null
      ? { headers: options.headers }
      : {
          headers: options.headers,
          // The cast is the overload set: `net.connect` is declared three ways - options, port+host,
          // path - and only the first is the shape an http agent calls it with, so a single-signature
          // function is not assignable to the whole set without saying so.
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
