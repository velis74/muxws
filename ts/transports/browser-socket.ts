/**
 * `SocketAdapter` over the platform's global `WebSocket` (WSM-API-021, WSM-CDC-020/024/028).
 *
 * There is no Python counterpart. It is the only module in the browser entry point that uses
 * `WebSocket`, and it has no dependencies.
 *
 * The socket delivers messages as events and the peer's read loop pulls them, so inbound messages are
 * queued here and `receive()` takes from the queue. When the socket closes, pending `receive()` calls
 * reject with `ConnectionClosed`, which signals the peer that the connection is lost.
 */

import { CodecMismatch, ConnectionClosed, ProtocolError, TransportUnsupportedError } from '../errors';
import { logger } from '../observability';
import { PREFIX, mismatchError, offer } from '../subprotocol';

import type { SocketAdapter } from './index';

/** `WebSocket.readyState` values, defined here so a test double need not define the constants. */
const OPEN = 1;
const CLOSED = 3;

/**
 * The close code used when a subprotocol mismatch is detected on an already-open socket, because the
 * transport offers no handshake hook (WSM-CDC-028, D1). Mirrors `websockets_.POLICY_VIOLATION`.
 */
export const POLICY_VIOLATION = 1008;

/**
 * Thrown for a `ws+unix:` url passed to the browser entry point, which has no filesystem socket
 * transport (WSM-ERR-016).
 *
 * The only concrete `TransportUnsupportedError` of the platform-`WebSocket` transport. It is defined in
 * this module and re-exported by `ts/index.ts` (WSM-ERR-016 placement): consumers import it from
 * `'muxws'`. `muxws/node` does not export it, because `muxws/node` can dial unix sockets.
 *
 * It has the same name as `muxws.transports.unix.UnixSocketsUnsupportedError`; both mean that the url
 * names a socket file that cannot be dialled from this environment. In Python the cause is an
 * interpreter without `AF_UNIX`; here it is the browser entry point, which excludes `ws`
 * (WSM-API-022). It is not a `TransportUrlError`, because the url is valid and `muxws/node` dials it.
 *
 * It is thrown by `refuseUnixScheme` in `ts/index.ts`, not by `BrowserSocket.connect`, because the
 * scheme is checked before `connect()` resolves the codec.
 */
export class UnixSocketsUnsupportedError extends TransportUnsupportedError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'UnixSocketsUnsupportedError';
  }
}

/** One WebSocket message: a frame encoded by the codec. */
type Message = string | ArrayBuffer;

export interface BrowserSocketOptions {
  /**
   * Application subprotocol entries, appended **after** the muxws one (WSM-CDC-020/021).
   *
   * For example a bearer token. The acceptor ignores them.
   */
  subprotocols?: readonly string[];
}

interface Waiter {
  resolve: (message: Message) => void;
  reject: (error: unknown) => void;
}

/**
 * The error for a socket that closed before opening.
 *
 * A refused handshake and an unreachable server produce the same events in a browser: `error`, then
 * `close` with code 1006 and no reason; the HTTP status and body are not exposed to JavaScript. The
 * message therefore names both possible causes, an unreachable server first (for example a dev-server
 * proxy pointing at a port with no listener) and a codec mismatch second.
 *
 * The class is `CodecMismatch`, because WSM-CDC-024 requires a refused handshake to surface as
 * `CodecMismatch` rather than as a generic connection failure, with the diagnostic composed by the
 * dialer.
 */
function unopenedError(configured: string, url: string): CodecMismatch {
  return new CodecMismatch(
    `the muxws socket to ${url} never opened. A browser is not shown the HTTP status, so this is ` +
      'either of two things and muxws cannot tell them apart: nothing is listening at that address ' +
      '(check the server is running, and that a dev-server proxy points at the port it is actually ' +
      `on), or the acceptor refused the handshake because it speaks a codec other than '${configured}' ` +
      '(VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the server; muxws asserts the codec at the ' +
      'handshake and never falls back, WSM-CDC-022/024).',
    { configured },
  );
}

/** `SocketAdapter` over one platform `WebSocket`. */
export class BrowserSocket implements SocketAdapter {
  readonly socket: WebSocket;

  private readonly queue: Message[] = [];
  private readonly waiting: Waiter[] = [];
  private readonly closedPromise: Promise<void>;
  private resolveClosed!: () => void;
  private failure: Error | null = null;
  private closed = false;

  /**
   * Wrap an already-constructed socket. Applications use `connect()`; tests pass a test double to
   * this constructor.
   *
   * Listeners are attached in the constructor, before the handshake completes, so a message that
   * arrives before the peer's read loop starts is queued.
   */
  constructor(socket: WebSocket) {
    this.socket = socket;
    socket.binaryType = 'arraybuffer';
    this.closedPromise = new Promise<void>((resolve) => {
      this.resolveClosed = resolve;
    });
    socket.addEventListener('message', (event) => {
      this.onMessage(event.data);
    });
    socket.addEventListener('close', (event) => {
      this.settleClosed(event.code, event.reason, event.wasClean);
    });
  }

  /**
   * Dial `url`, offering `muxws.v1.<codecName>` first (WSM-CDC-020), and resolve once the
   * handshake is complete and the negotiated subprotocol is exactly that value.
   *
   * A refused handshake reaches the browser as a generic error with no body, so the `CodecMismatch`
   * is composed here from the offered codec name and names both environment variables
   * (WSM-CDC-024). If the handshake completes with a different subprotocol or none, the socket is
   * closed with the policy-violation code and `CodecMismatch` is thrown (WSM-CDC-028).
   */
  static async connect(url: string, codecName: string, options: BrowserSocketOptions = {}): Promise<BrowserSocket> {
    const adapter = new BrowserSocket(new WebSocket(url, offer(codecName, options.subprotocols)));
    await adapter.waitOpen(codecName);

    const wanted = `${PREFIX}${codecName}`;
    if (adapter.socket.protocol !== wanted) {
      // The same line `websockets_.verify_negotiated` logs to `muxws.transport`. It is logged in
      // addition to the thrown error, so it is visible even if the caller discards the error, and it
      // goes through `logger` so `logger.level` controls it.
      logger.error(
        `muxws negotiated subprotocol is '${adapter.socket.protocol}', expected '${wanted}'; ` +
          `closing with ${POLICY_VIOLATION}`,
      );
      await adapter.close(POLICY_VIOLATION, 'muxws subprotocol mismatch');
      throw mismatchError(codecName);
    }
    return adapter;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  sendText(text: string): void {
    this.assertOpen();
    this.socket.send(text);
  }

  sendBytes(bytes: ArrayBuffer): void {
    this.assertOpen();
    this.socket.send(bytes);
  }

  receive(): Promise<Message> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.failure !== null) return Promise.reject(this.failure);
    return new Promise<Message>((resolve, reject) => {
      this.waiting.push({ resolve, reject });
    });
  }

  /** Close the socket and resolve once it is actually closed. Idempotent. */
  async close(code = 1000, reason = ''): Promise<void> {
    if (this.socket.readyState === CLOSED) {
      // No close event follows, so `wasClean` is derived from the code as in `websockets_`
      // (`_was_clean`): 1000 is clean, every other code is not.
      this.settleClosed(code, reason, code === 1000);
      return;
    }
    try {
      this.socket.close(code, reason);
    } catch {
      // Already closing, or a double close. The close event still settles everything below.
    }
    await this.closedPromise;
  }

  private waitOpen(codecName: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.socket.readyState === OPEN) {
        resolve();
        return;
      }
      const done = (settle: () => void) => () => {
        this.socket.removeEventListener('open', onOpen);
        this.socket.removeEventListener('error', onFail);
        this.socket.removeEventListener('close', onFail);
        settle();
      };
      const onOpen = done(resolve);
      const onFail = done(() => {
        reject(unopenedError(codecName, this.socket.url));
      });
      this.socket.addEventListener('open', onOpen);
      this.socket.addEventListener('error', onFail);
      this.socket.addEventListener('close', onFail);
    });
  }

  private assertOpen(): void {
    if (this.closed || this.socket.readyState !== OPEN) {
      throw this.failure ?? new ConnectionClosed('the socket is not open', { code: 1006 });
    }
  }

  private onMessage(data: unknown): void {
    const message = asMessage(data);
    if (message === null) {
      this.fail(
        new ProtocolError(
          `the socket delivered a ${describe(data)} message; muxws sets binaryType='arraybuffer', ` +
            'so an inbound message is a string or an ArrayBuffer and nothing else',
        ),
      );
      return;
    }
    const waiter = this.waiting.shift();
    if (waiter !== undefined) {
      waiter.resolve(message);
      return;
    }
    this.queue.push(message);
  }

  private settleClosed(code: number, reason: string, wasClean: boolean): void {
    if (this.closed) return;
    this.closed = true;
    this.fail(
      new ConnectionClosed(reason !== '' ? reason : `the socket closed with code ${code}`, { code, reason, wasClean }),
    );
    this.resolveClosed();
  }

  /** Record the first terminal error and reject every pending `receive()` with it. */
  private fail(error: Error): void {
    this.failure ??= error;
    this.waiting.splice(0).forEach((waiter) => {
      waiter.reject(this.failure);
    });
  }
}

function asMessage(data: unknown): Message | null {
  if (typeof data === 'string') return data;
  if (data instanceof ArrayBuffer) return data;
  // A view is copied, not unwrapped: its buffer may be longer than the message, or may be a
  // SharedArrayBuffer, which is not an ArrayBuffer.
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice().buffer;
  return null;
}

function describe(data: unknown): string {
  if (data === null) return 'null';
  if (typeof data === 'object') return data.constructor?.name ?? 'object';
  return typeof data;
}
