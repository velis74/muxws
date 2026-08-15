/**
 * `SocketAdapter` over the platform's global `WebSocket` (WSM-API-021, WSM-CDC-020/024/028).
 *
 * There is no Python twin: this is the browser half of the seam. It is the only file in the browser
 * entry point that knows what a WebSocket is, and it pulls in no dependency to do it.
 *
 * The socket is a push source and the peer's read loop is a pull loop, so inbound messages queue
 * here and `receive()` drains that queue. A pending `receive()` is rejected when the socket closes,
 * which is how the peer learns the connection died.
 */

import { CodecMismatch, ConnectionClosed, ProtocolError, TransportUnsupportedError } from '../errors';
import { logger } from '../observability';
import { PREFIX, mismatchError, offer } from '../subprotocol';

import type { SocketAdapter } from './index';

/** `WebSocket.readyState` values, spelled out rather than read off the instance so a test double
 *  need not carry them. */
const OPEN = 1;
const CLOSED = 3;

/**
 * The close code for a transport that offers no handshake hook at all, so the mismatch can only be
 * discovered on an already-open socket (WSM-CDC-028, D1). Mirrors `websockets_.POLICY_VIOLATION`.
 */
export const POLICY_VIOLATION = 1008;

/**
 * A `ws+unix:` url handed to the transport that has no filesystem to reach (WSM-ERR-016).
 *
 * The concrete `TransportUnsupportedError` of the platform-`WebSocket` transport, and the only one it
 * has. It is defined here, in the module that owns the dial, and re-exported by `ts/index.ts`, which
 * is WSM-ERR-016's placement clause: `import { UnixSocketsUnsupportedError } from 'muxws'` is what a
 * consumer writes, and `muxws/node` does not carry the name at all - that subpath's unix dial works.
 *
 * It shares the name of `muxws.transports.unix.UnixSocketsUnsupportedError`: the sentence to the
 * reader is the same one - *this url names a socket file and cannot be dialled from here* - and only
 * the reason underneath differs. Python has no `AF_UNIX` on the interpreter it is running on; this
 * build has no filesystem transport at all, because WSM-API-022 keeps `ws` out of anything a browser
 * can load. Both are permanent local conditions no retyped url can fix, which is why this is not a
 * `TransportUrlError`: the url is correct, and `muxws/node` dials it.
 *
 * The throw site is `refuseUnixScheme` in `ts/index.ts` rather than a method here, because the scheme
 * has to be refused *before* `connect()` resolves a codec: this class is about a url that will never
 * become a socket, and `BrowserSocket.connect` is only ever handed urls that might.
 */
export class UnixSocketsUnsupportedError extends TransportUnsupportedError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message, options);
    this.name = 'UnixSocketsUnsupportedError';
  }
}

/** What travels on the wire once the codec has encoded a frame. */
type Message = string | ArrayBuffer;

export interface BrowserSocketOptions {
  /**
   * Application subprotocol entries, appended **after** the muxws one (WSM-CDC-020/021).
   *
   * A bearer token is the common case. The acceptor ignores every one of them.
   */
  subprotocols?: readonly string[];
}

interface Waiter {
  resolve: (message: Message) => void;
  reject: (error: unknown) => void;
}

/**
 * The socket never opened, and a browser cannot say why.
 *
 * A refused handshake and an unreachable server are **the same events** here: `error` then `close`
 * with code 1006 and no reason, because the HTTP status and body are not exposed to JavaScript. A
 * message asserting that the acceptor refused the codec would name one of the two possibilities and
 * read as a diagnosis - against a dev-server proxy pointing at a port nothing serves, it sends the
 * reader looking at codec configuration that was never wrong. This one names both causes and puts the
 * reachable one first, which is the one a reader can check in a second.
 *
 * `CodecMismatch` is still the class: WSM-CDC-024 requires a refused handshake to surface it and
 * forbids a bare connection failure, and this is exactly the case the rule was written for - the
 * dialer composes the diagnostic itself *because* the browser cannot read the rejection.
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

/** One browser WebSocket, seen the only way the peer is allowed to see it. */
export class BrowserSocket implements SocketAdapter {
  readonly socket: WebSocket;

  private readonly queue: Message[] = [];
  private readonly waiting: Waiter[] = [];
  private readonly closedPromise: Promise<void>;
  private resolveClosed!: () => void;
  private failure: Error | null = null;
  private closed = false;

  /**
   * Wrap an already-constructed socket. `connect()` is the usual entry point; this constructor is
   * what a test double is handed to.
   *
   * Listeners are attached here rather than after the handshake: a message that arrives before the
   * peer's read loop starts must be queued, not dropped.
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
   * is composed here from the codec name we offered, naming both environment variables
   * (WSM-CDC-024). A server that completed the handshake having negotiated something else - or
   * nothing - is the same failure caught one step later, and the socket is closed with the
   * policy-violation code (WSM-CDC-028).
   */
  static async connect(url: string, codecName: string, options: BrowserSocketOptions = {}): Promise<BrowserSocket> {
    const adapter = new BrowserSocket(new WebSocket(url, offer(codecName, options.subprotocols)));
    await adapter.waitOpen(codecName);

    const wanted = `${PREFIX}${codecName}`;
    if (adapter.socket.protocol !== wanted) {
      // The same line `websockets_.verify_negotiated` logs: the throw below reaches the caller, this
      // reaches whoever is reading the console when the caller swallowed it. Through the logger, for
      // the reason `subprotocol.ts` gives - the Python twin logs it to `muxws.transport` and is
      // therefore silenceable, and a port that mirrors the message but not the control is not a port.
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
      // No close event is coming, so the outcome is judged from the code the way `websockets_`
      // judges it (`_was_clean`): 1000 is clean and every other code is not.
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

  /** Record why no further message will arrive and hand it to everyone already waiting. */
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
  // A view is copied rather than unwrapped: its buffer may be longer than the message, and may be
  // a SharedArrayBuffer, which is not an ArrayBuffer.
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice().buffer;
  return null;
}

function describe(data: unknown): string {
  if (data === null) return 'null';
  if (typeof data === 'object') return data.constructor?.name ?? 'object';
  return typeof data;
}
