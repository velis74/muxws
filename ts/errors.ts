/**
 * Reset codes and the muxws exception hierarchy (WSM-ERR-002..009, §8.1-8.2).
 *
 * Mirrors `muxws/errors.py` class for class. Every class sets `name`, so a cross-language test can
 * assert on error identity as Python asserts on `__class__` (WSM-ERR-004).
 *
 * No concrete transport error is defined here. The two transport base classes at the end of this
 * file are defined here so an application can catch them without importing a transport;
 * `UnixUrlError`, `WsUrlError` and the other concrete classes are defined in the transport that
 * throws them and exported from the entry point that ships it (WSM-ERR-016). `ConnectionClosed` is
 * defined here because the `SocketAdapter` contract requires every adapter to throw it (WSM-API-021).
 */

/**
 * Numeric on the wire, named in both APIs; shared by `reset` and `goaway`.
 *
 * There are nine members. `5` is retired (WSM-STM-022): it MUST NOT be reused and MUST NOT appear
 * on the wire, so no name maps to it.
 */
export enum ResetCode {
  NO_ERROR = 0,
  CANCELLED = 1,
  APPLICATION_ERROR = 2,
  PROTOCOL_ERROR = 3,
  REFUSED = 4,
  // 5 is retired; see the doc comment above.
  TIMEOUT = 6,
  PAYLOAD_TOO_LARGE = 7,
  INTERNAL_ERROR = 8,
  CONNECTION_CLOSED = 9,
}

/** Root of every error this library raises. */
export class MuxwsError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'MuxwsError';
  }
}

/** This peer or the remote violated the specification. */
export class ProtocolError extends MuxwsError {
  constructor(message?: string) {
    super(message);
    this.name = 'ProtocolError';
  }
}

/** The socket died. Raised by `serve()` and peer-level calls, never by a stream (WSM-ERR-002). */
export class ConnectionClosed extends MuxwsError {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;

  constructor(message?: string, options: { code?: number; reason?: string; wasClean?: boolean } = {}) {
    const { code = 1006, reason = '', wasClean = false } = options;
    // An empty string falls through as well as `undefined`, as in Python's `message or reason or ...`.
    super(message || reason || 'connection closed');
    this.name = 'ConnectionClosed';
    this.code = code;
    this.reason = reason;
    this.wasClean = wasClean;
  }
}

/** `open()` was called after a `goaway` arrived. Thrown synchronously at the call site. */
export class ConnectionGoingAway extends MuxwsError {
  constructor(message?: string) {
    super(message);
    this.name = 'ConnectionGoingAway';
  }
}

/** A stream was awaited and iterated, or iterated twice (WSM-API-014). */
export class StreamAlreadyConsumed extends MuxwsError {
  constructor(message?: string) {
    super(message);
    this.name = 'StreamAlreadyConsumed';
  }
}

/**
 * `send()`/`end()`/`reply()` on a stream that closed **normally**.
 *
 * Not a `StreamReset` and not a `ProtocolError` (WSM-ERR-009): a `send()` that races a normal close
 * is an expected outcome, not a stream failure or a protocol violation.
 */
export class StreamClosed extends MuxwsError {
  constructor(message?: string) {
    super(message);
    this.name = 'StreamClosed';
  }
}

/** Configuration failure. Outside `StreamReset`: not a stream failure, not retryable. */
export class CodecError extends MuxwsError {
  readonly configured: string | null;
  readonly available: string[];

  constructor(message: string, options: { configured?: string; available?: string[] } = {}) {
    super(message);
    this.name = 'CodecError';
    this.configured = options.configured ?? null;
    this.available = options.available ?? [];
  }
}

/** The configured codec name was never registered. Thrown before any socket is opened. */
export class CodecNotRegistered extends CodecError {
  constructor(message: string, options: { configured?: string; available?: string[] } = {}) {
    super(message, options);
    this.name = 'CodecNotRegistered';
  }
}

/** The acceptor's codec differs from ours; the WebSocket handshake was rejected. */
export class CodecMismatch extends CodecError {
  constructor(message: string, options: { configured?: string; available?: string[] } = {}) {
    super(message, options);
    this.name = 'CodecMismatch';
  }
}

/**
 * The handshake's `Origin` is not allowed by `accept(socket, { allowedOrigins })`; the socket was
 * refused.
 *
 * `origin` is the request's `Origin` header value, several values joined with `', '`, or `undefined`
 * when the request carried none.
 */
export class OriginRefused extends MuxwsError {
  readonly origin: string | undefined;

  constructor(message: string, options: { origin?: string } = {}) {
    super(message);
    this.name = 'OriginRefused';
    this.origin = options.origin;
  }
}

/** A stream ended early. Carries the reset code, its reason and the stream id. */
export class StreamReset extends MuxwsError {
  readonly code: ResetCode;
  readonly reason: string | null;
  readonly streamId: number | null;

  constructor(reason?: string | null, options: { code?: ResetCode; streamId?: number | null } = {}) {
    const code = options.code ?? ResetCode.NO_ERROR;
    super(reason ?? ResetCode[code]);
    this.name = 'StreamReset';
    this.code = code;
    this.reason = reason ?? null;
    this.streamId = options.streamId ?? null;
  }
}

/** The remote handler threw. Carries the serialized error object, if the remote sent one. */
export class RemoteError extends StreamReset {
  readonly payload: unknown;

  constructor(reason?: string | null, options: { streamId?: number | null; payload?: unknown } = {}) {
    super(reason, { code: ResetCode.APPLICATION_ERROR, streamId: options.streamId });
    this.name = 'RemoteError';
    this.payload = options.payload ?? null;
  }
}

/** A local deadline expired; the remote was told to stop working. */
export class StreamTimeout extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {}) {
    super(reason, { code: ResetCode.TIMEOUT, streamId: options.streamId });
    this.name = 'StreamTimeout';
  }
}

/** Not accepted and definitively not processed. Retry - elsewhere, or after a delay. */
export class StreamRefused extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {}) {
    super(reason, { code: ResetCode.REFUSED, streamId: options.streamId });
    this.name = 'StreamRefused';
  }
}

/** Synthesised locally when the socket dies. MUST NEVER appear on the wire (reset code 9). */
export class ConnectionLost extends StreamReset {
  constructor(reason?: string | null, options: { streamId?: number | null } = {}) {
    super(reason, { code: ResetCode.CONNECTION_CLOSED, streamId: options.streamId });
    this.name = 'ConnectionLost';
  }
}

/**
 * A transport cannot open the address it was given (WSM-ERR-016).
 *
 * Never thrown directly. An application can test `error instanceof TransportUrlError` without
 * importing the transport that refused the url (in a browser build, that transport cannot be
 * imported). The concrete classes are defined in their transport's module (`UnixUrlError` and
 * `WsUrlError` in `muxws/node`); third-party adapters (WSM-API-021) define their own subclasses the
 * same way.
 *
 * Python declares `TransportUrlError(MuxwsError, ValueError)`. A JavaScript class has a single
 * prototype chain, and this one extends `MuxwsError` so that an `instanceof MuxwsError` handler
 * catches every muxws error. (jsdom and undici throw a `DOMException` named `SyntaxError` for an
 * invalid WebSocket url, and `ws` throws a `SyntaxError`; there is no corresponding builtin base to
 * keep.) `name` carries the Python class identity (WSM-ERR-004), so every subclass sets its own.
 */
export class TransportUrlError extends MuxwsError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message);
    this.name = 'TransportUrlError';
    // Assigned here, not passed to `super`: `MuxwsError` takes only a message, and
    // `new Error(msg, { cause: undefined })` would create an own `cause` property with the value
    // `undefined`. `cause` is set only when there is an underlying error (WSM-ERR-016).
    if (options.cause !== undefined) this.cause = options.cause;
  }
}

/**
 * This runtime, build or entry point cannot provide the transport at all (WSM-ERR-016).
 *
 * Never thrown directly. A `TransportUrlError` means the url is invalid for the transport; this error
 * means the url is valid but the environment cannot open it: an optional dependency is not
 * installed, the kernel has no `AF_UNIX`, or the entry point excludes the dependency (WSM-API-022).
 *
 * Python declares `RuntimeError` as a second base. As with `TransportUrlError`, this class extends
 * only `MuxwsError`, and `name` carries the class identity.
 */
export class TransportUnsupportedError extends MuxwsError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message);
    this.name = 'TransportUnsupportedError';
    if (options.cause !== undefined) this.cause = options.cause;
  }
}

/** Build the `StreamReset` subclass that represents `code`, falling back to `StreamReset` itself. */
export function exceptionForReset(
  code: ResetCode,
  reason?: string | null,
  options: { streamId?: number | null } = {},
): StreamReset {
  switch (code) {
    case ResetCode.APPLICATION_ERROR:
      return new RemoteError(reason, options);
    case ResetCode.TIMEOUT:
      return new StreamTimeout(reason, options);
    case ResetCode.REFUSED:
      return new StreamRefused(reason, options);
    case ResetCode.CONNECTION_CLOSED:
      return new ConnectionLost(reason, options);
    default:
      return new StreamReset(reason, { code, streamId: options.streamId });
  }
}
