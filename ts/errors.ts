/**
 * Reset codes and the muxws exception hierarchy (WSM-ERR-002..009, §8.1-8.2).
 *
 * Mirrors `muxws/errors.py` class for class. Every class sets `name`, so a cross-language test can
 * assert on error identity the way Python asserts on `__class__` (WSM-ERR-004).
 *
 * What is deliberately **not** here is as load-bearing as what is: no concrete transport error. The
 * two transport bases at the bottom of this file are shared because an application must be able to
 * catch them without importing a transport; `UnixUrlError`, `WsUrlError` and the rest belong to the
 * one transport that produces them and are exported from the entry point that ships it (WSM-ERR-016).
 * `ConnectionClosed` is the case that fixes the boundary the other way: every adapter throws it, but
 * the `SocketAdapter` contract *requires* it of every adapter (WSM-API-021), so it is owned by the
 * seam and stays shared. Ownership decides, not who throws.
 */

/**
 * Numeric on the wire, named in both APIs; shared by `reset` and `goaway`.
 *
 * There are **nine** members. `5` is retired (it was `STREAM_LIMIT`, WSM-STM-022): the number MUST
 * NOT be reused and MUST NOT appear on the wire, so no name maps to it.
 */
export enum ResetCode {
  NO_ERROR = 0,
  CANCELLED = 1,
  APPLICATION_ERROR = 2,
  PROTOCOL_ERROR = 3,
  REFUSED = 4,
  // 5 is retired - see the doc comment above.
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
    super(message ?? reason ?? 'connection closed');
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
 * Deliberately neither a `StreamReset` nor a `ProtocolError` (WSM-ERR-009): a normal close racing a
 * last `send()` is an expected outcome, not a failure and not a caller bug.
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
 * Never thrown directly. It exists so that `error instanceof TransportUrlError` can be written by an
 * application that has not imported - and, in a browser build, cannot import - the transport that
 * refused the url. The concrete classes live in their transport's own module (`UnixUrlError` and
 * `WsUrlError` behind `muxws/node`), because the adapter seam is public (WSM-API-021): a third party
 * writing an adapter cannot add a class to this file, so a convention that required one would be a
 * convention only this repository could follow.
 *
 * Python spells this `TransportUrlError(MuxwsError, ValueError)`. JavaScript has one prototype chain
 * and `MuxwsError` is the half that has to survive: the whole purpose of the rule is that a single
 * `instanceof MuxwsError` handler cannot be leaked through, and there is no builtin habit to preserve
 * on this side - no runtime raises `TypeError` for a bad WebSocket url. jsdom and undici raise a
 * `DOMException` named `SyntaxError`, `ws` raises a real `SyntaxError`, and neither is what a caller
 * would have written a `catch` for. `name` therefore carries the whole of the Python class's identity
 * across (WSM-ERR-004), which is why every subclass must set its own.
 */
export class TransportUrlError extends MuxwsError {
  constructor(message?: string, options: { cause?: unknown } = {}) {
    super(message);
    this.name = 'TransportUrlError';
    // Assigned rather than passed through `super`: `MuxwsError` takes a message only, and
    // `new Error(msg, { cause: undefined })` leaves an own `cause` property reading "the original was
    // nothing" where the truth is "there was no original". WSM-ERR-016 requires the chain when there
    // is one, and says nothing about inventing one when there is not.
    if (options.cause !== undefined) this.cause = options.cause;
  }
}

/**
 * This runtime, build or entry point cannot provide the transport at all (WSM-ERR-016).
 *
 * Never thrown directly. The distinction from `TransportUrlError` is the one the caller acts on:
 * a `TransportUrlError` means *retype the url*, this means *the url is fine, change where or how you
 * run it* - an optional dependency that is not installed, a kernel with no `AF_UNIX`, a bundle that
 * WSM-API-022 keeps the dependency out of. A port that collapsed the two would send a reader who has
 * nothing to fix off to re-read a url that was already correct.
 *
 * Python adds `RuntimeError` to the bases for the same reason `TransportUrlError` adds `ValueError`;
 * here the single prototype chain goes to `MuxwsError` and `name` carries the identity, exactly as
 * for the sibling above.
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
