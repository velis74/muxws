/**
 * Connection liveness and orderly shutdown (§6).
 *
 * Mirrors `muxws/lifecycle.py`.
 *
 * There is no handshake after the WebSocket upgrade. A connection is established when the socket is
 * open with the subprotocol accepted (WSM-CON-030); a peer sends and waits for nothing before using
 * it. This module provides liveness checks that do not depend on WebSocket control frames, which a
 * browser cannot observe (WSM-CON-011), and an orderly shutdown that lets in-flight streams finish.
 *
 * There is no `settings` frame (WSM-CON-031), and therefore no announced settings or acknowledgement
 * state.
 */

/**
 * The largest stream id the protocol allows. A peer that has exhausted its ids shuts the connection
 * down in an orderly way instead of wrapping around (WSM-SID-007).
 */
export const MAX_STREAM_ID = 2 ** 31 - 1;

/**
 * A ping nonce: sixteen lowercase hex characters, the format of Python's `secrets.token_hex(8)`.
 *
 * Uses `crypto.getRandomValues`, not `Math.random`, as Python uses `secrets`, not `random`, so the
 * nonce is not predictable. (The reconnect jitter and the connection id in `peer.ts` use
 * `Math.random`.) `crypto` is a Web Crypto global available in browsers and Node without a dependency
 * (WSM-PKG-003). It is accessed through `globalThis` because the shared eslint configuration does not
 * declare it as a global, and a `no-undef` disable comment would cover more than this one name.
 */
export function newNonce(): string {
  const bytes = new Uint8Array(8);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

interface Pending {
  promise: Promise<number>;
  resolve(elapsedMs: number): void;
  reject(error: Error): void;
  settled: boolean;
}

/**
 * Outstanding pings, keyed by nonce, not by order.
 *
 * A `pong` may arrive after its ping has timed out and been removed; matching by position would
 * attribute it to the next ping and report a wrong round-trip time. A `pong` with an unknown nonce is
 * a late reply and is ignored, not treated as an error.
 */
export class PingRegistry {
  private readonly pending = new Map<string, Pending>();

  /** The number of pings awaiting a `pong`. Python: `len(registry)`. */
  get size(): number {
    return this.pending.size;
  }

  /** Start waiting for `nonce`. The promise resolves with the round-trip time in milliseconds. */
  open(nonce: string): Promise<number> {
    let resolve!: (elapsedMs: number) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<number>((settle, fail) => {
      resolve = settle;
      reject = fail;
    });
    const entry: Pending = { promise, resolve, reject, settled: false };
    this.pending.set(nonce, entry);
    // A ping removed by `giveUp` and later rejected by `failAll` has no remaining holder. The no-op
    // handler (as in the `Stream` constructor) prevents an unhandled rejection, which terminates a
    // Node process by default.
    void promise.catch(() => undefined);
    return promise;
  }

  /** Resolve the ping this nonce belongs to. False when nothing was waiting for it. */
  settle(nonce: string, elapsedMs: number): boolean {
    const entry = this.pending.get(nonce);
    this.pending.delete(nonce);
    if (entry === undefined || entry.settled) return false;
    entry.settled = true;
    entry.resolve(elapsedMs);
    return true;
  }

  giveUp(nonce: string): void {
    this.pending.delete(nonce);
  }

  /** Reject every outstanding ping with `error`. Called when the socket is lost. */
  failAll(error: Error): void {
    this.pending.forEach((entry) => {
      if (entry.settled) return;
      entry.settled = true;
      entry.reject(error);
    });
    this.pending.clear();
  }
}

/**
 * The `goaway` state of a connection, tracked separately per direction.
 *
 * After sending a `goaway` this peer refuses new streams (WSM-CON-021). After receiving one, `open()`
 * throws (WSM-CON-022), and streams above the remote's `lastStream` were not processed and may be
 * retried elsewhere (WSM-CON-023).
 */
export class GoawayState {
  sent = false;

  received = false;

  /**
   * The `last_stream` from the received `goaway`: the highest stream id this peer opened that the
   * remote will still process. It has this peer's parity, not the remote's.
   */
  remoteLastStream: number | null = null;

  sentCode: number | null = null;

  receivedCode: number | null = null;

  receivedReason: string | null = null;

  get isGoingAway(): boolean {
    return this.sent || this.received;
  }

  /**
   * True when `streamId` is at or below the remote's `last_stream`.
   *
   * When no `goaway` has been received (`remoteLastStream` is `null`), every live stream gets the
   * full drain window.
   */
  survivesDrain(streamId: number): boolean {
    if (this.remoteLastStream === null) return true;
    return streamId <= this.remoteLastStream;
  }
}
