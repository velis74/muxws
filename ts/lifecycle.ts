/**
 * Connection liveness and orderly shutdown (§6).
 *
 * A port of `muxws/lifecycle.py`, which is the specification of behaviour.
 *
 * There is **no post-socket handshake**. A connection is established the moment the socket is open
 * with the subprotocol accepted (WSM-CON-030), and this module adds nothing a peer must send or wait
 * for before anything else. What it adds is a defined *end*: liveness that does not depend on
 * WebSocket control frames a browser cannot see (WSM-CON-011), and a shutdown that lets in-flight
 * work finish.
 *
 * There is no `settings` frame anywhere in this project (WSM-CON-031), so there is no value object
 * here holding an announced/effective pair and no ack bookkeeping. If you are looking for one, it was
 * deleted from the protocol.
 */

/**
 * The largest stream id the protocol allows. The next allocation past it is impossible, so the
 * exhausting peer shuts the connection down in an orderly way rather than wrapping (WSM-SID-007).
 */
export const MAX_STREAM_ID = 2 ** 31 - 1;

/**
 * A ping nonce: sixteen lowercase hex characters, the same shape Python's `secrets.token_hex(8)`
 * produces.
 *
 * `crypto.getRandomValues` rather than `Math.random`, for the same reason Python reaches for
 * `secrets` rather than `random` here: this one is cheap and there is no reason to make a nonce
 * guessable. (The reconnect jitter in M5b is the opposite case and deliberately uses `Math.random`,
 * as does the connection id in `peer.ts`.) The function is a Web Crypto global, so it exists in a
 * browser and in Node alike and pulls in no dependency (WSM-PKG-003). It is reached through
 * `globalThis` because the shared eslint configuration does not list it as a global, and a
 * `no-undef` disable comment would suppress more than the one name it is about.
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
 * Outstanding pings, keyed by **nonce rather than by order**.
 *
 * Order would be wrong: a `pong` may arrive after its ping's deadline has already expired and been
 * given up on, and matching by position would then credit it to the next ping and report a
 * round-trip time that never happened. An unknown nonce is simply dropped (it is a late echo, not an
 * error).
 */
export class PingRegistry {
  private readonly pending = new Map<string, Pending>();

  /** How many pings are still waiting for an answer. Python spells this `len(registry)`. */
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
    // A ping given up on before `failAll` runs leaves a promise nobody is holding. Attaching the
    // no-op handler here - the same guarantee `Stream` makes in its constructor - is what keeps that
    // from surfacing as an unhandled rejection, which in Node is fatal by default.
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

  /** Socket death: nobody is going to answer, so nobody should keep waiting. */
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
 * What each peer knows about the other's intention to stop.
 *
 * The two directions are tracked separately because they mean different things. Having *sent* one
 * means this peer refuses new work (WSM-CON-021); having *received* one means `open()` throws here
 * (WSM-CON-022) and that streams above the remote's `lastStream` were never processed and are safe to
 * retry elsewhere (WSM-CON-023).
 */
export class GoawayState {
  sent = false;

  received = false;

  /**
   * The highest id **the remote opened** that it promises to still complete. Their parity, not ours -
   * getting that backwards silently resets everything on every drain.
   */
  remoteLastStream: number | null = null;

  sentCode: number | null = null;

  receivedCode: number | null = null;

  receivedReason: string | null = null;

  get isGoingAway(): boolean {
    return this.sent || this.received;
  }

  /**
   * True when a stream of ours is at or below what the remote promised to finish.
   *
   * With no `lastStream` known - a `goaway` we sent rather than received - every live stream is
   * allowed its drain window; the remote has told us nothing that would cut it short.
   */
  survivesDrain(streamId: number): boolean {
    if (this.remoteLastStream === null) return true;
    return streamId <= this.remoteLastStream;
  }
}
