/**
 * Observability: the log seam, the one-line `muxws.frames` logger, and `CloseReason` (§12).
 *
 * A mirror of `muxws/observability.py`. Python gets its logger from the standard library and merely
 * names it; TypeScript has no logging module it may depend on - the browser entry point has zero
 * runtime dependencies (WSM-PKG-003) - so the level-filtered `console` shim that used to live in
 * `ts/peer.ts` moves here, which is what its own comment said M5a would do with it.
 */

import type { Frame } from './frames';

// --------------------------------------------------------------------------- the logger shim

/** What `logging.getLogger("muxws.frames").setLevel(...)` selects from, spelled as strings. */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

/**
 * The whole of `muxws.frames` in TypeScript: four methods over `console`, plus a level.
 *
 * `level` starts at `'warn'` because that is what an unconfigured Python logger does - `logger.info`
 * and `logger.debug` on the Python side print nothing until an application configures logging, and a
 * port that spammed every frame to the console by default would not be mirroring it.
 *
 * @internal Exported so a test can raise the level the way `caplog.at_level` does in Python, and so
 * an application can turn frame logging on. Re-exported from `ts/peer.ts` for the call sites that
 * already import it from there; it is a seam, not public API.
 */
export const logger = {
  level: 'warn' as LogLevel,
  isEnabledFor(level: LogLevel): boolean {
    return LEVEL_ORDER[level] >= LEVEL_ORDER[this.level];
  },
  debug(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('debug')) console.debug(message, ...rest);
  },
  info(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('info')) console.info(message, ...rest);
  },
  warn(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('warn')) console.warn(message, ...rest);
  },
  error(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('error')) console.error(message, ...rest);
  },
};

// --------------------------------------------------------------------------- close reason

/** Why a socket ended. The same four fields in both languages (WSM-RCN-045). */
export interface CloseReason {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;
  /**
   * False only when `maxAttempts` is exhausted or `close()` was called deliberately. Until the
   * reconnect helper lands in M5b there is nothing that retries, so it is always false here.
   */
  readonly willRetry: boolean;
}

// --------------------------------------------------------------------------- the frame line

/** `'tx'` before encode, `'rx'` after decode (WSM-OBS-003). */
export type FrameDirection = 'tx' | 'rx';

function isPresent(value: unknown): boolean {
  return value !== null && value !== undefined;
}

/**
 * One line per frame at DEBUG, under the `muxws.frames` logger (WSM-OBS-001).
 *
 * `conn=` is `peer.id` - a per-process prefix plus a per-connection counter (WSM-API-009) - so two
 * lines carrying the same `conn=` are always the same connection, and a reconnect shows as a new one
 * rather than as a continuation.
 *
 * The payload's **contents** never appear, at any level. Application data routinely holds secrets,
 * and a frame line is emitted for every frame (WSM-OBS-002). Everything below is either a field of
 * the envelope or a count of one; `payload`, `fragment` and `trailers` are read for their shape and
 * never for their value.
 */
export function logFrame(connectionId: string, direction: FrameDirection, frame: Frame, byteLength: number): void {
  if (!logger.isEnabledFor('debug')) return;

  const parts = [`muxws conn=${connectionId} dir=${direction} type=${frame.type.padEnd(6)}`];
  if (isPresent(frame.stream)) parts.push(`stream=${frame.stream}`);
  if (frame.type === 'open' || frame.type === 'data') parts.push(`end=${frame.end === true ? 1 : 0}`);
  parts.push(`bytes=${byteLength}`);
  if (isPresent(frame.fragment)) parts.push(`frag=${frame.more === true ? 'more' : 'last'}`);
  if (isPresent(frame.headers) && Object.keys(frame.headers as object).length > 0) {
    parts.push(`headers=${Object.keys(frame.headers as object).length}`);
  }
  if (isPresent(frame.code)) parts.push(`code=${frame.code}`);
  if (isPresent(frame.last_stream)) parts.push(`last=${frame.last_stream}`);
  // `reason` is diagnostic text this library or the remote wrote, never application payload.
  if (isPresent(frame.reason) && frame.reason !== '') parts.push(`reason=${JSON.stringify(frame.reason)}`);
  logger.debug(parts.join(' '));
}
