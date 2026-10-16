/**
 * Observability: `logger`, the one-line `muxws.frames` frame log, and `CloseReason` (§12).
 *
 * Mirrors `muxws/observability.py`. Python uses a standard-library logger. The browser entry point has
 * no runtime dependencies (WSM-PKG-003), so the TypeScript implementation defines a level-filtered
 * `console` wrapper here.
 */

import type { Frame } from './frames';

// --------------------------------------------------------------------------- the logger shim

/** Log levels, equivalent to the levels `logging.getLogger("muxws.frames").setLevel(...)` accepts. */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

/**
 * The `muxws.frames` logger in TypeScript: four methods over `console`, plus a level.
 *
 * `level` defaults to `'warn'`, matching an unconfigured Python logger, which drops `info` and `debug`
 * records. Frame lines are therefore not printed by default.
 *
 * Exported from the package root; setting `logger.level` is how an application enables frame
 * logging, and how a test raises the level (as `caplog.at_level` does in Python).
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
   * False only when `maxAttempts` is exhausted or `close()` was called. A peer without the reconnect
   * helper never redials, so for such a peer it is always false.
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
 * `conn=` is `peer.id`, a per-process prefix plus a per-connection counter (WSM-API-009). Lines with
 * the same `conn=` belong to the same connection; a reconnect has a new `conn=` value.
 *
 * Payload contents are never logged, at any level, because application data may contain secrets
 * (WSM-OBS-002). The line contains only envelope fields and counts; `payload`, `fragment` and
 * `trailers` are checked for presence or size, never printed.
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
