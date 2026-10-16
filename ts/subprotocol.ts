/**
 * The `muxws.v1.<codec>` subprotocol assertion (§2.3, WSM-CDC-020..029).
 *
 * Mirrors `muxws/subprotocol.py`. The dialer asserts one codec and the acceptor accepts or refuses it;
 * there is no negotiation, fallback encoding or per-connection choice among several codecs
 * (WSM-CDC-023).
 */

import { CodecMismatch } from './errors';
import { logger } from './observability';

/**
 * The generation prefix. Its version component is the only version on the wire (WSM-CON-009). A
 * change that requires the remote to act on a new frame type increments it, and a v1 acceptor rejects
 * an offer of another generation at the handshake (WSM-CDC-025).
 */
export const PREFIX = 'muxws.v1.';

/**
 * The subprotocol list a dialer offers: the muxws entry **first** (WSM-CDC-020).
 *
 * An application may append its own entries, for example a bearer token; the acceptor ignores them
 * (WSM-CDC-021).
 */
export function offer(codecName: string, extra: readonly string[] = []): string[] {
  return [`${PREFIX}${codecName}`, ...extra];
}

/**
 * Matches any generation, so the refusal message for an offer of another generation names that
 * generation instead of reporting a missing muxws entry (WSM-CDC-025).
 */
const ANY_GENERATION = /^muxws\.v(\d+)\./;

/** The generation integer in a muxws subprotocol name, or `null` if it is not one. */
export function generationOf(entry: string): number | null {
  const match = ANY_GENERATION.exec(entry);
  return match === null ? null : Number.parseInt(match[1], 10);
}

/**
 * The single offered entry carrying the muxws prefix, or `null`.
 *
 * Other entries are not inspected; they belong to the application, typically its authentication
 * (WSM-CDC-021).
 */
export function findOffer(offered: readonly string[]): string | null {
  return offered.find((entry) => entry.startsWith(PREFIX)) ?? null;
}

/**
 * The value an acceptor selects, or `null` to refuse the handshake (WSM-CDC-022).
 *
 * Returns `null` for a mismatched codec, a different generation, or an offer without a muxws entry,
 * and logs the refusal on the acceptor side (WSM-CDC-029). The dialer composes its own error
 * (WSM-CDC-024), because a browser cannot read the rejection response.
 */
export function select(offered: readonly string[], configured: string): string | null {
  const wanted = `${PREFIX}${configured}`;
  const entry = findOffer(offered);
  if (entry === wanted) return wanted;

  // Logged through `logger`, not `console` directly, so `logger.level` controls it, as
  // `logging.getLogger("muxws.codec")` does in Python. At the default level the line reaches
  // `console.error`.
  logger.error(
    `muxws refusing the upgrade: the dialer offered ${describe(offered, entry)}, this acceptor is ` +
      `configured for '${configured}'. Set MUXWS_CODEC here or VITE_MUXWS_CODEC / MUXWS_CODEC ` +
      'there so both ends agree; muxws never negotiates a fallback (WSM-CDC-022/023).',
  );
  return null;
}

function describe(offered: readonly string[], entry: string | null): string {
  if (entry !== null) return `'${entry}'`;
  const other = offered.find((value) => generationOf(value) !== null);
  if (other !== undefined) {
    return (
      `'${other}', which is generation ${generationOf(other)} and not 1 - a frame type the remote ` +
      'must act on requires a new generation, and a v1 acceptor rejects it here (WSM-CDC-025)'
    );
  }
  return `no ${PREFIX}* subprotocol at all (offered ${JSON.stringify(offered)})`;
}

/**
 * The error a dialer composes **for itself** when its handshake is refused (WSM-CDC-024).
 *
 * A browser cannot read a rejection body, so the diagnostic is composed on the dialer side. It names
 * both environment variables, because the dialer cannot tell which end is misconfigured.
 */
export function mismatchError(configured: string): CodecMismatch {
  return new CodecMismatch(
    `the acceptor refused the muxws handshake for codec '${configured}'. Both ends must be ` +
      'configured for the same codec: VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the server. ' +
      'muxws asserts the codec at the handshake and never falls back (WSM-CDC-022/024).',
    { configured },
  );
}
