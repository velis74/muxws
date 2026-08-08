/**
 * The `muxws.v1.<codec>` subprotocol assertion (§2.3, WSM-CDC-020..029).
 *
 * Mirrors `muxws/subprotocol.py`. This is an **assertion, not a negotiation**. There is no fallback
 * encoding, no list of acceptable alternatives, no per-connection multi-codec support and no runtime
 * codec branching anywhere in the peer (WSM-CDC-023).
 */

import { CodecMismatch } from './errors';

/**
 * The generation prefix. The version component of this name is the **only** version on the wire
 * (WSM-CON-009); a change requiring the remote to act on a new frame type bumps it, and a v1
 * acceptor then rejects the offer at the handshake (WSM-CDC-025).
 */
export const PREFIX = 'muxws.v1.';

/**
 * The subprotocol list a dialer offers: the muxws entry **first** (WSM-CDC-020).
 *
 * An application may append its own entries - a bearer token is the common case - and the acceptor
 * ignores every one of them (WSM-CDC-021).
 */
export function offer(codecName: string, extra: readonly string[] = []): string[] {
  return [`${PREFIX}${codecName}`, ...extra];
}

/**
 * Any generation, so an offer from a future peer can be named as such rather than reported as an
 * absent muxws entry. WSM-CDC-025 makes rejecting it the acceptor's job; saying *why* is this one's.
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
 * Every other value is left entirely alone: it belongs to the application's authentication, and
 * interpreting it here would be muxws deciding something that is not its business (WSM-CDC-021).
 */
export function findOffer(offered: readonly string[]): string | null {
  return offered.find((entry) => entry.startsWith(PREFIX)) ?? null;
}

/**
 * The value an acceptor selects, or `null` to refuse the handshake (WSM-CDC-022).
 *
 * Refusal is the answer for a mismatched codec, for a different generation, and for an offer with
 * no muxws entry at all. The acceptor logs the failure itself (WSM-CDC-029): this is the half of
 * the diagnostic readable where a response body is readable, and the dialer's half (WSM-CDC-024)
 * is not complete on its own.
 */
export function select(offered: readonly string[], configured: string): string | null {
  const wanted = `${PREFIX}${configured}`;
  const entry = findOffer(offered);
  if (entry === wanted) return wanted;

  console.error(
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
 * A browser cannot read a rejection body, so the diagnostic cannot come from the server. It names
 * both environment variables, because the reader does not yet know which end is wrong.
 */
export function mismatchError(configured: string): CodecMismatch {
  return new CodecMismatch(
    `the acceptor refused the muxws handshake for codec '${configured}'. Both ends must be ` +
      'configured for the same codec: VITE_MUXWS_CODEC in the browser, MUXWS_CODEC on the server. ' +
      'muxws asserts the codec at the handshake and never falls back (WSM-CDC-022/024).',
    { configured },
  );
}
