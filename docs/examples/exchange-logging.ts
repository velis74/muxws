/**
 * One JSON line per finished stream, appended to a file, built on `peer.onExchange`.
 */

import { createWriteStream, type WriteStream } from 'node:fs';

import type { Exchange, ExchangeDict, ExchangeSideDict, Peer } from 'muxws';

/** Longest payload written, in UTF-8 bytes of its JSON text; a longer one is written trimmed, as a string. */
export const TRIM_PAYLOAD_SIZE = 4096;

function masked(payload: unknown): unknown {
  if (payload !== null && typeof payload === 'object' && 'password' in payload) return { ...payload, password: '***' };
  // A payload over TRIM_PAYLOAD_SIZE arrives as its trimmed JSON text.
  if (typeof payload === 'string' && payload.includes('"password"')) return '***';
  return payload;
}

/** Masks the `password` payload key and the `cookie` header on both sides of `record`. */
export function withoutSecrets(record: ExchangeDict): ExchangeDict {
  [record.request, record.response].forEach((side: ExchangeSideDict) => {
    if (side.headers && 'cookie' in side.headers) side.headers.cookie = '***';
    side.payloads = side.payloads.map(masked);
  });
  return record;
}

/** An append-mode file whose write errors are reported instead of ending the process. */
export function openExchangeLog(path: string): WriteStream {
  return createWriteStream(path, { flags: 'a' }).on('error', (error: Error) => {
    console.error('exchange log write failed', error);
  });
}

/** Appends every finished stream of `peer` to `file` as one JSON line, `redact` applied first. */
export function logExchanges(
  peer: Peer,
  file: WriteStream,
  redact: ((record: ExchangeDict) => ExchangeDict) | null = withoutSecrets,
): void {
  peer.onExchange((exchange: Exchange) => {
    const record = exchange.asDict({ trimPayloadSize: TRIM_PAYLOAD_SIZE });
    file.write(`${JSON.stringify(redact ? redact(record) : record)}\n`);
  });
}
