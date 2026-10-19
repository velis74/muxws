/**
 * The logical frame model and its envelope mapping (§3).
 *
 * One frame per WebSocket message. Field names are unabbreviated and snake_case in both languages
 * (`last_stream` in TypeScript as well).
 */

import { ProtocolError } from './errors';

/**
 * The sentinel distinguishing "no payload" from an explicit `null`.
 *
 * A unique symbol: no other value compares equal to it, `JSON.stringify` does not serialize it, and
 * it prints as `Symbol(ABSENT)` in assertion output.
 */
export const ABSENT: unique symbol = Symbol('ABSENT');
export type Absent = typeof ABSENT;

/**
 * The frame types a v1 peer sends. `window_update` is reserved and unimplemented (WSM-BPR-001), and
 * there is no `settings` frame (WSM-CON-031). A receiver tolerates anything not in this set.
 */
export const V1_FRAME_TYPES: ReadonlySet<string> = new Set(['open', 'data', 'reset', 'ping', 'pong', 'goaway']);

/** One logical protocol unit. Readonly, and compared by value with `framesEqual`. */
export interface Frame {
  readonly type: string;
  readonly stream?: number | null;
  readonly payload?: unknown | Absent;
  readonly fragment?: string | ArrayBuffer | null;
  readonly more?: boolean;
  readonly headers?: Record<string, unknown> | null;
  readonly end?: boolean;
  readonly trailers?: Record<string, unknown> | null;
  readonly code?: number | null;
  readonly reason?: string | null;
  readonly nonce?: string | null;
  readonly last_stream?: number | null;
}

const FIELD_NAMES = [
  'type',
  'stream',
  'payload',
  'fragment',
  'more',
  'headers',
  'end',
  'trailers',
  'code',
  'reason',
  'nonce',
  'last_stream',
] as const;

type FieldName = (typeof FIELD_NAMES)[number];

const FIELD_DEFAULTS: Record<Exclude<FieldName, 'payload'>, unknown> = {
  type: undefined,
  stream: null,
  fragment: null,
  more: false,
  headers: null,
  end: false,
  trailers: null,
  code: null,
  reason: null,
  nonce: null,
  last_stream: null,
};

/** Envelope keys are emitted `type`, then `stream`, then the rest alphabetically (WSM-CDC-005). */
const LEADING_KEYS = ['type', 'stream'] as const;

/** Normalise a frame so that `undefined` and the field's default are indistinguishable. */
function fieldValue(frame: Frame, name: FieldName): unknown {
  const raw = (frame as unknown as Record<string, unknown>)[name];
  if (name === 'payload') return raw === undefined ? ABSENT : raw;
  return raw === undefined ? FIELD_DEFAULTS[name as Exclude<FieldName, 'payload'>] : raw;
}

/**
 * Render `frame` as an envelope, omitting every field still at its default.
 *
 * `payload` is the one field whose default is not `null`: `ABSENT` omits the key entirely, while
 * `null` emits `"payload": null`.
 */
export function toMapping(frame: Frame): Record<string, unknown> {
  const present: Record<string, unknown> = {};
  FIELD_NAMES.forEach((name) => {
    const value = fieldValue(frame, name);
    if (name === 'payload') {
      if (value !== ABSENT) present[name] = value;
      return;
    }
    if (name === 'type' || value !== FIELD_DEFAULTS[name as Exclude<FieldName, 'payload'>]) {
      present[name] = value;
    }
  });

  const ordered: Record<string, unknown> = {};
  LEADING_KEYS.forEach((key) => {
    if (key in present) {
      ordered[key] = present[key];
      delete present[key];
    }
  });
  Object.keys(present)
    .sort()
    .forEach((key) => {
      ordered[key] = present[key];
    });
  return ordered;
}

/**
 * Build a `Frame` from a decoded envelope.
 *
 * Unknown keys are dropped (WSM-FRM-001), so a decoded frame contains only envelope fields. An
 * unrecognised `type` is returned as an ordinary `Frame`; the peer, not the codec, ignores it
 * (WSM-FRM-002).
 *
 * Throws `ProtocolError` when `type` is missing, when both `payload` and `fragment` are present, or
 * when an envelope field has a value of the wrong type (WSM-FRM-005).
 */
export function fromMapping(mapping: Record<string, unknown>): Frame {
  if (!('type' in mapping)) {
    throw new ProtocolError("frame is missing the required 'type' field (WSM-FRM-005)");
  }
  if ('payload' in mapping && 'fragment' in mapping) {
    throw new ProtocolError("frame carries both 'payload' and 'fragment' (WSM-FRM-004)");
  }

  const frame: Record<string, unknown> = {};
  FIELD_NAMES.forEach((name) => {
    if (!(name in mapping)) return;
    const value = mapping[name];
    if (name !== 'payload' && !validField(name, value)) {
      const kind = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
      throw new ProtocolError(`envelope field '${name}' has an invalid value of type ${kind} (WSM-FRM-005)`);
    }
    frame[name] = value;
  });
  if (!('payload' in frame)) frame.payload = ABSENT;
  return frame as unknown as Frame;
}

/**
 * Whether `value` has the type envelope field `name` requires (WSM-FRM-005). `null` is accepted for
 * every field whose default is `null`.
 */
function validField(name: Exclude<FieldName, 'payload'>, value: unknown): boolean {
  switch (name) {
    case 'type':
      return typeof value === 'string';
    case 'more':
    case 'end':
      return typeof value === 'boolean';
    default:
      break;
  }
  if (value === null) return true;
  switch (name) {
    case 'stream':
    case 'last_stream':
      return Number.isInteger(value) && (value as number) >= 0;
    case 'code':
      return Number.isInteger(value);
    case 'fragment':
      return typeof value === 'string' || value instanceof ArrayBuffer;
    case 'headers':
    case 'trailers':
      return typeof value === 'object' && !Array.isArray(value) && !(value instanceof ArrayBuffer);
    default:
      return typeof value === 'string';
  }
}

/** Structural equality over the twelve envelope fields, with defaults normalised first. */
export function framesEqual(a: Frame, b: Frame): boolean {
  return FIELD_NAMES.every((name) => deepEqual(fieldValue(a, name), fieldValue(b, name)));
}

/** Value equality for codec-representable values: primitives, arrays and plain objects. */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;

  if (a instanceof ArrayBuffer || b instanceof ArrayBuffer) {
    if (!(a instanceof ArrayBuffer) || !(b instanceof ArrayBuffer) || a.byteLength !== b.byteLength) return false;
    const left = new Uint8Array(a);
    const right = new Uint8Array(b);
    return left.every((byte, index) => byte === right[index]);
  }

  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((item, index) => deepEqual(item, b[index]));
  }

  const aKeys = Object.keys(a as object);
  const bKeys = Object.keys(b as object);
  if (aKeys.length !== bKeys.length) return false;
  return aKeys.every(
    (key) =>
      Object.prototype.hasOwnProperty.call(b, key) &&
      deepEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]),
  );
}
