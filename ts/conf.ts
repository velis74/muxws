/**
 * Deployment configuration (§2.2, WSM-CDC-010/011).
 *
 * Mirrors `muxws/conf.py`. The codec name is read from the environment, not from a call argument,
 * because both ends of a connection must agree on it and it is a per-deployment setting. The
 * TypeScript implementation reads `import.meta.env.VITE_MUXWS_CODEC`; the Python implementation reads
 * `MUXWS_CODEC`.
 */

const DEFAULT_CODEC = 'json';

/**
 * `import.meta.env.VITE_MUXWS_CODEC`, or `undefined` where there is no such environment.
 *
 * Vite replaces the expression statically at build time for variables with the `VITE_` prefix.
 * Under Node, vitest and CommonJS consumers `import.meta.env` may be absent, so every step is
 * guarded: an absent environment yields `undefined` (and the `json` default), not an exception during
 * module load.
 */
function envCodec(): string | undefined {
  let env: Record<string, unknown> | undefined;
  try {
    // `@ts-ignore`, not `@ts-expect-error`: the root tsconfig compiles to CommonJS, where tsc rejects
    // `import.meta` (TS1343); `interop/tsconfig.json` compiles to ESM, where the expression is legal
    // and `@ts-expect-error` would be reported as an unused directive (TS2578). Vite and vitest
    // process this file as ESM, where this is the expression WSM-CDC-010 names.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    env = (import.meta as { env?: Record<string, unknown> }).env;
  } catch {
    return undefined;
  }
  const configured = env?.VITE_MUXWS_CODEC;
  // Any string counts as configured, including the empty one, as with `os.environ.get`. A name that
  // resolves to no codec raises an error instead of falling back to JSON (WSM-INV-015).
  return typeof configured === 'string' ? configured : undefined;
}

/** The settings singleton. Writable, so an application may set it during bootstrap. */
export class Settings {
  /** The configured codec name. Read at connection time, never at import time (WSM-CDC-011). */
  codec: string;

  constructor() {
    this.codec = envCodec() ?? DEFAULT_CODEC;
  }

  /** Re-read the environment. For tests; an application sets `codec` directly. */
  reload(): void {
    this.codec = envCodec() ?? DEFAULT_CODEC;
  }

  toString(): string {
    return `Settings(codec='${this.codec}')`;
  }
}

/**
 * Read at connection time, never at import time, so a value an application sets during bootstrap
 * applies even when muxws was imported earlier (WSM-CDC-011).
 */
export const settings = new Settings();
