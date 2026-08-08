/**
 * Deployment configuration (§2.2, WSM-CDC-010/011).
 *
 * Mirrors `muxws/conf.py`. The codec name is read from the environment, not from a call argument,
 * because both ends of a connection must agree on it and an argument is decided per call site
 * rather than per deployment. The TypeScript half reads `import.meta.env.VITE_MUXWS_CODEC`; its
 * Python twin reads `MUXWS_CODEC`.
 */

const DEFAULT_CODEC = 'json';

/**
 * `import.meta.env.VITE_MUXWS_CODEC`, or `undefined` where there is no such environment.
 *
 * Vite replaces the expression statically at build time, which is what the `VITE_` prefix is for.
 * Under Node, vitest and any CommonJS consumer `import.meta.env` may be missing entirely, so every
 * step is guarded: this must degrade to the `json` default rather than throw during module load.
 */
function envCodec(): string | undefined {
  let env: Record<string, unknown> | undefined;
  try {
    // @ts-expect-error - the repository's tsconfig compiles to CommonJS, where tsc rejects
    // `import.meta` outright (TS1343). Vite and vitest both process this file as ESM, where the
    // expression is legal and is the one WSM-CDC-010 names.
    env = (import.meta as { env?: Record<string, unknown> }).env;
  } catch {
    return undefined;
  }
  const configured = env?.VITE_MUXWS_CODEC;
  // Any string is taken as configured, including the empty one: `os.environ.get` does the same, and
  // a name that resolves to no codec must fail loudly rather than fall back to JSON (WSM-INV-015).
  return typeof configured === 'string' ? configured : undefined;
}

/** The one settings singleton. Writable, so an application may set it during bootstrap. */
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
 * Read at connection time, never at import time - or WSM-CDC-011's "an application may set it
 * during bootstrap" stops being true for anything that imports muxws early.
 */
export const settings = new Settings();
