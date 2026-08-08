// @vitest-environment node
/**
 * Deployment configuration (§2.2, WSM-CDC-010/011/012).
 *
 * Mirrors the settings half of `muxws/codec_test.py`: `test_env_default_and_runtime_override`,
 * `test_settings_is_read_at_call_time_not_import_time`, `test_codec_argument_overrides_settings` and
 * the "before any socket" half of `test_unregistered_name_raises_before_socket`. The handshake half
 * of that file lives in `subprotocol.spec.ts`, the registry half in `codec.spec.ts`.
 *
 * Python's twin moves the environment with `monkeypatch.setenv`. There is no equivalent here: under
 * vitest every module is handed its **own** `import.meta.env`, built from the Vite config rather
 * than from `process.env`, so neither `vi.stubEnv` nor mutating this file's own `import.meta.env`
 * reaches the copy `ts/conf.ts` reads. The module is therefore compiled the way a bundler compiles
 * it and evaluated against an `import.meta` this file supplies - which is also the only way to reach
 * the case WSM-CDC-010 names explicitly and vitest itself cannot produce: no `import.meta.env` at
 * all.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { transformSync } from 'esbuild';
// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not declare it as one.
import { vi } from 'vitest';

import { type Codec, JsonCodec, clearCodecs, registerCodec } from './codec';
import { Settings, settings } from './conf';
import { CodecMismatch, CodecNotRegistered } from './errors';
import { accept, handleProtocols } from './node';

type ConfModule = typeof import('./conf');

/** The name `import.meta` is rewritten to, so the evaluated module reads what a test passes in. */
const META = 'muxwsImportMeta';

/**
 * `ts/conf.ts`, compiled to CommonJS and evaluated with an `import.meta` of our choosing.
 *
 * `replaceImportMeta: false` leaves esbuild's own CommonJS substitution in place, which turns
 * `import.meta` into `{}` - exactly the environment WSM-CDC-010's guard exists for, and the one a
 * CommonJS consumer gets.
 */
function loadConf(meta: unknown, replaceImportMeta = true): ConfModule {
  const source = readFileSync(join(process.cwd(), 'ts', 'conf.ts'), 'utf8');
  const { code } = transformSync(source, {
    loader: 'ts',
    format: 'cjs',
    target: 'node20',
    ...(replaceImportMeta ? { define: { 'import.meta': META } } : {}),
  });
  const evaluated = { exports: {} as ConfModule };

  new Function('exports', 'module', META, code)(evaluated.exports, evaluated, meta);
  return evaluated.exports;
}

/** Enough of a `ws` connection for `accept()`, and a record of everything it was asked to do. */
class FakeWsConnection {
  binaryType = 'nodebuffer';

  readonly listeners: string[] = [];

  readonly closes: [number, string][] = [];

  constructor(readonly protocol: string) {}

  on(event: string): this {
    this.listeners.push(event);
    return this;
  }

  close(code: number, reason: string): void {
    this.closes.push([code, reason]);
  }

  /** True while nothing has touched the socket - the TypeScript half of Python's dial recorder. */
  get untouched(): boolean {
    return this.listeners.length === 0 && this.closes.length === 0 && this.binaryType === 'nodebuffer';
  }
}

function asSocket(fake: FakeWsConnection): Parameters<typeof accept>[0] {
  return fake as unknown as Parameters<typeof accept>[0];
}

describe('Settings', () => {
  it('defaults to json, reads VITE_MUXWS_CODEC, and is writable at runtime - WSM-CDC-010/011', () => {
    // The exact shape of Python's `test_env_default_and_runtime_override`, one mutable env object
    // standing in for `monkeypatch.setenv`.
    const env: Record<string, unknown> = {};
    const conf = loadConf({ env });
    expect(new conf.Settings().codec).toBe('json');

    env.VITE_MUXWS_CODEC = 'msgpack';
    expect(new conf.Settings().codec).toBe('msgpack');

    const fresh = new conf.Settings();
    fresh.codec = 'json';
    expect(fresh.codec).toBe('json');
    env.VITE_MUXWS_CODEC = 'other';
    // Still the value the application set: the environment is read at construction and on `reload`,
    // never behind the application's back.
    expect(fresh.codec).toBe('json');
    fresh.reload();
    expect(fresh.codec).toBe('other');
  });

  it('reads VITE_MUXWS_CODEC and nothing else - WSM-CDC-010', () => {
    // MUXWS_CODEC is the Python half's variable; a browser bundle never sees it, and reading it here
    // would make the two halves disagree about which name configures which end.
    expect(loadConf({ env: { MUXWS_CODEC: 'msgpack' } }).settings.codec).toBe('json');
    expect(loadConf({ env: { VITE_MUXWS_CODEC: 'msgpack' } }).settings.codec).toBe('msgpack');
  });

  it('takes an empty value as configured rather than falling back to json - WSM-INV-015', () => {
    // `os.environ.get` returns the empty string too, and a name that resolves to no codec must fail
    // loudly at `getCodec` rather than quietly become JSON.
    expect(loadConf({ env: { VITE_MUXWS_CODEC: '' } }).settings.codec).toBe('');
    // A non-string is not a configured name; a bundler that injected one has said nothing.
    expect(loadConf({ env: { VITE_MUXWS_CODEC: 7 } }).settings.codec).toBe('json');
  });

  it('degrades to json where import.meta.env does not exist - the Node and vitest case', () => {
    // Three shapes of "no environment": an import.meta without an env, an env-less object, and no
    // import.meta at all, which is what a CommonJS consumer evaluates. None of them may throw during
    // module load - the singleton is constructed at import time, so a throw here is unrecoverable.
    expect(loadConf({}).settings.codec).toBe('json');
    expect(loadConf({ env: undefined }).settings.codec).toBe('json');
    expect(loadConf(undefined, false).settings.codec).toBe('json');
  });

  it('is the one singleton, constructed at import time and readable ever after', () => {
    // vitest is itself the environment above: its `import.meta.env` names no codec.
    expect(settings).toBeInstanceOf(Settings);
    expect(settings.codec).toBe('json');
    expect(String(settings)).toBe("Settings(codec='json')");
  });
});

describe('the acceptor', () => {
  const json = new JsonCodec();

  /**
   * A codec carrying a name of its own, so the subprotocol the acceptor asserts on says *which*
   * name it read. A `JsonCodec` registered under another key would still put 'json' on the wire.
   */
  const marker: Codec = {
    name: 'bootstrap-set',
    binary: false,
    encode: (frame) => json.encode(frame),
    decode: (message) => json.decode(message),
    encodePayload: (payload) => json.encodePayload(payload),
    decodePayload: (data) => json.decodePayload(data),
  };

  beforeEach(() => {
    clearCodecs();
    registerCodec('json', new JsonCodec());
  });

  afterEach(() => {
    settings.codec = 'json';
    clearCodecs();
    registerCodec('json', new JsonCodec());
  });

  it('reads settings.codec at connection time, not at import time - WSM-CDC-011', async () => {
    // `./node` was imported before this line ran, so a value read at import time is 'json' and an
    // acceptor holding one would refuse the socket below.
    registerCodec('bootstrap-set', marker);
    settings.codec = 'bootstrap-set';

    const socket = new FakeWsConnection('muxws.v1.bootstrap-set');
    const peer = await accept(asSocket(socket));

    expect(peer.isOpen).toBe(true);
    expect(peer.isDialer).toBe(false);
    expect(socket.binaryType).toBe('arraybuffer');
    expect(socket.closes).toEqual([]);
  });

  it('refuses the codec it was configured with at import time, once that is no longer the one', async () => {
    // The converse of the test above, and the reason it proves anything: 'muxws.v1.json' is now the
    // wrong answer, so a captured value would have been visible as a *pass*.
    registerCodec('bootstrap-set', marker);
    settings.codec = 'bootstrap-set';

    const socket = new FakeWsConnection('muxws.v1.json');
    await expect(accept(asSocket(socket))).rejects.toBeInstanceOf(CodecMismatch);
    expect(socket.closes).toEqual([[1008, 'codec mismatch']]);
  });

  it('resolves the codec before it touches the socket - WSM-CDC-016', async () => {
    settings.codec = 'msgpack'; // configured, never registered
    const socket = new FakeWsConnection('muxws.v1.msgpack');

    const error = await accept(asSocket(socket)).catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(CodecNotRegistered);
    expect((error as CodecNotRegistered).message).toContain('VITE_MUXWS_CODEC');
    expect((error as CodecNotRegistered).message).toContain('msgpack');
    expect((error as CodecNotRegistered).message).toContain('json');
    // Python asserts a dial double recorded zero calls; the acceptor's equivalent is a socket that
    // was never listened to, never reconfigured and never closed.
    expect(socket.untouched).toBe(true);
  });

  it('lets an explicit codec argument win over settings - WSM-CDC-012', async () => {
    settings.codec = 'msgpack'; // unregistered: consulting it at all would raise
    const socket = new FakeWsConnection('muxws.v1.json');

    const peer = await accept(asSocket(socket), { codec: json });

    expect(peer.isOpen).toBe(true);
  });

  it('reads settings.codec at handshake time in the ws hook too - WSM-CDC-011/027', () => {
    settings.codec = 'bootstrap-set';
    expect(handleProtocols(new Set(['muxws.v1.bootstrap-set', 'bearer.abc123']))).toBe('muxws.v1.bootstrap-set');

    const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      expect(handleProtocols(new Set(['muxws.v1.json']))).toBe(false);
    } finally {
      spy.mockRestore();
    }
  });
});
