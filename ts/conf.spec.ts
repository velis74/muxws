// @vitest-environment node
/**
 * Deployment configuration (§2.2, WSM-CDC-010/011/012).
 *
 * Mirrors the settings half of `muxws/codec_test.py`: `test_env_default_and_runtime_override`,
 * `test_settings_is_read_at_call_time_not_import_time`, `test_codec_argument_overrides_settings` and
 * the "before any socket" half of `test_unregistered_name_raises_before_socket`. The handshake half
 * of that file lives in `subprotocol.spec.ts`, the registry half in `codec.spec.ts`.
 *
 * The Python tests set the environment with `monkeypatch.setenv`. Under vitest each module receives
 * its own `import.meta.env`, built from the Vite config and not from `process.env`, so neither
 * `vi.stubEnv` nor mutating this file's `import.meta.env` changes the copy `ts/conf.ts` reads. This
 * file therefore compiles the module as a bundler does and evaluates it against an `import.meta` it
 * supplies. This also covers the case WSM-CDC-010 names and vitest cannot produce: no
 * `import.meta.env` at all.
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
 * `replaceImportMeta: false` keeps esbuild's CommonJS substitution, which turns `import.meta` into
 * `{}`. That is the environment a CommonJS consumer gets and the one WSM-CDC-010's guard handles.
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

  /** True while no method of the socket has been called; the counterpart of Python's dial recorder. */
  get untouched(): boolean {
    return this.listeners.length === 0 && this.closes.length === 0 && this.binaryType === 'nodebuffer';
  }
}

function asSocket(fake: FakeWsConnection): Parameters<typeof accept>[0] {
  return fake as unknown as Parameters<typeof accept>[0];
}

describe('Settings', () => {
  it('defaults to json, reads VITE_MUXWS_CODEC, and is writable at runtime - WSM-CDC-010/011', () => {
    // Same structure as Python's `test_env_default_and_runtime_override`; one mutable env object
    // replaces `monkeypatch.setenv`.
    const env: Record<string, unknown> = {};
    const conf = loadConf({ env });
    expect(new conf.Settings().codec).toBe('json');

    env.VITE_MUXWS_CODEC = 'msgpack';
    expect(new conf.Settings().codec).toBe('msgpack');

    const fresh = new conf.Settings();
    fresh.codec = 'json';
    expect(fresh.codec).toBe('json');
    env.VITE_MUXWS_CODEC = 'other';
    // The value the application set is kept: the environment is read only at construction and on
    // `reload`.
    expect(fresh.codec).toBe('json');
    fresh.reload();
    expect(fresh.codec).toBe('other');
  });

  it('reads VITE_MUXWS_CODEC and nothing else - WSM-CDC-010', () => {
    // MUXWS_CODEC is the Python variable. A browser bundle does not receive it, and each port reads
    // only its own variable.
    expect(loadConf({ env: { MUXWS_CODEC: 'msgpack' } }).settings.codec).toBe('json');
    expect(loadConf({ env: { VITE_MUXWS_CODEC: 'msgpack' } }).settings.codec).toBe('msgpack');
  });

  it('takes an empty value as configured rather than falling back to json - WSM-INV-015', () => {
    // `os.environ.get` also returns the empty string. A name that resolves to no codec must raise at
    // `getCodec`; it does not fall back to JSON.
    expect(loadConf({ env: { VITE_MUXWS_CODEC: '' } }).settings.codec).toBe('');
    // A non-string value is treated as not configured.
    expect(loadConf({ env: { VITE_MUXWS_CODEC: 7 } }).settings.codec).toBe('json');
  });

  it('degrades to json where import.meta.env does not exist - the Node and vitest case', () => {
    // Three cases without an environment: an import.meta without an env, an env-less object, and no
    // import.meta at all (the CommonJS case). None of them may throw during module load: the
    // singleton is constructed at import time, so a throw would make the module unloadable.
    expect(loadConf({}).settings.codec).toBe('json');
    expect(loadConf({ env: undefined }).settings.codec).toBe('json');
    expect(loadConf(undefined, false).settings.codec).toBe('json');
  });

  it('is a singleton constructed at import time', () => {
    // Under vitest, `import.meta.env` names no codec.
    expect(settings).toBeInstanceOf(Settings);
    expect(settings.codec).toBe('json');
    expect(String(settings)).toBe("Settings(codec='json')");
  });
});

describe('the acceptor', () => {
  const json = new JsonCodec();

  /**
   * A codec with its own name, so the subprotocol the acceptor selects shows which name it read. A
   * `JsonCodec` registered under another key would still send 'json' on the wire.
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
    // `./node` is imported before this line runs, so a value read at import time is 'json', and an
    // acceptor using it would refuse the socket below.
    registerCodec('bootstrap-set', marker);
    settings.codec = 'bootstrap-set';

    const socket = new FakeWsConnection('muxws.v1.bootstrap-set');
    const peer = await accept(asSocket(socket));

    expect(peer.isOpen).toBe(true);
    expect(peer.isDialer).toBe(false);
    expect(socket.binaryType).toBe('arraybuffer');
    expect(socket.closes).toEqual([]);
  });

  it('refuses the import-time codec after settings.codec has changed', async () => {
    // Converse of the test above: 'muxws.v1.json' no longer matches settings.codec, so an acceptor
    // that captured the value at import time would accept it and this test would fail.
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
    // Python asserts that a dial double recorded zero calls. Here the socket has no listeners, was
    // not reconfigured and was not closed.
    expect(socket.untouched).toBe(true);
  });

  it('lets an explicit codec argument win over settings - WSM-CDC-012', async () => {
    settings.codec = 'msgpack'; // unregistered: reading it raises
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
