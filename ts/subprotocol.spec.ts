/**
 * The subprotocol assertion (§2.3, WSM-CDC-020..029).
 *
 * Mirrors the handshake half of `muxws/codec_test.py`. The settings half lives in `conf.spec.ts`,
 * the registry half in `codec.spec.ts`, and the post-open verification - which is a transport's
 * job - in `transports/browser-socket.spec.ts`.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not declare it as one.
import { vi } from 'vitest';

import { CodecMismatch } from './errors';
import { PREFIX, findOffer, generationOf, mismatchError, offer, select } from './subprotocol';

/**
 * `select` with the acceptor's own half of the diagnostic captured (WSM-CDC-029).
 *
 * `caplog` in Python; the ERROR line goes to the console here, and a test that let it through
 * would print refusals over a passing run.
 */
function selectAndCapture(offered: readonly string[], configured: string): { selected: string | null; logged: string } {
  const lines: string[] = [];
  const spy = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    lines.push(args.map((argument) => String(argument)).join(' '));
  });
  try {
    return { selected: select(offered, configured), logged: lines.join('\n') };
  } finally {
    spy.mockRestore();
  }
}

describe('the dialer', () => {
  it('offers the muxws entry first, and leaves the application entries untouched - WSM-CDC-020/021', () => {
    const offered = offer('json', ['bearer.abc123', 'x-app']);
    expect(offered[0]).toBe('muxws.v1.json');
    expect(offered.slice(1)).toEqual(['bearer.abc123', 'x-app']);
    expect(findOffer(offered)).toBe('muxws.v1.json');
  });

  it('composes its own mismatch error, naming both variables - WSM-CDC-024', () => {
    // A browser cannot read a rejection body, so the diagnostic cannot come from the server.
    const error = mismatchError('msgpack');
    expect(error).toBeInstanceOf(CodecMismatch);
    expect(error.name).toBe('CodecMismatch');
    expect(error.message).toContain('msgpack');
    expect(error.message).toContain('VITE_MUXWS_CODEC');
    expect(error.message).toContain('MUXWS_CODEC');
    expect(error.configured).toBe('msgpack');
  });
});

describe('the acceptor', () => {
  it('ignores every offered entry but the muxws one, in either order - WSM-CDC-021', () => {
    expect(select(['muxws.v1.json', 'bearer.abc123'], 'json')).toBe('muxws.v1.json');
    expect(select(['bearer.abc123', 'muxws.v1.json'], 'json')).toBe('muxws.v1.json');
  });

  it('refuses a mismatched codec and logs its half - WSM-CDC-022/029', () => {
    const { selected, logged } = selectAndCapture(['muxws.v1.msgpack'], 'json');
    expect(selected).toBeNull();
    expect(logged).toContain('msgpack');
    expect(logged).toContain('json');
    expect(logged).toContain('MUXWS_CODEC');
    expect(logged).toContain('VITE_MUXWS_CODEC');
  });

  it('refuses a different generation - WSM-CDC-025', () => {
    expect(selectAndCapture(['muxws.v2.json'], 'json').selected).toBeNull();
    // Twice, because a refusal that only happens the first time is a refusal that leaks state.
    expect(selectAndCapture(['muxws.v2.json'], 'json').selected).toBeNull();
  });

  it('names a different generation as one, rather than as an absent entry - WSM-CDC-025', () => {
    const { selected, logged } = selectAndCapture(['muxws.v2.json'], 'json');
    expect(selected).toBeNull();
    expect(logged).toContain('generation 2');
    expect(logged).toContain('WSM-CDC-025');
  });

  it('refuses an offer carrying no muxws entry at all', () => {
    const { selected, logged } = selectAndCapture(['bearer.abc'], 'json');
    expect(selected).toBeNull();
    expect(logged).toContain('no muxws.v1.* subprotocol');
  });
});

describe('the generation prefix', () => {
  it('is the only version on the wire - WSM-CON-009/WSM-PKG-005', () => {
    expect(PREFIX).toBe('muxws.v1.');
    expect(offer('json')[0].match(/\./g)).toHaveLength(2);
  });

  it('is what generationOf reads - WSM-CDC-025', () => {
    expect(generationOf('muxws.v1.json')).toBe(1);
    expect(generationOf('muxws.v2.msgpack')).toBe(2);
    expect(generationOf('bearer.abc')).toBeNull();
    expect(generationOf('muxws.json')).toBeNull();
  });
});

describe('the peer', () => {
  it('has no codec branch to take, because this is an assertion - WSM-CDC-023', () => {
    // The Python twin reads its own source with `inspect.getsource`; the TypeScript sources are on
    // disk beside this file, and `process.cwd()` is the vitest root.
    ['peer.ts', 'stream.ts'].forEach((name) => {
      const source = readFileSync(join(process.cwd(), 'ts', name), 'utf8');
      expect(source).not.toContain('codec.name ==');
      expect(source).not.toContain("codec === 'json'");
      expect(source).not.toContain("=== 'json'");
      expect(source).not.toContain('instanceof JsonCodec');
    });
  });
});
