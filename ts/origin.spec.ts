/** `parseOrigin`, `SameOrigin` and `compilePolicy`, the `Origin` check behind `accept(socket, { allowedOrigins })`. */

import { vi } from 'vitest';

import { type HeaderValues, SameOrigin, compilePolicy, parseOrigin } from './origin';

import * as muxws from './index';

/** A request with the header lines `lines`, looked up by lower-case name. */
function headers(...lines: (readonly [string, string])[]): HeaderValues {
  return (name) => lines.filter(([header]) => header === name).map(([, value]) => value);
}

/** A request whose only headers are `values` as its `Origin` header lines. */
function origins(...values: string[]): HeaderValues {
  return headers(...values.map((value) => ['origin', value] as const));
}

describe('parseOrigin', () => {
  it.each([
    ['https://example.com', ['https', 'example.com', 443]],
    ['http://example.com', ['http', 'example.com', 80]],
    ['HTTPS://Example.COM', ['https', 'example.com', 443]],
    ['https://example.com:443', ['https', 'example.com', 443]],
    ['https://example.com/', ['https', 'example.com', 443]],
    [' https://example.com ', ['https', 'example.com', 443]],
    ['http://localhost:5173', ['http', 'localhost', 5173]],
    ['http://[::1]:8000', ['http', '[::1]', 8000]],
    ['http://[::1]', ['http', '[::1]', 80]],
  ])('parses %s to scheme, host and explicit port', (value, expected) => {
    expect(parseOrigin(value)).toEqual(expected);
  });

  it.each([
    'null',
    '',
    'example.com',
    'ftp://example.com',
    'ws://example.com',
    'constructor://example.com',
    'https://example.com/path',
    'https://example.com?query',
    'https://example.com#fragment',
    'https://user@example.com',
    'https://example.com:',
    'https://example.com:port',
    'https://example.com:65536',
    'https://example.com:+80',
    'https://a:1:2',
    'https://',
    'https://:443',
    'http://[]',
    'http://[::1]x',
    'https://exa mple.com',
  ])('rejects %j', (value) => {
    expect(parseOrigin(value)).toBeNull();
  });
});

describe('compilePolicy', () => {
  it('compiles no policy to no check', () => {
    expect(compilePolicy(undefined)).toBeNull();
  });

  it('allows a listed origin after normalisation', () => {
    const check = compilePolicy(['https://App.example.com', 'http://localhost:5173/'])!;
    expect(check(origins('https://app.example.com:443'))).toBe(true);
    expect(check(origins('http://LOCALHOST:5173'))).toBe(true);
    expect(check(origins('http://app.example.com'))).toBe(false);
    expect(check(origins('https://evil.example.com'))).toBe(false);
  });

  it('refuses a missing, a repeated and an unparsable origin', () => {
    const check = compilePolicy(['https://app.example.com'])!;
    expect(check(origins())).toBe(false);
    expect(check(origins('https://app.example.com', 'https://app.example.com'))).toBe(false);
    expect(check(origins('null'))).toBe(false);
  });

  it('accepts any iterable, and an empty one refuses everything', () => {
    expect(compilePolicy(new Set(['https://a.test']))!(origins('https://a.test'))).toBe(true);
    expect(compilePolicy([])!(origins('https://a.test'))).toBe(false);
  });

  it('calls a function with the raw value or undefined', () => {
    const seen: (string | undefined)[] = [];
    const check = compilePolicy((origin) => {
      seen.push(origin);
      return origin === undefined || origin.endsWith('.example.com');
    })!;
    expect(check(origins())).toBe(true);
    expect(check(origins('https://a.example.com'))).toBe(true);
    expect(check(origins('https://evil.test'))).toBe(false);
    expect(seen).toEqual([undefined, 'https://a.example.com', 'https://evil.test']);
  });

  it('does not call the function for a repeated origin', () => {
    const decide = vi.fn(() => true);
    expect(compilePolicy(decide)!(origins('https://a.test', 'https://b.test'))).toBe(false);
    expect(decide).not.toHaveBeenCalled();
  });

  it('rejects a single string', () => {
    expect(() => compilePolicy('https://app.example.com')).toThrow(/not a single string/);
  });

  it.each(['app.example.com', 'https://app.example.com/path', 'null', 443])('rejects the entry %j', (entry) => {
    expect(() => compilePolicy([entry] as unknown as string[])).toThrow(/not an origin/);
  });
});

describe('SameOrigin', () => {
  function sameOrigin(policy: SameOrigin, ...lines: (readonly [string, string])[]): boolean {
    return compilePolicy(policy)!(headers(...lines));
  }

  it.each([
    ['https://app.example.com', 'app.example.com'],
    ['http://app.example.com', 'app.example.com'],
    ['https://APP.example.com', 'app.EXAMPLE.com'],
    ['https://app.example.com', 'app.example.com:443'],
    ['http://app.example.com', 'app.example.com:80'],
    ['http://localhost:5173', 'localhost:5173'],
    ['http://[::1]:8000', '[::1]:8000'],
  ])('allows %s for Host %s', (origin, host) => {
    expect(sameOrigin(new SameOrigin(), ['origin', origin], ['host', host])).toBe(true);
  });

  it.each([
    ['https://evil.test', 'app.example.com'],
    ['https://app.example.com', 'app.example.com:8443'],
    ['https://app.example.com:8443', 'app.example.com'],
    ['http://app.example.com', 'app.example.com:443'],
    ['https://sub.app.example.com', 'app.example.com'],
    ['null', 'app.example.com'],
  ])('refuses %s for Host %s', (origin, host) => {
    expect(sameOrigin(new SameOrigin(), ['origin', origin], ['host', host])).toBe(false);
  });

  it.each([
    ['no Origin', [['host', 'app.example.com']]],
    [
      'a repeated Origin',
      [
        ['origin', 'https://app.example.com'],
        ['origin', 'https://app.example.com'],
        ['host', 'app.example.com'],
      ],
    ],
    ['no Host', [['origin', 'https://app.example.com']]],
    [
      'a repeated Host',
      [
        ['origin', 'https://app.example.com'],
        ['host', 'app.example.com'],
        ['host', 'app.example.com'],
      ],
    ],
    [
      'an invalid Host',
      [
        ['origin', 'https://app.example.com'],
        ['host', 'app.example.com/path'],
      ],
    ],
    [
      'an empty Host',
      [
        ['origin', 'https://app.example.com'],
        ['host', ''],
      ],
    ],
  ] as const)('refuses %s', (_name, lines) => {
    expect(sameOrigin(new SameOrigin(), ...lines)).toBe(false);
  });

  it('ignores X-Forwarded-Host unless trusted', () => {
    const lines = [
      ['origin', 'https://app.example.com'],
      ['host', 'backend:8000'],
      ['x-forwarded-host', 'app.example.com'],
    ] as const;
    expect(sameOrigin(new SameOrigin(), ...lines)).toBe(false);
    expect(sameOrigin(new SameOrigin({ trustForwardedHost: true }), ...lines)).toBe(true);
  });

  it('replaces Host with the first entry of a trusted X-Forwarded-Host', () => {
    const policy = new SameOrigin({ trustForwardedHost: true });
    const origin = ['origin', 'https://app.example.com'] as const;
    expect(sameOrigin(policy, origin, ['host', 'app.example.com'], ['x-forwarded-host', 'other.example.com'])).toBe(
      false,
    );
    expect(
      sameOrigin(policy, origin, ['host', 'backend'], ['x-forwarded-host', 'app.example.com, proxy.internal']),
    ).toBe(true);
    expect(
      sameOrigin(
        policy,
        origin,
        ['host', 'backend'],
        ['x-forwarded-host', 'app.example.com'],
        ['x-forwarded-host', 'b'],
      ),
    ).toBe(true);
    expect(sameOrigin(policy, origin, ['host', 'backend'], ['x-forwarded-host', ', app.example.com'])).toBe(false);
  });

  it('falls back to Host when a trusted X-Forwarded-Host is absent', () => {
    const policy = new SameOrigin({ trustForwardedHost: true });
    expect(sameOrigin(policy, ['origin', 'http://localhost:8000'], ['host', 'localhost:8000'])).toBe(true);
  });

  it('also allows the listed origins', () => {
    const policy = new SameOrigin({ also: ['http://localhost:5173'] });
    expect(sameOrigin(policy, ['origin', 'http://LOCALHOST:5173/'], ['host', 'localhost:8000'])).toBe(true);
    expect(sameOrigin(policy, ['origin', 'http://localhost:5173'])).toBe(true);
    expect(sameOrigin(policy, ['origin', 'http://localhost:8000'], ['host', 'localhost:8000'])).toBe(true);
    expect(sameOrigin(policy, ['origin', 'http://localhost:3000'], ['host', 'localhost:8000'])).toBe(false);
  });

  it('reads `also` once', () => {
    function* generated(): Generator<string> {
      yield 'https://a.test';
    }
    const policy = new SameOrigin({ also: generated() });
    expect(sameOrigin(policy, ['origin', 'https://a.test'])).toBe(true);
    expect(sameOrigin(policy, ['origin', 'https://a.test'])).toBe(true);
  });

  it('validates `also` on construction', () => {
    expect(() => new SameOrigin({ also: 'https://app.example.com' })).toThrow(/not a single string/);
    expect(() => new SameOrigin({ also: ['app.example.com'] })).toThrow(/in also is not an origin/);
  });

  it('is exported with parseOrigin from the package', () => {
    expect(muxws.SameOrigin).toBe(SameOrigin);
    expect(muxws.parseOrigin).toBe(parseOrigin);
  });
});
