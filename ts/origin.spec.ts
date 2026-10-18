/** `parseOrigin` and `compilePolicy`, the `Origin` check behind `accept(socket, { allowedOrigins })`. */

import { vi } from 'vitest';

import { compilePolicy, parseOrigin } from './origin';

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
    expect(check(['https://app.example.com:443'])).toBe(true);
    expect(check(['http://LOCALHOST:5173'])).toBe(true);
    expect(check(['http://app.example.com'])).toBe(false);
    expect(check(['https://evil.example.com'])).toBe(false);
  });

  it('refuses a missing, a repeated and an unparsable origin', () => {
    const check = compilePolicy(['https://app.example.com'])!;
    expect(check([])).toBe(false);
    expect(check(['https://app.example.com', 'https://app.example.com'])).toBe(false);
    expect(check(['null'])).toBe(false);
  });

  it('accepts any iterable, and an empty one refuses everything', () => {
    expect(compilePolicy(new Set(['https://a.test']))!(['https://a.test'])).toBe(true);
    expect(compilePolicy([])!(['https://a.test'])).toBe(false);
  });

  it('calls a function with the raw value or undefined', () => {
    const seen: (string | undefined)[] = [];
    const check = compilePolicy((origin) => {
      seen.push(origin);
      return origin === undefined || origin.endsWith('.example.com');
    })!;
    expect(check([])).toBe(true);
    expect(check(['https://a.example.com'])).toBe(true);
    expect(check(['https://evil.test'])).toBe(false);
    expect(seen).toEqual([undefined, 'https://a.example.com', 'https://evil.test']);
  });

  it('does not call the function for a repeated origin', () => {
    const decide = vi.fn(() => true);
    expect(compilePolicy(decide)!(['https://a.test', 'https://b.test'])).toBe(false);
    expect(decide).not.toHaveBeenCalled();
  });

  it('rejects a single string', () => {
    expect(() => compilePolicy('https://app.example.com')).toThrow(/not a single string/);
  });

  it.each(['app.example.com', 'https://app.example.com/path', 'null', 443])('rejects the entry %j', (entry) => {
    expect(() => compilePolicy([entry] as unknown as string[])).toThrow(/not an origin/);
  });
});
