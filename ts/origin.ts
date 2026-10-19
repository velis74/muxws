/**
 * The `Origin` check `accept()` runs when it is given `allowedOrigins`.
 *
 * A browser sends the cookies of the endpoint's site with a WebSocket handshake whatever page opened
 * it, and a WebSocket has no CSRF protection of its own. An endpoint whose session is a cookie answers
 * a handshake from another site's page with that session unless the `Origin` header is checked before
 * the socket is accepted.
 *
 * `muxws/origin.py` implements the same parsing and the same decision.
 */

/** The default port of each scheme a serialized origin may carry. */
const DEFAULT_PORTS: Readonly<Record<string, number>> = { http: 80, https: 443 };

/** Every value of one request header, looked up by lower-case name, in order. */
export type HeaderValues = (name: string) => readonly string[];

/** A compiled policy: the request headers of one handshake in, the decision out. */
export type OriginCheck = (headers: HeaderValues) => boolean;

/**
 * `[scheme, host, port]` of a serialized origin, or `null` when `value` is not one.
 *
 * An origin is `scheme://host[:port]` with the scheme `http` or `https`, surrounding whitespace and
 * one trailing `/` ignored. Scheme and host are lower-cased, and a missing port is the scheme's
 * default (80, 443). A path, a query, a fragment, userinfo, an empty or non-numeric port, a port
 * above 65535, another scheme and `null` all return `null`.
 */
export function parseOrigin(value: string): [string, string, number] | null {
  let text = value.trim();
  if (text.endsWith('/')) text = text.slice(0, -1);
  const separator = text.indexOf('://');
  if (separator < 0) return null;
  const scheme = text.slice(0, separator).toLowerCase();
  if (scheme !== 'http' && scheme !== 'https') return null;
  const parsed = parseAuthority(text.slice(separator + 3));
  if (parsed === null) return null;
  const [host, port] = parsed;
  return [scheme, host, port ?? DEFAULT_PORTS[scheme]];
}

/**
 * `[host, port]` of `host[:port]`, host lower-cased and port `null` when absent; `null` when invalid.
 *
 * The form of the authority in an origin and of a `Host` header value.
 */
function parseAuthority(authority: string): [string, number | null] | null {
  if (authority === '' || /[/?#@\s]/u.test(authority)) return null;

  let host: string;
  let portText: string | null;
  if (authority.startsWith('[')) {
    const end = authority.indexOf(']');
    if (end < 2) return null;
    host = authority.slice(0, end + 1);
    const rest = authority.slice(end + 1);
    if (rest !== '' && !rest.startsWith(':')) return null;
    portText = rest === '' ? null : rest.slice(1);
  } else {
    const parts = authority.split(':');
    if (parts.length > 2) return null;
    host = parts[0];
    portText = parts.length === 2 ? parts[1] : null;
    if (host === '') return null;
  }

  if (portText === null) return [host.toLowerCase(), null];
  if (!/^[0-9]+$/u.test(portText)) return null;
  const port = Number.parseInt(portText, 10);
  if (port > 65535) return null;
  return [host.toLowerCase(), port];
}

function originKey(parsed: [string, string, number]): string {
  return `${parsed[0]}://${parsed[1]}:${parsed[2]}`;
}

/** Every entry of `origins` under `parseOrigin`, as keys; `TypeError` for a string or a non-origin. */
function parseOrigins(origins: Iterable<string>, option: string): Set<string> {
  if (typeof origins === 'string') {
    throw new TypeError(`${option} is an iterable of origins, not a single string`);
  }
  const parsedOrigins = new Set<string>();
  for (const entry of origins as Iterable<unknown>) {
    const parsed = typeof entry === 'string' ? parseOrigin(entry) : null;
    if (parsed === null) {
      throw new TypeError(`${JSON.stringify(entry)} in ${option} is not an origin: scheme://host[:port]`);
    }
    parsedOrigins.add(originKey(parsed));
  }
  return parsedOrigins;
}

/** The request's `Origin` under `parseOrigin`, or `null` when it is missing, repeated or not an origin. */
function singleOrigin(headers: HeaderValues): [string, string, number] | null {
  const values = headers('origin');
  return values.length === 1 ? parseOrigin(values[0]) : null;
}

export interface SameOriginOptions {
  /** Origins allowed in addition to the request's own host. */
  also?: Iterable<string>;
  /** Compare with the first entry of `X-Forwarded-Host`, when the request has one, instead of `Host`. */
  trustForwardedHost?: boolean;
}

/**
 * The `allowedOrigins` policy that allows a page served by the host the handshake is addressed to.
 *
 * A request is allowed when its single `Origin` has the host and port of the request's single `Host`
 * header, or is one of the origins in `also`. A `Host` without a port stands for the default port of
 * the origin's scheme. The scheme is not compared: `Host` carries none, and behind a proxy that
 * terminates TLS the handshake reaching the acceptor is plain `ws`.
 *
 * With `trustForwardedHost`, the first entry of `X-Forwarded-Host` takes the place of `Host` when the
 * request has that header; a request without it is compared with `Host`. A browser cannot set either
 * header on a WebSocket handshake, so either one names the host the page addressed. Without
 * `trustForwardedHost`, a proxy that rewrites `Host` to the upstream's name makes every browser
 * request fail the comparison.
 *
 * A request without `Origin`, with more than one, with a value that is not an origin (`null`
 * included), or with no single parsable `Host` (or `X-Forwarded-Host` entry) is refused unless its
 * origin is in `also`.
 *
 * Throws `TypeError` when `also` is a single string or one of its entries is not an origin.
 *
 * `muxws/origin.py` implements the same decision as `SameOrigin`.
 */
export class SameOrigin {
  private readonly also: ReadonlySet<string>;
  private readonly trustForwardedHost: boolean;

  constructor(options: SameOriginOptions = {}) {
    this.also = parseOrigins(options.also ?? [], 'also');
    this.trustForwardedHost = options.trustForwardedHost ?? false;
  }

  private requestHost(headers: HeaderValues): [string, number | null] | null {
    if (this.trustForwardedHost) {
      const forwarded = headers('x-forwarded-host');
      if (forwarded.length > 0) return parseAuthority(forwarded.join(',').split(',')[0].trim());
    }
    const hosts = headers('host');
    return hosts.length === 1 ? parseAuthority(hosts[0].trim()) : null;
  }

  /** @internal The decision `accept()` runs; see the class. */
  allows(headers: HeaderValues): boolean {
    const origin = singleOrigin(headers);
    if (origin === null) return false;
    if (this.also.has(originKey(origin))) return true;
    const requestHost = this.requestHost(headers);
    if (requestHost === null) return false;
    const [scheme, host, port] = origin;
    const [requestName, requestPort] = requestHost;
    return requestName === host && (requestPort ?? DEFAULT_PORTS[scheme]) === port;
  }
}

/** The `allowedOrigins` option: exact origins, `SameOrigin`, or a function of the `Origin` header value. */
export type OriginPolicy = Iterable<string> | SameOrigin | ((origin: string | undefined) => boolean);

/**
 * The check `accept()` runs for `allowedOrigins`, or `null` when there is nothing to check.
 *
 * - A `SameOrigin` compares `Origin` with the request's host; see `SameOrigin`.
 * - A function is called with the request's single `Origin` value, or with `undefined` when the
 *   request has none; its truthiness is the decision. A request with more than one `Origin` header is
 *   refused without calling it.
 * - An iterable of origins allows a request whose single `Origin` value parses to one of them under
 *   `parseOrigin`. A request without `Origin`, with more than one, or with a value that is not an
 *   origin (`null` included) is refused.
 *
 * Throws `TypeError` for a string (an iterable of its characters) and for an entry that is not an
 * origin.
 */
export function compilePolicy(allowedOrigins: OriginPolicy | undefined): OriginCheck | null {
  if (allowedOrigins === undefined) return null;
  if (allowedOrigins instanceof SameOrigin) {
    const policy = allowedOrigins;
    return (headers) => policy.allows(headers);
  }
  if (typeof allowedOrigins === 'function') {
    const decide = allowedOrigins;
    return (headers) => {
      const values = headers('origin');
      return values.length <= 1 && Boolean(decide(values[0]));
    };
  }
  if (typeof allowedOrigins === 'string') {
    throw new TypeError('allowedOrigins is an iterable of origins or a function, not a single string');
  }
  const allowed = parseOrigins(allowedOrigins, 'allowedOrigins');
  return (headers) => {
    const origin = singleOrigin(headers);
    return origin !== null && allowed.has(originKey(origin));
  };
}
