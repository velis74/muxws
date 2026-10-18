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

/** The `allowedOrigins` option: exact origins, or a function of the `Origin` header value. */
export type OriginPolicy = Iterable<string> | ((origin: string | undefined) => boolean);

/** A compiled policy: the `Origin` header values of one request in, the decision out. */
export type OriginCheck = (values: readonly string[]) => boolean;

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
  const authority = text.slice(separator + 3);
  if (scheme !== 'http' && scheme !== 'https') return null;
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

  let port: number;
  if (portText === null) {
    port = DEFAULT_PORTS[scheme];
  } else {
    if (!/^[0-9]+$/u.test(portText)) return null;
    port = Number.parseInt(portText, 10);
    if (port > 65535) return null;
  }
  return [scheme, host.toLowerCase(), port];
}

function originKey(parsed: [string, string, number]): string {
  return `${parsed[0]}://${parsed[1]}:${parsed[2]}`;
}

/**
 * The check `accept()` runs for `allowedOrigins`, or `null` when there is nothing to check.
 *
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
  if (typeof allowedOrigins === 'function') {
    const decide = allowedOrigins;
    return (values) => values.length <= 1 && Boolean(decide(values[0]));
  }
  if (typeof allowedOrigins === 'string') {
    throw new TypeError('allowedOrigins is an iterable of origins or a function, not a single string');
  }
  const allowed = new Set<string>();
  for (const entry of allowedOrigins as Iterable<unknown>) {
    const parsed = typeof entry === 'string' ? parseOrigin(entry) : null;
    if (parsed === null) {
      throw new TypeError(`${JSON.stringify(entry)} in allowedOrigins is not an origin: scheme://host[:port]`);
    }
    allowed.add(originKey(parsed));
  }
  return (values) => {
    if (values.length !== 1) return false;
    const parsed = parseOrigin(values[0]);
    return parsed !== null && allowed.has(originKey(parsed));
  };
}
