/**
 * `ws+unix:` end to end: the same muxws handshake, carried over a filesystem socket.
 *
 * The tests assert that dialling a unix-domain socket changes **nothing** in the protocol.
 * `ts/node.ts` has one branch for it: the url is split into a file and a request target, and `ws`
 * receives an ordinary `ws:` url plus a `createConnection` that opens the file. The bytes on the wire
 * are the GET + `Upgrade` + `Sec-WebSocket-Protocol: muxws.v1.<codec>` the TCP tests pin, and the
 * answer is 101 or 400. The refusal path runs through `unexpected-response`, an event whose behaviour
 * over an AF_UNIX request is not documented by `ws`.
 *
 * A `ws+unix:` url names both a socket file and a request target, and the two ports must parse it
 * identically; otherwise the same url in both configurations reaches two different request targets
 * without an error. Every url shape that `ws`'s own split and `muxws/transports/unix.py` parse
 * differently is pinned below against the target the acceptor received.
 *
 * The checks are those `ts/node.spec.ts` uses over TCP (a raw HTTP status read from the upgrade, and
 * the acceptor's `connection` counter left at zero, WSM-CDC-022) plus a full round trip through
 * `connect()`, because a transport that completes the handshake and then cannot carry a frame would
 * pass every status assertion.
 *
 * The unix tests are skipped on Windows, which has no AF_UNIX. The browser-entry block at the bottom
 * is not skipped: it opens no socket.
 */

import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { type Codec, JsonCodec } from './codec';
import { CodecMismatch, MuxwsError, TransportUnsupportedError, TransportUrlError } from './errors';
import { Reconnect, UnixUrlError, WsUrlError, connect, handleProtocols, refuseMismatchedUpgrade, serve } from './node';

// Importing the browser entry point registers the JSON codec (WSM-CDC-004), which `ts/node.ts` does
// not do. It is also imported for its `connect`: the two entry points handle `ws+unix:` differently,
// and this file tests both.
import { UnixSocketsUnsupportedError, connect as platformConnect } from './index';

// --------------------------------------------------------------------------- the harness

/** The request target and `Host` the acceptor received. */
interface Seen {
  url: string | undefined;
  host: string | undefined;
}

interface UnixAcceptor {
  /** The socket file, absolute, short enough to fit `sun_path`. */
  path: string;
  /** The directory it lives in, so a test can name a sibling that does not exist. */
  directory: string;
  /** As in `ts/node.spec.ts`: a refused upgrade must leave this at zero (WSM-CDC-022). */
  handshakes: () => number;
  seen: () => Seen;
  /** Every payload that arrived as an incoming stream, hellos included, in order. */
  received: () => unknown[];
  close: () => Promise<void>;
}

/**
 * A real `ws` acceptor on a socket file, built as the documentation describes for applications.
 *
 * `WebSocketServer` cannot listen on a path itself, so the acceptor uses an `http.Server` bound to
 * the socket file, `{ server }` instead of `{ port }`, and both handshake hooks: `handleProtocols`
 * to select and `refuseMismatchedUpgrade` to refuse. An acceptor must install both.
 *
 * The directory comes from `mkdtemp` under the OS temp dir with a two-character prefix: `sun_path`
 * limits a socket address to ~108 bytes, and a longer path produces `EINVAL: invalid argument <the
 * whole path>`, which does not mention the length.
 *
 * `existing` is for the test that needs two acceptors at the same path in sequence: a reconnect must
 * reach the **new** inode a restarted acceptor binds at that path.
 */
async function startUnixAcceptor(existing?: string): Promise<UnixAcceptor> {
  let handshakes = 0;
  let seen: Seen = { url: undefined, host: undefined };
  const received: unknown[] = [];
  const directory = existing === undefined ? mkdtempSync(join(tmpdir(), 'mx-')) : dirname(existing);
  const path = existing ?? join(directory, 's');

  const httpServer = http.createServer();
  const server = refuseMismatchedUpgrade(new WebSocketServer({ server: httpServer, handleProtocols }));
  server.on('connection', (socket, request) => {
    handshakes += 1;
    seen = { url: request.url, host: request.headers.host };
    // Redundant `.catch`, as in `ts/node.spec.ts`: `serve()` returns on `CodecMismatch`, and an
    // unhandled rejection in a `ws` connection handler terminates the process.
    void serve(socket, {
      handler: (payload, stream) => {
        received.push(payload);
        return stream.reply({ echo: payload });
      },
    }).catch(() => undefined);
  });
  await new Promise<void>((resolve) => httpServer.listen(path, resolve));

  return {
    path,
    directory,
    handshakes: () => handshakes,
    seen: () => seen,
    received: () => received,
    // Both servers, in this order: `wss.close()` does not close a server passed to it. Closing the
    // `http.Server` unlinks the socket file; a server left open leaves a stale file for the next run.
    // It does not finish while a connection is open, so open connections are terminated first. The
    // directory is kept: the reconnect test closes one acceptor and binds the next at the same path.
    // `afterEach` removes the directory.
    close: async () => {
      server.clients.forEach((client) => client.terminate());
      server.close();
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    },
  };
}

interface Answer {
  status: number;
  protocol: string | undefined;
}

/**
 * `ts/node.spec.ts`'s `upgrade()` with `socketPath` where the host and port were.
 *
 * This substitution is the only difference between the two transports at the HTTP layer: the
 * request, the headers and the two response events are unchanged, so the status code can be read
 * over a socket file as over TCP.
 */
function upgradeOverUnix(socketPath: string, path: string, offered: string): Promise<Answer> {
  return new Promise<Answer>((resolve, reject) => {
    const request = http.request({
      socketPath,
      path,
      headers: {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        // Fixed, as over TCP: nothing here verifies `Sec-WebSocket-Accept`.
        'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Protocol': offered,
      },
    });
    const answer = (status: number | undefined, protocol: string | string[] | undefined): void => {
      resolve({ status: status ?? 0, protocol: Array.isArray(protocol) ? protocol.join(',') : protocol });
    };
    request.on('response', (response) => {
      response.resume();
      answer(response.statusCode, response.headers['sec-websocket-protocol']);
    });
    request.on('upgrade', (response, socket) => {
      socket.destroy();
      answer(response.statusCode, response.headers['sec-websocket-protocol']);
    });
    request.on('error', reject);
    request.end();
  });
}

/** Poll until `predicate` holds, as `ts/reconnect.spec.ts` does: a supervisor runs on its own timers. */
async function until(predicate: () => boolean, what: string, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`${what} never became true within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** A codec used only for its **name**, so the dialer offers `muxws.v1.msgpack`. */
function msgpackish(): Codec {
  const json = new JsonCodec();
  return {
    name: 'msgpack',
    binary: false,
    encode: (frame) => json.encode(frame),
    decode: (message) => json.decode(message),
    encodePayload: (payload) => json.encodePayload(payload),
    decodePayload: (data) => json.decodePayload(data),
  };
}

// --------------------------------------------------------------------------- over a socket file

describe.skipIf(process.platform === 'win32')('dialling ws+unix: from muxws/node', () => {
  let acceptor: UnixAcceptor;

  beforeEach(async () => {
    acceptor = await startUnixAcceptor();
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await acceptor.close();
    // `mkdtemp` has no context manager like Python's `TemporaryDirectory` in `websockets_test.py`, so
    // without this the suite leaves one empty directory per test in the OS temp dir. `force`, because
    // a test that failed before the socket was bound must still clean up.
    rmSync(acceptor.directory, { recursive: true, force: true });
  });

  it('round trips a request over a socket file, with the route intact', async () => {
    const peer = await connect(`ws+unix://${acceptor.path}:/route`);

    try {
      expect(await peer.request({ hi: 1 })).toEqual({ echo: { hi: 1 } });
    } finally {
      await peer.close();
    }

    // The handshake happened on this acceptor. Without the counter, a dial that reached another
    // server would also satisfy the assertion above.
    expect(acceptor.handshakes()).toBe(1);
    // The part of the url before the ':' opened the file, and the part after it became the request
    // target. The `Host` is node's default for an empty authority and equals the value the Python
    // port builds as `uri="ws://localhost<route>"`, so both ports send the same bytes for the same url.
    expect(acceptor.seen()).toEqual({ url: '/route', host: 'localhost' });
  });

  it('defaults the request target to / when the url carries no route', async () => {
    // `ws+unix:///path.sock` without ':' is the short form the documentation shows first.
    // `unixTarget` supplies '/' instead of an empty target, so the two ports send the same request
    // line for the same url.
    const peer = await connect(`ws+unix://${acceptor.path}`);

    try {
      expect(await peer.request({ hi: 2 })).toEqual({ echo: { hi: 2 } });
    } finally {
      await peer.close();
    }

    expect(acceptor.seen().url).toBe('/');
  });

  it('splits on the first colon only, so a target may contain one', async () => {
    // The row `only-the-first-colon-splits` in `muxws/transports/unix_test.py` is the Python
    // counterpart: both ports must reach the same request target for the same url. Passed to `ws`
    // unparsed these would become `/ws` and `/r?t=a` (`ws` runs `opts.path.split(':')` and keeps
    // `parts[1]`), a truncation the dialer does not see.
    for (const [route, expected] of [
      [':/ws:v2', '/ws:v2'],
      [':/r?t=a:b', '/r?t=a:b'],
      [':/ws?since=2026-08-14T10:00:00Z', '/ws?since=2026-08-14T10:00:00Z'],
    ]) {
      const peer = await connect(`ws+unix://${acceptor.path}${route}`);
      try {
        expect(await peer.request({ hi: route })).toEqual({ echo: { hi: route } });
      } finally {
        await peer.close();
      }
      expect(acceptor.seen().url, `the target for ${route}`).toBe(expected);
    }
  });

  it('opens the socket path as written in the url, without percent-encoding or decoding it', async () => {
    // Counterpart of the rows `the-socket-path-is-not-percent-encoded` and
    // `the-socket-path-is-not-percent-decoded` in `muxws/transports/unix_test.py`. A space and a
    // non-ASCII character name the directory as written, and `%20` in a file name is three characters.
    const root = mkdtempSync(join(tmpdir(), 'mx-'));
    const spaced = join(root, 'my dir ä');
    mkdirSync(spaced);
    const acceptors = [await startUnixAcceptor(join(spaced, 's')), await startUnixAcceptor(join(root, 'a%20b'))];
    try {
      for (const target of acceptors) {
        const peer = await connect(`ws+unix://${target.path}:/r`);
        try {
          expect(await peer.request({ hi: target.path })).toEqual({ echo: { hi: target.path } });
        } finally {
          await peer.close();
        }
        expect(target.handshakes(), `the acceptor at ${target.path}`).toBe(1);
        expect(target.seen().url).toBe('/r');
      }
    } finally {
      for (const target of acceptors) await target.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('keeps a trailing space of the url in the socket path, where new URL() would remove it', async () => {
    // Counterpart of the row `a-trailing-space-belongs-to-the-socket-path` in
    // `muxws/transports/unix_test.py`: `urlsplit` removes leading C0 controls and spaces only.
    const root = mkdtempSync(join(tmpdir(), 'mx-'));
    const target = await startUnixAcceptor(join(root, 'tail '));
    try {
      const peer = await connect(`ws+unix://${target.path}`);
      try {
        expect(await peer.request({ hi: 3 })).toEqual({ echo: { hi: 3 } });
      } finally {
        await peer.close();
      }
      expect(target.handshakes()).toBe(1);
      expect(target.seen().url).toBe('/');
    } finally {
      await target.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('refuses a request target that is not an absolute path, by name', async () => {
    // Counterpart of `test_a_request_target_that_is_not_an_absolute_path_is_refused`. Both ports
    // refuse the url before dialling: `ws://localhost` + `ws` is `ws://localhostws`, a valid url, so
    // the target becomes part of the authority and the dial requests `/` from the correct file with
    // an unintended `Host`. Against an acceptor that does not route (this one, and `unix_serve`) that
    // handshake succeeds. Passed to `ws` directly, it becomes `GET route HTTP/1.1`, which node answers
    // with 400 and `dialWs` reports as a refused codec, pointing at `MUXWS_CODEC` for a url typo.
    const caught = await connect(`ws+unix://${acceptor.path}:route`).then(
      () => null,
      (error: unknown) => error,
    );

    // A `MuxwsError`, so an `instanceof MuxwsError` handler around a dial catches it (WSM-ERR-016).
    expect(caught).toBeInstanceOf(UnixUrlError);
    expect(caught).toBeInstanceOf(TransportUrlError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect(caught).not.toBeInstanceOf(CodecMismatch);
    expect((caught as Error).message).toContain("must begin with '/'");
    expect(acceptor.handshakes(), 'nothing may have been dialled').toBe(0);
  });

  it('refuses a url that names no socket file, before `ws` is asked to read it', async () => {
    // `ws+unix://` parses with an empty authority and an empty path, so it names no file. `ws`
    // reports `SyntaxError: The URL's pathname is empty`, which does not describe the expected form
    // of a ws+unix: url; a caller who wrote `ws+unix://run`, with the file in the authority, needs
    // that form. Refused here as `parse_unix_url` refuses it, with the same class name.
    const caught = await connect('ws+unix://').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(UnixUrlError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toContain('names no socket file');
    expect(acceptor.handshakes(), 'nothing may have been dialled').toBe(0);
  });

  it('answers a mismatched offer with HTTP 400 on the upgrade - WSM-CDC-022', async () => {
    // Counterpart of the Python test. A dialer reports `CodecMismatch` both when the acceptor refused
    // the upgrade with 400 and when it completed the upgrade and WSM-CDC-028 detected the mismatch
    // afterwards. Only the raw status distinguishes the two, so this test checks that the refusal
    // over a unix socket is a 400.
    vi.spyOn(console, 'error').mockImplementation(() => undefined);

    expect(await upgradeOverUnix(acceptor.path, '/route', 'muxws.v1.msgpack')).toEqual({
      status: 400,
      protocol: undefined,
    });
    expect(acceptor.handshakes(), 'a refused upgrade must never reach the connection handler').toBe(0);

    // Control: an acceptor that refuses every offer would also pass the 400 test above.
    expect(await upgradeOverUnix(acceptor.path, '/route', 'muxws.v1.json')).toEqual({
      status: 101,
      protocol: 'muxws.v1.json',
    });
    expect(acceptor.handshakes()).toBe(1);
  });

  it('surfaces a refused upgrade to the dialer as CodecMismatch - WSM-CDC-024', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);

    const caught = await connect(`ws+unix://${acceptor.path}:/route`, { codec: msgpackish() }).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(CodecMismatch);
    // The full diagnostic, not only the class: this is the `unexpected-response` branch of `dialWs`,
    // which reads the status from the response object. The test checks that the event carries a
    // status when the request uses an AF_UNIX socket instead of TCP.
    const message = (caught as Error).message;
    expect(message).toContain('msgpack');
    expect(message).toContain('MUXWS_CODEC');
    expect(acceptor.handshakes(), 'the upgrade must have been refused, not completed and closed').toBe(0);
  });

  it('leaves a socket file that does not exist as its own error', async () => {
    // Unix counterpart of the ECONNREFUSED tests in `ts/node.spec.ts`: `dialWs` must not report a
    // missing acceptor as a codec mismatch. Nothing is listening, so no codec was refused.
    const caught = await connect(`ws+unix://${acceptor.directory}/missing.sock:/route`).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(CodecMismatch);
    expect((caught as Error).message).toContain('ENOENT');
  });

  it("rejects wss+unix:, which is not a scheme, as this port's own grammar error", async () => {
    // The refusal from `ws` lists the schemes **`ws`** accepts, as if `ws+unix:` were a `ws` feature
    // and not muxws url syntax. The refusal comes from this module, so the class does too
    // (WSM-ERR-016), and the message states why there is no TLS variant.
    const caught = await connect(`wss+unix://${acceptor.path}:/route`).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(UnixUrlError);
    expect(caught).toBeInstanceOf(TransportUrlError);
    expect((caught as Error).message).toContain('ws+unix:');
    expect(acceptor.handshakes(), 'nothing may have been dialled').toBe(0);
  });

  it('reconnects across a re-created socket file - WSM-RCN-020/030', async () => {
    // Counterpart of `test_a_peer_reconnects_across_a_re_created_socket_file`. The acceptor's file is
    // unlinked when it closes and the next acceptor binds a new inode at the same name, so a dialer
    // that resolved the path once (to a descriptor or anything else) reconnects to a socket nothing
    // listens on and hangs. The dial closure runs `unixTarget` and `net.connect` from the url on every
    // dial; this test fails if the connector is moved out of the closure.
    const path = acceptor.path;
    const reconnects: number[] = [];
    const peer = await connect(`ws+unix://${path}:/route`, {
      hello: { tab: 'abc' },
      reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      onReconnect: (attempt) => reconnects.push(attempt),
    });

    try {
      await until(() => acceptor.received().length === 1, 'the first hello');
      expect(acceptor.received()).toEqual([{ tab: 'abc' }]);

      await acceptor.close();
      // Reassigned so `afterEach` closes the running acceptor and not the closed one: closing a
      // `WebSocketServer` twice emits an `error` with no listener.
      acceptor = await startUnixAcceptor(path);

      await until(() => reconnects.length === 1, 'onReconnect');
      expect(acceptor.received(), 'the hello is replayed verbatim on the new inode').toEqual([{ tab: 'abc' }]);
      // The new connection carries a request, not only opens.
      expect(await peer.request({ after: 'the reconnect' })).toEqual({ echo: { after: 'the reconnect' } });
    } finally {
      await peer.close();
    }
  });
});

// --------------------------------------------------------------------------- the url, without a socket

describe('the ws+unix: grammar, where no socket is needed to read it', () => {
  // No skip: `new URL()` is the same parser on every platform.
  it('cannot address a Windows named pipe, whatever the platform', async () => {
    // `net.connect({ path })` opens a named pipe on Windows, but `\\.\pipe\name` cannot be written as
    // a url: a backslash is a forbidden code point in the authority of a non-special scheme, so the
    // WHATWG parser rejects the string on Linux as on Windows. Similar forms also fail:
    // `ws+unix:///\\.\pipe\name` parses to the socket path `/\\.\pipe\name`, which has a leading slash
    // and is not a pipe name, and percent-encoding the backslashes collapses the authority. Therefore
    // `muxws/node` has no Windows named-pipe transport and raises no `UnixSocketsUnsupportedError`; on
    // Windows, a POSIX-style path produces a connect error naming the path.
    const caught = await connect(String.raw`ws+unix://\\.\pipe\name:/route`).then(
      () => null,
      (error: unknown) => error,
    );

    // `unixTarget` cannot parse it either, so the refusal does not come from this port's url syntax:
    // the string reaches `ws`, `ws` reports `Invalid URL`, and that message is wrapped, not replaced
    // (WSM-ERR-016). The platform's error is kept as `cause`.
    expect(caught).toBeInstanceOf(WsUrlError);
    expect(caught).toBeInstanceOf(TransportUrlError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toContain('Invalid URL');
    expect((caught as Error).cause).toBeInstanceOf(SyntaxError);
    expect(String(((caught as Error).cause as Error).message)).toContain('Invalid URL');
    expect(() => new URL(String.raw`ws+unix://\\.\pipe\name:/route`)).toThrow();
  });
});

// --------------------------------------------------------------------------- the other entry point

describe('the platform entry point refuses ws+unix: before it opens anything', () => {
  // No skip: the guard is a string comparison and behaves identically on Windows, where a developer
  // must also get this message and not a platform DOMException.
  it('names muxws/node as the entry point that can dial a socket file', async () => {
    const caught = await platformConnect('ws+unix:///run/app.sock:/route').then(
      () => null,
      (error: unknown) => error,
    );

    // A `UnixSocketsUnsupportedError`, not a `TransportUrlError`: the url is correct and `muxws/node`
    // dials it (WSM-ERR-016).
    expect(caught).toBeInstanceOf(UnixSocketsUnsupportedError);
    expect(caught).toBeInstanceOf(TransportUnsupportedError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect(caught).not.toBeInstanceOf(TransportUrlError);
    // Asserted on the muxws message only. The platform message differs by runtime (undici:
    // `expected a ws: or wss: url`; jsdom: the scheme must be 'ws' or 'wss'), and neither names the
    // subpath export that can dial the url.
    expect((caught as Error).message).toContain('muxws/node');
    expect((caught as Error).message).toContain('ws+unix:');
  });

  it('compares the scheme case-insensitively and ignores surrounding space, as the url parser does', async () => {
    // `new URL('WS+UNIX://...')` normalises the scheme and strips leading whitespace. A guard that
    // compared raw text would pass these urls to the platform, which refuses them with its own
    // message.
    const caught = await platformConnect('  WS+Unix:///run/app.sock').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(UnixSocketsUnsupportedError);
    expect((caught as Error).message).toContain('muxws/node');
  });

  it('leaves every other url to the platform, malformed ones included', async () => {
    // The guard is not a general url validator. `new URL('nonsense')` throws `ERR_INVALID_URL`, a
    // different error for a different mistake, and the platform's diagnostic is kept.
    const caught = await platformConnect('nonsense').then(
      () => null,
      (error: unknown) => error,
    );

    // Not `toBeInstanceOf(Error)`: the rejection is whatever the runtime threw, and under jsdom that
    // is a `DOMException`, which is not an `Error`. The test asserts only that muxws did not intercept
    // it.
    expect(caught).not.toBeNull();
    expect(caught).not.toBeInstanceOf(TypeError);
    // The scheme guard does not handle a malformed url: a `ws+unix:` refusal names another entry
    // point, which would be wrong for `nonsense`.
    expect(caught).not.toBeInstanceOf(UnixSocketsUnsupportedError);
    expect(String((caught as Error).message)).not.toContain('muxws/node');
  });
});
