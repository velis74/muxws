/**
 * `ws+unix:` end to end: the same muxws handshake, carried over a filesystem socket.
 *
 * The claim under test is a negative one - that dialling a unix-domain socket changes **nothing**.
 * `ts/node.ts` has one branch for it and no more: the url is split into a file and a request target,
 * and `ws` is handed an ordinary `ws:` url plus a `createConnection` that opens the file. So the bytes
 * on the wire are the GET + `Upgrade` + `Sec-WebSocket-Protocol: muxws.v1.<codec>` every TCP test
 * already pins, and the answer is still 101 or 400. A test suite is the only way to know that rather
 * than to believe it, and the only way to keep knowing it when `ws` changes: the refusal path in
 * particular runs through `unexpected-response`, an event whose contract over an AF_UNIX request is
 * nowhere written down.
 *
 * The grammar half of the file is not decoration either. `ws+unix:` is one url naming two things, and
 * the two ports must read it the same way or a deployment that pastes it into both configurations
 * reaches two different request targets with no error anywhere. Every url shape whose reading differs
 * between `ws`'s own split and `muxws/transports/unix.py`'s is pinned below against the target the
 * acceptor actually saw, which is the only place the agreement is observable.
 *
 * So the witnesses here are deliberately the same ones `ts/node.spec.ts` uses over TCP - a raw HTTP
 * status read off the upgrade, and the acceptor's `connection` counter left at zero (WSM-CDC-022) -
 * plus the one thing that file has no reason to do: a full round trip through `connect()`, because a
 * transport that handshakes and then cannot carry a frame would satisfy every status assertion in it.
 *
 * The whole unix half skips on Windows, which has no AF_UNIX. The browser-entry block at the bottom
 * does not: it never opens anything.
 */

import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { type Codec, JsonCodec } from './codec';
import { CodecMismatch, MuxwsError, TransportUnsupportedError, TransportUrlError } from './errors';
import { Reconnect, UnixUrlError, WsUrlError, connect, handleProtocols, refuseMismatchedUpgrade, serve } from './node';

// Importing the browser entry point is also what registers the JSON codec (WSM-CDC-004), which
// `ts/node.ts` deliberately does not do; here it is imported for its `connect` as well - the two
// entry points disagree about `ws+unix:` on purpose, and this file asserts both halves.
import { UnixSocketsUnsupportedError, connect as platformConnect } from './index';

// --------------------------------------------------------------------------- the harness

/** The request target and `Host` the acceptor was actually given, which is half of what is claimed. */
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
 * A real `ws` acceptor on a socket file, built the way the documentation tells an application to.
 *
 * `WebSocketServer` cannot listen on a path itself, so this is the recipe: an `http.Server` bound to
 * the socket file, `{ server }` rather than `{ port }`, and both handshake hooks - `handleProtocols`
 * to select and `refuseMismatchedUpgrade` to refuse, which is the pair `ts/node.ts` explains at
 * length and which no acceptor may install only half of.
 *
 * The directory comes from `mkdtemp` under the OS temp dir with a two-character prefix, not from the
 * repository and not from any longer path: `sun_path` caps a socket address at ~108 bytes, and what
 * an overrun produces is `EINVAL: invalid argument <the whole path>` - accurate, and no help at all
 * in working out that a length is the problem.
 *
 * `existing` is for the one test that needs two acceptors at the same path in sequence: a socket file
 * is not a port, and a reconnect has to reach the **new** inode a restarted acceptor binds there.
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
    // The `.catch` belt `ts/node.spec.ts` explains: `serve()` returns quietly on `CodecMismatch`, and
    // an unhandled rejection out of a `ws` connection handler takes the process with it.
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
    // Both servers, in this order: `wss.close()` does not close a server it was merely handed.
    // Closing the `http.Server` is also what unlinks the socket file, so a leaked one is a stale file
    // the next run binds against - and it does not finish while a connection is still open, which is
    // why the live ones are terminated first rather than waited for. The directory outlives this call
    // on purpose: the reconnect test closes one acceptor and binds the next at the same path, so
    // removing it here would take the path with it. `afterEach` owns the directory.
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
 * That one substitution is the entire difference between the two transports at the HTTP layer, and
 * spelling it out here is the point: the request, the headers and the two events answering it are
 * unchanged, so a status code is available as a witness over a socket file exactly as it is over TCP.
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

/** Poll until `predicate` holds, as `ts/reconnect.spec.ts` does: a supervisor answers on its own clock. */
async function until(predicate: () => boolean, what: string, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`${what} never became true within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** A codec whose only interesting property is its **name**, so the dialer offers `muxws.v1.msgpack`. */
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
    // `mkdtemp` has no context manager to lean on the way Python's `TemporaryDirectory` does in
    // `websockets_test.py`, so without this the suite leaves one empty directory per test in the OS
    // temp dir. `force` because a test that failed before the socket was bound must still tear down.
    rmSync(acceptor.directory, { recursive: true, force: true });
  });

  it('round trips a request over a socket file, with the route intact', async () => {
    const peer = await connect(`ws+unix://${acceptor.path}:/route`);

    try {
      expect(await peer.request({ hi: 1 })).toEqual({ echo: { hi: 1 } });
    } finally {
      await peer.close();
    }

    // A handshake actually happened here, on this acceptor. Without the counter a dial that had
    // somehow reached anything else would satisfy the assertion above just as well.
    expect(acceptor.handshakes()).toBe(1);
    // And the two halves of the url arrived where they belong: the part before the ':' opened the
    // file, the part after it became the request target. The `Host` is node's default for an empty
    // authority, and it is the value the Python port synthesises as `uri="ws://localhost<route>"` -
    // which is what makes the two ports put the same bytes on the wire for the same url.
    expect(acceptor.seen()).toEqual({ url: '/route', host: 'localhost' });
  });

  it('defaults the request target to / when the url carries no route', async () => {
    // `ws+unix:///path.sock` with no ':' at all is the short form the documentation shows first, and
    // the default is node's, not muxws's: `ClientRequest` falls back to '/' for an absent path.
    const peer = await connect(`ws+unix://${acceptor.path}`);

    try {
      expect(await peer.request({ hi: 2 })).toEqual({ echo: { hi: 2 } });
    } finally {
      await peer.close();
    }

    expect(acceptor.seen().url).toBe('/');
  });

  it('splits on the first colon only, so a target may contain one', async () => {
    // The row `only-the-first-colon-splits` in `muxws/transports/unix_test.py` is the Python half of
    // this claim, and this is the half that makes it a contract: the two ports must reach the same
    // request target for the same url or a deployment pasting one into both configurations is
    // silently talking to two different endpoints. Handed to `ws` unparsed these would be `/ws` and
    // `/r?t=a` - `ws` runs `opts.path.split(':')` and keeps `parts[1]` - which is exactly the
    // truncation `unixTarget` exists to prevent, and it is invisible from the dialing end.
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

  it('refuses a request target that is not an absolute path, by name', async () => {
    // The twin of `test_a_request_target_that_is_not_an_absolute_path_is_refused`, and the reason
    // both ports check it rather than letting the dial happen: `ws://localhost` + `ws` is
    // `ws://localhostws`, a valid url naming a host that does not exist, so the target folds into the
    // authority and the dial reaches the right file asking for `/` with a `Host` nobody chose. Against
    // an acceptor that does not route - this one, and `unix_serve` - that handshake *succeeds*.
    // Handed to `ws` instead it becomes `GET route HTTP/1.1`, which node answers 400, which `dialWs`
    // reads as a refused codec: a confidently wrong diagnosis pointing at `MUXWS_CODEC` for a typo in
    // a url. Neither outcome is acceptable, so the url never becomes a request.
    const caught = await connect(`ws+unix://${acceptor.path}:route`).then(
      () => null,
      (error: unknown) => error,
    );

    // A `UnixUrlError`, which was a bare `TypeError` until WSM-ERR-016: the wording was already
    // right, and what was missing was that an application wrapping its dials in one
    // `instanceof MuxwsError` handler never saw this one at all.
    expect(caught).toBeInstanceOf(UnixUrlError);
    expect(caught).toBeInstanceOf(TransportUrlError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect(caught).not.toBeInstanceOf(CodecMismatch);
    expect((caught as Error).message).toContain("must begin with '/'");
    expect(acceptor.handshakes(), 'nothing may have been dialled').toBe(0);
  });

  it('refuses a url that names no socket file, before `ws` is asked to read it', async () => {
    // `ws+unix://` parses - the authority is empty and so is the path - and there is nothing in it to
    // open. Left to `ws` it comes back as `SyntaxError: The URL's pathname is empty`, which is true
    // of the string and silent about the shape a ws+unix: url is supposed to have; the reader who
    // wrote `ws+unix://run/app.sock`, putting the file in the authority, needs the shape. Refused
    // here for the same reason `parse_unix_url` refuses it, and by the same class name.
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
    // The direct twin of the Python proof, and the assertion no dial in this language can make: a
    // dialer reports `CodecMismatch` whether the acceptor refused the upgrade with 400 or completed
    // it and was caught afterwards by WSM-CDC-028. Only the raw status tells the two apart, so only
    // this test can show that a unix socket did not quietly demote the refusal to the later check.
    vi.spyOn(console, 'error').mockImplementation(() => undefined);

    expect(await upgradeOverUnix(acceptor.path, '/route', 'muxws.v1.msgpack')).toEqual({
      status: 400,
      protocol: undefined,
    });
    expect(acceptor.handshakes(), 'a refused upgrade must never reach the connection handler').toBe(0);

    // Without this the 400 above is also what an acceptor refusing everybody would answer.
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
    // The full diagnostic, not just the class: this is the `unexpected-response` branch of `dialWs`,
    // which reads the status off the response object, and the whole question here is whether that
    // event still carries one when the request rode an AF_UNIX socket instead of a TCP one.
    const message = (caught as Error).message;
    expect(message).toContain('msgpack');
    expect(message).toContain('MUXWS_CODEC');
    expect(acceptor.handshakes(), 'the upgrade must have been refused, not completed and closed').toBe(0);
  });

  it('leaves a socket file that does not exist as its own error', async () => {
    // The unix twin of the ECONNREFUSED tests in `ts/node.spec.ts`, and for the same reason: `dialWs`
    // must not read a missing acceptor as a codec mismatch. Nothing is listening, so nothing refused
    // anything, and sending the reader off to check MUXWS_CODEC on both ends would be a wrong answer
    // delivered confidently.
    const caught = await connect(`ws+unix://${acceptor.directory}/missing.sock:/route`).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(CodecMismatch);
    expect((caught as Error).message).toContain('ENOENT');
  });

  it("rejects wss+unix:, which is not a scheme, as this port's own grammar error", async () => {
    // This was pinned rather than implemented, on the argument that inventing a muxws-shaped error for
    // a scheme nobody types by accident would hide where the rule lives. WSM-ERR-016 overturns it, and
    // the argument turned out to point the other way: `ws`'s refusal lists the schemes **`ws`** takes,
    // which reads as though `ws+unix:` were `ws`'s feature and leaves muxws's own grammar - the
    // first-colon split this whole file exists for - looking borrowed. The refusal is this module's,
    // so the class is too, and it says why there is no TLS to offer rather than only that there is not.
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
    // The twin of `test_a_peer_reconnects_across_a_re_created_socket_file`, and the reason the Python
    // one has a docstring about inodes: a socket file is not a port. The acceptor's file is unlinked
    // when it closes and the next acceptor binds a *new* inode at the same name, so a dialer that
    // resolved the path once - to a descriptor, or to anything else - reconnects to a socket nobody is
    // listening on and hangs. Here the dial closure re-runs `unixTarget` and `net.connect` from the
    // url every time, so the property holds by construction; without this test it would only hold
    // until someone hoisted the connector out of the closure, and the Node side would be the half
    // that failed silently.
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
      // Reassigned so `afterEach` closes the live acceptor and not the corpse: closing a
      // `WebSocketServer` twice emits an `error` with nothing listening for it.
      acceptor = await startUnixAcceptor(path);

      await until(() => reconnects.length === 1, 'onReconnect');
      expect(acceptor.received(), 'the hello is replayed verbatim on the new inode').toEqual([{ tab: 'abc' }]);
      // And the new connection is a working one, not merely an open one.
      expect(await peer.request({ after: 'the reconnect' })).toEqual({ echo: { after: 'the reconnect' } });
    } finally {
      await peer.close();
    }
  });
});

// --------------------------------------------------------------------------- the url, without a socket

describe('the ws+unix: grammar, where no socket is needed to read it', () => {
  // No skip: `new URL()` is the same parser everywhere, which is the whole point of the test below.
  it('cannot address a Windows named pipe, whatever the platform', async () => {
    // Recorded because the opposite was believed, written down in two places, and reasoned from
    // `net.connect`'s behaviour rather than measured. `net.connect({ path })` really does open a named
    // pipe on Windows - but `\\.\pipe\name` cannot be got into a url to begin with: a backslash is a
    // forbidden code point in the authority of a non-special scheme, so the WHATWG parser rejects the
    // string on Linux exactly as it would on Windows. The near misses fail too and are worth having
    // here so nobody re-derives them: `ws+unix:///\\.\pipe\name` parses to the socket path
    // `/\\.\pipe\name`, which has a leading slash and is not a pipe name, and percent-encoding the
    // backslashes collapses the authority instead. So `muxws/node` has no Windows named-pipe
    // transport to protect, and no `UnixSocketsUnsupportedError` to raise either: what a Windows
    // reader gets for a POSIX-looking path is a connect error naming the path.
    const caught = await connect(String.raw`ws+unix://\\.\pipe\name:/route`).then(
      () => null,
      (error: unknown) => error,
    );

    // `unixTarget` cannot read it either, so it is not this port's grammar that refuses it: the
    // string reaches `ws`, `ws` says `Invalid URL`, and that sentence is framed rather than replaced
    // (WSM-ERR-016). The `cause` is where the platform's own diagnosis is kept - a translation that
    // dropped it would leave a reader with muxws's paraphrase of a parse it did not perform.
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
  // No skip: the guard is a string comparison, so it is as true on Windows as anywhere, and a
  // developer there must still get the message rather than a platform DOMException.
  it('names muxws/node as the entry point that can dial a socket file', async () => {
    const caught = await platformConnect('ws+unix:///run/app.sock:/route').then(
      () => null,
      (error: unknown) => error,
    );

    // A `UnixSocketsUnsupportedError`, not a `TransportUrlError`: the url is correct and `muxws/node`
    // dials it, so there is nothing here for the reader to retype (WSM-ERR-016). It was a bare
    // `TypeError` until this rule, on an argument about *when* the refusal happens; the rule is about
    // who has to catch it, and this one is composed by muxws itself.
    expect(caught).toBeInstanceOf(UnixSocketsUnsupportedError);
    expect(caught).toBeInstanceOf(TransportUnsupportedError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect(caught).not.toBeInstanceOf(TransportUrlError);
    // Asserted on **our** words only. What the platform says instead differs by runtime - undici says
    // `expected a ws: or wss: url`, jsdom says the scheme must be 'ws' or 'wss' - and both leave the
    // reader to discover on their own that a subpath export exists which would have worked.
    expect((caught as Error).message).toContain('muxws/node');
    expect((caught as Error).message).toContain('ws+unix:');
  });

  it('folds the scheme and ignores surrounding space, because the url parser would have', async () => {
    // `new URL('WS+UNIX://...')` normalises the scheme and strips leading whitespace, so a guard that
    // compared raw text would wave through exactly the urls the platform is about to refuse anyway -
    // and the reader would be back to reading someone else's prose for a typo of muxws's own feature.
    const caught = await platformConnect('  WS+Unix:///run/app.sock').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(UnixSocketsUnsupportedError);
    expect((caught as Error).message).toContain('muxws/node');
  });

  it('leaves every other url to the platform, malformed ones included', async () => {
    // The guard must not become the library's url validator. `new URL('nonsense')` throws
    // `ERR_INVALID_URL`, which is a different error for a different mistake, and replacing the
    // platform's diagnostic for every bad url with one about unix sockets would be a net loss.
    const caught = await platformConnect('nonsense').then(
      () => null,
      (error: unknown) => error,
    );

    // Not `toBeInstanceOf(Error)`: what comes back is whatever the runtime threw, and under jsdom
    // that is a `DOMException` which is not an `Error` at all. Pinning its class would pin the test
    // environment; the claim is only that muxws did not intercept it.
    expect(caught).not.toBeNull();
    expect(caught).not.toBeInstanceOf(TypeError);
    // The scheme guard did not claim a url that was merely malformed: a `ws+unix:` refusal names an
    // entry point that would have worked, and saying that about `nonsense` would be a wrong answer
    // delivered confidently.
    expect(caught).not.toBeInstanceOf(UnixSocketsUnsupportedError);
    expect(String((caught as Error).message)).not.toContain('muxws/node');
  });
});
