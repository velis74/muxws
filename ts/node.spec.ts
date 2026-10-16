/**
 * `ts/node.ts`'s handshake: which HTTP status the acceptor answers, and what the dialer makes of it.
 *
 * Mirrors the handshake block in `muxws/transports/websockets_test.py`; every test sends a raw
 * **HTTP request**. WSM-CDC-022 specifies a status code, and no dial in this language exposes one:
 * `connect()` falls back to the WSM-CDC-028 check on the open socket and reports `CodecMismatch`
 * whether the acceptor answered 400 or 101 without a subprotocol. Only a test that reads the status
 * line detects an acceptor that completes a handshake it should refuse.
 *
 * The rest of `ts/node.ts` is exercised through `ts/reconnect.spec.ts`, which dials it for real.
 */

import http from 'node:http';
import type { AddressInfo } from 'node:net';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { type Codec, JsonCodec } from './codec';
import { CodecMismatch, MuxwsError, TransportUrlError } from './errors';
// The browser entry point registers the JSON codec (WSM-CDC-004); `ts/node.ts` does not.
import './index';
import { WsUrlError, accept, connect, handleProtocols, refuseMismatchedUpgrade, serve } from './node';
import { logger } from './observability';
import type { Peer } from './peer';

// --------------------------------------------------------------------------- the harness

interface Acceptor {
  url: string;
  /**
   * How many times `ws` emitted its `connection` event. A refusal must leave this at zero: when the
   * connection handler runs, the handshake is already complete, which WSM-CDC-022 forbids.
   */
  handshakes: () => number;
  close: () => Promise<void>;
}

/** A real `ws` acceptor configured for `json`, with both handshake hooks installed. */
async function startAcceptor(refuse = true): Promise<Acceptor> {
  let handshakes = 0;
  const built = new WebSocketServer({ port: 0, handleProtocols });
  const server = refuse ? refuseMismatchedUpgrade(built) : built;
  server.on('connection', (socket) => {
    handshakes += 1;
    // `.catch` is redundant with `serve()`, which catches a `CodecMismatch` and returns, as
    // `muxws.serve()` does in Python, so a mismatch cannot become an unhandled rejection in a `ws`
    // connection handler and terminate the process.
    void serve(socket, { handler: (payload, stream) => stream.reply({ echo: payload }) }).catch(() => undefined);
  });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  return {
    url: `ws://127.0.0.1:${(server.address() as AddressInfo).port}`,
    handshakes: () => handshakes,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

interface Answer {
  status: number;
  protocol: string | undefined;
}

/** Send the upgrade request directly and return the HTTP status line. */
function upgrade(url: string, offered: string): Promise<Answer> {
  const { hostname, port, pathname } = new URL(url);
  return new Promise<Answer>((resolve, reject) => {
    const request = http.request({
      host: hostname,
      port,
      // Taken from the URL, so a test can address a server restricted to one path.
      path: pathname,
      headers: {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        // A fixed key: nothing here verifies `Sec-WebSocket-Accept`, and a constant keeps the
        // request byte-identical between runs.
        'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Protocol': offered,
      },
    });
    const answer = (status: number | undefined, protocol: string | string[] | undefined): void => {
      resolve({ status: status ?? 0, protocol: Array.isArray(protocol) ? protocol.join(',') : protocol });
    };
    // A refusal arrives as an ordinary response; an acceptance arrives as `upgrade`. The tests assert
    // which of the two is received.
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

// --------------------------------------------------------------------------- the acceptor half

describe('the acceptor refuses a codec it does not speak - WSM-CDC-022', () => {
  let acceptor: Acceptor;
  let refusals: string[];

  beforeEach(async () => {
    acceptor = await startAcceptor();
    refusals = [];
    // The acceptor's log line (WSM-CDC-029), captured and not printed.
    vi.spyOn(console, 'error').mockImplementation((line: unknown) => {
      refusals.push(String(line));
    });
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await acceptor.close();
  });

  it('answers a mismatched offer with HTTP 400 and no subprotocol', async () => {
    const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.msgpack');

    expect(status).toBe(400);
    expect(protocol).toBeUndefined();
    expect(acceptor.handshakes(), 'a refused upgrade must never reach the connection handler').toBe(0);
    expect(refusals.join('\n')).toContain('muxws refusing the upgrade');
  });

  it('answers a matching offer with HTTP 101 and echoes the subprotocol', async () => {
    // Control: an acceptor that refuses every offer would also pass the 400 test above.
    const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.json');

    expect(status).toBe(101);
    expect(protocol).toBe('muxws.v1.json');
  });

  it.each([
    ['a later generation', 'muxws.v2.json'],
    ['no muxws entry at all', 'bearer.abc123'],
  ])('answers %s with HTTP 400', async (_name, offered) => {
    // A generation this acceptor does not support is rejected at the handshake, so the version
    // component of the subprotocol name is enforced (WSM-CDC-025, WSM-CON-009).
    const { status } = await upgrade(acceptor.url, offered);

    expect(status).toBe(400);
    expect(acceptor.handshakes()).toBe(0);
  });

  it('ignores the application entries around the muxws one - WSM-CDC-021', async () => {
    const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.json, bearer.abc123');

    expect(status).toBe(101);
    expect(protocol).toBe('muxws.v1.json');
  });

  it("keeps the server's own shouldHandle, so a `path` option still applies", async () => {
    // `refuseMismatchedUpgrade` wraps `shouldHandle`, which the server may already define:
    // `new WebSocketServer({ path })` is implemented as `shouldHandle`. Replacing it would make an
    // acceptor restricted to one path answer 101 on every path.
    const scoped = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, path: '/muxws', handleProtocols }));
    await new Promise<void>((resolve) => scoped.once('listening', resolve));
    const url = `ws://127.0.0.1:${(scoped.address() as AddressInfo).port}`;

    try {
      expect((await upgrade(`${url}/muxws`, 'muxws.v1.json')).status).toBe(101);
      expect((await upgrade(`${url}/elsewhere`, 'muxws.v1.json')).status).toBe(400);
      expect((await upgrade(`${url}/muxws`, 'muxws.v1.msgpack')).status).toBe(400);
    } finally {
      await new Promise<void>((resolve) => scoped.close(() => resolve()));
    }
  });
});

describe('handleProtocols alone cannot refuse, so a second hook is installed', () => {
  it('completes the handshake with 101 and no subprotocol when only handleProtocols is installed', async () => {
    // Pins the `ws` behaviour that violates WSM-CDC-022: when `handleProtocols` returns `false`, no
    // subprotocol is selected and `ws` still answers 101. If a `ws` release makes the hook refuse,
    // this test fails and `refuseMismatchedUpgrade` is no longer needed.
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const acceptor = await startAcceptor(false);
    try {
      const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.msgpack');

      expect(status).toBe(101);
      expect(protocol).toBeUndefined();

      // The dialer detects the mismatch on the open socket and closes it with the policy-violation
      // code (WSM-CDC-028). For this reason a dial cannot distinguish this 101 from a correct 400;
      // the check does not replace the 400.
      const caught = await connect(acceptor.url, { codec: msgpackish() }).then(
        () => null,
        (error: unknown) => error,
      );
      expect(caught).toBeInstanceOf(CodecMismatch);
    } finally {
      vi.restoreAllMocks();
      await acceptor.close();
    }
  });
});

// --------------------------------------------------------------------------- the dialer half

describe('a dialer whose upgrade is refused - WSM-CDC-024', () => {
  let acceptor: Acceptor;

  beforeEach(async () => {
    acceptor = await startAcceptor();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await acceptor.close();
  });

  it('raises CodecMismatch rather than a bare connection failure, and exchanges no frame', async () => {
    // A refused handshake exchanges no frame, as the rule requires. Both peers would run in this
    // process, so the `muxws.frames` logger sees every frame either sends or receives, and a dialer
    // that obtains a socket sends `hello` immediately (WSM-RCN-021), so at least one frame would be
    // logged.
    const frames: string[] = [];
    vi.spyOn(console, 'debug').mockImplementation((line: unknown) => {
      frames.push(String(line));
    });
    const level = logger.level;
    logger.level = 'debug';

    try {
      const caught = await connect(acceptor.url, { codec: msgpackish(), hello: { session: 'abc' } }).then(
        () => null,
        (error: unknown) => error,
      );

      expect(caught).toBeInstanceOf(CodecMismatch);
      const message = (caught as Error).message;
      expect(message).toContain('msgpack');
      expect(message).toContain('VITE_MUXWS_CODEC');
      expect(message).toContain('MUXWS_CODEC');

      expect(frames, `a refused handshake exchanged ${frames.length} muxws frame(s)`).toEqual([]);
      expect(acceptor.handshakes(), 'the upgrade must have been refused, not completed and closed').toBe(0);
    } finally {
      logger.level = level;
    }
  });

  it('falls back to the check on the open socket when a 101 negotiated something else - WSM-CDC-028', async () => {
    // This acceptor selects the application's entry instead of the muxws one. `ws` opens the socket
    // because that value was offered, so the handshake succeeds from the client's side. The check on
    // the open socket detects the mismatch; it is the only check available in a browser.
    const rogue = new WebSocketServer({ port: 0, handleProtocols: () => 'bearer.abc123' });
    await new Promise<void>((resolve) => rogue.once('listening', resolve));
    const url = `ws://127.0.0.1:${(rogue.address() as AddressInfo).port}`;

    try {
      const caught = await connect(url, { subprotocols: ['bearer.abc123'] }).then(
        () => null,
        (error: unknown) => error,
      );

      expect(caught).toBeInstanceOf(CodecMismatch);
    } finally {
      await new Promise<void>((resolve) => rogue.close(() => resolve()));
    }
  });

  it('leaves any status other than 400 as its own error', async () => {
    // The structured path checks for status 400, not for any incomplete handshake. A proxy answering
    // 503 in front of a working acceptor is not a codec mismatch and must not be reported as one.
    const gateway = http.createServer((_request, response) => {
      response.writeHead(503).end();
    });
    await new Promise<void>((resolve) => gateway.listen(0, '127.0.0.1', resolve));
    const port = (gateway.address() as AddressInfo).port;

    try {
      const caught = await connect(`ws://127.0.0.1:${port}`).then(
        () => null,
        (error: unknown) => error,
      );

      expect(caught).toBeInstanceOf(Error);
      expect(caught).not.toBeInstanceOf(CodecMismatch);
      expect((caught as Error).message).toContain('503');
    } finally {
      await new Promise<void>((resolve) => gateway.close(() => resolve()));
    }
  });

  it.each([
    // Two ports, each matched by a different incorrect way of parsing a status from the message.
    // `/400/` anywhere in the message matches this one.
    ['a port whose number contains 400', 14_000],
    // `/\b400\b/` matches this one: `connect ECONNREFUSED 127.0.0.1:400`. Both would report a codec
    // mismatch for a connection where nothing is listening, which is outside WSM-CDC-024. The second
    // port is the stricter case and the one to keep if port 400 is in use on the test machine.
    ['a port whose number is 400', 400],
  ])('leaves an acceptor that is simply not running as its own error - %s', async (_name, port) => {
    const caught = await connect(`ws://127.0.0.1:${port}`).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(CodecMismatch);
    expect((caught as Error).message).toContain('ECONNREFUSED');
  });
});

describe('a url this transport cannot open - WSM-ERR-016', () => {
  it("arrives as a WsUrlError wrapping the message of `ws`, not as ws's own SyntaxError", async () => {
    // Without translation, `ws`'s constructor throws a bare `SyntaxError: Invalid URL: nonsense`, which
    // an `instanceof MuxwsError` handler around a dial does not catch. The message of `ws` is kept,
    // because it names the invalid text; the wrapper adds which transport was asked to dial it.
    const caught = await connect('nonsense').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(WsUrlError);
    expect(caught).toBeInstanceOf(TransportUrlError);
    expect(caught).toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toContain('nonsense');
    expect((caught as Error).message).toContain('Invalid URL');
    expect((caught as Error).cause).toBeInstanceOf(SyntaxError);
  });

  it('passes through unchanged a dial that failed for any other reason (control)', async () => {
    // A host that does not resolve is a valid url that nothing answers at, not a url this transport
    // cannot open, so it must not be reported as a url error. This is the same kind of control as
    // the ECONNREFUSED tests above, for the url error class.
    //
    // `.invalid` and not a bare label: RFC 6761 reserves it to never resolve, while `bad` on a
    // network with a search domain may be resolved. The assertion is on `getaddrinfo` plus a set of
    // codes and not on `ENOTFOUND`, because the code depends on the resolver. The test asserts that
    // name resolution failed and the error reached the caller untranslated.
    const caught = await connect('ws://no-such-host.invalid').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(TransportUrlError);
    expect(caught).not.toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toMatch(/getaddrinfo (ENOTFOUND|EAI_AGAIN|EAI_NODATA|ESERVFAIL)\b/);
  });

  it('passes a bad subprotocol error through untranslated; the url guard is not a blanket catch', async () => {
    // `ws` throws a `SyntaxError` both for `An invalid or duplicated subprotocol was specified` and
    // for `Invalid URL`, and the two objects are otherwise identical, so a `catch` that translated
    // every error from the constructor would report a space in a subprotocol token as a malformed
    // address. The guard parses the address instead (`new URL()` parses `ws://127.0.0.1:1/x` to a
    // scheme `ws` accepts) and passes this error through unchanged.
    const caught = await connect('ws://127.0.0.1:1/x', { subprotocols: ['bad protocol'] }).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(SyntaxError);
    expect(caught).not.toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toContain('subprotocol');
  });
});

describe('serve() reports a refused handshake by returning, not by rejecting', () => {
  it('matches Python, whose serve() also catches CodecMismatch', async () => {
    // When `CodecMismatch` is raised, the refusal has already been sent as HTTP 400 (WSM-CDC-022)
    // and logged with both codec names (WSM-CDC-029). Rejecting adds no diagnostic, and in Node it
    // causes an unhandled rejection inside a `ws` connection handler, which terminates the
    // acceptor's process.
    //
    // A stub socket instead of a live server: `accept()` reads two properties of the socket on this
    // path, and a `ws` server configured to negotiate the wrong subprotocol would mostly test that
    // configuration.
    let closedWith: [number, string] | null = null;
    const negotiatedSomethingElse = {
      protocol: 'muxws.v1.msgpack',
      close: (code: number, reason: string) => {
        closedWith = [code, reason];
      },
    } as unknown as Parameters<typeof serve>[0];

    await expect(serve(negotiatedSomethingElse, { handler: () => undefined })).resolves.toBeUndefined();
    expect(closedWith, 'the socket is still closed - returning quietly is not ignoring it').toEqual([
      1008,
      'codec mismatch',
    ]);
  });
});

describe('accept() and serve() register the interceptor arrays they are given - WSM-API-026', () => {
  it('runs them on the peer each builds, in order, before its first frame', async () => {
    const seen: unknown[] = [];
    const server = new WebSocketServer({ port: 0, handleProtocols });
    server.on('connection', (socket, request) => {
      if (request.url === '/accept') {
        void accept(socket, {
          openInterceptors: [
            (headers) => {
              headers.by = 'acceptor';
            },
          ],
        })
          .then(async (peer) => {
            peer.onStream(async (_payload, stream) => {
              peer.open('pushed', { end: true });
              await stream.reply('ok');
            });
            await peer.serve();
          })
          .catch(() => undefined);
        return;
      }
      void serve(socket, {
        handler: (_payload, stream) => stream.reply({ ...stream.headers }),
        replyHeadersInterceptors: [
          (headers) => {
            headers.served = 'yes';
          },
        ],
        incomingHeadersInterceptors: [
          (headers) => {
            seen.push({ ...headers });
          },
          () => {
            seen.push('second');
          },
        ],
      }).catch(() => undefined);
    });
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const base = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const dialers: Peer[] = [];
    try {
      const served = await connect(`${base}/serve`, { pingIntervalMs: 0 });
      dialers.push(served);
      const stream = served.open('q', { headers: { trace: 't-1' }, end: true });
      expect(await stream).toEqual({ trace: 't-1' });
      expect(stream.replyHeaders).toEqual({ served: 'yes' });
      expect(seen).toEqual([{ trace: 't-1' }, 'second']);

      const pushedHeaders: Record<string, unknown>[] = [];
      const accepted = await connect(`${base}/accept`, {
        pingIntervalMs: 0,
        onStream: (_payload, pushed) => {
          pushedHeaders.push({ ...pushed.headers });
        },
      });
      dialers.push(accepted);
      expect(await accepted.request('q', { timeoutMs: 3000 })).toBe('ok');
      await vi.waitFor(() => expect(pushedHeaders).toEqual([{ by: 'acceptor' }]));
    } finally {
      await Promise.all(dialers.map((dialer) => dialer.close({ drainMs: 50 })));
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
