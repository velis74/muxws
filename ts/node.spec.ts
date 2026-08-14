/**
 * `ts/node.ts`'s handshake: which HTTP status the acceptor answers, and what the dialer makes of it.
 *
 * A mirror of the handshake block in `muxws/transports/websockets_test.py`, and every witness in it
 * is an **HTTP request**. That is not ceremony: a status code is the whole of what WSM-CDC-022 asks
 * for, and no dial in this language can see one - `connect()` recovers through the WSM-CDC-028 check
 * on the already-open socket and reports `CodecMismatch` whether the acceptor answered 400 or
 * 101-with-no-subprotocol. That blind spot is how an acceptor answering 101 passed three milestones
 * of green suites in both languages.
 *
 * The rest of `ts/node.ts` is exercised through `ts/reconnect.spec.ts`, which dials it for real.
 */

import http from 'node:http';
import type { AddressInfo } from 'node:net';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { type Codec, JsonCodec } from './codec';
import { CodecMismatch, MuxwsError, TransportUrlError } from './errors';
// The browser entry point registers the JSON codec (WSM-CDC-004); `ts/node.ts` deliberately does not.
import './index';
import { WsUrlError, connect, handleProtocols, refuseMismatchedUpgrade, serve } from './node';
import { logger } from './observability';

// --------------------------------------------------------------------------- the harness

interface Acceptor {
  url: string;
  /**
   * How many times `ws` reached its `connection` event. A refusal must leave this at zero: an
   * acceptor that got as far as its connection handler has already completed the handshake, which
   * is what WSM-CDC-022 forbids whatever it does next.
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
    // `.catch` is a belt on top of braces: `serve()` swallows a `CodecMismatch` and returns, the way
    // `muxws.serve()` does in Python, precisely so an unrefused mismatch cannot surface here as an
    // unhandled rejection out of a `ws` connection handler and take the process with it.
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

/** Speak the upgrade by hand and read the HTTP status line back. */
function upgrade(url: string, offered: string): Promise<Answer> {
  const { hostname, port, pathname } = new URL(url);
  return new Promise<Answer>((resolve, reject) => {
    const request = http.request({
      host: hostname,
      port,
      // From the URL, so a test can address a server that was scoped to one path.
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
    // A refusal arrives as an ordinary response; an acceptance arrives as `upgrade`. Both are
    // answers to the same request, and which one comes back is the assertion.
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

// --------------------------------------------------------------------------- the acceptor half

describe('the acceptor refuses a codec it does not speak - WSM-CDC-022', () => {
  let acceptor: Acceptor;
  let refusals: string[];

  beforeEach(async () => {
    acceptor = await startAcceptor();
    refusals = [];
    // The acceptor's own half of the diagnostic (WSM-CDC-029), captured rather than printed.
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
    // Without this the 400 above is also what an acceptor that refuses everybody answers.
    const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.json');

    expect(status).toBe(101);
    expect(protocol).toBe('muxws.v1.json');
  });

  it.each([
    ['a later generation', 'muxws.v2.json'],
    ['no muxws entry at all', 'bearer.abc123'],
  ])('answers %s with HTTP 400', async (_name, offered) => {
    // A generation this acceptor cannot speak is rejected *here*, which is what makes the version
    // component of the subprotocol name a gate rather than a label (WSM-CDC-025, WSM-CON-009).
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
    // `refuseMismatchedUpgrade` replaces a method the server may already have been given a meaning
    // for: `new WebSocketServer({ path })` is implemented as `shouldHandle`, and an installer that
    // simply assigned over it would answer 101 on every path of an application that had scoped its
    // acceptor to one - a routing change made by a codec check, which nobody would look for here.
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

describe('handleProtocols alone cannot refuse, which is why the second hook exists', () => {
  it('completes the handshake with 101 and no subprotocol when only handleProtocols is installed', async () => {
    // Not an endorsement: this is the violation, pinned. `handleProtocols` returning `false` selects
    // nothing and `ws` answers 101 anyway, so an acceptor built with that hook alone breaks
    // WSM-CDC-022 while looking configured. If a later `ws` ever makes the hook refuse, this test
    // fails and `refuseMismatchedUpgrade` can go.
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const acceptor = await startAcceptor(false);
    try {
      const { status, protocol } = await upgrade(acceptor.url, 'muxws.v1.msgpack');

      expect(status).toBe(101);
      expect(protocol).toBeUndefined();

      // And the last resort that has been carrying this all along: the dialer finds the mismatch on
      // the already-open socket and closes it with the policy-violation code (WSM-CDC-028). It is
      // why every in-language test still passed, and it is not a substitute for the 400.
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
    // The frame count is the negative witness the rule has always named and nothing has asserted.
    // Both peers would live in this process, so the `muxws.frames` logger sees every frame either of
    // them sends or receives, and `hello` guarantees there would be one to see: a dialer that got a
    // socket puts its hello on the wire immediately (WSM-RCN-021).
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
    // This acceptor selects the application's own entry instead of the muxws one. `ws` opens the
    // socket, because that value *was* offered, so nothing about the handshake looks wrong from the
    // client's side: the check on the already-open socket is the last resort, and it is the only
    // route a browser ever has.
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
    // The structured path must stay a *400* check, not "the handshake did not complete". A proxy
    // answering 503 in front of a healthy acceptor is not a codec mismatch, and telling the reader
    // to go and set MUXWS_CODEC would be worse than saying nothing.
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
    // Two ports, because two different wrong ways of reading a status out of prose reach them.
    // `/400/` anywhere in the message matches this one...
    ['a port whose number contains 400', 14_000],
    // ...and `/\b400\b/`, which looks careful, matches this one: `connect ECONNREFUSED
    // 127.0.0.1:400`. Both send the reader off to check MUXWS_CODEC on both ends of a connection
    // where nothing is listening at all, which is not what WSM-CDC-024 is about. Only the second
    // port has teeth against the form this module actually shipped, so it is the one to keep if a
    // machine somewhere ever answers on 400 and this has to be re-thought.
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
  it("arrives as a WsUrlError framing the wording `ws` used, not as ws's own SyntaxError", async () => {
    // Measured before the class existed: `connect('nonsense')` rejected with
    // `SyntaxError: Invalid URL: nonsense`, thrown synchronously out of `ws`'s constructor inside
    // `dialWs` and not a `MuxwsError` at all - so an application that wrapped every muxws call in one
    // handler caught a bad `ws+unix:` url and missed a bad `ws:` one. The sentence is `ws`'s and stays
    // `ws`'s: it names the offending text, and this frame only says who was asked to dial it.
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

  it('leaves a dial that failed for any other reason exactly as it was - the control', async () => {
    // A host that does not resolve is not an address this transport cannot *open*; it is one nothing
    // answers at. The remedy is somewhere other than the address bar, and reporting it as a url error
    // would send the reader to re-read a string that is spelled correctly. This is the same shape as
    // the ECONNREFUSED controls above, one class over.
    //
    // `.invalid` rather than a bare label: RFC 6761 reserves it never to resolve, while `bad` on a
    // network with a search domain is a name a resolver may go looking for. And the assertion is on
    // `getaddrinfo` plus a *family* of codes rather than on `ENOTFOUND`, because which one comes back
    // is the resolver's answer and not this library's: the first CI run of this test failed with
    // `getaddrinfo EAI_AGAIN`, a temporary-failure code, where every local run had produced
    // `ENOTFOUND`. What the control actually claims is that name resolution failed and the failure
    // reached the caller untranslated, and that is what is pinned here.
    const caught = await connect('ws://no-such-host.invalid').then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught).toBeInstanceOf(Error);
    expect(caught).not.toBeInstanceOf(TransportUrlError);
    expect(caught).not.toBeInstanceOf(MuxwsError);
    expect((caught as Error).message).toMatch(/getaddrinfo (ENOTFOUND|EAI_AGAIN|EAI_NODATA|ESERVFAIL)\b/);
  });

  it('leaves a bad subprotocol as a bad subprotocol, which is why the guard is not a blanket catch', async () => {
    // The measurement that forced the shape of the translation: `ws` throws a `SyntaxError` for
    // `An invalid or duplicated subprotocol was specified` as well as for `Invalid URL`, and nothing
    // on either object tells them apart. A `catch` around the constructor that translated everything
    // would report a space in a subprotocol token as a malformed address. So the address is re-read -
    // `new URL()` parses `ws://127.0.0.1:1/x` to a scheme `ws` takes - and this throw is passed
    // through untouched.
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
  it('matches Python, whose serve() swallows CodecMismatch for the same reason', async () => {
    // By the time `CodecMismatch` is raised the refusal has already gone out as HTTP 400
    // (WSM-CDC-022) and already been logged with both codec names (WSM-CDC-029). Rejecting again
    // adds no diagnostic - and in Node it adds an unhandled rejection inside a `ws` connection
    // handler, which is how `interop/runner.ts`'s acceptor died during M6.
    //
    // A stand-in rather than a live server: `accept()` reads exactly two things off the socket on
    // this path, and standing up a `ws` server that negotiates the wrong subprotocol on purpose
    // would test the double more than the branch.
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
