// @vitest-environment node
/**
 * `peer.onExchange()` scenarios over a real WebSocket.
 *
 * A dialer (`connect()` from `muxws/node`) and an acceptor (`accept()` on a `ws` server on 127.0.0.1, ephemeral port)
 * exchange streams; each test reads the exchanges both peers report. Written from the `onExchange` contract only.
 */

import type { AddressInfo } from 'node:net';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { type Codec, JsonCodec, registerCodec } from './codec';
import { ResetCode } from './errors';
import { MsgpackCodec } from './msgpack';
import { Reconnect, accept, connect, handleProtocols } from './node';
import type { Peer, StreamHandler } from './peer';

import { type Exchange } from './index';

// --------------------------------------------------------------------------- the harness

// `connect()` and `accept()` without a `codec` option resolve the configured codec (json) from the registry.
registerCodec('json', new JsonCodec());

/** The hello every test dialer sends; the acceptor's handler acknowledges it and ignores it. */
const HELLO = { hello: 'exchange-scenario' };

interface ExchangeOptions {
  maxPayloads?: number | null;
}

interface PairOptions {
  codec?: Codec;
  /** Registration options of the collecting handler on each peer; `false` registers none on that peer. */
  client?: ExchangeOptions | false;
  server?: ExchangeOptions | false;
  reconnect?: Reconnect;
}

interface Pair {
  client: Peer;
  server: () => Peer;
  clientExchanges: Exchange[];
  serverExchanges: Exchange[];
  /** Terminates every socket the server holds, without a close handshake. */
  dropSockets: () => void;
  connections: () => number;
  close: () => Promise<void>;
}

function isHello(payload: unknown): boolean {
  return typeof payload === 'object' && payload !== null && (payload as { hello?: unknown }).hello === HELLO.hello;
}

/** `handleProtocols` for an acceptor speaking msgpack; the default hook selects the configured (json) codec. */
function msgpackProtocols(protocols: Set<string>): string | false {
  return protocols.has('muxws.v1.msgpack') ? 'muxws.v1.msgpack' : false;
}

async function startPair(handler: StreamHandler, options: PairOptions = {}): Promise<Pair> {
  const clientExchanges: Exchange[] = [];
  const serverExchanges: Exchange[] = [];
  const serverPeers: Peer[] = [];
  const server = new WebSocketServer({
    port: 0,
    handleProtocols: options.codec === undefined ? handleProtocols : msgpackProtocols,
  });
  server.on('connection', (socket) => {
    void (async () => {
      const peer = await accept(socket, { codec: options.codec });
      if (options.server !== false) {
        peer.onExchange((exchange) => serverExchanges.push(exchange), options.server ?? {});
      }
      peer.onStream(async (payload, stream) => {
        if (isHello(payload)) return;
        await handler(payload, stream);
      });
      serverPeers.push(peer);
      await peer.serve();
    })().catch(() => undefined);
  });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  const url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;

  const client = await connect(url, {
    hello: HELLO,
    codec: options.codec,
    reconnect: options.reconnect ?? new Reconnect({ maxAttempts: 0 }),
  });
  if (options.client !== false) {
    client.onExchange((exchange) => clientExchanges.push(exchange), options.client ?? {});
  }

  return {
    client,
    server: () => serverPeers[serverPeers.length - 1],
    clientExchanges,
    serverExchanges,
    dropSockets: () => server.clients.forEach((socket) => socket.terminate()),
    connections: () => serverPeers.length,
    close: async () => {
      await client.close({ drainMs: 100 }).catch(() => undefined);
      await Promise.all(serverPeers.map((peer) => peer.close({ drainMs: 100 }).catch(() => undefined)));
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    },
  };
}

async function until(predicate: () => boolean, what: string, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

function forStream(exchanges: readonly Exchange[], stream: number): Exchange[] {
  return exchanges.filter((exchange) => exchange.stream === stream);
}

/** Waits until both peers have reported the stream, then returns the one exchange each reported. */
async function bothSides(pair: Pair, stream: number): Promise<{ client: Exchange; server: Exchange }> {
  await until(
    () => forStream(pair.clientExchanges, stream).length > 0 && forStream(pair.serverExchanges, stream).length > 0,
    `both exchanges of stream ${stream}`,
  );
  // Fires exactly once per stream on each peer.
  await sleep(20);
  const client = forStream(pair.clientExchanges, stream);
  const server = forStream(pair.serverExchanges, stream);
  expect(client).toHaveLength(1);
  expect(server).toHaveLength(1);
  return { client: client[0], server: server[0] };
}

const ISO_MS_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

let pair: Pair | null = null;

beforeEach(() => {
  // Handler errors and resets are logged; the lines are captured, not printed.
  vi.spyOn(console, 'error').mockImplementation(() => undefined);
  vi.spyOn(console, 'warn').mockImplementation(() => undefined);
});

afterEach(async () => {
  if (pair !== null) await pair.close();
  pair = null;
  vi.restoreAllMocks();
});

// --------------------------------------------------------------------------- unary request

describe('a unary request is reported by both peers', () => {
  it('reports opener, sides, headers, trailers and the ok outcome on the dialer and the acceptor', async () => {
    pair = await startPair(async (payload, stream) => {
      await stream.reply({ echo: payload }, { headers: { 'x-reply': 'r1' }, trailers: { status: 'done' } });
    });

    const stream = pair.client.open({ ask: 1 }, { headers: { 'x-trace': 't1' }, end: true });
    const answer = await stream;
    expect(answer).toEqual({ echo: { ask: 1 } });

    const { client, server } = await bothSides(pair, stream.id);

    expect(client.openedBy).toBe('local');
    expect(server.openedBy).toBe('remote');
    expect(client.conn).toBe(pair.client.id);
    expect(server.conn).toBe(pair.server().id);

    [client, server].forEach((exchange) => {
      expect(exchange.stream).toBe(stream.id);
      expect(exchange.oneway).toBe(false);
      expect(exchange.outcome).toBe('ok');
      expect(exchange.resetBy).toBeNull();
      expect(exchange.code).toBeNull();
      expect(exchange.reason).toBeNull();

      // The request side is what the opener sent, whichever peer reports it.
      expect(exchange.request.headers).toEqual({ 'x-trace': 't1' });
      expect(exchange.request.payloads).toEqual([{ ask: 1 }]);
      expect(exchange.request.payloadCount).toBe(1);
      expect(exchange.request.trailers).toBeNull();
      expect(exchange.request.ended).toBe(true);

      expect(exchange.response.headers).toEqual({ 'x-reply': 'r1' });
      expect(exchange.response.payloads).toEqual([{ echo: { ask: 1 } }]);
      expect(exchange.response.payloadCount).toBe(1);
      expect(exchange.response.trailers).toEqual({ status: 'done' });
      expect(exchange.response.ended).toBe(true);

      expect(exchange.startedAt).toBeInstanceOf(Date);
    });
  });

  it('reports a peer.request() call as a locally opened exchange', async () => {
    pair = await startPair(async (payload, stream) => {
      await stream.reply(payload);
    });

    expect(await pair.client.request('ping')).toBe('ping');

    await until(() => pair!.clientExchanges.length === 1, 'the dialer exchange');
    const [client] = pair.clientExchanges;
    expect(client.openedBy).toBe('local');
    expect(client.request.headers).toBeNull();
    expect(client.request.payloads).toEqual(['ping']);
    expect(client.response.headers).toBeNull();
    expect(client.response.trailers).toBeNull();
    expect(client.response.payloads).toEqual(['ping']);

    const { server } = await bothSides(pair, client.stream);
    expect(server.openedBy).toBe('remote');
    expect(server.request.payloads).toEqual(['ping']);
  });
});

// --------------------------------------------------------------------------- streamed response and limits

describe('a streamed exchange and maxPayloads', () => {
  const streamFive: StreamHandler = async (_payload, stream) => {
    // A payload-less headers frame and a payload-less end add no payload.
    await stream.sendHeaders({ 'x-stream': 's' });
    for (let index = 0; index < 5; index += 1) {
      await stream.send({ index });
    }
    await stream.end({ trailers: { rows: 5 } });
  };

  async function collect(peer: Peer, payload: unknown): Promise<{ id: number; items: unknown[] }> {
    const stream = peer.open(payload, { end: true });
    const items: unknown[] = [];
    for await (const item of stream) items.push(item);
    return { id: stream.id, items };
  }

  it('records every payload in order, with payloadCount, on both peers', async () => {
    pair = await startPair(streamFive);

    const { id, items } = await collect(pair.client, 'export');
    expect(items).toHaveLength(5);

    const { client, server } = await bothSides(pair, id);
    [client, server].forEach((exchange) => {
      expect(exchange.outcome).toBe('ok');
      expect(exchange.response.headers).toEqual({ 'x-stream': 's' });
      expect(exchange.response.payloads).toEqual([0, 1, 2, 3, 4].map((index) => ({ index })));
      expect(exchange.response.payloadCount).toBe(5);
      expect(exchange.response.trailers).toEqual({ rows: 5 });
      expect(exchange.response.ended).toBe(true);
      expect(exchange.request.payloads).toEqual(['export']);
      expect(exchange.request.payloadCount).toBe(1);
    });
  });

  it('cuts payloads to maxPayloads and keeps the full payloadCount', async () => {
    pair = await startPair(streamFive, {
      client: { maxPayloads: 2 },
      server: { maxPayloads: 2 },
    });

    const { id } = await collect(pair.client, 'export');

    const { client, server } = await bothSides(pair, id);
    [client, server].forEach((exchange) => {
      expect(exchange.response.payloads).toEqual([{ index: 0 }, { index: 1 }]);
      expect(exchange.response.payloadCount).toBe(5);
      expect(exchange.request.payloads).toEqual(['export']);
    });
  });

  it('with maxPayloads 0 reports timing and outcome only', async () => {
    pair = await startPair(streamFive, {
      client: { maxPayloads: 0 },
      server: { maxPayloads: 0 },
    });

    const { id } = await collect(pair.client, 'export');

    const { client, server } = await bothSides(pair, id);
    [client, server].forEach((exchange) => {
      expect(exchange.outcome).toBe('ok');
      expect(exchange.request.payloads).toEqual([]);
      expect(exchange.request.payloadCount).toBe(1);
      expect(exchange.response.payloads).toEqual([]);
      expect(exchange.response.payloadCount).toBe(5);
      expect(exchange.response.trailers).toEqual({ rows: 5 });
      expect(exchange.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  it('gives each of several handlers its own cut, in registration order', async () => {
    pair = await startPair(streamFive, { client: false });
    const calls: string[] = [];
    const seen: Record<string, Exchange> = {};
    pair.client.onExchange(
      (exchange) => {
        calls.push('two');
        seen.two = exchange;
      },
      { maxPayloads: 2 },
    );
    pair.client.onExchange((exchange) => {
      calls.push('default');
      seen.default = exchange;
    });
    pair.client.onExchange(
      (exchange) => {
        calls.push('none');
        seen.none = exchange;
      },
      { maxPayloads: 0 },
    );
    pair.client.onExchange(
      (exchange) => {
        calls.push('unlimited');
        seen.unlimited = exchange;
      },
      { maxPayloads: null },
    );

    await collect(pair.client, 'export');
    await until(() => calls.length === 4, 'four handler calls');
    await sleep(20);

    expect(calls).toEqual(['two', 'default', 'none', 'unlimited']);
    expect(seen.two.response.payloads).toHaveLength(2);
    expect(seen.default.response.payloads).toHaveLength(5);
    expect(seen.none.response.payloads).toHaveLength(0);
    expect(seen.unlimited.response.payloads).toHaveLength(5);
    Object.values(seen).forEach((exchange) => expect(exchange.response.payloadCount).toBe(5));
  });

  it('records the request side of a stream the opener feeds with several payloads', async () => {
    pair = await startPair(async (first, stream) => {
      const received: unknown[] = [first];
      for await (const item of stream) received.push(item);
      await stream.reply(received.length);
    });

    const stream = pair.client.open('a');
    await stream.send('b');
    await stream.end({ payload: 'c', trailers: { last: true } });
    expect(await stream).toBe(3);

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.request.payloads).toEqual(['a', 'b', 'c']);
      expect(exchange.request.payloadCount).toBe(3);
      expect(exchange.request.trailers).toEqual({ last: true });
      expect(exchange.request.ended).toBe(true);
      expect(exchange.response.payloads).toEqual([3]);
    });
  });

  it('rejects a negative or non-integer maxPayloads at registration', async () => {
    pair = await startPair(async (payload, stream) => stream.reply(payload));

    expect(() => pair!.client.onExchange(() => undefined, { maxPayloads: -1 })).toThrow(RangeError);
    expect(() => pair!.client.onExchange(() => undefined, { maxPayloads: 1.5 })).toThrow(RangeError);
  });

  it('gives a handler registered after the stream started what was recorded under the earlier limit', async () => {
    pair = await startPair(
      async (_payload, stream) => {
        // Waits for the dialer's go-ahead, so the second handler is registered while the stream is live.
        for await (const _item of stream) {
          // drained
        }
        await stream.send(1);
        await stream.send(2);
        await stream.end({ payload: 3 });
      },
      { client: { maxPayloads: 1 } },
    );

    const stream = pair.client.open('start');
    const late: Exchange[] = [];
    pair.client.onExchange((exchange) => late.push(exchange), {
      maxPayloads: null,
    });
    await stream.end();
    const items: unknown[] = [];
    for await (const item of stream) items.push(item);
    expect(items).toEqual([1, 2, 3]);

    await until(() => late.length === 1 && pair!.clientExchanges.length === 1, 'both handlers');
    expect(late[0].response.payloads).toEqual([1]);
    expect(late[0].response.payloadCount).toBe(3);
    expect(pair.clientExchanges[0].response.payloads).toEqual([1]);
  });
});

// --------------------------------------------------------------------------- fragmentation

describe('a payload larger than 64 KiB', () => {
  it('is reported once, reassembled and decoded, on both peers', async () => {
    const big = 'x'.repeat(200_000);
    pair = await startPair(async (payload: string, stream) => {
      await stream.reply({ length: payload.length });
    });

    const stream = pair.client.open(big, { end: true });
    expect(await stream).toEqual({ length: 200_000 });

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.request.payloadCount).toBe(1);
      expect(exchange.request.payloads).toHaveLength(1);
      expect(exchange.request.payloads[0]).toBe(big);
      expect(exchange.response.payloads).toEqual([{ length: 200_000 }]);
    });
  });
});

// --------------------------------------------------------------------------- resets and losses

describe('a stream that does not end normally', () => {
  it('reports a handler error as a reset with APPLICATION_ERROR and the reason on both peers', async () => {
    pair = await startPair(() => {
      throw new Error('boom');
    });

    const stream = pair.client.open('x', { end: true });
    await expect(stream.result()).rejects.toThrow();

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.outcome).toBe('reset');
      expect(exchange.code).toBe(ResetCode.APPLICATION_ERROR);
      expect(exchange.code).toBe(2);
      expect(exchange.reason).toBe('boom');
    });
    expect(server.resetBy).toBe('local');
    expect(client.resetBy).toBe('remote');
  });

  it('reports a cancel as reset by local on the dialer and by remote on the acceptor', async () => {
    let started = false;
    pair = await startPair(async (_payload, stream) => {
      started = true;
      try {
        for await (const _item of stream) {
          // waits for the cancel
        }
      } catch {
        // the reset ends the iteration
      }
    });

    const stream = pair.client.open('long');
    await until(() => started, 'the handler to start');
    await stream.cancel('user gave up');

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.outcome).toBe('reset');
      expect(exchange.code).toBe(ResetCode.CANCELLED);
      expect(exchange.reason).toBe('user gave up');
      expect(exchange.request.payloads).toEqual(['long']);
      expect(exchange.request.ended).toBe(false);
    });
    expect(client.resetBy).toBe('local');
    expect(server.resetBy).toBe('remote');
  });

  it('reports a stream refused by a received goaway as reset by remote with REFUSED on the opener', async () => {
    let started = false;
    pair = await startPair(async (_payload, stream) => {
      started = true;
      try {
        for await (const _item of stream) {
          // waits until the connection closes
        }
      } catch {
        // the close ends the iteration
      }
    });

    const stream = pair.client.open('refused');
    stream.result().catch(() => undefined);
    await until(() => started, 'the handler to start');
    pair.server().enqueue({ type: 'goaway', code: 1000, reason: 'shutting down', last_stream: 0 });

    await until(() => forStream(pair!.clientExchanges, stream.id).length > 0, 'the opener exchange');
    const [client] = forStream(pair.clientExchanges, stream.id);
    expect(client.outcome).toBe('reset');
    expect(client.resetBy).toBe('remote');
    expect(client.code).toBe(ResetCode.REFUSED);
  });

  it('reports a stream live when the connection drops as lost on both peers', async () => {
    let started = false;
    pair = await startPair(async (_payload, stream) => {
      started = true;
      try {
        for await (const _item of stream) {
          // the client never ends the stream
        }
      } catch {
        // the connection loss ends the iteration
      }
    });

    const stream = pair.client.open('stay');
    stream.result().catch(() => undefined);
    await until(() => started, 'the handler to start');
    pair.dropSockets();

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.outcome).toBe('lost');
      expect(exchange.resetBy).toBeNull();
      expect(exchange.code).toBeNull();
      expect(exchange.reason).toBeNull();
      expect(exchange.request.payloads).toEqual(['stay']);
      expect(exchange.request.ended).toBe(false);
      expect(exchange.response.ended).toBe(false);
      expect(exchange.firstResponseMs).toBeNull();
    });
  });
});

// --------------------------------------------------------------------------- oneway

describe('a notify', () => {
  it('is a oneway exchange on both peers', async () => {
    const received: unknown[] = [];
    pair = await startPair((payload) => {
      received.push(payload);
    });

    await pair.client.notify({ event: 'tick' }, { headers: { 'x-n': 1 } });

    await until(() => pair!.clientExchanges.length === 1, 'the dialer exchange');
    const [client] = pair.clientExchanges;
    expect(client.openedBy).toBe('local');

    const { server } = await bothSides(pair, client.stream);
    expect(server.openedBy).toBe('remote');
    expect(received).toEqual([{ event: 'tick' }]);

    [client, server].forEach((exchange) => {
      expect(exchange.oneway).toBe(true);
      expect(exchange.outcome).toBe('ok');
      expect(exchange.request.headers).toEqual({ 'x-n': 1 });
      expect(exchange.request.payloads).toEqual([{ event: 'tick' }]);
      expect(exchange.request.payloadCount).toBe(1);
      expect(exchange.request.ended).toBe(true);
      expect(exchange.response.headers).toBeNull();
      expect(exchange.response.payloads).toEqual([]);
      expect(exchange.response.payloadCount).toBe(0);
      expect(exchange.response.trailers).toBeNull();
      expect(exchange.response.ended).toBe(false);
    });
    expect(client.firstResponseMs).toBeNull();
  });
});

// --------------------------------------------------------------------------- timing

describe('timing', () => {
  it('reports milliseconds, first response within the duration, both non-negative', async () => {
    pair = await startPair(async (payload, stream) => {
      await sleep(60);
      await stream.reply(payload);
    });

    const stream = pair.client.open('slow', { end: true });
    await stream;

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      expect(exchange.firstResponseMs).not.toBeNull();
      const first = exchange.firstResponseMs as number;
      expect(first).toBeGreaterThanOrEqual(0);
      expect(first).toBeLessThanOrEqual(exchange.durationMs);
      expect(exchange.durationMs).toBeGreaterThanOrEqual(0);
    });
    // Milliseconds, not seconds: the 60 ms delay precedes the first answering frame on both peers.
    expect(client.firstResponseMs as number).toBeGreaterThanOrEqual(50);
    expect(server.firstResponseMs as number).toBeGreaterThanOrEqual(50);
    expect(client.durationMs).toBeLessThan(10_000);
  });
});

// --------------------------------------------------------------------------- asDict

describe('asDict()', () => {
  async function oneExchange(payload: unknown, reply: unknown): Promise<Exchange> {
    pair = await startPair(async (_payload, stream) => {
      await stream.reply(reply, {
        headers: { 'x-reply': 'long-header-value-never-trimmed' },
      });
    });
    const stream = pair.client.open(payload, {
      headers: { 'x-trace': 't' },
      end: true,
    });
    await stream;
    return (await bothSides(pair, stream.id)).client;
  }

  it('returns the keys in the specified order with an ISO millisecond timestamp', async () => {
    const exchange = await oneExchange({ q: 1 }, { a: 2 });
    const dict = exchange.asDict() as unknown as Record<string, unknown>;

    expect(Object.keys(dict)).toEqual([
      'ts',
      'conn',
      'stream',
      'opened_by',
      'oneway',
      'request',
      'response',
      'outcome',
      'reset_by',
      'code',
      'reason',
      'first_response_ms',
      'duration_ms',
    ]);
    const sideKeys = ['headers', 'payloads', 'payload_count', 'trailers', 'ended'];
    expect(Object.keys(dict.request as object)).toEqual(sideKeys);
    expect(Object.keys(dict.response as object)).toEqual(sideKeys);

    expect(dict.ts).toMatch(ISO_MS_UTC);
    expect(dict.ts).toBe(exchange.startedAt.toISOString());
    expect(dict.conn).toBe(exchange.conn);
    expect(dict.stream).toBe(exchange.stream);
    expect(dict.opened_by).toBe('local');
    expect(dict.oneway).toBe(false);
    expect(dict.outcome).toBe('ok');
    expect(dict.reset_by).toBeNull();
    expect(dict.code).toBeNull();
    expect(dict.reason).toBeNull();
    expect(dict.request).toEqual({
      headers: { 'x-trace': 't' },
      payloads: [{ q: 1 }],
      payload_count: 1,
      trailers: null,
      ended: true,
    });
    expect(dict.response).toEqual({
      headers: { 'x-reply': 'long-header-value-never-trimmed' },
      payloads: [{ a: 2 }],
      payload_count: 1,
      trailers: null,
      ended: true,
    });

    // Milliseconds rounded to three decimals.
    [dict.first_response_ms, dict.duration_ms].forEach((value) => {
      expect(typeof value).toBe('number');
      expect(Math.round((value as number) * 1000) / 1000).toBe(value);
    });
    expect(JSON.parse(JSON.stringify(dict))).toEqual(dict);
  });

  it('replaces a payload longer than the limit by its cut JSON text and the trim marker', async () => {
    const exchange = await oneExchange('x'.repeat(100), 'ok');
    const dict = exchange.asDict({ trimPayloadSize: 10 }) as {
      request: { payloads: unknown[]; headers: unknown };
      response: { payloads: unknown[]; headers: unknown };
    };

    // JSON text `"xxx…x"` is 102 bytes; the first 10 are kept and 92 cut.
    expect(dict.request.payloads).toEqual([`"${'x'.repeat(9)}…(trimmed 92 bytes)…`]);
    // `"ok"` is 4 bytes, within the limit, and stays a value.
    expect(dict.response.payloads).toEqual(['ok']);
    // Headers are never trimmed.
    expect(dict.request.headers).toEqual({ 'x-trace': 't' });
    expect(dict.response.headers).toEqual({
      'x-reply': 'long-header-value-never-trimmed',
    });
  });

  it('cuts back to a UTF-8 character boundary and counts the bytes cut', async () => {
    // JSON text: `"` + ten two-byte characters + `"` = 22 bytes. A limit of 4 falls inside the second
    // character, so 3 bytes are kept and 19 cut.
    const exchange = await oneExchange('č'.repeat(10), 'ok');
    const dict = exchange.asDict({ trimPayloadSize: 4 }) as {
      request: { payloads: unknown[] };
    };

    expect(dict.request.payloads).toEqual(['"č…(trimmed 19 bytes)…']);
  });

  it('trims nothing by default and rejects a negative limit', async () => {
    const exchange = await oneExchange('y'.repeat(5000), 'ok');
    const dict = exchange.asDict() as { request: { payloads: unknown[] } };

    expect(dict.request.payloads).toEqual(['y'.repeat(5000)]);
    expect(() => exchange.asDict({ trimPayloadSize: -1 })).toThrow(RangeError);
  });

  it('returns a fresh structure: mutating it leaves the exchange unchanged', async () => {
    const exchange = await oneExchange({ secret: 's' }, 'ok');
    const dict = exchange.asDict() as {
      request: {
        headers: Record<string, unknown>;
        payloads: Record<string, unknown>[];
      };
    };

    dict.request.headers['x-trace'] = 'redacted';
    dict.request.payloads[0].secret = 'redacted';

    expect(exchange.request.headers).toEqual({ 'x-trace': 't' });
    expect(exchange.request.payloads).toEqual([{ secret: 's' }]);
    expect(exchange.asDict()).toMatchObject({
      request: { headers: { 'x-trace': 't' }, payloads: [{ secret: 's' }] },
    });
  });

  it('encodes binary values as base64 over a codec that carries bytes (msgpack)', async () => {
    pair = await startPair(
      async (_payload, stream) => {
        await stream.reply(new Uint8Array([1, 2, 3]));
      },
      { codec: new MsgpackCodec() },
    );

    const stream = pair.client.open(new Uint8Array([0, 1, 2, 250]), {
      headers: { blob: new Uint8Array([255]) },
      end: true,
    });
    await stream;

    const { client, server } = await bothSides(pair, stream.id);
    [client, server].forEach((exchange) => {
      const dict = exchange.asDict() as {
        request: { headers: Record<string, unknown>; payloads: unknown[] };
        response: { payloads: unknown[] };
      };
      expect(dict.request.payloads).toEqual(['AAEC+g==']);
      expect(dict.request.headers).toEqual({ blob: '/w==' });
      expect(dict.response.payloads).toEqual(['AQID']);
      expect(JSON.parse(JSON.stringify(dict))).toEqual(dict);
    });
  });
});

// --------------------------------------------------------------------------- containment and registration

describe('handler errors and registration', () => {
  it('logs a throwing handler and still runs the others; the connection keeps working', async () => {
    pair = await startPair(async (payload, stream) => stream.reply(payload), {
      client: false,
    });
    const seen: Exchange[] = [];
    pair.client.onExchange(() => {
      throw new Error('handler bug');
    });
    pair.client.onExchange((exchange) => seen.push(exchange));

    expect(await pair.client.request('one')).toBe('one');
    await until(() => seen.length === 1, 'the second handler');
    expect(console.error).toHaveBeenCalled();
    expect(seen[0].outcome).toBe('ok');

    expect(await pair.client.request('two')).toBe('two');
    await until(() => seen.length === 2, 'the second exchange');
    expect(seen[1].request.payloads).toEqual(['two']);
    expect(pair.client.isOpen).toBe(true);
  });

  it('logs a rejection of an async handler; the connection keeps working', async () => {
    pair = await startPair(async (payload, stream) => stream.reply(payload), {
      client: false,
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      const seen: Exchange[] = [];
      pair.client.onExchange(async () => {
        throw new Error('boom-async');
      });
      pair.client.onExchange((exchange) => seen.push(exchange));

      expect(await pair.client.request('one')).toBe('one');
      await until(() => seen.length === 1, 'the second handler');
      await until(
        () =>
          vi
            .mocked(console.error)
            .mock.calls.some((call) => call.some((arg) => arg instanceof Error && arg.message === 'boom-async')),
        'the logged rejection',
      );
      await sleep(20);
      expect(unhandled).toEqual([]);
      expect(await pair.client.request('two')).toBe('two');
      expect(pair.client.isOpen).toBe(true);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('does not report a stream created before the first handler was registered', async () => {
    pair = await startPair(
      async (payload, stream) => {
        // Only 'early' leaves its side open; a stream whose open carried `end` has nothing to drain.
        if (payload === 'early') {
          for await (const _item of stream) {
            // drained
          }
        }
        await stream.end({ payload: 'done' });
      },
      { client: false },
    );

    const early = pair.client.open('early');
    const seen: Exchange[] = [];
    pair.client.onExchange((exchange) => seen.push(exchange));
    await early.end();
    expect(await early).toBe('done');

    expect(await pair.client.request('late')).toBe('done');
    await until(() => seen.length === 1, 'the later exchange');
    await sleep(20);

    expect(seen).toHaveLength(1);
    expect(seen[0].stream).not.toBe(early.id);
    expect(seen[0].request.payloads).toEqual(['late']);
  });

  it('keeps the registration across a reconnect and reports the new connection id', async () => {
    pair = await startPair(async (payload, stream) => stream.reply(payload), {
      reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
    });
    const reconnected: number[] = [];
    pair.client.onReconnect((attempt) => reconnected.push(attempt));

    expect(await pair.client.request('before')).toBe('before');
    await until(() => pair!.clientExchanges.length === 1, 'the first exchange');
    const firstConn = pair.clientExchanges[0].conn;

    pair.dropSockets();
    await until(() => reconnected.length === 1, 'the reconnect');

    expect(await pair.client.request('after')).toBe('after');
    // The reconnect helper's replayed hello is a stream of the new connection and is reported too.
    const requests = (): Exchange[] =>
      pair!.clientExchanges.filter((exchange) => !isHello(exchange.request.payloads[0]));
    await until(() => requests().length === 2, 'the exchange after the reconnect');
    const after = requests()[1];
    expect(after.request.payloads).toEqual(['after']);
    expect(after.conn).toBe(pair.client.id);
    expect(after.conn).not.toBe(firstConn);
  });
});
