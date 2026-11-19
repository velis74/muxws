// @vitest-environment node
/**
 * Scenarios for the `ExchangeLog` recipe (`exchange-log.ts`) over a real WebSocket connection.
 *
 * Every test runs a `muxws/node` dialer against a `ws` acceptor on 127.0.0.1, attaches an `ExchangeLog`
 * to one or both peers with a `write` callback that collects lines, and asserts on the parsed JSON
 * records.
 */

import type { AddressInfo } from 'node:net';

import { WebSocketServer, type WebSocket } from 'ws';

import {
  type Frame,
  JsonCodec,
  type Peer,
  RemoteError,
  ResetCode,
  type Stream,
  type StreamHandler,
} from '../../ts/index';
import { accept, connect, handleProtocols } from '../../ts/node';

import { ExchangeLog } from './exchange-log';

// --------------------------------------------------------------------------- the record

interface Side {
  headers: Record<string, unknown> | null;
  payloads: unknown[];
  trailers: Record<string, unknown> | null;
  dropped: number;
  bytes: number;
}

interface ExchangeRecord {
  ts: string;
  conn: string;
  stream: number;
  opened_by: 'local' | 'remote';
  oneway: boolean;
  request: Side;
  response: Side;
  outcome: 'ok' | 'reset' | 'lost';
  code: number | null;
  reason: string | null;
  first_response_ms: number | null;
  duration_ms: number;
}

const RECORD_KEYS = [
  'ts',
  'conn',
  'stream',
  'opened_by',
  'oneway',
  'request',
  'response',
  'outcome',
  'code',
  'reason',
  'first_response_ms',
  'duration_ms',
];

const SIDE_KEYS = ['headers', 'payloads', 'trailers', 'dropped', 'bytes'];

/** One `ExchangeLog` writing into an array; `records()` parses every line written so far. */
class Collector {
  readonly lines: string[] = [];

  readonly write = (line: string): void => {
    this.lines.push(line);
  };

  records(): ExchangeRecord[] {
    return this.lines.map((line) => JSON.parse(line) as ExchangeRecord);
  }

  only(): ExchangeRecord {
    expect(this.lines).toHaveLength(1);
    return this.records()[0];
  }
}

// --------------------------------------------------------------------------- the connection

type LogOptions = Omit<ConstructorParameters<typeof ExchangeLog>[0], 'codec' | 'write'>;

interface Connection {
  dialer: Peer;
  acceptor: Peer;
  dialerLog: Collector;
  acceptorLog: Collector;
  /** Bytes per `${direction}:${stream}` as reported by `onFrame` on the dialer. */
  dialerBytes: Map<string, number>;
  /** Frames the dialer sent or received, per stream. */
  dialerFrames: Frame[];
  /** Terminates the acceptor's socket without a close handshake. */
  drop: () => void;
}

const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0, cleanups.length).reverse()) await cleanup();
});

/**
 * A dialer and an acceptor over a real `ws` server, each with its own `ExchangeLog` when the matching
 * options are given (`null` = no log on that side).
 */
async function open(
  handler: StreamHandler,
  logs: { dialer?: LogOptions | null; acceptor?: LogOptions | null } = {},
): Promise<Connection> {
  const dialerLog = new Collector();
  const acceptorLog = new Collector();
  const server = new WebSocketServer({ port: 0, handleProtocols });
  let acceptorSocket: WebSocket | null = null;
  const acceptorReady = new Promise<Peer>((resolve, reject) => {
    server.on('connection', (socket) => {
      acceptorSocket = socket;
      accept(socket).then((peer) => {
        if (logs.acceptor !== null) {
          new ExchangeLog({ codec: new JsonCodec(), write: acceptorLog.write, ...logs.acceptor }).attach(peer);
        }
        peer.onStream(handler);
        void peer.serve().catch(() => undefined);
        resolve(peer);
      }, reject);
    });
  });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  cleanups.push(() => new Promise<void>((resolve) => server.close(() => resolve())));

  const url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const dialer = await connect(url);
  cleanups.push(() => dialer.close({ drainMs: 0 }).catch(() => undefined));
  const acceptor = await acceptorReady;
  cleanups.push(() => acceptor.close({ drainMs: 0 }).catch(() => undefined));

  const dialerBytes = new Map<string, number>();
  const dialerFrames: Frame[] = [];
  dialer.onFrame((direction, frame, byteLength) => {
    if (frame.stream === undefined || frame.stream === null) return;
    dialerFrames.push(frame);
    const key = `${direction}:${frame.stream}`;
    dialerBytes.set(key, (dialerBytes.get(key) ?? 0) + byteLength);
  });
  if (logs.dialer !== null) {
    new ExchangeLog({ codec: new JsonCodec(), write: dialerLog.write, ...logs.dialer }).attach(dialer);
  }

  return {
    dialer,
    acceptor,
    dialerLog,
    acceptorLog,
    dialerBytes,
    dialerFrames,
    drop: () => acceptorSocket?.terminate(),
  };
}

async function until(predicate: () => boolean, what: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`${what} never happened within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function drain(stream: Stream): Promise<unknown[]> {
  const items: unknown[] = [];
  for await (const item of stream) items.push(item);
  return items;
}

const echo: StreamHandler = (payload, stream) => stream.reply({ echo: payload });

const ISO_UTC_MS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;

// --------------------------------------------------------------------------- unary request/reply

describe('a unary request/reply', () => {
  it('is one record on each side, with request and response paired the same way on both', async () => {
    const c = await open(echo, { dialer: {}, acceptor: {} });
    const before = Date.now();

    const answer = await c.dialer.request({ ask: 'hi' }, { headers: { route: 'greet' } });
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(answer).toEqual({ echo: { ask: 'hi' } });
    const mine = c.dialerLog.only();
    const theirs = c.acceptorLog.only();

    expect(mine.conn).toBe(c.dialer.id);
    expect(theirs.conn).toBe(c.acceptor.id);
    expect(mine.stream).toBe(theirs.stream);
    expect(mine.opened_by).toBe('local');
    expect(theirs.opened_by).toBe('remote');
    for (const record of [mine, theirs]) {
      expect(record.oneway).toBe(false);
      expect(record.request.headers).toEqual({ route: 'greet' });
      expect(record.request.payloads).toEqual([{ ask: 'hi' }]);
      expect(record.request.dropped).toBe(0);
      expect(record.response.payloads).toEqual([{ echo: { ask: 'hi' } }]);
      expect(record.response.dropped).toBe(0);
      expect(record.outcome).toBe('ok');
      expect(record.code).toBeNull();
      expect(record.reason).toBeNull();
      expect(record.ts).toMatch(ISO_UTC_MS);
      const ts = Date.parse(record.ts);
      expect(ts).toBeGreaterThanOrEqual(before - 1000);
      expect(ts).toBeLessThanOrEqual(Date.now() + 1000);
    }
  });

  it('counts the bytes of each side as the sum of its frames on the wire', async () => {
    const c = await open(echo, { dialer: {} });

    await c.dialer.request({ ask: 'size me' });
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(record.request.bytes).toBe(c.dialerBytes.get(`tx:${record.stream}`));
    expect(record.response.bytes).toBe(c.dialerBytes.get(`rx:${record.stream}`));
    expect(record.request.bytes).toBeGreaterThan(0);
    expect(record.response.bytes).toBeGreaterThan(0);
  });

  it('writes the keys in the documented order, on the record and on each side', async () => {
    const c = await open(echo, { dialer: {} });

    await c.dialer.request('order');
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(Object.keys(record)).toEqual(RECORD_KEYS);
    expect(Object.keys(record.request)).toEqual(SIDE_KEYS);
    expect(Object.keys(record.response)).toEqual(SIDE_KEYS);
  });

  it('records a request opened by the acceptor as local on the acceptor and remote on the dialer', async () => {
    const c = await open(echo, { dialer: {}, acceptor: {} });
    c.dialer.onStream((payload, stream) => stream.reply({ back: payload }));

    const answer = await c.acceptor.request('push');
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(answer).toEqual({ back: 'push' });
    expect(c.acceptorLog.only().opened_by).toBe('local');
    expect(c.dialerLog.only().opened_by).toBe('remote');
    for (const record of [c.acceptorLog.only(), c.dialerLog.only()]) {
      expect(record.request.payloads).toEqual(['push']);
      expect(record.response.payloads).toEqual([{ back: 'push' }]);
    }
  });

  it('logs one record per stream when several run concurrently', async () => {
    const c = await open(echo, { dialer: {} });

    await Promise.all([c.dialer.request(1), c.dialer.request(2), c.dialer.request(3)]);
    await until(() => c.dialerLog.lines.length === 3, 'three records');

    const records = c.dialerLog.records();
    expect(new Set(records.map((r) => r.stream)).size).toBe(3);
    for (const record of records) {
      expect(record.response.payloads).toEqual([{ echo: record.request.payloads[0] }]);
    }
  });
});

// --------------------------------------------------------------------------- streamed responses

describe('a streamed response', () => {
  const streamer: StreamHandler = async (payload, stream) => {
    const { count } = payload as { count: number };
    for (let i = 0; i < count; i += 1) await stream.send({ i });
    await stream.end({ trailers: { total: count } });
  };

  it('keeps every message in order, with the trailers, in one record', async () => {
    const c = await open(streamer, { dialer: {}, acceptor: {} });

    const items = await drain(c.dialer.open({ count: 4 }, { end: true }));
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(items).toEqual([{ i: 0 }, { i: 1 }, { i: 2 }, { i: 3 }]);
    for (const record of [c.dialerLog.only(), c.acceptorLog.only()]) {
      expect(record.request.payloads).toEqual([{ count: 4 }]);
      expect(record.response.payloads).toEqual([{ i: 0 }, { i: 1 }, { i: 2 }, { i: 3 }]);
      expect(record.response.trailers).toEqual({ total: 4 });
      expect(record.response.dropped).toBe(0);
      expect(record.outcome).toBe('ok');
    }
  });

  it('keeps maxMessages payloads per side and counts the rest as dropped', async () => {
    const c = await open(streamer, { dialer: { maxMessages: 2 } });

    const items = await drain(c.dialer.open({ count: 5 }, { end: true }));
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    expect(items).toHaveLength(5);
    const record = c.dialerLog.only();
    expect(record.response.payloads).toEqual([{ i: 0 }, { i: 1 }]);
    expect(record.response.dropped).toBe(3);
    expect(record.request.payloads).toEqual([{ count: 5 }]);
    expect(record.request.dropped).toBe(0);
  });

  it('applies maxMessages to the request side as well', async () => {
    const collect: StreamHandler = async (_payload, stream) => {
      const rest = await drain(stream);
      await stream.end({ payload: { received: rest.length + 1 } });
    };
    const c = await open(collect, { dialer: { maxMessages: 1 } });

    const stream = c.dialer.open('a');
    await stream.send('b');
    await stream.send('c');
    await stream.end();
    const answer = await drain(stream);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    expect(answer).toEqual([{ received: 3 }]);
    const record = c.dialerLog.only();
    expect(record.request.payloads).toEqual(['a']);
    expect(record.request.dropped).toBe(2);
    expect(record.response.payloads).toEqual([{ received: 3 }]);
  });

  it('keeps every payload when maxMessages is null', async () => {
    const c = await open(streamer, { dialer: { maxMessages: null } });

    await drain(c.dialer.open({ count: 150 }, { end: true }));
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(record.response.payloads).toHaveLength(150);
    expect(record.response.dropped).toBe(0);
  });
});

// --------------------------------------------------------------------------- trimming

describe('trimPayloadSize', () => {
  /** Larger than 64 KiB, so muxws fragments it. */
  const LARGE = 'x'.repeat(100_000);

  it('logs a fragmented payload once, decoded, and trims its JSON text with the exact marker', async () => {
    const c = await open(echo, { dialer: { trimPayloadSize: 1000 } });

    const answer = await c.dialer.request(LARGE);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    expect(answer).toEqual({ echo: LARGE });
    const record = c.dialerLog.only();
    const sent = c.dialerFrames.filter((f) => f.stream === record.stream && f.fragment != null);
    expect(sent.length).toBeGreaterThan(1);

    const requestText = JSON.stringify(LARGE);
    expect(record.request.payloads).toEqual([
      `${requestText.slice(0, 1000)}…(trimmed ${requestText.length - 1000} bytes)…`,
    ]);
    const responseText = JSON.stringify({ echo: LARGE });
    expect(record.response.payloads).toEqual([
      `${responseText.slice(0, 1000)}…(trimmed ${responseText.length - 1000} bytes)…`,
    ]);
    expect(record.request.bytes).toBe(c.dialerBytes.get(`tx:${record.stream}`));
    expect(record.request.bytes).toBeGreaterThan(100_000);
  });

  it('trims at 4096 bytes by default', async () => {
    const c = await open(echo, { dialer: {} });

    await c.dialer.request(LARGE);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const text = JSON.stringify(LARGE);
    expect(c.dialerLog.only().request.payloads).toEqual([
      `${text.slice(0, 4096)}…(trimmed ${text.length - 4096} bytes)…`,
    ]);
  });

  it('logs the whole payload when trimPayloadSize is null', async () => {
    const c = await open(echo, { dialer: { trimPayloadSize: null } });

    await c.dialer.request(LARGE);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(record.request.payloads).toEqual([LARGE]);
    expect(record.response.payloads).toEqual([{ echo: LARGE }]);
  });

  it('logs a payload whose JSON text is exactly trimPayloadSize bytes as its JSON value', async () => {
    const c = await open(echo, { dialer: { trimPayloadSize: 12 } });
    const fits = { a: 'bcde' };
    expect(Buffer.byteLength(JSON.stringify(fits))).toBe(12);

    await c.dialer.request(fits);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    expect(c.dialerLog.only().request.payloads).toEqual([fits]);
  });

  it('counts UTF-8 bytes and cuts on a character boundary', async () => {
    const c = await open(echo, { dialer: { trimPayloadSize: 4 } });
    // `"` is one byte and every `č` two, so a 4-byte cut keeps `"č` (3 bytes).
    const payload = 'čččč';
    const total = Buffer.byteLength(JSON.stringify(payload));
    expect(total).toBe(10);

    await c.dialer.request(payload);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    expect(c.dialerLog.only().request.payloads).toEqual([`"č…(trimmed ${total - 3} bytes)…`]);
  });

  it('never trims headers or trailers', async () => {
    const long = 'h'.repeat(500);
    const c = await open((_payload, stream) => stream.reply('ok', { trailers: { note: long } }), {
      dialer: { trimPayloadSize: 10 },
    });

    await c.dialer.request('q', { headers: { trace: long } });
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(record.request.headers).toEqual({ trace: long });
    expect(record.response.trailers).toEqual({ note: long });
  });
});

// --------------------------------------------------------------------------- redact and include

describe('redact', () => {
  it('replaces each payload before it is logged and is told which side it came from', async () => {
    const calls: [unknown, string][] = [];
    const redact = (payload: unknown, side: 'request' | 'response'): unknown => {
      calls.push([payload, side]);
      if (typeof payload === 'object' && payload !== null && 'password' in payload) {
        return { ...payload, password: '***' };
      }
      return payload;
    };
    const login: StreamHandler = (_payload, stream) => stream.reply({ token: 'secret-token' });
    const c = await open(login, {
      dialer: {
        redact: (payload: unknown, side: 'request' | 'response') =>
          side === 'response' ? { token: '***' } : redact(payload, side),
      },
      acceptor: { redact },
    });

    const answer = await c.dialer.request({ user: 'ana', password: 'hunter2' });
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(answer).toEqual({ token: 'secret-token' });
    const mine = c.dialerLog.only();
    expect(mine.request.payloads).toEqual([{ user: 'ana', password: '***' }]);
    expect(mine.response.payloads).toEqual([{ token: '***' }]);
    expect(c.dialerLog.lines[0]).not.toContain('hunter2');
    expect(c.dialerLog.lines[0]).not.toContain('secret-token');

    const theirs = c.acceptorLog.only();
    expect(theirs.request.payloads).toEqual([{ user: 'ana', password: '***' }]);
    expect(theirs.response.payloads).toEqual([{ token: 'secret-token' }]);
    expect(calls).toContainEqual([{ user: 'ana', password: 'hunter2' }, 'request']);
    expect(calls).toContainEqual([{ token: 'secret-token' }, 'response']);
  });

  it('runs before trimming', async () => {
    const c = await open(echo, {
      dialer: { trimPayloadSize: 5, redact: (_payload: unknown) => 'r'.repeat(20) },
    });

    await c.dialer.request('anything');
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const text = JSON.stringify('r'.repeat(20));
    expect(c.dialerLog.only().request.payloads).toEqual([`${text.slice(0, 5)}…(trimmed ${text.length - 5} bytes)…`]);
  });
});

describe('include', () => {
  it('leaves out a stream for which it returns false, judged on the opening headers and payload', async () => {
    const seen: [unknown, unknown][] = [];
    const include = (headers: Record<string, unknown> | null, payload: unknown): boolean => {
      seen.push([headers, payload]);
      return headers?.route !== 'health';
    };
    const c = await open(echo, { dialer: { include }, acceptor: { include } });

    await c.dialer.request('ping', { headers: { route: 'health' } });
    await c.dialer.request('work', { headers: { route: 'jobs' } });
    await until(() => c.dialerLog.lines.length >= 1 && c.acceptorLog.lines.length >= 1, 'the included records');
    await c.dialer.request('ping again', { headers: { route: 'health' } });
    await c.dialer.request('more', { headers: { route: 'jobs' } });
    await until(() => c.dialerLog.lines.length >= 2 && c.acceptorLog.lines.length >= 2, 'the second records');

    for (const log of [c.dialerLog, c.acceptorLog]) {
      const records = log.records();
      expect(records).toHaveLength(2);
      expect(records.map((r) => r.request.payloads[0])).toEqual(['work', 'more']);
      expect(log.lines.join('\n')).not.toContain('ping');
    }
    expect(seen).toContainEqual([{ route: 'health' }, 'ping']);
    expect(seen).toContainEqual([{ route: 'jobs' }, 'work']);
  });
});

describe('callbacks that throw', () => {
  it('drop only their own stream; the connection keeps serving', async () => {
    const c = await open(echo, {
      dialer: null,
      acceptor: {
        redact: (payload) => {
          if (payload === 'fail-redact') throw new Error('redact failed');
          return payload;
        },
        include: (_headers, payload) => {
          if (payload === 'fail-include') throw new Error('include failed');
          return true;
        },
      },
    });
    const failing = new Collector();
    new ExchangeLog({
      codec: new JsonCodec(),
      write: (line) => {
        if (line.includes('"fail-write"')) throw new Error('disk full');
        failing.write(line);
      },
    }).attach(c.acceptor);

    for (const payload of ['fail-write', 'fail-redact', 'fail-include', 'fine']) {
      expect(await c.dialer.request(payload)).toEqual({ echo: payload });
    }
    await until(() => c.acceptorLog.lines.length >= 2 && failing.lines.length >= 3, 'the surviving records');

    expect(c.acceptorLog.records().map((r) => r.request.payloads[0])).toEqual(['fail-write', 'fine']);
    expect(failing.records().map((r) => r.request.payloads[0])).toEqual(['fail-redact', 'fail-include', 'fine']);
    expect(c.dialer.isOpen).toBe(true);
  });
});

// --------------------------------------------------------------------------- reset, oneway, lost

describe('outcomes', () => {
  it('records a handler error as a reset with the code and reason of the reset frame', async () => {
    const failing: StreamHandler = () => {
      throw new Error('boom');
    };
    const c = await open(failing, { dialer: {}, acceptor: {} });

    await expect(c.dialer.request({ go: 1 })).rejects.toBeInstanceOf(RemoteError);
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    for (const record of [c.dialerLog.only(), c.acceptorLog.only()]) {
      expect(record.outcome).toBe('reset');
      expect(record.code).toBe(ResetCode.APPLICATION_ERROR);
      expect(record.reason).toBe('boom');
      expect(record.request.payloads).toEqual([{ go: 1 }]);
      expect(record.response.payloads).toEqual([]);
    }
  });

  it('records a reset sent by the opener with its code and reason', async () => {
    const waiting: StreamHandler = async (_payload, stream) => {
      await drain(stream).catch(() => undefined);
    };
    const c = await open(waiting, { dialer: {}, acceptor: {} });

    const stream = c.dialer.open('started');
    await until(() => c.acceptor.streams.size === 1, 'the remote stream');
    await stream.reset(ResetCode.CANCELLED, 'changed my mind');
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    for (const record of [c.dialerLog.only(), c.acceptorLog.only()]) {
      expect(record.outcome).toBe('reset');
      expect(record.code).toBe(ResetCode.CANCELLED);
      expect(record.reason).toBe('changed my mind');
    }
  });

  it('writes a notify at once as a oneway record with no response', async () => {
    const received: unknown[] = [];
    const c = await open(
      (payload) => {
        received.push(payload);
      },
      { dialer: {}, acceptor: {} },
    );

    await c.dialer.notify({ event: 'saved' }, { headers: { kind: 'audit' } });
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(c.dialerLog.only().opened_by).toBe('local');
    expect(c.acceptorLog.only().opened_by).toBe('remote');
    for (const record of [c.dialerLog.only(), c.acceptorLog.only()]) {
      expect(record.oneway).toBe(true);
      expect(record.request.headers).toEqual({ kind: 'audit' });
      expect(record.request.payloads).toEqual([{ event: 'saved' }]);
      expect(record.response.payloads).toEqual([]);
      expect(record.outcome).toBe('ok');
      expect(record.code).toBeNull();
      expect(record.first_response_ms).toBeNull();
    }
    await until(() => received.length === 1, 'the notification');
  });

  it('writes an unfinished stream as lost when the connection drops, and nothing more for finished ones', async () => {
    const handler: StreamHandler = async (payload, stream) => {
      if (payload === 'quick') {
        await stream.reply('done');
        return;
      }
      await drain(stream).catch(() => undefined);
    };
    const c = await open(handler, { dialer: {}, acceptor: {} });
    const dialerId = c.dialer.id;
    const acceptorId = c.acceptor.id;

    await c.dialer.request('quick');
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'the finished records');
    const pending = c.dialer.open('slow');
    const pendingDone = drain(pending).catch(() => undefined);
    await until(() => c.acceptor.streams.size === 1, 'the remote stream');

    c.drop();
    await until(() => c.dialerLog.lines.length === 2 && c.acceptorLog.lines.length === 2, 'the lost records');
    await pendingDone;

    for (const [log, conn] of [
      [c.dialerLog, dialerId],
      [c.acceptorLog, acceptorId],
    ] as const) {
      const [finished, lost] = log.records();
      expect(finished.outcome).toBe('ok');
      expect(lost.outcome).toBe('lost');
      expect(lost.conn).toBe(conn);
      expect(lost.stream).toBe(pending.id);
      expect(lost.request.payloads).toEqual(['slow']);
      expect(lost.response.payloads).toEqual([]);
      expect(lost.first_response_ms).toBeNull();
      expect(lost.duration_ms).toBeGreaterThanOrEqual(0);
    }
  });
});

// --------------------------------------------------------------------------- timing

describe('timing', () => {
  it('has non-negative durations with first_response_ms not after duration_ms', async () => {
    const slow: StreamHandler = async (_payload, stream) => {
      await stream.send(1);
      await new Promise((resolve) => setTimeout(resolve, 20));
      await stream.end({ payload: 2 });
    };
    const c = await open(slow, { dialer: {}, acceptor: {} });

    await drain(c.dialer.open('t', { end: true }));
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    for (const record of [c.dialerLog.only(), c.acceptorLog.only()]) {
      expect(record.first_response_ms).not.toBeNull();
      expect(record.first_response_ms).toBeGreaterThanOrEqual(0);
      expect(record.duration_ms).toBeGreaterThanOrEqual(0);
      expect(record.first_response_ms as number).toBeLessThanOrEqual(record.duration_ms);
    }
  });

  it('measures from the open to the first response frame and to the finish, on the injected clock', async () => {
    let now = 1000;
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = false;
    const handler: StreamHandler = async (_payload, stream) => {
      started = true;
      await gate;
      await stream.send('first');
      await drain(stream);
      await stream.end();
    };
    const c = await open(handler, { dialer: { clock: () => now }, acceptor: null });

    const stream = c.dialer.open('begin');
    await until(() => started, 'the handler');
    now = 1250;
    release();
    const iterator = stream[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toBe('first');
    now = 1600;
    await stream.end();
    expect((await iterator.next()).done).toBe(true);
    await until(() => c.dialerLog.lines.length === 1, 'the record');

    const record = c.dialerLog.only();
    expect(record.first_response_ms).toBe(250);
    expect(record.duration_ms).toBe(600);
  });
});

// --------------------------------------------------------------------------- frames without a stream

describe('frames without a stream', () => {
  it('ignores ping and pong', async () => {
    const c = await open(echo, { dialer: {}, acceptor: {} });

    await c.dialer.ping();
    await c.acceptor.ping();
    await c.dialer.request('after pings');
    await until(() => c.dialerLog.lines.length === 1 && c.acceptorLog.lines.length === 1, 'both records');

    expect(c.dialerLog.only().request.payloads).toEqual(['after pings']);
    expect(c.acceptorLog.only().request.payloads).toEqual(['after pings']);
  });
});
