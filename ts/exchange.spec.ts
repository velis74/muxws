/**
 * White-box tests of `Exchange` and the peer's exchange recording: what the scenarios over a real
 * socket cannot reach deterministically. `ts/exchange-scenario.spec.ts` covers the behaviour on the wire.
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported for spies.
import { vi } from 'vitest';

import { JsonCodec } from './codec';
import { Exchange, type ExchangeInit, type ExchangeSide } from './exchange';
import { Peer } from './peer';
import { StreamState } from './stream';
import { memoryPair } from './transports/memory';

const ENCODER = new TextEncoder();
const EMPTY_SIDE: ExchangeSide = { headers: null, payloads: [], payloadCount: 0, trailers: null, ended: true };

function exchangeWith(overrides: Partial<ExchangeInit> = {}): Exchange {
  return new Exchange({
    conn: 'c-1',
    stream: 1,
    openedBy: 'local',
    oneway: false,
    request: EMPTY_SIDE,
    response: EMPTY_SIDE,
    outcome: 'ok',
    resetBy: null,
    code: null,
    reason: null,
    startedAt: new Date(Date.UTC(2026, 10, 19, 9, 32, 22, 359)),
    firstResponseMs: 1.23456,
    durationMs: 2.0004,
    ...overrides,
  });
}

function withPayloads(...payloads: unknown[]): Exchange {
  return exchangeWith({ request: { ...EMPTY_SIDE, payloads, payloadCount: payloads.length } });
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 12; turn += 1) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}

/** A dialer and an acceptor over memory sockets; the acceptor echoes every payload and ends after the opener. */
function startPair(): { dialer: Peer; acceptor: Peer; stop: () => Promise<void> } {
  const [left, right] = memoryPair();
  const dialer = new Peer(left, { codec: new JsonCodec(), isDialer: true });
  const acceptor = new Peer(right, { codec: new JsonCodec(), isDialer: false });
  acceptor.onStream(async (payload, stream) => {
    await stream.send(payload);
    if (stream.state === StreamState.HALF_CLOSED_REMOTE) return;
    for await (const item of stream) await stream.send(item);
  });
  const served = [dialer.serve(), acceptor.serve()];
  served.forEach((task) => {
    void task.catch(() => undefined);
  });
  return {
    dialer,
    acceptor,
    stop: async () => {
      left.drop();
      right.drop();
      await settle();
    },
  };
}

describe('Exchange.asDict', () => {
  it('returns the keys in the order Python does, with ts and milliseconds formatted', () => {
    const result = exchangeWith().asDict();
    expect(Object.keys(result)).toEqual([
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
    expect(Object.keys(result.request)).toEqual(['headers', 'payloads', 'payload_count', 'trailers', 'ended']);
    expect(result.ts).toBe('2026-11-19T09:32:22.359Z');
    expect(result.first_response_ms).toBe(1.235);
    expect(result.duration_ms).toBe(2);
    expect(exchangeWith({ firstResponseMs: null }).asDict().first_response_ms).toBeNull();
  });

  it('cuts a trimmed payload back to a UTF-8 character boundary', () => {
    // `"žžž"` is 8 bytes: the quote, three 2-byte characters, the quote. Byte 4 is inside the second `ž`.
    expect(withPayloads('žžž').asDict({ trimPayloadSize: 4 }).request.payloads).toEqual(['"ž…(trimmed 5 bytes)…']);
    // `"😀"` is 6 bytes; any limit from 1 to 4 keeps only the quote.
    [1, 2, 3, 4].forEach((limit) => {
      expect(withPayloads('😀').asDict({ trimPayloadSize: limit }).request.payloads).toEqual(['"…(trimmed 5 bytes)…']);
    });
    expect(withPayloads('😀').asDict({ trimPayloadSize: 5 }).request.payloads).toEqual(['"😀…(trimmed 1 bytes)…']);
    expect(withPayloads({ a: 1 }).asDict({ trimPayloadSize: 0 }).request.payloads).toEqual(['…(trimmed 7 bytes)…']);
  });

  it('keeps a payload within the limit as its value and never trims headers or trailers', () => {
    const long = 'x'.repeat(50);
    const exchange = exchangeWith({
      request: {
        headers: { h: long },
        payloads: [{ a: 1 }, long],
        payloadCount: 2,
        trailers: { t: long },
        ended: true,
      },
    });
    const side = exchange.asDict({ trimPayloadSize: 7 }).request;
    expect(side.payloads).toEqual([{ a: 1 }, `"xxxxxx…(trimmed 45 bytes)…`]);
    expect(side.headers).toEqual({ h: long });
    expect(side.trailers).toEqual({ t: long });
    expect(exchange.asDict().request.payloads).toEqual([{ a: 1 }, long]);
  });

  it('converts binary values anywhere to base64 before trimming', () => {
    const backing = new Uint8Array([0, 1, 2, 3, 255]);
    const exchange = exchangeWith({
      request: {
        headers: { key: new Uint8Array([104, 105]) },
        payloads: [backing.subarray(1, 4), { nested: [backing.buffer] }],
        payloadCount: 2,
        trailers: { sig: new DataView(backing.buffer, 4, 1) },
        ended: true,
      },
    });
    const side = exchange.asDict().request;
    expect(side.headers).toEqual({ key: 'aGk=' });
    expect(side.payloads).toEqual(['AQID', { nested: ['AAECA/8='] }]);
    expect(side.trailers).toEqual({ sig: '/w==' });
    expect(exchange.asDict({ trimPayloadSize: 4 }).request.payloads[0]).toBe('"AQI…(trimmed 2 bytes)…');
  });

  it('returns a fresh structure that a mutation does not carry back into the exchange', () => {
    const exchange = exchangeWith({
      request: {
        headers: { auth: 'secret' },
        payloads: [{ password: 'p' }],
        payloadCount: 1,
        trailers: null,
        ended: true,
      },
    });
    const first = exchange.asDict();
    (first.request.headers as Record<string, unknown>).auth = '***';
    (first.request.payloads[0] as Record<string, unknown>).password = '***';
    first.request.payloads.push('extra');
    expect(exchange.asDict().request).toMatchObject({ headers: { auth: 'secret' }, payloads: [{ password: 'p' }] });
  });

  // The same inputs and expected strings are in `muxws/exchange_test.py` (WSM-OBS-011).
  it.each([
    [
      [1.0, -0.0, 1.5, 1e21, 1e-7, 1.2345678901234568e20, 0.000001, 2.5e-8, 1e300, -1e-300],
      '[1,0,1.5,1e+21,1e-7,123456789012345680000,0.000001,2.5e-8,1e+300,-1e-300]',
    ],
    [[Number.NaN, Infinity, -Infinity, 9007199254740991], '[null,null,null,9007199254740991]'],
    [{ b: 1, '2': 2, a: 3, '1': 4, '01': 5, '4294967295': 6 }, '{"1":4,"2":2,"b":1,"a":3,"01":5,"4294967295":6}'],
    [{ 2: 'a', 1.5: 'b', 1.0: 'c' }, '{"1":"c","2":"a","1.5":"b"}'],
    [['\ud800x', '\u0000\u001f\u007f\u2028é'], '["\\ud800x","\\u0000\\u001f\u007f\u2028é"]'],
    [{ bin: new Uint8Array([0, 1]), nested: [{ x: [true, null] }] }, '{"bin":"AAE=","nested":[{"x":[true,null]}]}'],
  ])('trims %j to the same text as the Python port', (payload, expected) => {
    const limit = ENCODER.encode(expected).length - 1;
    expect(withPayloads(payload).asDict({ trimPayloadSize: limit }).request.payloads).toEqual([
      `${expected.slice(0, -1)}…(trimmed 1 bytes)…`,
    ]);
  });

  it('measures an integral float as an integer', () => {
    expect(withPayloads(1.0, 100.0).asDict({ trimPayloadSize: 3 }).request.payloads).toEqual([1, 100]);
  });

  it('writes a bigint as its decimal string, as Python writes a value json cannot serialise', () => {
    expect(withPayloads({ a: 150n }).asDict({ trimPayloadSize: 10 }).request.payloads).toEqual([
      '{"a":"150"…(trimmed 1 bytes)…',
    ]);
  });

  it('throws RangeError for a negative or non-integer trimPayloadSize', () => {
    expect(() => exchangeWith().asDict({ trimPayloadSize: -1 })).toThrow(RangeError);
    expect(() => exchangeWith().asDict({ trimPayloadSize: 1.5 })).toThrow(RangeError);
  });

  it('is immutable', () => {
    const exchange = withPayloads(1);
    expect(Object.isFrozen(exchange)).toBe(true);
    expect(Object.isFrozen(exchange.request)).toBe(true);
    expect(Object.isFrozen(exchange.request.payloads)).toBe(true);
  });
});

describe('peer.onExchange recording', () => {
  let pair: ReturnType<typeof startPair> | null = null;

  afterEach(async () => {
    vi.restoreAllMocks();
    await pair?.stop();
    pair = null;
  });

  it('throws RangeError at registration for a negative or non-integer maxPayloads', () => {
    pair = startPair();
    const { dialer } = pair;
    expect(() => dialer.onExchange(() => undefined, { maxPayloads: -1 })).toThrow(RangeError);
    expect(() => dialer.onExchange(() => undefined, { maxPayloads: 0.5 })).toThrow(RangeError);
    expect(() => dialer.onExchange(() => undefined, { maxPayloads: null })).not.toThrow();
  });

  it('records nothing for a stream created while no handler is registered', async () => {
    pair = startPair();
    const { dialer } = pair;
    const unrecorded = dialer.open('before', { end: true });
    expect(unrecorded.recorder).toBeNull();
    const seen: Exchange[] = [];
    dialer.onExchange((exchange) => seen.push(exchange));
    const recorded = dialer.open('after', { end: true });
    expect(recorded.recorder).not.toBeNull();
    await Promise.all([unrecorded, recorded]);
    await settle();
    expect(seen.map((exchange) => exchange.stream)).toEqual([recorded.id]);
    expect(recorded.recorder, 'released once the exchange fires').toBeNull();
  });

  it('cuts each side to every handler’s own maxPayloads and counts every payload', async () => {
    pair = startPair();
    const { dialer } = pair;
    const byLimit = new Map<string, Exchange>();
    dialer.onExchange((exchange) => byLimit.set('0', exchange), { maxPayloads: 0 });
    dialer.onExchange((exchange) => byLimit.set('2', exchange), { maxPayloads: 2 });
    dialer.onExchange((exchange) => byLimit.set('all', exchange), { maxPayloads: null });
    dialer.onExchange((exchange) => byLimit.set('default', exchange));

    const stream = dialer.open(0);
    await Promise.all([1, 2, 3].map((n) => stream.send(n)));
    await stream.end({ payload: 4 });
    await stream.closed;
    await settle();

    expect(byLimit.get('0')?.request).toMatchObject({ payloads: [], payloadCount: 5 });
    expect(byLimit.get('0')?.response).toMatchObject({ payloads: [], payloadCount: 5 });
    expect(byLimit.get('2')?.request.payloads).toEqual([0, 1]);
    expect(byLimit.get('2')?.response.payloads).toEqual([0, 1]);
    expect(byLimit.get('all')?.request.payloads).toEqual([0, 1, 2, 3, 4]);
    expect(byLimit.get('default')?.response.payloads).toEqual([0, 1, 2, 3, 4]);
  });

  it('records up to the limit in effect when the stream was created', async () => {
    pair = startPair();
    const { dialer } = pair;
    dialer.onExchange(() => undefined, { maxPayloads: 1 });
    const stream = dialer.open('a');
    const late: Exchange[] = [];
    dialer.onExchange((exchange) => late.push(exchange), { maxPayloads: null });
    await stream.end({ payload: 'b' });
    await stream.closed;
    await settle();

    expect(late).toHaveLength(1);
    expect(late[0]?.request).toMatchObject({ payloads: ['a'], payloadCount: 2 });
  });

  it('logs a handler that throws, runs the other handlers and does not affect the stream or connection', async () => {
    pair = startPair();
    const { dialer } = pair;
    const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const seen: Exchange[] = [];
    dialer.onExchange(() => {
      throw new Error('handler failure');
    });
    dialer.onExchange((exchange) => seen.push(exchange));

    expect(await dialer.open('one', { end: true })).toBe('one');
    expect(await dialer.open('two', { end: true })).toBe('two');
    await settle();

    expect(seen.map((exchange) => exchange.outcome)).toEqual(['ok', 'ok']);
    expect(dialer.isOpen).toBe(true);
    expect(errors).toHaveBeenCalledWith(expect.stringContaining('onExchange handler threw'), expect.any(Error));
  });
});
