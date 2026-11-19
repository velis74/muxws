// @vitest-environment node
/**
 * The exchange log fed with frames directly: fragment reassembly, a lost connection and the trim
 * boundary on multi-byte characters, which a scenario over a real socket does not reach
 * deterministically.
 */

import { type CloseReason, type Frame, JsonCodec, logger, splitFrame } from 'muxws';

import { ExchangeLog, type ExchangeLogOptions, type ExchangePeer, trimText } from './exchange-log';

type FrameHandler = (direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void;

class FakePeer implements ExchangePeer {
  id = 'p-1';

  frame: FrameHandler = () => undefined;

  close: (reason: CloseReason) => void = () => undefined;

  onFrame(handler: FrameHandler): void {
    this.frame = handler;
  }

  onClose(handler: (reason: CloseReason) => void): void {
    this.close = handler;
  }
}

const codec = new JsonCodec();

function setUp(options: Partial<ExchangeLogOptions> = {}): { peer: FakePeer; lines: Record<string, any>[] } {
  const lines: Record<string, any>[] = [];
  let now = 0;
  const log = new ExchangeLog({
    codec,
    write: (line) => lines.push(JSON.parse(line)),
    clock: () => {
      now += 10;
      return now;
    },
    ...options,
  });
  const peer = new FakePeer();
  log.attach(peer);
  return { peer, lines };
}

describe('ExchangeLog', () => {
  it('writes a reassembled fragmented payload once, decoded, with every fragment counted in bytes', () => {
    const { peer, lines } = setUp();
    const big = 'é'.repeat(300);
    const fragments = splitFrame({ type: 'data', stream: 2, payload: { big }, end: true }, 128, codec);
    expect(fragments.length).toBeGreaterThan(2);

    peer.frame('rx', { type: 'open', stream: 2, headers: { op: 'up' }, payload: 1 }, 30);
    fragments.forEach((fragment) => peer.frame('rx', fragment, 100));
    peer.frame('tx', { type: 'data', stream: 2, payload: 'ok', end: true }, 20);

    expect(lines).toHaveLength(1);
    const [line] = lines;
    expect(Object.keys(line)).toEqual([
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
    ]);
    expect(line.opened_by).toBe('remote');
    expect(line.request).toEqual({
      headers: { op: 'up' },
      payloads: [1, { big }],
      trailers: null,
      dropped: 0,
      bytes: 30 + 100 * fragments.length,
    });
    expect(line.response.payloads).toEqual(['ok']);
    expect(line.outcome).toBe('ok');
  });

  it('counts a fragmented payload over maxMessages in dropped without reassembling it', () => {
    const { peer, lines } = setUp({ maxMessages: 1 });
    const fragments = splitFrame({ type: 'data', stream: 1, payload: 'x'.repeat(500), end: true }, 128, codec);

    peer.frame('tx', { type: 'open', stream: 1, payload: 'first' }, 10);
    fragments.forEach((fragment) => peer.frame('tx', fragment, 50));
    peer.frame('rx', { type: 'data', stream: 1, end: true }, 5);

    expect(lines[0].request.payloads).toEqual(['first']);
    expect(lines[0].request.dropped).toBe(1);
    expect(lines[0].opened_by).toBe('local');
  });

  it('writes every unfinished stream as lost when the connection closes', () => {
    const { peer, lines } = setUp();
    peer.frame('tx', { type: 'open', stream: 1, payload: 'a' }, 10);
    peer.frame('tx', { type: 'open', stream: 3, payload: 'b', end: true }, 10);
    peer.frame('rx', { type: 'data', stream: 3, payload: 'partial' }, 10);
    peer.close({ code: 1006, reason: '', wasClean: false, willRetry: true });

    expect(lines.map((line) => [line.stream, line.outcome, line.code, line.reason])).toEqual([
      [1, 'lost', null, null],
      [3, 'lost', null, null],
    ]);
    expect(lines[0].first_response_ms).toBeNull();
    expect(lines[1].response.payloads).toEqual(['partial']);

    peer.close({ code: 1006, reason: '', wasClean: false, willRetry: false });
    expect(lines).toHaveLength(2);
  });

  it('starts afresh after a close, so a reused stream id on the next connection is a new exchange', () => {
    const { peer, lines } = setUp();
    peer.frame('tx', { type: 'open', stream: 1, payload: 'old' }, 10);
    peer.close({ code: 1006, reason: 'gone', wasClean: false, willRetry: true });
    peer.id = 'p-2';
    peer.frame('tx', { type: 'open', stream: 1, payload: 'new', end: true }, 10);
    peer.frame('rx', { type: 'data', stream: 1, payload: 'r', end: true }, 10);

    expect(lines.map((line) => [line.conn, line.outcome, line.request.payloads])).toEqual([
      ['p-1', 'lost', ['old']],
      ['p-2', 'ok', ['new']],
    ]);
  });

  it('writes a reset with the code and reason of the reset frame', () => {
    const { peer, lines } = setUp();
    peer.frame('tx', { type: 'open', stream: 5, payload: 1 }, 10);
    peer.frame('rx', { type: 'reset', stream: 5, code: 8, reason: 'refused' }, 7);
    peer.frame('rx', { type: 'data', stream: 5, payload: 'late', end: true }, 7);

    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ outcome: 'reset', code: 8, reason: 'refused', first_response_ms: 10 });
    expect(lines[0].response.bytes).toBe(7);
  });

  it('writes a oneway stream on its open', () => {
    const { peer, lines } = setUp();
    peer.frame('rx', { type: 'open', stream: 2, payload: { tick: 1 }, end: true, oneway: true }, 12);
    expect(lines[0]).toMatchObject({
      oneway: true,
      outcome: 'ok',
      response: { headers: null, payloads: [], trailers: null, dropped: 0, bytes: 0 },
      first_response_ms: null,
    });
  });

  it('ignores frames without a stream and streams the include callback rejects', () => {
    const { peer, lines } = setUp({ include: (headers) => headers?.secret !== true });
    peer.frame('tx', { type: 'ping', nonce: 'n' }, 5);
    peer.frame('tx', { type: 'open', stream: 1, headers: { secret: true }, payload: 1, end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 1, payload: 2, end: true }, 5);
    expect(lines).toHaveLength(0);
  });

  it('decides include on the reassembled opening payload', () => {
    const seen: unknown[] = [];
    const { peer, lines } = setUp({
      include: (_headers, payload) => {
        seen.push(payload);
        return (payload as { keep?: boolean } | null)?.keep === true;
      },
    });
    [true, false].forEach((keep, index) => {
      const id = 2 * index + 1;
      const open = { type: 'open', stream: id, payload: { keep, pad: 'x'.repeat(400) }, end: true };
      splitFrame(open, 128, codec).forEach((fragment) => peer.frame('tx', fragment, 50));
      peer.frame('rx', { type: 'data', stream: id, payload: 'r', end: true }, 5);
    });
    peer.frame('tx', { type: 'open', stream: 5, end: true }, 5);

    expect(seen.map((payload) => (payload as { keep?: boolean } | null)?.keep ?? null)).toEqual([true, false, null]);
    expect(lines.map((line) => line.stream)).toEqual([1]);
    expect(lines[0].request.payloads[0].keep).toBe(true);
  });

  it('logs bytes as base64 and applies redact before trimming', () => {
    const { peer, lines } = setUp({
      trimPayloadSize: 12,
      redact: (payload, side) => (side === 'request' ? { ...(payload as object), password: '***' } : payload),
    });
    peer.frame('tx', { type: 'open', stream: 1, payload: { password: 'hunter2' }, end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 1, payload: new Uint8Array([1, 2, 3]), end: true }, 5);

    expect(lines[0].request.payloads).toEqual(['{"password":…(trimmed 6 bytes)…']);
    expect(lines[0].response.payloads).toEqual(['AQID']);
  });

  it('survives a throwing write and keeps logging', () => {
    const lines: string[] = [];
    let fail = true;
    const log = new ExchangeLog({
      codec,
      write: (line) => {
        if (fail) throw new Error('disk full');
        lines.push(line);
      },
    });
    const peer = new FakePeer();
    log.attach(peer);
    peer.frame('tx', { type: 'open', stream: 1, end: true, oneway: true }, 5);
    fail = false;
    peer.frame('tx', { type: 'open', stream: 3, end: true, oneway: true }, 5);
    expect(lines).toHaveLength(1);
  });

  it('survives a write that throws on close and logs a reused stream id on the next connection', () => {
    const lines: Record<string, any>[] = [];
    let fail = true;
    const log = new ExchangeLog({
      codec,
      write: (line) => {
        if (fail) throw new Error('disk full');
        lines.push(JSON.parse(line));
      },
    });
    const peer = new FakePeer();
    log.attach(peer);
    peer.frame('tx', { type: 'open', stream: 1, payload: 'a' }, 5);
    peer.frame('tx', { type: 'open', stream: 3, payload: 'b' }, 5);
    expect(() => peer.close({ code: 1006, reason: '', wasClean: false, willRetry: true })).not.toThrow();

    fail = false;
    peer.id = 'p-2';
    [1, 3].forEach((id) => {
      peer.frame('tx', { type: 'open', stream: id, payload: `new ${id}`, end: true }, 5);
      peer.frame('rx', { type: 'data', stream: id, payload: 'r', end: true }, 5);
    });
    expect(lines.map((line) => [line.conn, line.stream, line.outcome, line.request.payloads])).toEqual([
      ['p-2', 1, 'ok', ['new 1']],
      ['p-2', 3, 'ok', ['new 3']],
    ]);
  });

  it('drops the record of a stream whose redact throws and keeps logging other streams', () => {
    const { peer, lines } = setUp({
      redact: (payload) => {
        if (payload === 'boom') throw new Error('redact failed');
        return payload;
      },
    });
    peer.frame('tx', { type: 'open', stream: 1, payload: 'boom', end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 1, payload: 'r', end: true }, 5);
    peer.frame('tx', { type: 'open', stream: 3, payload: 'fine', end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 3, payload: 'r', end: true }, 5);

    expect(lines.map((line) => [line.stream, line.request.payloads])).toEqual([[3, ['fine']]]);
  });

  it('drops the record of a stream whose include throws, with a warning on the muxws logger', () => {
    const warn = vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
    try {
      const { peer, lines } = setUp({
        include: (_headers, payload) => {
          if (payload === 'odd') throw new Error('include failed');
          return true;
        },
      });
      peer.frame('tx', { type: 'open', stream: 1, payload: 'odd', end: true }, 5);
      peer.frame('rx', { type: 'data', stream: 1, payload: 'r', end: true }, 5);
      peer.frame('tx', { type: 'open', stream: 3, payload: 'fine', end: true }, 5);
      peer.frame('rx', { type: 'data', stream: 3, payload: 'r', end: true }, 5);

      expect(lines.map((line) => line.stream)).toEqual([3]);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0][0])).toContain('exchange log dropped stream 1');
    } finally {
      warn.mockRestore();
    }
  });

  it('asks include about a stream that ends before its opening payload is complete', () => {
    const seen: [unknown, unknown][] = [];
    const { peer, lines } = setUp({
      include: (headers, payload) => {
        seen.push([headers, payload]);
        return headers?.authorization === undefined;
      },
    });
    [
      [2, { authorization: 'Bearer x' }],
      [4, { trace: 't' }],
    ].forEach(([id, headers]) => {
      const opening = { type: 'open', stream: id, headers, payload: 'x'.repeat(500), end: true } as Frame;
      peer.frame('rx', splitFrame(opening, 128, codec)[0], 50);
    });
    peer.close({ code: 1006, reason: '', wasClean: false, willRetry: false });

    expect(seen).toEqual([
      [{ authorization: 'Bearer x' }, null],
      [{ trace: 't' }, null],
    ]);
    expect(lines.map((line) => [line.stream, line.outcome])).toEqual([[4, 'lost']]);
  });

  it('writes a lone surrogate as an escape, so the line is valid UTF-8 JSON', () => {
    const lines: string[] = [];
    const log = new ExchangeLog({ codec, write: (line) => lines.push(line) });
    const peer = new FakePeer();
    log.attach(peer);
    peer.frame('tx', { type: 'open', stream: 1, payload: 'a\uD800b', end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 1, end: true }, 5);

    expect(lines[0]).toContain('"a\\ud800b"');
    expect(JSON.parse(lines[0]).request.payloads).toEqual(['a\uD800b']);
  });

  it('writes non-finite numbers as null', () => {
    const { peer, lines } = setUp();
    peer.frame('tx', { type: 'open', stream: 1, headers: { x: Infinity }, payload: NaN, end: true }, 5);
    peer.frame('rx', { type: 'data', stream: 1, payload: [-Infinity], end: true }, 5);

    expect(lines[0].request.headers).toEqual({ x: null });
    expect(lines[0].request.payloads).toEqual([null]);
    expect(lines[0].response.payloads).toEqual([[null]]);
  });
});

describe('trimText', () => {
  it('leaves text within the limit alone', () => {
    expect(trimText('"abc"', 5)).toBeNull();
  });

  it('cuts before a two-byte character that straddles the limit', () => {
    // "aé" is 0x61 0xC3 0xA9: a cut at 2 falls inside é.
    expect(trimText('aéb', 2)).toBe('a…(trimmed 3 bytes)…');
  });

  it('cuts before a four-byte character and counts every byte cut off', () => {
    const text = 'ab\u{1F600}cd';
    [3, 4, 5].forEach((limit) => {
      expect(trimText(text, limit)).toBe('ab…(trimmed 6 bytes)…');
    });
    expect(trimText(text, 6)).toBe('ab\u{1F600}…(trimmed 2 bytes)…');
  });

  it('keeps nothing when the limit falls inside the first character', () => {
    expect(trimText('€', 2)).toBe('…(trimmed 3 bytes)…');
  });
});
