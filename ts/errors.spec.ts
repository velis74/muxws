import {
  CodecError,
  CodecMismatch,
  CodecNotRegistered,
  ConnectionClosed,
  ConnectionGoingAway,
  ConnectionLost,
  MuxwsError,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamAlreadyConsumed,
  StreamClosed,
  StreamRefused,
  StreamReset,
  StreamTimeout,
  exceptionForReset,
} from './errors';

describe('the exception hierarchy', () => {
  it('mirrors the Python tree - WSM-ERR-002/005/009', () => {
    expect(new ConnectionLost()).toBeInstanceOf(StreamReset);
    expect(new ConnectionClosed()).not.toBeInstanceOf(StreamReset);

    // A normal close racing a last send() is an expected outcome, not a failure and not a caller bug.
    expect(new StreamClosed()).not.toBeInstanceOf(StreamReset);
    expect(new StreamClosed()).not.toBeInstanceOf(ProtocolError);

    // Neither codec error is a stream failure and neither is retryable.
    expect(new CodecError('x')).not.toBeInstanceOf(StreamReset);
    expect(new CodecNotRegistered('x')).toBeInstanceOf(CodecError);
    expect(new CodecMismatch('x')).toBeInstanceOf(CodecError);

    [new RemoteError(), new StreamTimeout(), new StreamRefused(), new ConnectionLost()].forEach((error) => {
      expect(error).toBeInstanceOf(StreamReset);
    });

    [
      new ProtocolError(),
      new ConnectionClosed(),
      new ConnectionGoingAway(),
      new StreamAlreadyConsumed(),
      new StreamClosed(),
      new CodecError('x'),
      new StreamReset(),
    ].forEach((error) => {
      expect(error).toBeInstanceOf(MuxwsError);
      expect(error).toBeInstanceOf(Error);
    });
  });

  it('sets a name discriminator on every class - WSM-ERR-004', () => {
    const cases: [Error, string][] = [
      [new MuxwsError(), 'MuxwsError'],
      [new ProtocolError(), 'ProtocolError'],
      [new ConnectionClosed(), 'ConnectionClosed'],
      [new ConnectionGoingAway(), 'ConnectionGoingAway'],
      [new StreamAlreadyConsumed(), 'StreamAlreadyConsumed'],
      [new StreamClosed(), 'StreamClosed'],
      [new CodecError('x'), 'CodecError'],
      [new CodecNotRegistered('x'), 'CodecNotRegistered'],
      [new CodecMismatch('x'), 'CodecMismatch'],
      [new StreamReset(), 'StreamReset'],
      [new RemoteError(), 'RemoteError'],
      [new StreamTimeout(), 'StreamTimeout'],
      [new StreamRefused(), 'StreamRefused'],
      [new ConnectionLost(), 'ConnectionLost'],
    ];
    cases.forEach(([error, name]) => expect(error.name).toBe(name));
  });
});

describe('ResetCode', () => {
  it('is the pinned nine, with 5 a hole', () => {
    expect(ResetCode.NO_ERROR).toBe(0);
    expect(ResetCode.CANCELLED).toBe(1);
    expect(ResetCode.APPLICATION_ERROR).toBe(2);
    expect(ResetCode.PROTOCOL_ERROR).toBe(3);
    expect(ResetCode.REFUSED).toBe(4);
    expect(ResetCode.TIMEOUT).toBe(6);
    expect(ResetCode.PAYLOAD_TOO_LARGE).toBe(7);
    expect(ResetCode.INTERNAL_ERROR).toBe(8);
    expect(ResetCode.CONNECTION_CLOSED).toBe(9);

    const names = Object.keys(ResetCode).filter((key) => Number.isNaN(Number(key)));
    expect(names).toHaveLength(9);
    expect(ResetCode[5]).toBeUndefined();
  });

  it('has no StreamLimit sibling - WSM-ERR-001 is retired', async () => {
    const errors = await import('./errors');
    expect('StreamLimit' in errors).toBe(false);
  });
});

describe('each StreamReset subclass', () => {
  it('pins its own code and carries the stream id', () => {
    const cases: [StreamReset, ResetCode][] = [
      [new RemoteError('why', { streamId: 7 }), ResetCode.APPLICATION_ERROR],
      [new StreamTimeout('why', { streamId: 7 }), ResetCode.TIMEOUT],
      [new StreamRefused('why', { streamId: 7 }), ResetCode.REFUSED],
      [new ConnectionLost('why', { streamId: 7 }), ResetCode.CONNECTION_CLOSED],
    ];
    cases.forEach(([error, code]) => {
      expect(error.code).toBe(code);
      expect(error.streamId).toBe(7);
      expect(error.reason).toBe('why');
    });
  });

  it('falls back to the code name when given no reason', () => {
    expect(new StreamRefused().message).toBe('REFUSED');
    expect(new StreamReset().message).toBe('NO_ERROR');
  });

  it('carries the structured payload on RemoteError - WSM-ERR-006', () => {
    const error = new RemoteError('handler raised', { payload: { type: 'ValueError', message: 'nope' } });
    expect(error.payload).toEqual({ type: 'ValueError', message: 'nope' });
    expect(new RemoteError().payload).toBeNull();
  });
});

describe('exceptionForReset', () => {
  it('maps a wire code to its class', () => {
    expect(exceptionForReset(ResetCode.APPLICATION_ERROR)).toBeInstanceOf(RemoteError);
    expect(exceptionForReset(ResetCode.TIMEOUT)).toBeInstanceOf(StreamTimeout);
    expect(exceptionForReset(ResetCode.REFUSED)).toBeInstanceOf(StreamRefused);
    expect(exceptionForReset(ResetCode.CONNECTION_CLOSED)).toBeInstanceOf(ConnectionLost);
  });

  it('falls back to StreamReset for a code with no dedicated class', () => {
    const error = exceptionForReset(ResetCode.PAYLOAD_TOO_LARGE, 'too big', { streamId: 3 });
    expect(error.name).toBe('StreamReset');
    expect(error.code).toBe(ResetCode.PAYLOAD_TOO_LARGE);
    expect(error.streamId).toBe(3);
  });
});

describe('the connection and codec errors', () => {
  it('carry their diagnostic fields', () => {
    const closed = new ConnectionClosed('bye', { code: 1001, reason: 'going away', wasClean: true });
    expect([closed.code, closed.reason, closed.wasClean]).toEqual([1001, 'going away', true]);
    expect(new ConnectionClosed().code).toBe(1006);

    const codec = new CodecError('bad', { configured: 'msgpack', available: ['json'] });
    expect(codec.configured).toBe('msgpack');
    expect(codec.available).toEqual(['json']);
    expect(new CodecError('bad').configured).toBeNull();
    expect(new CodecError('bad').available).toEqual([]);
  });
});
