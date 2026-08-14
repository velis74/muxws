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
  TransportUnsupportedError,
  TransportUrlError,
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

  it('places the two transport bases under MuxwsError and outside StreamReset - WSM-ERR-016', () => {
    // The claim an application relies on: one `instanceof MuxwsError` handler around a dial catches a
    // url the transport cannot open and a transport this build cannot provide, whichever transport
    // was named and without importing it. Python reaches the same place through
    // `TransportUrlError(MuxwsError, ValueError)`; here `MuxwsError` is the only branch of that
    // diamond a single prototype chain can keep, and it is the one that matters.
    [new TransportUrlError(), new TransportUnsupportedError()].forEach((error) => {
      expect(error).toBeInstanceOf(MuxwsError);
      expect(error).toBeInstanceOf(Error);
      // Neither is a stream failure: nothing was ever opened, so there is no stream to reset and no
      // reset code to carry.
      expect(error).not.toBeInstanceOf(StreamReset);
    });

    // And they are siblings, not a chain. Collapsing them would tell a reader whose url is fine and
    // whose runtime cannot dial it to go and retype the url - the one wrong answer this split exists
    // to prevent.
    expect(new TransportUrlError()).not.toBeInstanceOf(TransportUnsupportedError);
    expect(new TransportUnsupportedError()).not.toBeInstanceOf(TransportUrlError);
  });

  it('chains the original on a transport base, and invents no chain when there was none', () => {
    // WSM-ERR-016 requires the underlying library's own throw to survive translation: `ws` names the
    // offending text, jsdom and undici word the same refusal differently, and a frame that dropped
    // any of it would leave the reader with muxws's paraphrase of a diagnosis it did not make.
    const original = new SyntaxError('Invalid URL: nonsense');

    expect(new TransportUrlError('framed', { cause: original }).cause).toBe(original);
    expect(new TransportUnsupportedError('framed', { cause: original }).cause).toBe(original);
    // `undefined` and absent are different answers to "what caused this": the second is the honest
    // one for a refusal muxws composed itself, and `new Error(msg, { cause: undefined })` would give
    // the first.
    expect('cause' in new TransportUrlError('composed here')).toBe(false);
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
      [new TransportUrlError(), 'TransportUrlError'],
      [new TransportUnsupportedError(), 'TransportUnsupportedError'],
    ];
    cases.forEach(([error, name]) => expect(error.name).toBe(name));
  });

  it('defines no concrete transport error, because each one belongs to its transport - WSM-ERR-016', async () => {
    // A concrete class that drifted into the shared module would be invisible to every behavioural
    // test - the throw sites would go on passing - and a third party writing an adapter against the
    // public seam (WSM-API-021) cannot add a class to `ts/errors.ts`, so the convention has to be
    // one anybody can keep.
    const errors = await import('./errors');

    // Enumerated, not listed. A name list can only see the classes somebody remembered to add to it,
    // so a *new* concrete error written into the shared module walks straight past it. Python's twin,
    // `errors_test.py::test_every_muxws_error_defined_outside_errors_py_derives_from_a_transport_base`,
    // recurses through `MuxwsError.__subclasses__()` for the same reason, and this is the prototype
    // chain saying the same thing: anything in this module that *extends* either base is a concrete
    // transport class in the one file that must hold none.
    const concreteInSharedModule = Object.entries(errors)
      .filter(
        ([, value]) =>
          typeof value === 'function' &&
          value !== TransportUrlError &&
          value !== TransportUnsupportedError &&
          (Object.prototype.isPrototypeOf.call(TransportUrlError, value) ||
            Object.prototype.isPrototypeOf.call(TransportUnsupportedError, value)),
      )
      .map(([name]) => name);
    expect(concreteInSharedModule, 'a concrete transport error belongs to its transport, not here').toEqual([]);

    // The name list stays as the non-vacuity control, exactly as the Python test keeps
    // `_TRANSPORT_ERRORS_THAT_MUST_EXIST`: the walk above is a comparison of two empty lists unless
    // the four classes it is meant to keep out really do exist somewhere else. `UnixSocketsUnsupportedError`
    // is `ts/transports/browser-socket.ts`'s, because the platform-WebSocket transport is what refuses
    // a `ws+unix:` url; the other three are `muxws/node`'s. None of the four is here.
    const elsewhere = { ...(await import('./index')), ...(await import('./node')) };
    ['UnixSocketsUnsupportedError', 'UnixUrlError', 'WsUrlError', 'WsNotInstalledError'].forEach((name) => {
      expect(name in errors, `${name} belongs to its transport's module, not to the shared one`).toBe(false);
      expect(name in elsewhere, `${name} must exist, or the walk above proves nothing`).toBe(true);
    });
    // And the same assertion against a name that *is* here, so a typo in the import cannot make the
    // loop pass by testing an empty module.
    expect('TransportUrlError' in errors).toBe(true);
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
