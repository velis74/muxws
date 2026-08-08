/**
 * The browser half of the adapter seam (WSM-API-021, WSM-CDC-020/024/028).
 *
 * There is no Python twin for the adapter itself; what it must prove is the twin of
 * `transports/websockets_test.py`'s dialer tests - the muxws entry leads the offer, a refused
 * handshake is a `CodecMismatch` the dialer composes for itself, and a server that negotiated
 * something else is closed with 1008. The socket is a mock: a real one would need a server, and
 * "the handshake was refused" is not a state a real browser lets a test arrange.
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not declare it as one.
import { vi } from 'vitest';

import { CodecMismatch, ConnectionClosed, ProtocolError } from '../errors';

import { BrowserSocket, POLICY_VIOLATION } from './browser-socket';

/** Every mock the code under test constructed, newest last. */
const sockets: MockWebSocket[] = [];

const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

type Listener = (event: Event) => void;

/**
 * The global `WebSocket`, reduced to what the adapter is allowed to touch plus the server's half.
 *
 * The three `server*` methods are the only way a test moves the connection: everything else mirrors
 * the platform object, so a call the adapter makes on a real socket is a call it makes on this one.
 */
class MockWebSocket {
  readonly protocols: string[];

  readonly sent: (string | ArrayBuffer)[] = [];

  readonly closeCalls: [number | undefined, string | undefined][] = [];

  binaryType = 'blob';

  protocol = '';

  readyState = CONNECTING;

  private readonly listeners = new Map<string, Listener[]>();

  constructor(
    readonly url: string,
    protocols: string | readonly string[] = [],
  ) {
    this.protocols = typeof protocols === 'string' ? [protocols] : [...protocols];
    sockets.push(this);
  }

  addEventListener(type: string, listener: Listener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.set(
      type,
      (this.listeners.get(type) ?? []).filter((entry) => entry !== listener),
    );
  }

  send(data: string | ArrayBuffer): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.closeCalls.push([code, reason]);
    if (this.readyState === CLOSED) return;
    this.serverClose(code ?? 1000, reason ?? '');
  }

  /** The handshake completed, negotiating `protocol` - `''` for "no subprotocol at all". */
  serverOpen(protocol: string): void {
    this.protocol = protocol;
    this.readyState = OPEN;
    this.emit('open', {});
  }

  /** The upgrade was refused. All a browser reports is a generic error and then a close. */
  serverRefuse(): void {
    this.emit('error', {});
    this.serverClose(1006, '');
  }

  serverClose(code: number, reason: string): void {
    if (this.readyState === CLOSED) return;
    this.readyState = CLOSED;
    this.emit('close', { code, reason, wasClean: code === 1000 });
  }

  serverSend(data: unknown): void {
    this.emit('message', { data });
  }

  private emit(type: string, event: Record<string, unknown>): void {
    [...(this.listeners.get(type) ?? [])].forEach((listener) => {
      listener(event as unknown as Event);
    });
  }
}

/** The socket the code under test constructed most recently. */
function lastSocket(): MockWebSocket {
  const socket = sockets.at(-1);
  if (socket === undefined) throw new Error('no WebSocket was constructed');
  return socket;
}

function wrap(mock: MockWebSocket): BrowserSocket {
  return new BrowserSocket(mock as unknown as WebSocket);
}

/** A connected adapter, handshake and all, for the tests that are about what happens afterwards. */
async function connected(codecName = 'json'): Promise<{ adapter: BrowserSocket; mock: MockWebSocket }> {
  const pending = BrowserSocket.connect('ws://localhost/ws', codecName);
  const mock = lastSocket();
  mock.serverOpen(`muxws.v1.${codecName}`);
  return { adapter: await pending, mock };
}

beforeEach(() => {
  sockets.length = 0;
  vi.stubGlobal('WebSocket', MockWebSocket);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('the browser socket', () => {
  it('sets binaryType to arraybuffer in the constructor, before a message can arrive', () => {
    const mock = new MockWebSocket('ws://localhost/ws');
    expect(mock.binaryType).toBe('blob');

    const adapter = wrap(mock);

    // Not "eventually": the listeners are attached in the same constructor, so a message delivered
    // on the next turn would already have been decoded under the wrong type.
    expect(mock.binaryType).toBe('arraybuffer');
    expect(adapter.isClosed).toBe(false);
  });

  it('offers the muxws entry first even when the application appended its own - WSM-CDC-020/021', async () => {
    const pending = BrowserSocket.connect('ws://localhost/ws', 'json', {
      subprotocols: ['bearer.abc123', 'x-app'],
    });
    const mock = lastSocket();

    expect(mock.protocols[0]).toBe('muxws.v1.json');
    expect(mock.protocols.slice(1)).toEqual(['bearer.abc123', 'x-app']);
    expect(mock.url).toBe('ws://localhost/ws');

    mock.serverOpen('muxws.v1.json');
    const adapter = await pending;
    expect(adapter.socket).toBe(mock as unknown as WebSocket);
    expect(mock.binaryType).toBe('arraybuffer');
  });

  it('offers the codec it was given, and only the muxws entry when there is nothing to append', () => {
    void BrowserSocket.connect('ws://localhost/ws', 'msgpack').catch(() => undefined);
    expect(lastSocket().protocols).toEqual(['muxws.v1.msgpack']);
    lastSocket().serverRefuse();
  });

  it('surfaces CodecMismatch naming both variables on a refused handshake - WSM-CDC-024', async () => {
    const pending = BrowserSocket.connect('ws://localhost/ws', 'msgpack');
    lastSocket().serverRefuse();

    const error = await pending.catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(CodecMismatch);
    expect((error as CodecMismatch).name).toBe('CodecMismatch');
    // A browser cannot read the rejection body, so a bare connection failure would leave the reader
    // with nothing to act on - which is the whole reason WSM-CDC-024 exists.
    expect(error).not.toBeInstanceOf(ConnectionClosed);
    const { message } = error as CodecMismatch;
    expect(message).toContain('msgpack');
    expect(message).toContain('VITE_MUXWS_CODEC');
    expect(message).toContain('MUXWS_CODEC');
    expect((error as CodecMismatch).configured).toBe('msgpack');
  });

  it('surfaces the same error when the refusal arrives as a close with no error event', async () => {
    const pending = BrowserSocket.connect('ws://localhost/ws', 'json');
    lastSocket().serverClose(1006, '');

    await expect(pending).rejects.toBeInstanceOf(CodecMismatch);
  });

  it('closes with the policy-violation code when the server negotiated something else - WSM-CDC-028', async () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      const pending = BrowserSocket.connect('ws://localhost/ws', 'json');
      const mock = lastSocket();
      mock.serverOpen('muxws.v1.msgpack');

      await expect(pending).rejects.toBeInstanceOf(CodecMismatch);
      expect(mock.closeCalls).toEqual([[POLICY_VIOLATION, 'muxws subprotocol mismatch']]);
      expect(POLICY_VIOLATION).toBe(1008);
    } finally {
      spy.mockRestore();
    }
  });

  it('treats a handshake that negotiated nothing as the same failure - WSM-CDC-028', async () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      const pending = BrowserSocket.connect('ws://localhost/ws', 'json');
      const mock = lastSocket();
      mock.serverOpen('');

      await expect(pending).rejects.toBeInstanceOf(CodecMismatch);
      expect(mock.closeCalls).toEqual([[POLICY_VIOLATION, 'muxws subprotocol mismatch']]);
    } finally {
      spy.mockRestore();
    }
  });

  it('rejects a pending receive when the socket closes', async () => {
    const { adapter, mock } = await connected();
    const pending = adapter.receive();
    let settled = false;
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    // Nothing has arrived, so the receive is genuinely parked rather than already answered.
    await Promise.resolve();
    expect(settled).toBe(false);

    mock.serverClose(1006, 'the tab went away');

    const error = await pending.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(ConnectionClosed);
    expect((error as ConnectionClosed).code).toBe(1006);
    expect((error as ConnectionClosed).reason).toBe('the tab went away');
    expect((error as ConnectionClosed).wasClean).toBe(false);
    expect(adapter.isClosed).toBe(true);
    // The failure is recorded, so a receive issued afterwards is refused too rather than parking
    // forever on a socket that will never deliver again.
    await expect(adapter.receive()).rejects.toBeInstanceOf(ConnectionClosed);
  });

  it('queues what arrived before the close, and only then reports it', async () => {
    const { adapter, mock } = await connected();
    mock.serverSend('early');
    mock.serverClose(1000, '');

    expect(await adapter.receive()).toBe('early');
    await expect(adapter.receive()).rejects.toBeInstanceOf(ConnectionClosed);
  });

  it('hands a queued message to a later receive rather than dropping it', async () => {
    const { adapter, mock } = await connected();
    mock.serverSend('one');
    mock.serverSend('two');

    expect(await adapter.receive()).toBe('one');
    expect(await adapter.receive()).toBe('two');
  });

  it('delivers an ArrayBuffer untouched and copies a view out of its buffer', async () => {
    const { adapter, mock } = await connected();
    const buffer = new Uint8Array([0x00, 0xff]).buffer;
    mock.serverSend(buffer);
    // Identity: the adapter is a wire, and re-copying every inbound frame would cost the peer a copy
    // per message for nothing.
    expect(await adapter.receive()).toBe(buffer);

    // A view may sit inside a longer buffer; unwrapping it would hand the peer the neighbours too.
    const view = new Uint8Array([1, 2, 3, 4, 5, 6]).subarray(2, 4);
    mock.serverSend(view);
    const received = await adapter.receive();
    expect(received).toBeInstanceOf(ArrayBuffer);
    expect(new Uint8Array(received as ArrayBuffer)).toEqual(new Uint8Array([3, 4]));
  });

  it('treats anything that is neither text nor bytes as a protocol error', async () => {
    const { adapter, mock } = await connected();
    const pending = adapter.receive();
    mock.serverSend(new Blob(['x']));

    // binaryType is 'arraybuffer', so a Blob means the socket disobeyed - not that muxws should
    // start sniffing message types.
    const error = await pending.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(ProtocolError);
    expect((error as ProtocolError).message).toContain('Blob');
  });

  it('sends text and bytes through the socket, and refuses to once it is closed', async () => {
    const { adapter, mock } = await connected();
    const bytes = new Uint8Array([1, 2]).buffer;

    adapter.sendText('hello');
    adapter.sendBytes(bytes);
    expect(mock.sent[0]).toBe('hello');
    expect(mock.sent[1]).toBe(bytes);

    mock.serverClose(1006, 'gone');
    expect(() => {
      adapter.sendText('too late');
    }).toThrow(ConnectionClosed);
    expect(() => {
      adapter.sendBytes(bytes);
    }).toThrow(ConnectionClosed);
    expect(mock.sent).toHaveLength(2);
  });

  it('closes idempotently, and resolves even when the socket is already gone', async () => {
    const { adapter, mock } = await connected();

    await adapter.close();
    expect(adapter.isClosed).toBe(true);
    expect(mock.closeCalls).toEqual([[1000, '']]);

    // The second close finds a CLOSED socket: no close event is coming, so it must answer from what
    // it knows rather than await one that never fires.
    await adapter.close();
    expect(adapter.isClosed).toBe(true);
  });
});
