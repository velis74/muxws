/**
 * Two socket adapters wired to each other, with no socket anywhere.
 *
 * Mirrors `muxws/transports/memory.py`. Shipped in the package rather than the test tree because the
 * reconnect tests and the conformance sequence runner both need it (D2).
 */

import { ConnectionClosed } from '../errors';

import type { SocketAdapter } from './index';

/** What travels between the two ends. One WebSocket message, already encoded by the codec. */
type Message = string | ArrayBuffer;

/** The inbox's end-of-stream marker. `receive()` turns it into `ConnectionClosed`, never returns it. */
const END = Symbol('muxws.memory.end');

type Inbound = Message | typeof END;

/** One end of an in-memory pair. Implements `SocketAdapter`. */
export class MemorySocket implements SocketAdapter {
  /** Every message this side put on the wire, in order. The conformance runner reads it. */
  readonly sent: Message[] = [];

  private readonly inbox: Inbound[] = [];
  private readonly waiting: ((item: Inbound) => void)[] = [];
  private closed = false;
  private peerSocket: MemorySocket | null = null;

  get isClosed(): boolean {
    return this.closed;
  }

  sendText(text: string): void {
    this.deliver(text);
  }

  sendBytes(bytes: ArrayBuffer): void {
    this.deliver(bytes);
  }

  async receive(): Promise<Message> {
    const item = await this.take();
    if (item === END) throw new ConnectionClosed('socket closed while receiving', { code: 1006 });
    return item;
  }

  /** Close this side cleanly, waking both ends' pending receives. */
  close(code = 1000, reason = ''): void {
    // The pair has no wire, so neither value travels anywhere; the parameters exist because the
    // port declares them (memory.py discards them the same way).
    void code;
    void reason;
    if (this.closed) return;
    this.closed = true;
    this.put(END);
    const twin = this.peerSocket;
    if (twin !== null && !twin.closed) {
      twin.closed = true;
      twin.put(END);
    }
  }

  /** Simulate socket death: no close frame, no warning, both ends simply stop. */
  drop(): void {
    this.close(1006, 'dropped');
  }

  /**
   * Push a raw message into this side's inbox, bypassing the other peer.
   *
   * The conformance runner uses it to deliver frames no correct implementation would send.
   */
  inject(message: Message): void {
    this.put(message);
  }

  /** @internal Wire this end to its twin. `memoryPair` is the only caller. */
  wireTo(twin: MemorySocket): void {
    this.peerSocket = twin;
  }

  private deliver(message: Message): void {
    if (this.closed) throw new ConnectionClosed('socket is closed', { code: 1006 });
    this.sent.push(message);
    this.peerSocket?.put(message);
  }

  private put(item: Inbound): void {
    const waiter = this.waiting.shift();
    if (waiter !== undefined) {
      waiter(item);
      return;
    }
    this.inbox.push(item);
  }

  private take(): Promise<Inbound> {
    const queued = this.inbox.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    // A closed and drained inbox answers immediately rather than parking forever: the Python queue
    // holds exactly one sentinel too, and a promise that never settles hangs a test runner.
    if (this.closed) return Promise.resolve(END);
    return new Promise<Inbound>((resolve) => {
      this.waiting.push(resolve);
    });
  }
}

/** Two adapters wired to each other. `drop()` on either simulates socket death. */
export function memoryPair(): [MemorySocket, MemorySocket] {
  const left = new MemorySocket();
  const right = new MemorySocket();
  left.wireTo(right);
  right.wireTo(left);
  return [left, right];
}
