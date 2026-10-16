/**
 * Two socket adapters connected to each other in memory, without a network socket.
 *
 * Mirrors `muxws/transports/memory.py`. Part of the package, not the test tree, because the reconnect
 * tests and the conformance sequence runner use it (D2).
 */

import { ConnectionClosed } from '../errors';

import type { SocketAdapter } from './index';

/** One WebSocket message passed between the two ends, already encoded by the codec. */
type Message = string | ArrayBuffer;

/** The inbox's end-of-stream marker. `receive()` turns it into `ConnectionClosed`, never returns it. */
const END = Symbol('muxws.memory.end');

type Inbound = Message | typeof END;

/** One end of an in-memory pair. Implements `SocketAdapter`. */
export class MemorySocket implements SocketAdapter {
  /** Every message this side sent, in order. Read by the conformance runner. */
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
    // The pair sends no close frame, so both values are discarded, as in memory.py; the parameters
    // exist because `SocketAdapter` declares them.
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

  /** Simulate an abrupt socket loss: both ends close without a close frame. */
  drop(): void {
    this.close(1006, 'dropped');
  }

  /**
   * Push a raw message into this side's inbox, bypassing the other peer.
   *
   * The conformance runner uses it to deliver frames a conforming implementation does not send.
   */
  inject(message: Message): void {
    this.put(message);
  }

  /** @internal Connect this end to the other end of the pair. Called only by `memoryPair`. */
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
    // A closed, drained inbox returns END immediately on every call. The inbox holds a single END
    // (as the Python queue holds a single sentinel), so without this check a second `receive()`
    // after close would never settle.
    if (this.closed) return Promise.resolve(END);
    return new Promise<Inbound>((resolve) => {
      this.waiting.push(resolve);
    });
  }
}

/** Two adapters connected to each other. `drop()` on either simulates an abrupt socket loss. */
export function memoryPair(): [MemorySocket, MemorySocket] {
  const left = new MemorySocket();
  const right = new MemorySocket();
  left.wireTo(right);
  right.wireTo(left);
  return [left, right];
}
