/**
 * The Node acceptor's socket adapter, over the `ws` package.
 *
 * Imported **only** by ts/node.ts. Nothing reachable from ts/index.ts may import it, or the browser
 * bundle pulls in `ws` (WSM-API-022) - a dependency a browser has no use for and cannot run.
 */

import type { WebSocket as NodeWebSocket } from 'ws';

import { ConnectionClosed } from '../errors';

import type { SocketAdapter } from './index';

/** `SocketAdapter` over a `ws` connection. */
export class WsSocket implements SocketAdapter {
  private readonly inbox: (string | ArrayBuffer)[] = [];
  private readonly waiting: {
    resolve: (message: string | ArrayBuffer) => void;
    reject: (error: unknown) => void;
  }[] = [];
  private closure: ConnectionClosed | null = null;

  constructor(private readonly socket: NodeWebSocket) {
    socket.binaryType = 'arraybuffer';

    socket.on('message', (data: unknown, isBinary: boolean) => {
      this.deliver(normalise(data, isBinary));
    });
    socket.on('close', (code: number, reason: Buffer) => {
      this.die(new ConnectionClosed('websocket closed', { code, reason: reason.toString(), wasClean: code === 1000 }));
    });
    socket.on('error', (error: Error) => {
      this.die(new ConnectionClosed(`websocket failed: ${error.message}`, { code: 1006 }));
    });
  }

  sendText(text: string): void {
    this.assertOpen();
    this.socket.send(text);
  }

  sendBytes(bytes: ArrayBuffer): void {
    this.assertOpen();
    this.socket.send(Buffer.from(bytes));
  }

  receive(): Promise<string | ArrayBuffer> {
    const queued = this.inbox.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closure !== null) return Promise.reject(this.closure);
    return new Promise((resolve, reject) => {
      this.waiting.push({ resolve, reject });
    });
  }

  close(code = 1000, reason = ''): void {
    this.socket.close(code, reason);
  }

  private assertOpen(): void {
    if (this.closure !== null) throw this.closure;
  }

  private deliver(message: string | ArrayBuffer): void {
    const waiter = this.waiting.shift();
    if (waiter === undefined) {
      this.inbox.push(message);
      return;
    }
    waiter.resolve(message);
  }

  private die(closure: ConnectionClosed): void {
    if (this.closure !== null) return;
    this.closure = closure;
    // Everyone still waiting learns at once; a receive that lands afterwards rejects from `closure`.
    while (this.waiting.length > 0) {
      this.waiting.shift()?.reject(closure);
    }
  }
}

function normalise(data: unknown, isBinary: boolean): string | ArrayBuffer {
  if (!isBinary) return data instanceof Buffer ? data.toString('utf8') : String(data);
  if (data instanceof ArrayBuffer) return data;
  if (data instanceof Buffer) {
    return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
  }
  if (Array.isArray(data)) return normalise(Buffer.concat(data as Buffer[]), true);
  return new Uint8Array(0).buffer;
}
