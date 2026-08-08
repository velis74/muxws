/**
 * The socket adapter port.
 *
 * Mirrors `muxws/transports/__init__.py`. WSM-API-021: this is the **only** place transport-specific
 * code lives. Text and binary sends are separate methods, never one polymorphic `send` - the peer
 * picks between them from `codec.binary`, which is declared rather than sniffed (WSM-CDC-002).
 */

/**
 * One WebSocket, seen the only way the peer is allowed to see it.
 *
 * Every method may be synchronous: the memory pair has nothing to await, and a browser socket's
 * `send` returns immediately. The peer awaits them regardless, so an implementation is free to
 * return either.
 */
export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
