/**
 * The socket adapter port.
 *
 * Mirrors `muxws/transports/__init__.py`. Transport-specific code lives only in implementations of
 * this interface (WSM-API-021). Text and binary sends are separate methods; the peer chooses between
 * them from the declared `codec.binary` (WSM-CDC-002).
 */

/**
 * The interface through which the peer uses one WebSocket.
 *
 * Every method except `receive` may return synchronously or return a promise; the peer awaits the
 * result either way. The memory pair and a browser socket's `send` complete synchronously.
 */
export interface SocketAdapter {
  sendText(text: string): Promise<void> | void;
  sendBytes(bytes: ArrayBuffer): Promise<void> | void;
  receive(): Promise<string | ArrayBuffer>;
  close(code?: number, reason?: string): Promise<void> | void;
}
