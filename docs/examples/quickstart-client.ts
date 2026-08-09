/**
 * Quick-start dialer in TypeScript, on Node, against the same `quickstart_server.py`.
 *
 * Run it with `npx tsx quickstart-client.ts`. The URL is `ws://127.0.0.1:8000/ws` unless the
 * `MUXWS_URL` environment variable says otherwise.
 *
 * Two imports, and both are load-bearing. `muxws` is the package itself, and importing it is what
 * registers the built-in `json` codec; `muxws/node` is the subpath that owns the `ws` socket, and it
 * is the only place in the package that touches `ws`. A browser dialer imports `connect` from
 * `muxws` instead and changes nothing else.
 */

import 'muxws';
import { connect } from 'muxws/node';

const url = process.env.MUXWS_URL ?? 'ws://127.0.0.1:8000/ws';

interface Greeting {
  greeting: string;
}

interface Chunk {
  chunk: number;
  of: number;
}

const peer = await connect(url);
try {
  // Unary. `request()` sends one payload, waits for exactly one back, and rejects if the remote
  // sends a second - which `await stream` deliberately does not.
  const answer = await peer.request<Greeting>({ say: 'hello' });
  console.log(`greeting: ${answer.greeting}`);

  // Streaming response. `open()` is synchronous: it hands back the Stream in the same turn it
  // allocated the id. `end: true` says this side has nothing more to send, so the acceptor is free
  // to stream back immediately.
  const stream = peer.open<Chunk>({ count: 3 }, { end: true });
  for await (const chunk of stream) {
    console.log(`chunk ${chunk.chunk} of ${chunk.of}`);
  }
} finally {
  await peer.close();
}
