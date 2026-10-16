/**
 * Quick-start dialer in TypeScript, on Node, against the same `quickstart_server.py`.
 *
 * Run it with `npx tsx quickstart-client.ts`. The URL is `ws://127.0.0.1:8000/ws` unless the
 * `MUXWS_URL` environment variable says otherwise.
 *
 * Importing `muxws` registers the built-in `json` codec. `muxws/node` provides `connect` over the
 * `ws` package; it is the only module in the package that imports `ws`. A browser dialer imports
 * `connect` from `muxws` instead; the rest of the code is the same.
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
  // sends a second. `await stream` does not reject in that case.
  const answer = await peer.request<Greeting>({ say: 'hello' });
  console.log(`greeting: ${answer.greeting}`);

  // Streaming response. `open()` is synchronous: it returns the Stream in the same turn that
  // allocates the id. `end: true` closes the sending side, so the acceptor can start streaming back
  // immediately.
  const stream = peer.open<Chunk>({ count: 3 }, { end: true });
  for await (const chunk of stream) {
    console.log(`chunk ${chunk.chunk} of ${chunk.of}`);
  }
} finally {
  await peer.close();
}
