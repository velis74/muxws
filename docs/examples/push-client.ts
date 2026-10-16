/**
 * Server push, dialer half, in TypeScript on Node - the other end of `push_server.py`.
 *
 * Run it with `npx tsx push-client.ts`. The URL is `ws://127.0.0.1:8001/ws` unless `MUXWS_URL` says
 * otherwise.
 *
 * `onStream` is passed to `connect()` rather than registered after it returns, because the acceptor
 * may push a stream as soon as the connection is established. A stream that arrives before a handler is
 * registered is reset with `REFUSED`.
 */

import 'muxws';
import { connect } from 'muxws/node';

const url = process.env.MUXWS_URL ?? 'ws://127.0.0.1:8001/ws';

interface Topic {
  topic: string;
}

interface Tick {
  tick: number;
}

let finished: () => void = () => undefined;
const pushDone = new Promise<void>((resolve) => {
  finished = resolve;
});

const peer = await connect(url, {
  onStream: async (payload, stream) => {
    console.log(`push: ${(payload as Topic).topic}`);
    for await (const tick of stream) {
      console.log(`tick ${(tick as Tick).tick}`);
    }
    finished();
  },
});

try {
  // One-shot: `notify()` sends a payload, ends the stream and returns nothing to await; no response
  // is expected.
  await peer.notify({ subscribe: 'ticks' });
  // 10000 milliseconds, so a stuck example fails instead of hanging.
  const deadline = new Promise<never>((_resolve, reject) => {
    setTimeout(() => {
      reject(new Error('the push never arrived within 10000 milliseconds'));
    }, 10_000).unref();
  });
  await Promise.race([pushDone, deadline]);
} finally {
  await peer.close();
}
