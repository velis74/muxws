/**
 * The TypeScript half of the cross-language interop check (M3 section 7, tests 22-24).
 *
 * Run as an acceptor:  npx tsx interop/runner.ts accept <port>
 * Run as a dialer:     npx tsx interop/runner.ts dial ws://127.0.0.1:<port>
 *
 * The scenario is the same in both roles and in both languages, so a disagreement shows up as a
 * named failure rather than as a hang. Exits non-zero on any mismatch.
 */

import { WebSocket, WebSocketServer } from 'ws';

// Imported through the public entry point, exactly as an application would: `ts/index.ts` is what
// registers the JSON codec (WSM-CDC-004), because a codec module must never register itself
// (WSM-CDC-014). Reaching past it into `ts/codec` gives an empty registry - which is the correct
// behaviour, and was the first thing this script got wrong.
import {
  ConnectionLost,
  getCodec,
  Peer,
  type RemoteError,
  type Stream,
  type StreamHandler,
  offer,
} from '../ts/index';
import { accept, WsSocket } from '../ts/node';

function check(condition: boolean, what: string): void {
  if (!condition) {
    console.error(`interop FAILED: ${what}`);
    process.exit(1);
  }
}

/** Every shape the scenario exercises, chosen by the opening payload. */
const handler: StreamHandler = async (payload: any, stream: Stream) => {
  const action = (payload ?? {}).action as string | undefined;

  if (action === 'echo') {
    await stream.reply({ echo: payload.value });
  } else if (action === 'export') {
    for (let index = 0; index < 4; index += 1) {
      await stream.send({ row: index });
    }
    await stream.end({ payload: { row: 4 }, trailers: { rows: '5' } });
  } else if (action === 'raise') {
    throw new Error('interop handler said no');
  } else if (action === 'forever') {
    await stream.closed;
  } else if (action === 'big') {
    await stream.reply({ blob: 'š'.repeat(40_000) });
  } else {
    check(false, `unknown action ${String(action)}`);
  }
};

async function runScenario(peer: Peer, label: string): Promise<void> {
  const echoed = await peer.request({ action: 'echo', value: 42 });
  check(JSON.stringify(echoed) === JSON.stringify({ echo: 42 }), `${label}: unary returned ${JSON.stringify(echoed)}`);

  const rows: unknown[] = [];
  for await (const row of peer.open({ action: 'export' })) rows.push(row);
  const wanted = JSON.stringify([0, 1, 2, 3, 4].map((row) => ({ row })));
  check(JSON.stringify(rows) === wanted, `${label}: export returned ${JSON.stringify(rows)}`);

  await peer.notify({ action: 'echo', value: 1 });

  let applicationError: RemoteError | null = null;
  await peer.request({ action: 'raise' }).catch((error: unknown) => {
    applicationError = error as RemoteError;
  });
  check(applicationError !== null, `${label}: a throwing handler did not produce an error`);
  const errorPayload = applicationError!.payload as { type?: unknown; message?: unknown } | null;
  // `message` is portable; `type` is NOT. WSM-ERR-006's default serializer reports the remote's own
  // exception class name, so a Python acceptor says 'ValueError' where a TypeScript one says 'Error'.
  // Asserting equality on it would be asserting which language answered. See GAPS.md.
  check(
    errorPayload?.message === 'interop handler said no',
    `${label}: application error message was ${JSON.stringify(errorPayload)}`,
  );
  check(
    typeof errorPayload?.type === 'string' && errorPayload.type.length > 0,
    `${label}: application error carried no type name: ${JSON.stringify(errorPayload)}`,
  );

  const held = peer.open({ action: 'forever' });
  void held.catch(() => undefined);
  await new Promise((resolve) => setTimeout(resolve, 100));
  await held.cancel('interop cancel');
  check(held.signal.aborted, `${label}: cancel did not close the stream locally`);

  const big = (await peer.request({ action: 'big' })) as { blob: string };
  check(big.blob.length === 40_000, `${label}: fragmented payload came back as ${big.blob.length} chars`);
  check(big.blob[0] === 'š', `${label}: fragmented payload lost its non-ASCII content`);

  console.log(JSON.stringify({ role: label, ok: true }));
}

async function acceptForever(port: number): Promise<void> {
  const server = new WebSocketServer({
    port,
    host: '127.0.0.1',
    handleProtocols: (protocols: Set<string>) => {
      const wanted = `muxws.v1.${getCodec('json').name}`;
      return protocols.has(wanted) ? wanted : false;
    },
  });

  server.on('connection', (socket: WebSocket) => {
    void (async () => {
      const peer = await accept(socket);
      peer.onStream(handler);
      await peer.serve().catch((error: unknown) => {
        if (!(error instanceof ConnectionLost)) throw error;
      });
    })();
  });

  await new Promise<void>((resolve) => server.on('listening', () => resolve()));
  const address = server.address();
  const bound = typeof address === 'object' && address !== null ? address.port : port;
  console.log(JSON.stringify({ role: 'ts-acceptor', port: bound }));
  await new Promise(() => undefined);
}

async function dial(url: string): Promise<void> {
  const socket = new WebSocket(url, offer('json'));
  await new Promise<void>((resolve, reject) => {
    socket.on('open', () => resolve());
    socket.on('error', reject);
  });
  check(socket.protocol === 'muxws.v1.json', `negotiated subprotocol was ${socket.protocol}`);

  const peer = new Peer(new WsSocket(socket), { codec: getCodec('json'), isDialer: true });
  peer.onStream(handler);
  void peer.serve().catch(() => undefined);
  try {
    await runScenario(peer, 'ts-dialer');
  } finally {
    socket.close();
  }
}

const [mode, argument] = process.argv.slice(2);
if (mode === 'accept') {
  void acceptForever(Number(argument));
} else if (mode === 'dial') {
  void dial(argument);
} else {
  console.error('usage: runner.ts accept <port> | runner.ts dial <url>');
  process.exit(1);
}
