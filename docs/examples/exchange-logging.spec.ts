/**
 * Runs `exchange-logging.ts` over a memory pair and reads the JSON lines it appended.
 */

import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { JsonCodec, memoryPair, Peer, type Stream } from 'muxws';

import { logExchanges, openExchangeLog } from './exchange-logging';

let dir: string;
let dialer: Peer;
let acceptor: Peer;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'muxws-exchange-logging-'));
  const [left, right] = memoryPair();
  dialer = new Peer(left, { codec: new JsonCodec(), isDialer: true });
  acceptor = new Peer(right, { codec: new JsonCodec(), isDialer: false });
  void dialer.serve();
  void acceptor.serve();
});

afterEach(async () => {
  await dialer.close({ drainMs: 100 });
  rmSync(dir, { recursive: true, force: true });
});

async function linesOf(path: string, write: () => Promise<void>): Promise<Record<string, unknown>[]> {
  const file = openExchangeLog(path);
  logExchanges(dialer, file);
  await write();
  await new Promise<void>((resolve) => file.end(resolve));
  return readFileSync(path, 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

test('a request is appended as one redacted JSON line', async () => {
  acceptor.onStream(async (payload: unknown, stream: Stream) =>
    stream.reply({ user: (payload as { user: string }).user, ok: true }, { headers: { status: 200 } }),
  );
  const path = join(dir, 'exchanges.jsonl');

  const [record] = await linesOf(path, async () => {
    await dialer.request({ user: 'ana', password: 'hunter2' }, { headers: { cookie: 'sid=1' }, timeoutMs: 5000 });
  });

  expect(record).toMatchObject({
    opened_by: 'local',
    outcome: 'ok',
    request: { headers: { cookie: '***' }, payloads: [{ user: 'ana', password: '***' }] },
    response: { headers: { status: 200 }, payloads: [{ user: 'ana', ok: true }] },
  });
  expect(readFileSync(path, 'utf8')).not.toContain('hunter2');
});

test('a long payload is trimmed and still redacted', async () => {
  acceptor.onStream(async (payload: unknown, stream: Stream) =>
    stream.reply({ size: (payload as { blob: string }).blob.length }),
  );

  const [record] = await linesOf(join(dir, 'exchanges.jsonl'), async () => {
    await dialer.request({ password: 'hunter2', blob: 'z'.repeat(10_000) }, { timeoutMs: 5000 });
  });

  expect(record).toMatchObject({ request: { payloads: ['***'] }, response: { payloads: [{ size: 10_000 }] } });
});
