// @vitest-environment node
/**
 * `muxws/node` with the `ws` peer dependency absent (WSM-ERR-016).
 *
 * Without translation, the resolver throws `Error: Cannot find package 'ws' imported from
 * <...>/node_modules/muxws/dist/node.js` with `code: 'ERR_MODULE_NOT_FOUND'` and no `MuxwsError`; the
 * message names a file the reader did not write and no remedy. This is the TypeScript counterpart of
 * the bare `ImportError` covered by WSM-ERR-016.
 *
 * The tests use a subprocess, not `vi.mock('ws')`: vitest wraps anything a mock factory throws in
 * `Error: [vitest] There was an error when mocking a module`, so the `code` the translation reads does
 * not reach `requireWs` and the branch under test is not entered. The child process installs a
 * `node:module` resolve hook that throws the resolver's error for the `ws` specifier only, as on a
 * machine without `npm install ws`. The Python counterpart uses a `sys.meta_path` finder. Because the
 * hook runs in another process, it does not affect other dials in this suite.
 *
 * The child imports `ts/index.ts` first in every probe. `ts/node.ts` registers no codec, so without
 * that import `connect()` fails earlier with `CodecNotRegistered`, and an assertion that `connect`
 * rejects would pass for a reason unrelated to `ws`.
 */

import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** Vitest resolves its root from `vite.config.ts`, so this is the repository root. */
const ROOT = process.cwd();

/** What the child reports back about the one error it caught, since `instanceof` cannot cross a pipe. */
interface Probe {
  name: string;
  muxws: boolean;
  unsupported: boolean;
  url: boolean;
  message: string;
  causeCode: string | undefined;
  /** What `handleProtocols` answered *after* the failed dial, in the same process. */
  hook: string | false;
}

/**
 * Run `connect()` in a child where `import('ws')` fails, and bring back what it caught.
 *
 * `failure` is the body of the resolve hook's throw. The two tests below differ only in the error the
 * resolver produces: the absent package, and an install that is present and broken. The child is
 * otherwise identical, so the second test is a control for the first.
 */
function probeWithoutWs(failure: string): Probe {
  const directory = mkdtempSync(join(tmpdir(), 'mx-nows-'));
  try {
    const hook = join(directory, 'blocker.mjs');
    writeFileSync(
      hook,
      [
        "import { registerHooks } from 'node:module';",
        'registerHooks({',
        '  resolve(specifier, context, next) {',
        // Only the exact specifier `ws`. A hook that also failed other specifiers would make the child
        // fail elsewhere, and the assertions below would test that failure instead.
        "    if (specifier !== 'ws') return next(specifier, context);",
        `    ${failure}`,
        '  },',
        '});',
        '',
      ].join('\n'),
    );

    const script = [
      "const entry = import('./ts/index.ts');",
      "const node = import('./ts/node.ts');",
      "const errors = import('./ts/errors.ts');",
      'Promise.all([entry, node, errors]).then(async ([, n, e]) => {',
      "  const caught = await n.connect('ws://127.0.0.1:1/x').then(() => null, (error) => error);",
      '  console.log(JSON.stringify({',
      '    name: caught.name,',
      '    muxws: caught instanceof e.MuxwsError,',
      '    unsupported: caught instanceof e.TransportUnsupportedError,',
      '    url: caught instanceof e.TransportUrlError,',
      '    message: caught.message,',
      '    causeCode: caught.cause && caught.cause.code,',
      "    hook: n.handleProtocols(new Set(['muxws.v1.json'])),",
      '  }));',
      '});',
    ].join('\n');

    const output = execFileSync(process.execPath, ['--import', 'tsx', '--import', hook, '-e', script], {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const line = output.trim().split('\n').at(-1) ?? '';
    return JSON.parse(line) as Probe;
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

/** The resolver's own words for a package that was never installed, `code` included. */
const ABSENT = [
  'const error = new Error("Cannot find package \'ws\' imported from /app/node_modules/muxws/dist/node.js");',
  "    error.code = 'ERR_MODULE_NOT_FOUND';",
  '    throw error;',
].join('\n    ');

/** An install that is present and unusable, which must never be reported as one that is missing. */
const BROKEN = "throw new SyntaxError('Unexpected token in /node_modules/ws/lib/websocket.js');";

describe('dialling with the ws peer dependency absent - WSM-ERR-016', () => {
  it('names the install rather than the module the resolver could not find', { timeout: 60_000 }, () => {
    const probe = probeWithoutWs(ABSENT);

    expect(probe.name).toBe('WsNotInstalledError');
    expect(probe.muxws, 'an application-wide `instanceof MuxwsError` handler must catch it').toBe(true);
    expect(probe.unsupported).toBe(true);
    // Not a url error: the address is valid. The two transport bases are separate so that this case
    // is not reported as a url error.
    expect(probe.url).toBe(false);
    // The class names the remedy; "Cannot find package" is the runtime's own message.
    expect(probe.message).toContain('npm install ws');
    // The runtime's message is kept, because it names the file that imported `ws`.
    expect(probe.message).toContain('Cannot find package');
    // The original error is chained as `cause`, so its stack and `code` are preserved.
    expect(probe.causeCode).toBe('ERR_MODULE_NOT_FOUND');
  });

  it('rethrows a broken install untouched, so it is never reported as an absent one', { timeout: 60_000 }, () => {
    // Control for the test above. `requireWs` reads `code` and does not translate every error: for a
    // corrupt `node_modules/ws`, `npm install ws` is the wrong remedy, and only the original message
    // names the fault. Without this test, a blanket `catch` in the translation would also pass.
    const probe = probeWithoutWs(BROKEN);

    expect(probe.name).toBe('SyntaxError');
    expect(probe.muxws).toBe(false);
    expect(probe.message).toContain('websocket.js');
    expect(probe.message).not.toContain('npm install ws');
  });

  it('answers the acceptor hooks after a dial failed on a missing ws', { timeout: 60_000 }, () => {
    // Same process, after the failed dial. `handleProtocols` belongs to the acceptor, and an
    // application that only accepts connections may have no `ws` of its own because it receives
    // sockets from another component. Importing `ws` at module scope would make `muxws/node` fail to
    // import in that case, and `catch (e) { e instanceof WsNotInstalledError }` would never run
    // because the import of `muxws/node` throws the untranslated error first.
    expect(probeWithoutWs(ABSENT).hook).toBe('muxws.v1.json');
  });
});
