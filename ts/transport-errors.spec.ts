// @vitest-environment node
/**
 * The one failure of `muxws/node` that has exactly one remedy: the `ws` peer dependency is absent.
 *
 * Untranslated, the resolver answers `Error: Cannot find package 'ws' imported from
 * <...>/node_modules/muxws/dist/node.js` with `code: 'ERR_MODULE_NOT_FOUND'` and no `MuxwsError` in
 * sight - a file the reader did not write, naming no remedy. That is the TypeScript twin of the bare
 * `ImportError` WSM-ERR-016 was written for.
 *
 * A subprocess rather than `vi.mock('ws')`: vitest wraps anything a mock factory throws in its own
 * `Error: [vitest] There was an error when mocking a module`, so the `code` the translation reads
 * never reaches `requireWs` and the branch under test is never entered. What is wanted is the
 * resolver failing the way it fails on a machine that has not run `npm install ws`, so the child
 * process installs a `node:module` resolve hook that throws exactly that error for exactly that
 * specifier. It is the same instrument as the `sys.meta_path` finder the Python port's twin uses, and
 * it leaves every other dial in this suite alone because it lives in another process.
 *
 * The child imports `ts/index.ts` first in every probe. `ts/node.ts` registers no codec, so without it
 * `connect()` fails earlier with `CodecNotRegistered` and a test asserting "connect rejects" would be
 * green for a reason that has nothing to do with `ws`.
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
 * `failure` is the body of the resolve hook's throw, so the two tests below differ only in the error
 * the resolver produces - the absent package, and an install that is present and broken. Everything
 * else about the child is identical, which is what makes the second one a control on the first rather
 * than a different experiment.
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
        // Only `ws`, and only by exact specifier: a hook that swallowed anything else would make the
        // child fail somewhere else entirely and the assertions below would be about that instead.
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
    // Not a url error: the address is fine and retyping it changes nothing. Collapsing the two bases
    // is the one wrong answer available here, and it is the reason there are two.
    expect(probe.url).toBe(false);
    // The whole point of the class. "Cannot find package" is what the runtime already said.
    expect(probe.message).toContain('npm install ws');
    // ...and it is still said, because the resolver's sentence names the file that reached for it.
    expect(probe.message).toContain('Cannot find package');
    // Chained rather than swallowed, so the stack and the machine-readable code both survive.
    expect(probe.causeCode).toBe('ERR_MODULE_NOT_FOUND');
  });

  it('rethrows a broken install untouched, so it is never reported as an absent one', { timeout: 60_000 }, () => {
    // The control, and the reason `requireWs` reads `code` instead of catching everything: telling a
    // reader whose `node_modules/ws` is corrupt to run `npm install ws` is a confident wrong answer,
    // and the original message is the only thing that names the real fault. Without this test the
    // translation above would pass equally well written as a blanket `catch`.
    const probe = probeWithoutWs(BROKEN);

    expect(probe.name).toBe('SyntaxError');
    expect(probe.muxws).toBe(false);
    expect(probe.message).toContain('websocket.js');
    expect(probe.message).not.toContain('npm install ws');
  });

  it('still answers the acceptor hooks, which is why the import is on the dial path only', { timeout: 60_000 }, () => {
    // Same process, after the failed dial: `handleProtocols` is the acceptor's half, and an application
    // that only accepts connections legitimately has no `ws` of its own - it is handed sockets by
    // whoever does. Guarding the import at module scope would break that, and would also make
    // `catch (e) { e instanceof WsNotInstalledError }` throw the very error the class exists to
    // replace, because importing `muxws/node` would already have failed.
    expect(probeWithoutWs(ABSENT).hook).toBe('muxws.v1.json');
  });
});
