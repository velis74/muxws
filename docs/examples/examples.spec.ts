/**
 * Runs the shipped TypeScript examples and checks them against the output the guide prints.
 *
 * The acceptor of every example is Python, so this is also a cross-language check:
 * `quickstart-client.ts` must print, byte for byte, the block that `docs/guide/getting-started.md`
 * shows, which is also what `quickstart_client.py` prints.
 *
 * The Python interpreter is `MUXWS_PYTHON`, or `python3`. It runs the example scripts with this
 * checkout on `PYTHONPATH`, so `muxws` need not be installed. If that interpreter cannot import
 * `fastapi` and `uvicorn`, the suite is skipped, because the TypeScript package has no Python
 * dependency.
 */

import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { connect as netConnect, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const EXAMPLES = dirname(fileURLToPath(import.meta.url));
const DOCS = resolve(EXAMPLES, '..');
const REPOSITORY = resolve(DOCS, '..');
const GETTING_STARTED = join(DOCS, 'guide', 'getting-started.md');
const API = join(DOCS, 'api');
const TSCONFIG = join(EXAMPLES, 'tsconfig.json');
const TSX = join(REPOSITORY, 'node_modules', '.bin', 'tsx');

/** How long to wait for an example server to start listening, in milliseconds. */
const STARTUP_TIMEOUT_MS = 30_000;
/** How long any one example may run before the test gives up, in milliseconds. */
const RUN_TIMEOUT_MS = 60_000;
/** How long to wait between attempts to connect to a starting server, in milliseconds. */
const POLL_INTERVAL_MS = 50;

const PYTHON = process.env.MUXWS_PYTHON ?? 'python3';

/**
 * The environment for every Python subprocess, with **this checkout** on `PYTHONPATH` ahead of any
 * installed copy.
 *
 * Python puts the *script's* directory on `sys.path`, not the working directory, and
 * `docs/examples/` contains no package, so without this the example servers import `muxws` only if
 * it is installed. With it they import the checkout under test, not an installed copy.
 */
const PYTHON_ENV: NodeJS.ProcessEnv = {
  ...process.env,
  PYTHONPATH: [REPOSITORY, process.env.PYTHONPATH].filter(Boolean).join(delimiter),
};

/**
 * Whether the example servers can run, probed with the same environment they are started with.
 *
 * `cwd` is a directory without `muxws` in it: from the repository root, `sys.path[0]` would let
 * `./muxws/` import, so the probe would pass even when the servers, run as scripts from
 * `docs/examples/`, cannot import it.
 */
const pythonReady =
  spawnSync(PYTHON, ['-c', 'import muxws, fastapi, uvicorn'], {
    cwd: tmpdir(),
    env: PYTHON_ENV,
    stdio: 'ignore',
  }).status === 0;

/** `<!-- expected-output: name -->` followed by the fenced block the page prints. */
const EXPECTED_OUTPUT = /<!--\s*expected-output:\s*([a-z][a-z-]*)\s*-->\s*\n+```[a-z]*\n([\s\S]*?)^```/gm;
/** A fenced `ts` block under an `### Example` heading on an API page. */
const API_EXAMPLE = /^#{3,}[ \t]+Example\b[^\n]*\n(?:(?!^#{1,6}[ \t])[\s\S])*?^```ts\n([\s\S]*?)^```/gm;

/** The exact block `getting-started.md` prints for `name`. */
function documentedOutput(name: string): string {
  const text = readFileSync(GETTING_STARTED, 'utf8');
  EXPECTED_OUTPUT.lastIndex = 0;
  let match = EXPECTED_OUTPUT.exec(text);
  while (match !== null) {
    if (match[1] === name) return match[2];
    match = EXPECTED_OUTPUT.exec(text);
  }
  throw new Error(`getting-started.md has no <!-- expected-output: ${name} --> block`);
}

/** A port the operating system is not using, released again before the server takes it. */
async function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      probe.close(() => {
        resolvePort(port);
      });
    });
  });
}

/** One connect attempt, resolving false rather than throwing while the server is still starting. */
async function listening(port: number): Promise<boolean> {
  return new Promise((resolveProbe) => {
    const probe = netConnect({ port, host: '127.0.0.1' });
    probe.setTimeout(POLL_INTERVAL_MS);
    const settle = (value: boolean) => {
      probe.destroy();
      resolveProbe(value);
    };
    probe.once('connect', () => {
      settle(true);
    });
    probe.once('timeout', () => {
      settle(false);
    });
    probe.once('error', () => {
      settle(false);
    });
  });
}

/** Resolve after `ms` milliseconds. */
async function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => {
    setTimeout(resolveDelay, ms);
  });
}

interface RunningServer {
  url: string;
  stop: () => void;
}

/** Start one of the Python example servers on an ephemeral port. */
async function startServer(script: string): Promise<RunningServer> {
  const port = await freePort();
  const child = spawn(PYTHON, [join(EXAMPLES, script)], {
    cwd: REPOSITORY,
    env: { ...PYTHON_ENV, MUXWS_PORT: String(port) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (chunk: Buffer) => {
    stderr += chunk.toString();
  });

  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`${script} exited with ${child.exitCode}:\n${stderr}`);
    // eslint-disable-next-line no-await-in-loop -- port polling is sequential
    if (await listening(port)) {
      return {
        url: `ws://127.0.0.1:${port}/ws`,
        stop: () => {
          child.kill();
        },
      };
    }
    // A refused connection returns immediately; the delay keeps the poll from busy-looping.
    // eslint-disable-next-line no-await-in-loop -- see above
    await delay(POLL_INTERVAL_MS);
  }
  child.kill();
  throw new Error(`${script} did not listen on port ${port} within ${STARTUP_TIMEOUT_MS} milliseconds`);
}

interface Completed {
  code: number | null;
  stdout: string;
  stderr: string;
}

/** The exit code reported for a run that exceeded its time limit. */
const TIMED_OUT = -1;

/** Run one file through `tsx`, with `muxws` and `muxws/node` pointed at this repository's sources. */
async function runTypescript(file: string, env: Record<string, string> = {}): Promise<Completed> {
  return new Promise((resolveRun, reject) => {
    const child = spawn(TSX, ['--tsconfig', TSCONFIG, file], {
      cwd: REPOSITORY,
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    // A run that exceeds its time limit is killed and reported as a result, not thrown: rejecting
    // here would end the loop in `every api example executes` and skip the remaining examples.
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
      stderr += `\n${file} did not finish within ${RUN_TIMEOUT_MS} milliseconds and was killed`;
    }, RUN_TIMEOUT_MS);
    child.once('error', reject);
    child.once('close', (code) => {
      clearTimeout(timer);
      resolveRun({ code: timedOut ? TIMED_OUT : code, stdout, stderr });
    });
  });
}

describe.skipIf(!pythonReady)('the documented examples', () => {
  it(
    'quickstart client produces the documented output',
    async () => {
      const server = await startServer('quickstart_server.py');
      try {
        const run = await runTypescript(join(EXAMPLES, 'quickstart-client.ts'), { MUXWS_URL: server.url });
        // `stderr` is the failure message, not asserted on: Node may print an ExperimentalWarning
        // there for a correct example.
        expect(run.code, run.stderr).toBe(0);
        expect(run.stdout, run.stderr).toBe(documentedOutput('quickstart'));
      } finally {
        server.stop();
      }
    },
    RUN_TIMEOUT_MS + STARTUP_TIMEOUT_MS,
  );

  it(
    'push client receives the stream the acceptor opened',
    async () => {
      const server = await startServer('push_server.py');
      try {
        const run = await runTypescript(join(EXAMPLES, 'push-client.ts'), { MUXWS_URL: server.url });
        expect(run.code, run.stderr).toBe(0);
        expect(run.stdout, run.stderr).toBe(documentedOutput('push'));
      } finally {
        server.stop();
      }
    },
    RUN_TIMEOUT_MS + STARTUP_TIMEOUT_MS,
  );
});

/**
 * Where the extracted API examples are written: the repository root.
 *
 * `import 'ws'` needs Node's upward search for `node_modules` to reach this repository's copy, which
 * excludes /tmp; `tsx` applies tsconfig `paths` only to files **outside** `node_modules`, which
 * excludes `node_modules/.cache`. The directory is removed in the test's `finally`.
 */
const SCRATCH_PREFIX = '.muxws-api-examples-';

function scratchParent(): string {
  try {
    mkdirSync(REPOSITORY, { recursive: true });
    // Remove directories left behind by runs that were killed before their `finally`.
    readdirSync(REPOSITORY)
      .filter((entry) => entry.startsWith(SCRATCH_PREFIX))
      .forEach((entry) => {
        rmSync(join(REPOSITORY, entry), { recursive: true, force: true });
      });
    return REPOSITORY;
  } catch {
    return tmpdir();
  }
}

/** Every fenced `ts` block under an `### Example` heading in `docs/api/`. */
function apiExamples(): { name: string; code: string }[] {
  const found: { name: string; code: string }[] = [];
  let pages: string[] = [];
  try {
    pages = readdirSync(API)
      .filter((entry) => entry.endsWith('.md'))
      .sort();
  } catch {
    return found;
  }
  pages.forEach((page) => {
    const text = readFileSync(join(API, page), 'utf8');
    API_EXAMPLE.lastIndex = 0;
    let index = 0;
    let match = API_EXAMPLE.exec(text);
    while (match !== null) {
      found.push({ name: `${page}[${index}]`, code: match[1] });
      index += 1;
      match = API_EXAMPLE.exec(text);
    }
  });
  return found;
}

/** Collected at load time so the test timeout below is computed from the number of examples. */
const API_EXAMPLES = apiExamples();
/** Per-example time limit, in milliseconds. It is mostly Node start-up; a connecting example takes longer. */
const PER_API_EXAMPLE_MS = 30_000;

describe('every api example executes', () => {
  it(
    'runs every ts example block in docs/api',
    async () => {
      const examples = API_EXAMPLES;
      if (examples.length === 0) {
        // No examples to run. `run_examples_test.py` skips in the same case.
        expect(examples).toEqual([]);
        return;
      }
      const scratch = mkdtempSync(join(scratchParent(), SCRATCH_PREFIX));
      const failures: string[] = [];
      try {
        for (const example of examples) {
          // `.mts` makes the block ESM, which a top-level `await` requires.
          const file = join(scratch, `${example.name.replace(/[^a-z0-9]+/gi, '-')}.mts`);
          writeFileSync(file, example.code, 'utf8');
          // eslint-disable-next-line no-await-in-loop -- runs one subprocess at a time
          const run = await runTypescript(file);
          if (run.code !== 0) failures.push(`${example.name} exited with ${run.code}:\n${run.stderr}`);
        }
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
      expect(failures).toEqual([]);
    },
    Math.max(RUN_TIMEOUT_MS, API_EXAMPLES.length * PER_API_EXAMPLE_MS),
  );
});
