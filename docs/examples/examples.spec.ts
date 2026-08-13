/**
 * Runs the shipped TypeScript examples and checks them against the output the guide prints.
 *
 * The acceptor half of every example is Python, which makes this a cross-language check as well:
 * `quickstart-client.ts` must print, byte for byte, what `quickstart_client.py` prints and what
 * `docs/guide/getting-started.md` says they both print.
 *
 * The Python interpreter is `MUXWS_PYTHON`, or `python3`. It runs the example scripts with this
 * checkout on `PYTHONPATH`, so `muxws` itself need not be installed; where that interpreter cannot
 * import `fastapi` and `uvicorn` the suite skips rather than fails, because the TypeScript package
 * has no Python dependency and a checkout without one is not a broken example.
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
 * The environment every Python subprocess here gets, with **this checkout** ahead of anything
 * installed.
 *
 * Without it the example servers cannot import `muxws` at all unless the interpreter happens to have
 * it installed, because Python puts the *script's* directory on `sys.path` and not the working
 * directory - `docs/examples/` holds no package. With it they import the tree the test is testing,
 * which is the version a repository test wants anyway: an installed copy would let a green run mean
 * nothing about the code in front of you.
 */
const PYTHON_ENV: NodeJS.ProcessEnv = {
  ...process.env,
  PYTHONPATH: [REPOSITORY, process.env.PYTHONPATH].filter(Boolean).join(delimiter),
};

/**
 * Whether the example servers can actually run - asked the way they are actually started.
 *
 * `cwd` is a directory with no `muxws` in it, deliberately. The previous form ran `python -c` from
 * the repository root, where `sys.path[0]` is the working directory and `./muxws/` therefore imports
 * cleanly - so the probe passed on every checkout while the servers, run as scripts from
 * `docs/examples/`, could not import a thing. A guard that answers a different question from the one
 * it is guarding does not skip: it lets the suite fail somewhere else, which is what it did.
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

/** `ms` milliseconds of nothing. */
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
    // eslint-disable-next-line no-await-in-loop -- polling a port is sequential by nature
    if (await listening(port)) {
      return {
        url: `ws://127.0.0.1:${port}/ws`,
        stop: () => {
          child.kill();
        },
      };
    }
    // A refused connection comes back instantly, so without this the poll is a busy loop.
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

/** What a run that outlived its budget reports, so one hanging example cannot mask the rest. */
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
    // A run that outlives its budget is killed and **reported**, not thrown: rejecting here would
    // abandon the loop in `every api example executes`, and one example that never exits would hide
    // every example after it.
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
        // `stderr` is the failure message rather than an assertion of its own: a Node release that
        // prints an ExperimentalWarning there must not turn a correct example into a red build.
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
 * Where the extracted API examples are written. Two constraints pin this to the repository root.
 *
 * `import 'ws'` needs Node's upward search for `node_modules` to reach this repository's copy, so a
 * directory under /tmp is out; and `tsx` applies tsconfig `paths` only to files **outside**
 * `node_modules`, so a directory under `node_modules/.cache` is out too. The directory is removed
 * again in the test's `finally`.
 */
const SCRATCH_PREFIX = '.muxws-api-examples-';

function scratchParent(): string {
  try {
    mkdirSync(REPOSITORY, { recursive: true });
    // A run killed mid-flight leaves its directory behind, and an untracked directory in the
    // repository root is exactly the kind of cruft nobody notices until it is committed.
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

/** Collected at load time so the budget below can be counted rather than guessed. */
const API_EXAMPLES = apiExamples();
/** Per-example budget, in milliseconds. Node start-up dominates; a connecting example is slower. */
const PER_API_EXAMPLE_MS = 30_000;

describe('every api example executes', () => {
  it(
    'runs every ts example block in docs/api',
    async () => {
      const examples = API_EXAMPLES;
      if (examples.length === 0) {
        // Nothing to run yet. The Python mirror in `run_examples_test.py` skips for the same reason.
        expect(examples).toEqual([]);
        return;
      }
      const scratch = mkdtempSync(join(scratchParent(), SCRATCH_PREFIX));
      const failures: string[] = [];
      try {
        for (const example of examples) {
          // `.mts` so the block is always ESM, which is what a top-level `await` in an example needs.
          const file = join(scratch, `${example.name.replace(/[^a-z0-9]+/gi, '-')}.mts`);
          writeFileSync(file, example.code, 'utf8');
          // eslint-disable-next-line no-await-in-loop -- one subprocess at a time, on purpose
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
