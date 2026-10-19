#!/usr/bin/env node
/**
 * The published type declarations, checked the way a consumer resolves them.
 *
 * Run it after `npm run build` with `npm run types:check`. It packs the package with `npm pack`,
 * unpacks the tarball into a temporary `node_modules/muxws`, and type-checks consumer files under
 * every `moduleResolution` a consumer may use:
 *
 *   bundler    an ES module, resolved through the `import` condition of `exports`.
 *   node16     an ES module (`import` condition, `.d.ts`) and a CommonJS module (`require` condition,
 *   nodenext   `.d.cts`). An ES module's relative imports must name their file under these modes.
 *   node10     an ES module; `exports` is not read, so the subpaths resolve through `typesVersions`.
 *
 * Each consumer imports `muxws`, `muxws/node` and `muxws/msgpack`, and passes a `SameOrigin` from
 * `muxws` to `AcceptOptions` from `muxws/node`. That assignment compiles only when both entry points
 * share one declaration of the class: `SameOrigin` has private members, which make two declarations
 * of it incompatible types.
 *
 * `ws`, `@msgpack/msgpack` and `@types/*` are linked from the repository's `node_modules`, so the check
 * installs nothing.
 */

import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const TSC = join(ROOT, 'node_modules', '.bin', 'tsc');

const CONSUMER = `import { Peer, SameOrigin, parseOrigin } from 'muxws';
import { MsgpackCodec } from 'muxws/msgpack';
import { type AcceptOptions, accept } from 'muxws/node';

const options: AcceptOptions = { allowedOrigins: new SameOrigin({ also: ['http://localhost:5173'] }) };
export { MsgpackCodec, Peer, accept, options, parseOrigin };
`;

/** `moduleResolution` and `module` of each run, and the consumer files it checks. */
const RUNS = [
  { resolution: 'bundler', module: 'esnext', files: ['consumer.mts'] },
  { resolution: 'node16', module: 'node16', files: ['consumer.mts', 'consumer.cts'] },
  { resolution: 'nodenext', module: 'nodenext', files: ['consumer.mts', 'consumer.cts'] },
  { resolution: 'node10', module: 'commonjs', files: ['consumer.mts'] },
];

const LINKED = ['ws', '@msgpack', '@types'];

const work = mkdtempSync(join(tmpdir(), 'muxws-types-'));
let failed = false;
try {
  const packed = execFileSync('npm', ['pack', '--silent', '--pack-destination', work], { cwd: ROOT, encoding: 'utf8' });
  const tarball = packed.trim().split('\n').at(-1);
  const modules = join(work, 'node_modules');
  mkdirSync(join(modules, 'muxws'), { recursive: true });
  execFileSync('tar', ['-xzf', join(work, tarball), '-C', join(modules, 'muxws'), '--strip-components=1']);
  for (const name of LINKED) symlinkSync(join(ROOT, 'node_modules', name), join(modules, name), 'dir');
  writeFileSync(join(work, 'consumer.mts'), CONSUMER);
  writeFileSync(join(work, 'consumer.cts'), CONSUMER);

  for (const run of RUNS) {
    const args = ['--noEmit', '--strict', '--types', 'node', '--target', 'es2022'];
    args.push('--module', run.module, '--moduleResolution', run.resolution, ...run.files);
    try {
      execFileSync(TSC, args, { cwd: work, encoding: 'utf8', stdio: 'pipe' });
      console.log(`PASS  ${run.resolution}  (${run.files.join(', ')})`);
    } catch (error) {
      failed = true;
      console.log(`FAIL  ${run.resolution}  (${run.files.join(', ')})`);
      console.log(`${error.stdout}${error.stderr}`.replace(/^/gmu, '      '));
    }
  }
} finally {
  rmSync(work, { recursive: true, force: true });
}

if (failed) {
  console.log('\nThe published declarations do not resolve for every consumer above.');
  process.exit(1);
}
console.log('\nThe published declarations resolve under every moduleResolution.');
