/**
 * Runs the package's linters and type checks. Without arguments it only reports; with `--fix`, eslint and
 * markdownlint also rewrite what they can fix. Every step runs, and the exit code is non-zero if any step fails.
 *
 *   npm run lint              report only
 *   npm run lint -- --fix     report and fix
 *
 * `npm run lint --fix` (without `--`) is equivalent: npm exposes the unknown flag as `npm_config_fix`.
 */
import { spawnSync } from 'node:child_process';

const fix = process.argv.includes('--fix') || process.env.npm_config_fix === 'true';
const fixArg = fix ? ['--fix'] : [];

const steps = [
  [
    'eslint',
    ...fixArg,
    'ts',
    'interop',
    'demo/frontend/src',
    'demo/backend_node',
    'docs/examples',
    'docs/.vitepress/config.ts',
  ],
  ['markdownlint-cli2', ...fixArg, '**/*.md'],
  ['tsc', '--noEmit'],
  ['tsc', '-p', 'interop/tsconfig.json'],
  ['tsc', '-p', 'demo/backend_node/tsconfig.json'],
  ['tsc', '-p', 'docs/examples/tsconfig.json'],
  ['tsc', '-p', 'docs/.vitepress/tsconfig.json'],
  ['vue-tsc', '--noEmit', '-p', 'demo/frontend/tsconfig.json'],
];

let failed = false;
for (const [command, ...args] of steps) {
  const { status } = spawnSync(command, args, { stdio: 'inherit', shell: process.platform === 'win32' });
  if (status !== 0) failed = true;
}
process.exit(failed ? 1 : 0);
