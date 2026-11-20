import { fileURLToPath } from 'node:url';

import { defineConfig, devices } from '@playwright/test';

/** The backend `demo.py --browser` starts: `python` (default) or `node`, from `MUXWS_DEMO_BACKEND`. */
const backend = process.env.MUXWS_DEMO_BACKEND ?? 'python';
if (backend !== 'python' && backend !== 'node') {
  throw new Error(`MUXWS_DEMO_BACKEND must be 'python' or 'node', not '${backend}'`);
}

export default defineConfig({
  testDir: '.',
  // Not `*.spec.ts`: vitest collects those, and this file runs only under Playwright.
  testMatch: '*.e2e.ts',
  outputDir: 'test-results',
  // The demo has one backend and one board per run; the steps share it.
  workers: 1,
  fullyParallel: false,
  timeout: 120_000,
  reporter: 'list',
  use: {
    ...devices['Desktop Chrome'],
    baseURL: 'http://127.0.0.1:5173',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: `uv run python demo.py --browser ${backend}`,
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    url: 'http://127.0.0.1:5173',
    // A server already on :5173 or :8020 may be an older build or the other backend.
    reuseExistingServer: false,
    timeout: 120_000,
    stdout: 'pipe',
    // SIGTERM, as an IDE's stop button sends: `demo.py` stops the dev server's and the backend's process
    // groups; SIGKILL would leave them running.
    gracefulShutdown: { signal: 'SIGTERM', timeout: 10_000 },
  },
});
