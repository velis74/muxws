/// <reference types="vitest" />
import { resolve } from 'path';

import { visualizer } from 'rollup-plugin-visualizer';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';

export default defineConfig({
  plugins: [
    dts({ tsconfigPath: './tsconfig.build.json', rollupTypes: true }),
    visualizer({ open: false, filename: 'coverage/stats.html', gzipSize: true, brotliSize: true }),
  ],
  resolve: {
    alias: { '@': resolve(import.meta.dirname, './ts') },
    extensions: ['.js', '.mjs', '.ts'],
  },
  build: {
    target: 'es2020',
    sourcemap: true,
    lib: {
      entry: {
        index: resolve(import.meta.dirname, 'ts/index.ts'),
        node: resolve(import.meta.dirname, 'ts/node.ts'),
        msgpack: resolve(import.meta.dirname, 'ts/msgpack.ts'),
      },
      formats: ['es', 'cjs'],
      fileName: (format, name) => (format === 'cjs' ? `${name}.cjs` : `${name}.js`),
    },
    rollupOptions: {
      // `ws` and `@msgpack/msgpack` are optional peer dependencies the consumer installs; bundling
      // either would duplicate it and break `instanceof` across the seam.
      //
      // `/^node:/` because a Vite library build resolves with browser conditions: a `node:` builtin
      // that is not externalised is rewritten to `__vite-browser-external`, a module whose body is
      // `module.exports = {}`. In the shipped artifact `await import('node:net')` would then yield an
      // object with no `connect`, and every `ws+unix:` dial would die as `TypeError: n is not a
      // function` - naming neither the url nor the transport, and not a `MuxwsError`, which
      // WSM-ERR-016 forbids. A predicate rather than the one specifier, so the next builtin imported
      // is covered too.
      external: [/^node:/, 'ws', '@msgpack/msgpack'],
    },
  },
  test: {
    coverage: { provider: 'v8', include: ['ts/**/*'], exclude: ['**/index.ts'] },
    globals: true,
    environment: 'jsdom',
  },
});
