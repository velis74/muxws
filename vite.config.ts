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
      // `/^node:/` is here for a failure that had no test and no symptom until the bundle was run:
      // a Vite library build resolves with browser conditions, so a `node:` builtin that is not
      // externalised is rewritten to `__vite-browser-external`, a module whose body is
      // `module.exports = {}`. `ts/node.ts`'s unix dial is this package's first runtime import of
      // one, and in the shipped artifact `await import('node:net')` therefore yielded an object with
      // no `connect`, so every `ws+unix:` dial died as `TypeError: n is not a function` - naming
      // neither the url nor the transport, and not a `MuxwsError`, which WSM-ERR-016 forbids. The
      // predicate rather than the single specifier because the next builtin to be imported must not
      // have to rediscover this.
      external: [/^node:/, 'ws', '@msgpack/msgpack'],
    },
  },
  test: {
    coverage: { provider: 'v8', include: ['ts/**/*'], exclude: ['**/index.ts'] },
    globals: true,
    environment: 'jsdom',
  },
});
