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
      // either would duplicate the package and break `instanceof` checks between the bundled copy and
      // the consumer's copy.
      //
      // `/^node:/`: a Vite library build resolves with browser conditions, and a `node:` builtin that
      // is not external is rewritten to `__vite-browser-external`, whose body is
      // `module.exports = {}`. `await import('node:net')` would then return an object without
      // `connect`, and every `ws+unix:` dial would throw `TypeError` instead of a `MuxwsError`
      // (WSM-ERR-016). The regular expression covers every `node:` builtin.
      external: [/^node:/, 'ws', '@msgpack/msgpack'],
    },
  },
  test: {
    coverage: { provider: 'v8', include: ['ts/**/*'], exclude: ['**/index.ts'] },
    globals: true,
    environment: 'jsdom',
  },
});
