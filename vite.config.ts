/// <reference types="vitest" />
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

import { visualizer } from 'rollup-plugin-visualizer';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';

const OUT_DIR = resolve(import.meta.dirname, 'dist');

/** A relative module specifier in a declaration: `from './x'`, `export * from '../x'`, `import('./x')`. */
const RELATIVE_SPECIFIER = /(\bfrom\s*|\bimport\s*\()(['"])(\.{1,2}\/[^'"]*)\2/gu;

function declarationFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return declarationFiles(path);
    return entry.name.endsWith('.d.ts') ? [path] : [];
  });
}

/** `specifier` as a file path without extension: `./transports` names `./transports/index`. */
function modulePath(fromDir: string, specifier: string): string {
  if (/\.c?js$/u.test(specifier)) return specifier.replace(/\.c?js$/u, '');
  if (existsSync(join(fromDir, `${specifier}.d.ts`))) return specifier;
  return `${specifier}/index`;
}

/**
 * Make the emitted declarations resolve under `moduleResolution: node16` / `nodenext`.
 *
 * TypeScript emits relative specifiers as written in the source, without an extension. Under
 * `node16` an ES module's relative import must name its file, so every specifier gets `.js`. The
 * package is `type: module`, so a `.d.ts` file describes an ES module; the `require` condition of
 * `exports` points at a `.d.cts` copy whose specifiers end in `.cjs`, which TypeScript maps to the
 * neighbouring `.d.cts`. The declarations stay one file per module, so a class reached through two
 * entry points (`SameOrigin` from `muxws` and in `AcceptOptions` of `muxws/node`) is one type.
 */
function finishDeclarations(): void {
  for (const file of declarationFiles(OUT_DIR)) {
    const source = readFileSync(file, 'utf8');
    const withExtension = (extension: string): string =>
      source.replace(
        RELATIVE_SPECIFIER,
        (_match, lead: string, quote: string, specifier: string) =>
          `${lead}${quote}${modulePath(dirname(file), specifier)}${extension}${quote}`,
      );
    writeFileSync(file, withExtension('.js'));
    writeFileSync(file.replace(/\.d\.ts$/u, '.d.cts'), withExtension('.cjs').replace(/^\/\/# sourceMappingURL=.*$/mu, ''));
  }
}

export default defineConfig({
  plugins: [
    dts({ tsconfigPath: './tsconfig.build.json', afterBuild: finishDeclarations }),
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
