// @vitest-environment node
/**
 * Packaging invariants: one version stream across both manifests, and a browser entry point that
 * pulls in nothing optional (WSM-PKG-001, WSM-PKG-003, WSM-PKG-005).
 *
 * The browser test **builds**. Grepping `ts/index.ts` for `from 'ws'` would prove nothing about what
 * ships: `ws` is reached through `ts/node.ts` -> `ts/transports/ws-socket.ts` and `@msgpack/msgpack`
 * through `ts/msgpack.ts`, each of them re-exports away from anything a reader would grep for, and a
 * bundler follows the whole graph where a grep sees one file. It also sees what a grep cannot: a
 * `import type { WebSocket } from 'ws'` is erased and costs a browser nothing, so a source scan that
 * counted it would fail on code that is correct.
 *
 * Node's environment is required rather than jsdom's: this file runs a real Vite build.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

import { VERSION } from './version';

/** Vitest resolves its root from `vite.config.ts`, so this is the repository root. */
const ROOT = process.cwd();

/** One emitted chunk, narrowed to the four things this file asks a build about. */
interface Chunk {
  type: string;
  code: string;
  modules: Record<string, unknown>;
  imports: string[];
  dynamicImports: string[];
}

/** The one member of Vite's API this file uses. */
interface ViteModule {
  build(config: Record<string, unknown>): Promise<{ output: Chunk[] } | { output: Chunk[] }[]>;
}

/**
 * Vite is reached through a variable specifier rather than `import { build } from 'vite'`.
 *
 * The project's `tsconfig` resolves modules the pre-`exports` way (`module: commonjs`, so
 * `moduleResolution: node`), and Vite publishes its Node types only through an `exports` map: a
 * static import is TS2307 against a package that is installed and works. The tsconfig belongs to the
 * scaffolding rather than to this change, and `@ts-expect-error` would become a second, opposite
 * error the day somebody widens it.
 */
const VITE_SPECIFIER = 'vite';

/** The two packages WSM-PKG-003 names, and the only two `package.json` declares optional. */
const OPTIONAL = ['@msgpack/msgpack', 'ws'];

// --------------------------------------------------------------------------- one version stream

/**
 * The version hatch builds a wheel from.
 *
 * `pyproject.toml` declares the version `dynamic` and points hatch at a Python source, so following
 * that pointer is what reading the Python manifest means. The pointer is asserted, not assumed:
 * a manifest that stopped being dynamic would otherwise leave this comparing `package.json` against
 * a file no wheel is built from.
 */
function pythonVersion(): string {
  const pyproject = readFileSync(resolve(ROOT, 'pyproject.toml'), 'utf8');
  expect(pyproject, 'the version must not be spelled out a second time').not.toMatch(/^version\s*=/m);
  expect(pyproject).toMatch(/^dynamic\s*=\s*\["version"\]/m);
  const pointer = /^path\s*=\s*"([^"]+)"/m.exec(pyproject);
  expect(pointer, '[tool.hatch.version] must say where the version lives').not.toBeNull();

  const source = readFileSync(resolve(ROOT, (pointer as RegExpExecArray)[1]), 'utf8');
  const found = source.match(/^__version__ = "([^"]+)"/m);
  expect(found, `no __version__ in ${(pointer as RegExpExecArray)[1]}`).not.toBeNull();
  return (found as RegExpMatchArray)[1];
}

describe('one version stream (WSM-PKG-001)', () => {
  it('matches the Python and TypeScript package versions', () => {
    const npmVersion = (
      JSON.parse(readFileSync(resolve(ROOT, 'package.json'), 'utf8')) as {
        version: string;
      }
    ).version;
    // Two empty strings, or two `undefined`s, compare equal and prove nothing. A version stream that
    // is not a version is not one manifest agreeing with another.
    expect(npmVersion).toMatch(/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/);
    expect(pythonVersion()).toBe(npmVersion);
  });

  it('carries no second, finer version anywhere - WSM-PKG-005', () => {
    // `ts/version.ts` is the npm version reflected into the bundle for `peer.hello`, not a protocol
    // version: the `muxws.v1.` subprotocol prefix is the only version on the wire (WSM-CON-009).
    const npmVersion = (
      JSON.parse(readFileSync(resolve(ROOT, 'package.json'), 'utf8')) as {
        version: string;
      }
    ).version;
    expect(VERSION).toBe(npmVersion);
  });

  it('declares no required runtime dependencies for the npm package - WSM-PKG-003', () => {
    const manifest = JSON.parse(readFileSync(resolve(ROOT, 'package.json'), 'utf8')) as {
      dependencies?: Record<string, string>;
      peerDependenciesMeta?: Record<string, { optional?: boolean }>;
    };
    expect(Object.keys(manifest.dependencies ?? {})).toEqual([]);
    // A peer dependency that is not marked optional is a required install for every consumer, which
    // is the same failure spelled differently.
    OPTIONAL.forEach((name) => {
      expect(manifest.peerDependenciesMeta?.[name]?.optional, `${name} must be an optional peer`).toBe(true);
    });
  });
});

// --------------------------------------------------------------------------- what the bundle contains

interface BundleFacts {
  /** Which of `OPTIONAL` the bundle reached for, by any of the four routes below. */
  offenders: string[];
  bytes: number;
  failure: string;
}

function specifierPattern(name: string): RegExp {
  return new RegExp(`(?:from|import)\\s*\\(?\\s*['"]${name.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&')}['"]`);
}

/**
 * Bundle `entry` with nothing externalised and report which optional packages came along.
 *
 * `external: []` is the point: the default for a library build leaves a bare `import 'ws'` in the
 * output, where it is a runtime dependency the consumer must install and this test would have to
 * infer. Bundling everything turns the question into one a build can answer directly - either the
 * package's files are in the module graph or they are not.
 *
 * Four routes, because a package can arrive by any of them: inlined from `node_modules` (installed),
 * left as an external import (resolvable but excluded), spelled in the emitted code, or named in the
 * resolution error a build throws when the optional package is *not* installed. That last one is not
 * hypothetical - `@msgpack/msgpack` is optional precisely so a checkout may be missing it, and a
 * test that only read a successful build would go quiet on exactly that machine.
 *
 * `alsoImport` injects real imports into the entry module, which is how the two control tests below
 * mutate this assertion without editing `ts/index.ts`. The imports are bound and re-exported, or
 * `sideEffects: false` in `package.json` tree-shakes them away before they reach a resolver and the
 * control silently passes.
 */
async function bundleFacts(entry: string, alsoImport: string[] = []): Promise<BundleFacts> {
  const absolute = resolve(ROOT, entry);
  const inject = {
    name: 'muxws-packaging-probe',
    transform(code: string, id: string): string | null {
      if (id !== absolute || alsoImport.length === 0) return null;
      const imports = alsoImport.map((name, index) => `import * as probe${index} from ${JSON.stringify(name)};`);
      const bindings = alsoImport.map((_name, index) => `probe${index}`);
      return `${imports.join('\n')}\n${code}\nexport const __probe = [${bindings.join(', ')}];`;
    },
  };

  let moduleIds: string[] = [];
  let externalImports: string[] = [];
  let code = '';
  let failure = '';
  try {
    const { build } = (await import(VITE_SPECIFIER)) as ViteModule;
    const result = await build({
      root: ROOT,
      configFile: false,
      logLevel: 'silent',
      plugins: [inject],
      build: {
        write: false,
        minify: false,
        target: 'es2020',
        lib: {
          entry: absolute,
          formats: ['es'],
          fileName: () => 'packaging-probe.js',
        },
        rollupOptions: { external: [] },
      },
    });
    const output = Array.isArray(result) ? result[0] : result;
    const chunks = output.output.filter((piece) => piece.type === 'chunk');
    moduleIds = chunks.flatMap((chunk) => Object.keys(chunk.modules));
    externalImports = chunks.flatMap((chunk) => [...chunk.imports, ...chunk.dynamicImports]);
    code = chunks.map((chunk) => chunk.code).join('\n');
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }

  const offenders = OPTIONAL.filter(
    (name) =>
      moduleIds.some((id) => id.includes(`node_modules/${name}/`)) ||
      externalImports.includes(name) ||
      specifierPattern(name).test(code) ||
      failure.includes(`"${name}"`),
  );
  return { offenders, bytes: code.length, failure };
}

describe('the browser entry point (WSM-PKG-003)', () => {
  it('imports nothing optional', { timeout: 60_000 }, async () => {
    const facts = await bundleFacts('ts/index.ts');
    expect(facts.failure, 'the browser bundle must build at all').toBe('');
    // Non-vacuity: a build that emitted an empty chunk would satisfy every assertion below without
    // having bundled the library. `Peer` is the entry point's largest export and cannot be shaken out.
    expect(facts.bytes).toBeGreaterThan(10_000);
    expect(facts.offenders).toEqual([]);
  });

  it('would see `ws` if the entry point imported it - the control', { timeout: 60_000 }, async () => {
    const facts = await bundleFacts('ts/index.ts', ['ws']);
    expect(facts.offenders).toEqual(['ws']);
  });

  it('would see `@msgpack/msgpack` if the entry point imported it - the control', { timeout: 60_000 }, async () => {
    const facts = await bundleFacts('ts/index.ts', ['@msgpack/msgpack']);
    expect(facts.offenders).toEqual(['@msgpack/msgpack']);
  });

  it(
    'sees `ws` arrive through ts/node.ts, which is where it is allowed to - WSM-API-022',
    { timeout: 60_000 },
    async () => {
      // The unsynthetic half of the control: no injection, a real entry point of this package, and
      // the same detector. `ws` is optional *because* only this subpath reaches it.
      const facts = await bundleFacts('ts/node.ts');
      expect(facts.failure).toBe('');
      expect(facts.offenders).toEqual(['ws']);
    },
  );
});

describe('the library imports nothing above it in the stack - WSM-INV-001', () => {
  // Read from the SOURCE, deliberately, and this is the whole point of the test. The bundle
  // assertions above prove what a build *emits*, and `import type` is erased before anything is
  // emitted - exactly as Python's `if TYPE_CHECKING:` is invisible to a test that watches a
  // subprocess import. A type-only dependency on a web framework is still a dependency: it lands in
  // the published .d.ts, it makes the package uninstallable without that framework's types, and no
  // runtime witness can see it. The Python twin walks the AST for the same reason
  // (`packaging_test.py::test_no_library_module_imports_anything_above_it_in_the_stack`).
  const ALLOWED_BARE_IMPORTS = new Set(['ws', '@msgpack/msgpack']);

  /** Source with block and line comments removed, so prose cannot be mistaken for code. */
  function withoutComments(source: string): string {
    return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
  }

  /**
   * Every module specifier a file imports, type-only imports included.
   *
   * Comments are stripped first. The first draft of this walker did not strip them and reported
   * three offenders that were sentences - a doc-comment containing the words "from 'gone'" reads
   * exactly like an import to a regex that spans lines. The control below now carries that case.
   */
  function importsOf(source: string): string[] {
    const code = withoutComments(source);
    const found: string[] = [];
    const fromClause = /(?:^|\n)\s*(?:import|export)\b[^;'"]*?\bfrom\s+['"]([^'"]+)['"]/g;
    let match = fromClause.exec(code);
    while (match !== null) {
      found.push(match[1]);
      match = fromClause.exec(code);
    }
    const sideEffect = /(?:^|\n)\s*import\s+['"]([^'"]+)['"]/g;
    let side = sideEffect.exec(code);
    while (side !== null) {
      found.push(side[1]);
      side = sideEffect.exec(code);
    }
    return found;
  }

  it('reaches for nothing but the standard library, itself and its declared optional peers', () => {
    const offenders: string[] = [];
    for (const file of readdirSync(join(ROOT, 'ts'), { recursive: true, encoding: 'utf8' })) {
      if (!file.endsWith('.ts') || file.endsWith('.spec.ts')) continue;
      const source = readFileSync(join(ROOT, 'ts', file), 'utf8');
      for (const specifier of importsOf(source)) {
        const isRelative = specifier.startsWith('.');
        const isNode = specifier.startsWith('node:');
        if (isRelative || isNode || ALLOWED_BARE_IMPORTS.has(specifier)) continue;
        offenders.push(`${file} imports ${specifier}`);
      }
    }
    expect(offenders, 'a library module reached above itself in the stack (WSM-INV-001)').toEqual([]);
  });

  it('confines each optional peer to the entry point its extra is named for', () => {
    const misplaced: string[] = [];
    for (const file of readdirSync(join(ROOT, 'ts'), { recursive: true, encoding: 'utf8' })) {
      if (!file.endsWith('.ts') || file.endsWith('.spec.ts')) continue;
      const source = readFileSync(join(ROOT, 'ts', file), 'utf8');
      for (const specifier of importsOf(source)) {
        if (specifier === 'ws' && !['node.ts', 'transports/ws-socket.ts'].includes(file)) {
          misplaced.push(`${file} imports ws`);
        }
        if (specifier === '@msgpack/msgpack' && file !== 'msgpack.ts') {
          misplaced.push(`${file} imports @msgpack/msgpack`);
        }
      }
    }
    expect(misplaced, 'an optional peer leaked out of the subpath its extra exists for').toEqual([]);
  });

  it('sees a type-only import, which is what a runtime witness cannot', () => {
    // The control. Without it the two assertions above pass equally well against a walker that reads
    // nothing at all, and a test that cannot fail is the failure this project has paid for four times.
    const source = [
      "/** A doc comment that says the stream is gone, from 'nowhere', and mentions import too. */",
      "import type { WebSocket } from 'ws';",
      "// import { NotReal } from 'not-real';",
      "import { Peer } from './peer';",
      "import './side-effect';",
      '',
    ].join('\n');
    // Both halves matter: the type-only import must be SEEN, and neither the doc comment nor the
    // commented-out import may be. The first draft of this walker failed the second half and
    // reported three sentences as dependencies.
    expect(importsOf(source)).toEqual(['ws', './peer', './side-effect']);
  });
});
