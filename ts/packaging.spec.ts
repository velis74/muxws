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

describe("the shipped build's externals (WSM-ERR-016)", () => {
  /**
   * Build `ts/node.ts` the way `npm run build` does, and report what the artifact says about `node:net`.
   *
   * The externals list is read out of the real `vite.config.ts` rather than restated here, because a
   * restated copy is a second source of truth that cannot go red when the shipped one changes - which
   * is precisely the failure this test exists for. Everything else is `bundleFacts`' recipe: an
   * in-memory `es` lib build, so no plugin writes a file and the run costs no artifacts.
   *
   * A Vite library build resolves with **browser** conditions, so a `node:` builtin that is not
   * externalised is silently swapped for `__vite-browser-external`, a module whose body is
   * `module.exports = {}`. Measured on the shipped artifact before `/^node:/` was added: `dist/node.js`
   * contained no `node:net` at all and `await import('node:net')` yielded an object with no `connect`,
   * so a consumer's `ws+unix:` dial died as `TypeError: n is not a function`.
   */
  async function shippedNodeBundle(): Promise<{
    code: string;
    failure: string;
  }> {
    // Reached through a variable specifier for the same reason `VITE_SPECIFIER` is, and one more:
    // `vite.config.ts` sits outside `tsconfig.json`'s `include` and is written as ESM, so a literal
    // `import('../vite.config')` would drag it into `tsc --noEmit`'s programme and fail on
    // `import.meta` (TS1343) and on Vite's `exports`-only types (TS2307). The config is data here,
    // not a typed dependency; what matters is that this reads the file `npm run build` reads.
    const CONFIG_SPECIFIER = '../vite.config';
    const shipped = ((await import(CONFIG_SPECIFIER)) as { default: unknown }).default as {
      build?: { rollupOptions?: { external?: unknown } };
    };
    const external = shipped.build?.rollupOptions?.external;
    expect(external, 'the shipped config must declare an externals list for this test to read').toBeDefined();

    try {
      const { build } = (await import(VITE_SPECIFIER)) as ViteModule;
      const result = await build({
        root: ROOT,
        configFile: false,
        logLevel: 'silent',
        build: {
          write: false,
          minify: false,
          target: 'es2020',
          lib: {
            entry: resolve(ROOT, 'ts/node.ts'),
            formats: ['es'],
            fileName: () => 'externals-probe.js',
          },
          rollupOptions: { external },
        },
      });
      const output = Array.isArray(result) ? result[0] : result;
      const code = output.output
        .filter((piece) => piece.type === 'chunk')
        .map((chunk) => chunk.code)
        .join('\n');
      return { code, failure: '' };
    } catch (error) {
      return {
        code: '',
        failure: error instanceof Error ? error.message : String(error),
      };
    }
  }

  it('leaves node:net a real import, so the unix dial has a socket to open', { timeout: 60_000 }, async () => {
    const { code, failure } = await shippedNodeBundle();
    expect(failure, 'the node bundle must build at all').toBe('');
    // Non-vacuity: an empty chunk would satisfy the `not.toContain` below without having bundled
    // anything. `unixConnector` is the only reason this entry point touches a builtin at all.
    expect(code).toContain('unixConnector');
    expect(code, 'node:net must survive as a specifier the runtime resolves').toMatch(
      /import\s*\(\s*['"]node:net['"]\s*\)/,
    );
    expect(code, 'a browser stub here makes every ws+unix: dial a bare TypeError').not.toContain(
      '__vite-browser-external',
    );
  });
});

describe('where an error class is reachable from - WSM-ERR-016', () => {
  /**
   * The two shared bases, and the concrete class of every transport, by the entry point that owns it.
   *
   * Read from the **runtime** surface rather than from the source: a class can arrive at an entry
   * point through a re-export chain three modules long, and what a consumer can import is what the
   * module object has, not what one file happens to spell. This file's other tests read source
   * because `import type` is erased before runtime; here the opposite is true, and erasure is not a
   * risk because an error class is a value or it is nothing.
   */
  const ROOT_ONLY = ['TransportUrlError', 'TransportUnsupportedError'];
  const ROOT_TRANSPORT = ['UnixSocketsUnsupportedError'];
  const NODE_TRANSPORT = ['UnixUrlError', 'WsUrlError', 'WsNotInstalledError'];

  it('keeps the two bases at the root and each concrete class behind its own entry point', async () => {
    const root = Object.keys(await import('./index'));
    const node = Object.keys(await import('./node'));

    // The bases are at the root because an application must be able to write
    // `instanceof TransportUrlError` without importing the transport that threw - and in a browser
    // build it *cannot* import it, because `muxws/node` reaches for `ws` (WSM-API-022).
    ROOT_ONLY.forEach((name) => expect(root, `${name} must be exported from the package root`).toContain(name));
    // `UnixSocketsUnsupportedError` is the concrete class of the transport the root entry point ships
    // - the platform `WebSocket`, which cannot open a socket file - so the root is exactly where it
    // belongs, and `muxws/node`, whose unix dial works, must not carry it at all.
    ROOT_TRANSPORT.forEach((name) => {
      expect(root, `${name} belongs to the entry point that refuses the url`).toContain(name);
      expect(node, `${name} names a refusal muxws/node never makes`).not.toContain(name);
    });
    // And the three `muxws/node` owns are reached as `from 'muxws/node'` and from nowhere else. This
    // is the half of the rule a third-party adapter has to be able to follow: it cannot add a class to
    // `ts/errors.ts`, so a convention requiring a root export would be one only this repository could
    // keep (WSM-API-021).
    NODE_TRANSPORT.forEach((name) => {
      expect(node, `${name} must be exported from muxws/node`).toContain(name);
      expect(root, `${name} must not be reachable from the package root`).not.toContain(name);
    });
  });

  it('lets each entry point export only the concrete classes its own transports own', async () => {
    // The enumerative half, and the one the three lists above cannot be: a *new* concrete class added
    // to an entry point is invisible to a name list, which is how a rule quietly stops being enforced.
    // This asks the prototype chain instead - anything an entry point exports that extends either base
    // is a concrete transport error - and then checks the answer against the list of transports that
    // entry point actually ships. Python's twin recurses through `MuxwsError.__subclasses__()` for the
    // same reason (`errors_test.py::test_the_transport_bases_are_root_exported_and_their_subclasses_are_not`).
    const { TransportUrlError, TransportUnsupportedError } = await import('./errors');

    function concreteErrorsIn(namespace: Record<string, unknown>): string[] {
      return Object.entries(namespace)
        .filter(
          ([, value]) =>
            typeof value === 'function' &&
            value !== TransportUrlError &&
            value !== TransportUnsupportedError &&
            (Object.prototype.isPrototypeOf.call(TransportUrlError, value) ||
              Object.prototype.isPrototypeOf.call(TransportUnsupportedError, value)),
        )
        .map(([name]) => name)
        .sort();
    }

    expect(concreteErrorsIn(await import('./index'))).toEqual([...ROOT_TRANSPORT].sort());
    expect(concreteErrorsIn(await import('./node'))).toEqual([...NODE_TRANSPORT].sort());
    // Non-vacuity: a detector that saw nothing would satisfy both equalities if the lists were empty,
    // and they are not - but it would also satisfy them if `isPrototypeOf` were the wrong test, so the
    // bases themselves are checked to be excluded by identity rather than by never having matched.
    expect(concreteErrorsIn({ TransportUrlError, TransportUnsupportedError })).toEqual([]);
    expect(concreteErrorsIn({ probe: class extends TransportUrlError {} })).toEqual(['probe']);
  });
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
