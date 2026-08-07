# muxws M0 - Repository scaffolding

## 1. Goal

At the end of M0 the `muxws` repository exists and is CI-runnable end to end. One
repository holds a Python package (`muxws/`, published to PyPI as `muxws`) and a
TypeScript package (`ts/`, published to npm as `muxws`) on **one shared version
stream**, plus a VitePress docs workspace and an empty `conformance/` corpus tree. No protocol code is
written here: what exists is an empty-but-valid package on both sides where `ruff check`, `pytest`,
`npm run lint`, `npm test`, `npm run build` and `npm run docs:build` all pass, and where the
version-parity rule is enforced by a test rather than by discipline.

> **Note on numbering.** The specification's §17 implementation-order table names M1-M6. M0
> (scaffolding) and M7 (documentation) were added when these briefs were written, because the
> specification's M1 assumes a repository that already lints and builds, and its M6 bundled the wire
> freeze together with the documentation site. No rule moved; the work was only split out.

## 2. Prerequisites

None. This is the first milestone. Every later milestone assumes the layout, the lint configuration,
the test-file naming and the build scripts established here, and none of them may change them.

## 3. Files to create

```
muxws/
├── pyproject.toml
├── package.json
├── tsconfig.json
├── tsconfig.build.json
├── vite.config.ts
├── eslint.config.js
├── .gitignore
├── LICENSE                       # MIT, "Copyright (c) 2025 Jure Erznožnik"
├── README.md
├── .github/workflows/ci.yml
├── muxws/
│   ├── __init__.py               # __version__ only, for now
│   ├── py.typed                  # empty file
│   └── version_test.py
├── ts/
│   ├── index.ts                  # browser entry
│   ├── version.ts
│   ├── version.spec.ts
│   ├── node.ts                   # placeholder; M3 fills it
│   └── msgpack.ts                # placeholder; M6 fills it
├── conformance/{frames,sequences,invalid}/.gitkeep
└── docs/
    ├── package.json
    ├── index.md
    ├── guide/getting-started.md
    ├── api/index.md
    └── .vitepress/config.ts
```

## 4. Normative rules in force

- **WSM-PKG-001** Both packages MUST ship from one repository on one version stream, with identical
  version numbers in `pyproject.toml` and `package.json`.
- **WSM-PKG-002** The Python package MUST have **zero required runtime dependencies**. `starlette`
  and `websockets` are optional extras selected by which transport is imported; `msgpack` is an
  optional extra selected by which codec is registered.
- **WSM-PKG-003** The TypeScript browser entry point MUST have zero runtime dependencies. `ws` is an
  optional peer dependency for `node.ts`; `@msgpack/msgpack` an optional peer dependency reachable
  only through the `/msgpack` subpath.
- **WSM-PKG-004** File names in the TypeScript package MUST be kebab-case; TypeScript strings use
  single quotes, Python strings double quotes; Python line length 120.
- **WSM-PKG-005** The wire format is versioned by the generation integer in the subprotocol name
  (`muxws.v1.<codec>`) - a single monotonically increasing integer, bumped only for a breaking wire
  change - independently of the packages' semver. There is no second, finer version anywhere
  (WSM-CON-009).
- **WSM-CDC-015** The npm package MUST declare `"sideEffects": false` (at minimum for the codec
  subpaths).
- **WSM-CDC-014** A codec module MUST NOT register itself at import time (a side-effecting import can
  never be tree-shaken out).
- **WSM-API-022** The Node acceptor MUST live behind the `muxws/node` subpath export so
  the browser entry point never pulls in `ws`. The peer implementation MUST be shared; only the socket
  adapter differs.
- **WSM-INV-001** muxws MUST NOT depend on any package above it in the stack - not backchannel, not
  fastapi-viewsets, not the frontend kit; not as an import, an optional extra, or a `TYPE_CHECKING`
  annotation - or the claim that anyone wanting multiplexed streams can install it stops being true.

## 5. File contents

### `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "muxws"
dynamic = ["version"]
description = "Multiplexed, cancellable, bidirectional streams over one WebSocket - protocol and reference implementation."
readme = "README.md"
license = { text = "MIT" }
authors = [
    { name = "Jure Erznožnik", email = "jure.erznoznik@gmail.com" },
]
requires-python = ">=3.10"
keywords = ["websocket", "multiplexing", "streams", "dynamicforms", "asyncio"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Internet :: WWW/HTTP",
    "Typing :: Typed",
]
dependencies = []

[project.optional-dependencies]
starlette = ["starlette>=0.37"]
websockets = ["websockets>=12"]
msgpack = ["msgpack>=1.0"]
dev = [
    "coverage",
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "starlette",
    "websockets",
    "msgpack",
    "httpx",
]

[project.urls]
Homepage = "https://github.com/dynamicforms/muxws"
Repository = "https://github.com/dynamicforms/muxws"
Issues = "https://github.com/dynamicforms/muxws/issues"
Documentation = "https://docs.velis.si/dynamicforms/muxws/"

[tool.hatch.version]
path = "muxws/__init__.py"

[tool.hatch.build.targets.wheel]
packages = ["muxws"]

[tool.hatch.build.targets.sdist]
include = [
    "/muxws",
    "/conformance",
    "/README.md",
    "/LICENSE",
]
exclude = [
    "**/*_test.py",
]

[tool.pytest.ini_options]
testpaths = ["muxws"]
python_files = ["*_test.py"]
asyncio_mode = "auto"

[tool.ruff]
exclude = ['ts', 'docs', 'dist', 'build', 'node_modules', '.git', '.idea', '.vscode',
    '.pytest_cache', '.ruff_cache']
line-length = 120

[tool.ruff.lint]
select = [
    "E",    # pycodestyle errors
    "W",    # pycodestyle warnings
    "F",    # Pyflakes
    "I",    # isort
    "Q",    # flake8-quotes
    "B",    # flake8-bugbear
    "UP",   # pyupgrade
    "N",    # pep8-naming
    "PT",   # flake8-pytest-style
    "S",    # flake8-bandit
    "C4",   # flake8-comprehensions
    "ARG",  # flake8-unused-arguments
]
ignore = ["E731", "E722", "C408"]

fixable = ["ALL"]
unfixable = []

dummy-variable-rgx = "^(_+|(_+[a-zA-Z0-9_]*[a-zA-Z0-9]+?))$"

[tool.ruff.lint.per-file-ignores]
"*_test.py" = ["S101"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
skip-magic-trailing-comma = false
line-ending = "auto"

[tool.ruff.lint.isort]
case-sensitive = false
lines-between-types = 1
combine-as-imports = true
order-by-type = false

[tool.coverage.run]
omit = ["*_test.py", "demo/*", "demo.py"]
```

### `muxws/__init__.py` and `muxws/py.typed`

```python
__version__ = "0.1.0"
```

`py.typed` is a zero-byte file. `hatchling` ships it with the package directory automatically; the
wheel test below proves it did.

### `muxws/version_test.py`

```python
import json

from importlib.metadata import requires
from pathlib import Path

import muxws


def test_python_and_npm_versions_match():
    """WSM-PKG-001: one version stream, two package manifests."""
    package_json = json.loads((Path(__file__).parent.parent / "package.json").read_text())
    assert package_json["version"] == muxws.__version__


def test_package_has_no_required_runtime_dependencies():
    """WSM-PKG-002: every declared dependency must sit behind an extra."""
    for requirement in requires("muxws") or []:
        assert "extra ==" in requirement, requirement
```

Note the blank line between `import json` and the `from ...` block: ruff isort runs with
`lines-between-types = 1` and will rewrite the file if it is missing.

### `package.json`

```json
{
  "name": "muxws",
  "private": false,
  "version": "0.1.0",
  "type": "module",
  "sideEffects": false,
  "description": "Multiplexed, cancellable, bidirectional streams over one WebSocket",
  "author": "Jure Erznožnik",
  "files": ["dist/*"],
  "main": "dist/index.cjs",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": { "require": "./dist/index.cjs", "import": "./dist/index.js" },
    "./node": { "require": "./dist/node.cjs", "import": "./dist/node.js" },
    "./msgpack": { "require": "./dist/msgpack.cjs", "import": "./dist/msgpack.js" }
  },
  "workspaces": ["docs"],
  "scripts": {
    "build": "vite build",
    "test": "vitest run --coverage",
    "lint": "eslint ts --fix && tsc --noEmit",
    "docs:dev": "npm run docs:dev -w docs",
    "docs:build": "npm run docs:build -w docs",
    "docs:preview": "npm run docs:preview -w docs"
  },
  "keywords": ["websocket", "multiplexing", "streams", "dynamicforms", "velis"],
  "license": "MIT",
  "repository": { "type": "git", "url": "git@github.com:dynamicforms/muxws.git" },
  "issues": "https://github.com/dynamicforms/muxws/issues",
  "peerDependencies": { "ws": "^8", "@msgpack/msgpack": "^3" },
  "peerDependenciesMeta": {
    "ws": { "optional": true },
    "@msgpack/msgpack": { "optional": true }
  },
  "devDependencies": {
    "@types/node": "^24",
    "@types/ws": "^8",
    "@vitest/coverage-v8": "^3",
    "eslint-config-velis": "^2.0.12",
    "jsdom": "^26.0.0",
    "rollup-plugin-visualizer": "^5.14.0",
    "typescript": "^5",
    "vite": "^8",
    "vite-plugin-dts": "^5",
    "vitest": "^3",
    "ws": "^8"
  }
}
```

`lint` runs `tsc --noEmit`, not `vue-tsc`: there is no Vue in this package.

### `vite.config.ts`

```ts
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
    alias: { '@': resolve(__dirname, './ts') },
    extensions: ['.js', '.mjs', '.ts'],
  },
  build: {
    target: 'es2020',
    sourcemap: true,
    lib: {
      entry: {
        index: resolve(__dirname, 'ts/index.ts'),
        node: resolve(__dirname, 'ts/node.ts'),
        msgpack: resolve(__dirname, 'ts/msgpack.ts'),
      },
      formats: ['es', 'cjs'],
      fileName: (format, name) => (format === 'cjs' ? `${name}.cjs` : `${name}.js`),
    },
    rollupOptions: { external: ['ws', '@msgpack/msgpack'] },
  },
  test: {
    coverage: { provider: 'v8', include: ['ts/**/*'], exclude: ['**/index.ts'] },
    globals: true,
    environment: 'jsdom',
  },
});
```

`formats` is `['es','cjs']` rather than the `['umd','es']` used elsewhere in this organisation because
vite forbids `umd` with a multi-entry `lib.entry`, and muxws needs three entries to satisfy
WSM-API-022 and WSM-CDC-015. Do **not** "fix" this by collapsing to one entry - that would drag `ws`
into the browser bundle.

### `tsconfig.json` and `tsconfig.build.json`

```json
{
  "extends": "./node_modules/eslint-config-velis/tsconfig.json",
  "compilerOptions": {
    "resolveJsonModule": true,
    "paths": { "@/*": ["./ts/*"] },
    "types": ["vitest/globals", "node"],
    "skipLibCheck": true
  },
  "include": ["ts/**/*"],
  "exclude": ["dist", "coverage", "node_modules", "docs", "muxws"]
}
```

```json
{
  "extends": "./tsconfig.json",
  "exclude": ["coverage", "dist", "node_modules", "docs", "./**/*.spec.*"],
  "compilerOptions": {
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "./dist",
    "skipLibCheck": true
  }
}
```

### `eslint.config.js`

```js
import velis from 'eslint-config-velis';

export default [
  ...velis,
  {
    rules: {
      'no-redeclare': 'off',
      '@typescript-eslint/no-redeclare': 'error',
    },
    ignores: ['dist/*', 'coverage/*', 'node_modules/*', 'docs/*', 'vite.config.ts'],
  },
];
```

### TypeScript stubs

```ts
// ts/version.ts
export const VERSION = '0.1.0';
```

```ts
// ts/index.ts - browser entry. M1..M3 add the real exports here.
export { VERSION } from './version';
```

```ts
// ts/node.ts - subpath export muxws/node. M3 adds accept()/serve() over `ws`.
export { VERSION } from './version';
```

```ts
// ts/msgpack.ts - subpath export muxws/msgpack. M6 adds MsgPackCodec.
// MUST NOT call registerCodec at import time (WSM-CDC-014).
export { VERSION } from './version';
```

```ts
// ts/version.spec.ts
import packageJson from '../package.json';

import { VERSION } from './version';

describe('packaging', () => {
  it('keeps VERSION and package.json in step - WSM-PKG-001', () => {
    expect(VERSION).toBe(packageJson.version);
  });

  it('declares sideEffects false - WSM-CDC-015', () => {
    expect(packageJson.sideEffects).toBe(false);
  });

  it('exposes node and msgpack as subpaths - WSM-API-022', () => {
    expect(Object.keys(packageJson.exports)).toEqual(['.', './node', './msgpack']);
  });
});
```

### `docs/package.json` and `docs/.vitepress/config.ts`

```json
{
  "name": "muxws-docs",
  "version": "1.0.0",
  "description": "Documentation for muxws",
  "type": "module",
  "private": true,
  "scripts": {
    "docs:dev": "vitepress dev",
    "docs:build": "vitepress build",
    "docs:preview": "vitepress preview"
  },
  "devDependencies": { "vitepress": "^1.6.3" }
}
```

```ts
import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'muxws',
  description: 'Multiplexed, cancellable streams over one WebSocket',
  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API Reference', link: '/api/' },
    ],
    sidebar: {
      '/guide/': [{ text: 'Guide', items: [{ text: 'Getting Started', link: '/guide/getting-started' }] }],
      '/api/': [{ text: 'API Reference', items: [{ text: 'Overview', link: '/api/' }] }],
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/dynamicforms/muxws' }],
    footer: { message: 'Released under the MIT License.', copyright: 'Copyright © 2025 Jure Erznožnik' },
  },
  ignoreDeadLinks: [/^http:\/\/localhost/],
});
```

`docs/index.md` is a VitePress `layout: home` page; `docs/guide/getting-started.md` and
`docs/api/index.md` are one-heading stubs so the site builds and later milestones have somewhere to
write.

### `.gitignore`

```
.idea
node_modules
coverage
dist

/docs/.vitepress/cache
/docs/.vitepress/dist
/package-lock.json

__pycache__/
*.py[cod]
*.egg-info/
.eggs/
build/
.pytest_cache/
.ruff_cache/
.coverage
```

### `.github/workflows/ci.yml`

Two jobs, both on `push` and `pull_request`:

- `python`: matrix over 3.10-3.13; `pip install -e .[dev]`; then `ruff check .`,
  `ruff format --check .`, `pytest --cov=muxws`.
- `node`: node 22; `npm ci`; then `npm run lint`, `npm test`, `npm run build`, `npm run docs:build`.

## 6. Implementation notes

1. Tests are **colocated** with the source as `<module>_test.py` (Python) and `<module>.spec.ts`
   (TypeScript). There is no `tests/` directory in this repository, ever. `S101` bans `assert`
   outside `*_test.py`, so a misnamed test file fails lint on its first assertion.
2. `asyncio_mode = "auto"` is set now, in M0, so that from M2 onward an `async def test_...` needs no
   decorator. Adding it later means editing every test written before it.
3. Prettier settings arrive with `eslint-config-velis`: `printWidth: 120`, `tabWidth: 2`,
   **`singleQuote: true`**, `trailingComma: 'all'`, `semi: true`, `arrowParens: 'always'`. Single
   quotes in TypeScript, double quotes in Python; both are enforced, and getting them backwards is the
   most common mistake in this repository pair.
4. `unicorn/filename-case: kebabCase` - every TS file is `stream-state.ts`, never `streamState.ts`.
5. `import/order` requires `newlines-between: 'always'` and case-insensitive ascending order within a
   group; that is why `version.spec.ts` has a blank line between the `../package.json` import and the
   `./version` import. `import/extensions` is `never` for `.js`/`.ts`.
6. `no-restricted-syntax` forbids `for...in` (and labels, and `with`). Use `Object.keys/values/entries`
   throughout this package - including in the version spec above.
7. `resolveJsonModule` is on so a spec file may import `package.json`. Keep `include` at `ts/**/*` so
   the JSON never lands in the emitted declarations.
8. Do not add `starlette`, `websockets`, `msgpack` or `ws` to required dependencies at any point. A
   later milestone importing one of them imports it inside the module that needs it, behind its extra.

## 7. Tests to write

| # | Test | Asserts |
|---|---|---|
| 1 | `muxws/version_test.py::test_python_and_npm_versions_match` | `muxws.__version__` equals `package.json`'s `version` (WSM-PKG-001). |
| 2 | `muxws/version_test.py::test_package_has_no_required_runtime_dependencies` | Every entry of `importlib.metadata.requires("muxws")` carries an `extra ==` marker (WSM-PKG-002). |
| 3 | `ts/version.spec.ts` - "keeps VERSION and package.json in step" | `VERSION === packageJson.version` (WSM-PKG-001). |
| 4 | `ts/version.spec.ts` - "declares sideEffects false" | `packageJson.sideEffects === false` (WSM-CDC-015). |
| 5 | `ts/version.spec.ts` - "exposes node and msgpack as subpaths" | `exports` has exactly `.`, `./node`, `./msgpack`, each with `require` and `import` (WSM-API-022, WSM-PKG-003). |

## 8. Done when

- [ ] `ruff check .` reports zero findings and `ruff format --check .` passes.
- [ ] `pytest` passes and collects the two Python tests above.
- [ ] `npm ci` succeeds with `dependencies` absent or empty, and `ws` / `@msgpack/msgpack` only under
      `peerDependencies` + `peerDependenciesMeta.optional`.
- [ ] `npm run lint` passes (`eslint ts --fix` clean, `tsc --noEmit` clean).
- [ ] `npm test` passes and writes v8 coverage.
- [ ] `npm run build` produces `dist/index.js`, `dist/index.cjs`, `dist/node.js`, `dist/node.cjs`,
      `dist/msgpack.js`, `dist/msgpack.cjs`, a rolled-up `dist/index.d.ts`, and `coverage/stats.html`.
- [ ] `npm run docs:build` produces a VitePress site.
- [ ] `python -m build` produces a wheel that contains `muxws/py.typed` and contains **no** `*_test.py`.
- [ ] CI runs every command above on push and on pull request.

## 9. Out of scope

Every line of protocol code: no `Frame`, no `Codec`, no `Peer`, no `Stream`, no `errors.py`, no
`conf.py`. The three `conformance/` directories are created empty; M1 fills `frames/` and `invalid/`,
M6 fills `sequences/`. `SPEC.md` is not written here (M6 promotes the normative spec into it). Docs
content beyond the three stub pages belongs to later milestones.
