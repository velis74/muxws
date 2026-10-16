import velis from 'eslint-config-velis';

export default [
  ...velis,
  {
    rules: {
      'no-redeclare': 'off',
      '@typescript-eslint/no-redeclare': 'error',
      // An underscore prefix marks an intentionally unused parameter, variable or caught error. The
      // Python side uses the same convention (ruff's ARG rules).
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' },
      ],
    },
    ignores: ['dist/*', 'coverage/*', 'node_modules/*', 'docs/*', 'vite.config.ts'],
  },
  {
    // The demo frontend. It has its own tsconfig; `lint:ci` includes `demo/frontend` in its eslint
    // globs.
    files: ['demo/frontend/**/*.ts', 'demo/frontend/**/*.vue'],
    languageOptions: {
      parserOptions: { project: './demo/frontend/tsconfig.json' },
      // Browser globals. Without them `no-undef` reports `window`, `performance` and
      // `requestAnimationFrame`.
      globals: {
        window: 'readonly',
        document: 'readonly',
        performance: 'readonly',
        requestAnimationFrame: 'readonly',
        sessionStorage: 'readonly',
        WebSocket: 'readonly',
        setTimeout: 'readonly',
        setInterval: 'readonly',
        clearInterval: 'readonly',
        clearTimeout: 'readonly',
        console: 'readonly',
      },
    },
    rules: {
      // Vue single-file components use PascalCase file names (`BoardGrid.vue`), the Vue convention.
      // `unicorn/filename-case` applies to `ts/` only.
      'unicorn/filename-case': 'off',
      // `Diagnostics` is a single-word name for a page section that is not published for reuse; the
      // rule guards against collisions with HTML element names, which do not apply to it.
      'vue/multi-word-component-names': 'off',
    },
  },
  {
    // `interop/` is Node ESM and has its own tsconfig, because the root one compiles to CommonJS and
    // rejects `import.meta` (TS1343). The typed rules need this tsconfig to parse the files.
    files: ['interop/**/*.ts'],
    languageOptions: { parserOptions: { project: './interop/tsconfig.json' } },
  },
  {
    // The Node demo backend. The root tsconfig `include`s only `ts/**/*`, so without this entry the
    // typed rules cannot parse these files and `eslint demo/backend_node` fails with a parsing error.
    // `lint:ci` includes `demo/backend_node` in its eslint globs.
    files: ['demo/backend_node/**/*.ts'],
    languageOptions: {
      parserOptions: { project: './demo/backend_node/tsconfig.json' },
      // Node globals. Without them `no-undef` reports `console` and `process`.
      globals: {
        console: 'readonly',
        process: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
      },
    },
  },
];
