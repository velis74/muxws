import velis from 'eslint-config-velis';

export default [
  ...velis,
  {
    rules: {
      'no-redeclare': 'off',
      '@typescript-eslint/no-redeclare': 'error',
      // An underscore prefix means "deliberately unused", which is already this repository's
      // convention on the Python side (ruff's ARG rules read it the same way). Without it the two
      // languages disagree about how to spell a parameter a signature forces you to accept and a
      // loop variable you only wanted for its iteration.
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' },
      ],
    },
    ignores: ['dist/*', 'coverage/*', 'node_modules/*', 'docs/*', 'vite.config.ts'],
  },
  {
    // The demo frontend, which had never been linted at all: `lint:ci` ran `eslint ts interop`, and
    // the demo was type-checked by `vue-tsc` at build time and by nothing else. Third instance of
    // this gap in one repository - a directory outside the lint globs is a directory whose style
    // rules are aspirational.
    files: ['demo/frontend/**/*.ts', 'demo/frontend/**/*.vue'],
    languageOptions: {
      parserOptions: { project: './demo/frontend/tsconfig.json' },
      // Browser code, so the browser's globals are defined. Without these `no-undef` fires on
      // `window`, `performance` and `requestAnimationFrame` - which would be a lint config declaring
      // that a browser has no browser in it.
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
      // Vue single-file components are PascalCase by convention - it is how the framework's own
      // documentation writes them, how `<BoardGrid />` reads in a template, and what the author's
      // other Vue projects do. `unicorn/filename-case` is right for `ts/` and wrong here, and the
      // repository already accepts this asymmetry between its Python and TypeScript halves.
      'unicorn/filename-case': 'off',
      // `Diagnostics` is a page section, not a component published for reuse; the rule exists to stop
      // a name colliding with a future HTML element, which this one cannot.
      'vue/multi-word-component-names': 'off',
    },
  },
  {
    // `interop/` is Node ESM and has its own tsconfig, because the root one compiles to CommonJS and
    // rejects `import.meta` (TS1343). The typed rules need to be told where its programme lives, or
    // they refuse to parse the file at all - which is how ~1700 lines of load-bearing WSM-CDC-007
    // machinery went unlinted until M7.
    files: ['interop/**/*.ts'],
    languageOptions: { parserOptions: { project: './interop/tsconfig.json' } },
  },
  {
    // The Node demo backend, for the same reason as the two blocks above and as the fourth instance
    // of the gap the frontend block names: the root tsconfig `include`s `ts/**/*` alone, so without
    // this entry the typed rules cannot parse these files and `eslint demo/backend_node` fails with a
    // parsing error rather than a style report. This block and `demo/backend_node` in `lint:ci`'s
    // eslint globs have to arrive together - either alone leaves the directory unlinted, one of them
    // silently and one of them loudly.
    files: ['demo/backend_node/**/*.ts'],
    languageOptions: {
      parserOptions: { project: './demo/backend_node/tsconfig.json' },
      // Server code, so Node's globals rather than the browser's. Without these `no-undef` fires on
      // `console` and `process`, which would be a lint config declaring that Node has no Node in it.
      globals: {
        console: 'readonly',
        process: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
      },
    },
  },
];
