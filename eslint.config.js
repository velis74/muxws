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
    // `interop/` is Node ESM and has its own tsconfig, because the root one compiles to CommonJS and
    // rejects `import.meta` (TS1343). The typed rules need to be told where its programme lives, or
    // they refuse to parse the file at all - which is how ~1700 lines of load-bearing WSM-CDC-007
    // machinery went unlinted until M7.
    files: ['interop/**/*.ts'],
    languageOptions: { parserOptions: { project: './interop/tsconfig.json' } },
  },
];
