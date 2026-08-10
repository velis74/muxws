/**
 * Types for `vue-cached-icon`, which its own package cannot serve.
 *
 * The package ships `dist/vue-cached-icon.vue.d.ts` and names it in `"types"`, but its `"exports"`
 * map lists only `require` and `import` conditions - and under `moduleResolution: "bundler"` the
 * exports map wins outright, so the top-level `"types"` key is never consulted and the import
 * resolves to `any` (TS7016).
 *
 * This is an upstream packaging bug, not a muxws one. The fix belongs in that package: add a
 * `"types"` condition **first** inside the `"."` export, which is where every resolver looks now.
 *
 *     "exports": { ".": { "types": "./dist/index.d.ts", "import": …, "require": … } }
 *
 * Until then this declaration keeps the demo type-checked rather than silently `any`. Delete it the
 * day the package ships the condition.
 */
declare module 'vue-cached-icon' {
  import type { DefineComponent } from 'vue';

  /** Renders a sanitised SVG fetched from the provider named by the prefix (`mdi-`, `ion-`, `fa-`). */
  export const CachedIcon: DefineComponent<{ name?: string }>;
}
