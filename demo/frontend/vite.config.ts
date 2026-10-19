import { fileURLToPath } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      // npm links only workspace members into `node_modules`, and `muxws` is the workspace root, so
      // the bare specifier `muxws` does not resolve inside this repository. Outside it the alias is
      // removed and the imports in `src/` stay unchanged.
      muxws: fileURLToPath(new URL('../../ts/index.ts', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    // When 5173 is taken, Vite's default is to use 5174 and print the new URL. `demo.py` has already
    // printed 5173, so the reader would open a port this run does not serve, possibly a dev server left
    // over from an earlier run. `strictPort` makes Vite exit instead.
    strictPort: true,
    proxy: {
      // `ws: true` makes Vite proxy the WebSocket upgrade. Without it Vite proxies the upgrade request as
      // an ordinary GET, the response carries no `Sec-WebSocket-Protocol`, and the dial fails as a
      // subprotocol mismatch (WSM-CDC-020/026).
      '/ws': { target: 'ws://127.0.0.1:8020', ws: true },
    },
  },
});
