import { fileURLToPath } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      // `muxws` is the *root* of this npm workspace, and npm links only workspace **members** into
      // `node_modules` - so the bare specifier every consumer outside this repository writes has
      // nothing to resolve to here. A consumer deletes this alias and changes nothing else: every
      // import in `src/` is the published one, spelled the published way.
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
