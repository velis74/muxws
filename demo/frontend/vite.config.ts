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
    // Vite's default is to step to 5174 when 5173 is taken, print the new URL in its own banner and
    // carry on. `demo.py` has already printed 5173 by then and cannot un-print it, so the reader
    // opens a port this run is not serving - which, if the thing holding 5173 is a dev server left
    // over from an earlier run, is a *stale muxws demo* that looks live and answers nothing new.
    // Failing to start is the honest outcome.
    strictPort: true,
    proxy: {
      // `ws: true` is the whole of it, and it is the one line in this demo most likely to be
      // omitted. Without it Vite proxies the upgrade request as an ordinary GET, the response never
      // carries `Sec-WebSocket-Protocol`, and the dial fails as a subprotocol mismatch
      // (WSM-CDC-020/026) - which reads exactly like a muxws bug and is not one.
      '/ws': { target: 'ws://127.0.0.1:8020', ws: true },
    },
  },
});
