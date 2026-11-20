import { fileURLToPath } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

// Set in GitHub Codespaces (`app.github.dev`), where the page is served as `<codespace>-5173.<domain>`
// over HTTPS.
const codespacesDomain = process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN;
// Set by `.devcontainer/devcontainer.json`.
const listenOnAllInterfaces = Boolean(process.env.MUXWS_DEMO_VITE_HOST);

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
    // `127.0.0.1`, the address `demo.py` prints: Vite's default `localhost` resolves to `::1` only on some
    // hosts (GitHub runners, the dev container). The dev container and Codespaces listen on all
    // interfaces; Codespaces also allows the forwarded host, which Vite otherwise answers with 403. The
    // HMR client connects to the page's own host and port.
    host: listenOnAllInterfaces || codespacesDomain ? true : '127.0.0.1',
    ...(codespacesDomain && { allowedHosts: [`.${codespacesDomain}`] }),
    proxy: {
      // `ws: true` makes Vite proxy the WebSocket upgrade. Without it Vite proxies the upgrade request as
      // an ordinary GET, the response carries no `Sec-WebSocket-Protocol`, and the dial fails as a
      // subprotocol mismatch (WSM-CDC-020/026).
      '/ws': { target: 'ws://127.0.0.1:8020', ws: true },
    },
  },
});
