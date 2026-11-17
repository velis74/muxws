import { fileURLToPath } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

// Set in GitHub Codespaces (`app.github.dev`), where the page is served as `<codespace>-5173.<domain>`
// over HTTPS.
const codespacesDomain = process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN;

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
    // In Codespaces: listen on IPv4 and IPv6 (`localhost` may resolve to `::1` only, which the port
    // forwarder does not always reach), and allow the forwarded host, which Vite otherwise answers with
    // 403. Elsewhere Vite's defaults apply. The HMR client connects to the page's own host and port.
    ...(codespacesDomain && { host: true, allowedHosts: [`.${codespacesDomain}`] }),
    proxy: {
      // `ws: true` makes Vite proxy the WebSocket upgrade. Without it Vite proxies the upgrade request as
      // an ordinary GET, the response carries no `Sec-WebSocket-Protocol`, and the dial fails as a
      // subprotocol mismatch (WSM-CDC-020/026).
      '/ws': { target: 'ws://127.0.0.1:8020', ws: true },
    },
  },
});
