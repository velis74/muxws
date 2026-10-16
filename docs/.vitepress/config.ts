import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'muxws',
  description: 'Multiplexed, cancellable streams over one WebSocket',
  // docs/design/ holds design documents that are not part of the published site.
  srcExclude: ['design/**'],
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/muxws-icon.svg' }]],
  themeConfig: {
    logo: '/muxws-icon.svg',
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Guide', link: '/guide/rationale' },
      { text: 'API Reference', link: '/api/' },
    ],
    // Sidebar order is reading order: Introduction (rationale and quick start), Concepts (behaviour
    // of a running connection), Integration (servers, codecs, logging).
    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'Rationale', link: '/guide/rationale' },
            { text: 'Comparison to HTTP/2 and HTTP/3', link: '/guide/comparison' },
            { text: 'Getting Started', link: '/guide/getting-started' },
            { text: 'Demos', link: '/guide/demos' },
          ],
        },
        {
          text: 'Concepts',
          items: [
            { text: 'Architecture', link: '/guide/architecture' },
            { text: 'Call shapes', link: '/guide/call-shapes' },
            { text: 'Streams & cancellation', link: '/guide/streams-and-cancellation' },
            { text: 'Connection lifecycle', link: '/guide/connection-lifecycle' },
            { text: 'Sizes & fragmentation', link: '/guide/sizes-and-fragmentation' },
            { text: 'Reconnect', link: '/guide/reconnect' },
            { text: 'Errors', link: '/guide/errors' },
          ],
        },
        {
          text: 'Integration',
          items: [
            { text: 'Transports', link: '/guide/transports' },
            { text: 'Codecs', link: '/guide/codecs' },
            { text: 'Registry', link: '/guide/registry' },
            { text: 'Observability', link: '/guide/observability' },
            { text: 'Interop', link: '/guide/interop' },
          ],
        },
        {
          text: 'Upgrading',
          items: [
            { text: 'Changelog', link: '/guide/changelog' },
            { text: 'Migration guide', link: '/guide/migration' },
          ],
        },
      ],
      // API pages in reading order: `connect` and `accept`, then `Peer` and `Stream`, then the
      // configuration and supporting types.
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'Overview', link: '/api/' },
            { text: 'connect', link: '/api/connect' },
            { text: 'accept', link: '/api/accept' },
            { text: 'Peer', link: '/api/peer' },
            { text: 'Stream', link: '/api/stream' },
            { text: 'Reconnect', link: '/api/reconnect' },
            { text: 'Errors', link: '/api/errors' },
            { text: 'Codec', link: '/api/codec' },
            { text: 'Registry', link: '/api/registry' },
            { text: 'Transports', link: '/api/transports' },
            { text: 'Types', link: '/api/types' },
          ],
        },
      ],
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/velis74/muxws' }],
    footer: { message: 'Released under the MIT License.', copyright: 'Copyright © 2025 Jure Erznožnik' },
  },
  // Only localhost links are ignored, so a dead internal link fails the build. `docs/check-docs.mjs`
  // asserts this value and also checks heading anchors, which VitePress does not.
  ignoreDeadLinks: [/^http:\/\/localhost/],
});
