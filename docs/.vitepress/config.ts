import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'muxws',
  description: 'Multiplexed, cancellable streams over one WebSocket',
  // The design material under docs/design/ is the record of how muxws was designed, not part of the
  // published site. It is excluded here rather than moved so the briefs stay next to the code they
  // describe. See GAPS.md, G-2.
  srcExclude: ['design/**'],
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/muxws-icon.svg' }]],
  themeConfig: {
    logo: '/muxws-icon.svg',
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Guide', link: '/guide/rationale' },
      { text: 'API Reference', link: '/api/' },
    ],
    // Sidebar order is reading order. The guide opens with why the library exists and a quick start
    // that works on its own; Concepts is what a reader needs to reason about a running connection;
    // Integration is what they need to attach it to a particular server, codec or log.
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
      ],
      // The API pages in reading order: the two ways a connection starts, then the two objects an
      // application holds, then everything a connection is configured with.
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
  // Deliberately narrow: a dead internal link is a build failure. `docs/check-docs.mjs` asserts this
  // value has not been widened, and resolves heading anchors as well, which VitePress does not.
  ignoreDeadLinks: [/^http:\/\/localhost/],
});
