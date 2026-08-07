import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'muxws',
  description: 'Multiplexed, cancellable streams over one WebSocket',
  // The design material under docs/design/ is the record of how muxws was designed, not part of the
  // published site. It is excluded here rather than moved so the briefs stay next to the code they
  // describe. See GAPS.md, G-2.
  srcExclude: ['design/**'],
  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API Reference', link: '/api/' },
    ],
    sidebar: {
      '/guide/': [{ text: 'Guide', items: [{ text: 'Getting Started', link: '/guide/getting-started' }] }],
      '/api/': [{ text: 'API Reference', items: [{ text: 'Overview', link: '/api/' }] }],
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/velis74/muxws' }],
    footer: { message: 'Released under the MIT License.', copyright: 'Copyright © 2025 Jure Erznožnik' },
  },
  ignoreDeadLinks: [/^http:\/\/localhost/],
});
