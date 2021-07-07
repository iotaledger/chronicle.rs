const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Chronicle',
  tagline: 'Official IOTA permanode solution.',
  url: 'https://docs.iota.org', // TODO: PROPER URL
  baseUrl: '/chronicle.rs/',
  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'throw',
  favicon: '/img/logo/img/favicon.ico',
  organizationName: 'iotaledger',
  projectName: 'chronicle.rs',
  stylesheets: [
    'https://fonts.googleapis.com/css?family=Material+Icons',
    'https://iota-community.github.io/iota-wiki/assets/css/styles.f9f708da.css',//replace this URL
  ],
  themeConfig: {
    navbar: {
      title: 'Chronicle',
      logo: {
        alt: 'IOTA',
        src: '/img/logo/Logo_Swirl_Dark.png',
      },
      items: [
        {
          type: 'doc',
          docId: 'welcome',
          position: 'left',
          label: 'Documentation',
        },
        {
          href: 'https://github.com/iotaledger/chronicle.rs',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      copyright: `Copyright © ${new Date().getFullYear()} IOTA Foundation, Built with Docusaurus.`,
    },
    prism: {
        additionalLanguages: ['rust'],
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/iotaledger/chronicle.rs/tree/main/docs',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
}; 