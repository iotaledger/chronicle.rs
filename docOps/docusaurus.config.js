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
  ],
  themeConfig: {
    navbar: {
      title: 'Chronicle',
      logo: {
        alt: 'IOTA',
        src: 'img/logo/Logo_Swirl_Dark.png',
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
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Welcome',
              to: '/docs/welcome',
            },
            {
              label: 'Getting Started',
              to: '/docs/getting_started',
            },
            {
              label: 'Config Reference',
              to: '/docs/config_reference',
            },
            {
              label: 'Contribute',
              to: '/docs/contribute',
            },
          ],
        },],
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
          customCss: require.resolve('./src/css/iota.css'),
        },
      },
    ],
  ],
}; 