const path = require('path');

module.exports = {
    title: 'Chronicle',
    url: '/',
    baseUrl: '/',
    themes: ['@docusaurus/theme-classic'],
    plugins: [
        [
            '@docusaurus/plugin-content-docs',
            {
                id: 'chronicle-rs',
                path: path.resolve(__dirname, 'docs'),
                routeBasePath: 'chronicle.rs',
                sidebarPath: path.resolve(__dirname, 'sidebars.js'),
                editUrl: 'https://github.com/iotaledger/chronicle.rs/edit/main/',
                remarkPlugins: [require('remark-code-import'), require('remark-import-partial')],
            }
        ],
    ],
    staticDirectories: [path.resolve(__dirname, 'static')],
};
