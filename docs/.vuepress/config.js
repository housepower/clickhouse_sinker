module.exports = {
  base: "/clickhouse_sinker/",
  title: "clickhouse_sinker",
  evergreen: true,
  plugins: ["mermaidjs"],
  locales: {
    "/": {
      lang: "en-US",
      title: "clickhouse_sinker",
      description: "clickhouse_sinker a tool to sink the data into ClickHouse",
    },
    "/zh/": {
      lang: "zh-CN",
      title: "clickhouse_sinker",
      description: "clickhouse_sinker 一个将数据摄入到ClickHouse的工具",
    },
  },
  themeConfig: {
    locales: {
      "/": {
        selectText: "Languages",
        label: "English",
        ariaLabel: "Languages",
        editLinkText: "Edit this page on GitHub",
        serviceWorker: {
          updatePopup: {
            message: "New content is available.",
            buttonText: "Refresh",
          },
        },
        algolia: {},
        nav: [
          { text: "Get Started", link: "/guide/install" },
          { text: "Introduction", link: "/dev/introduction" },
          { text: "Configuration", link: "/configuration/flag" },
          {
            text: "GitHub",
            link: "https://github.com/housepower/clickhouse_sinker",
          },
        ],
        sidebar: {
          "/guide/": [
            {
              title: "Install and Run",
              children: [
                ["install", "Install"],
                ["run", "Run"],
              ],
            },
          ],

          "/configuration/": [
            {
              title: "Configuration",
              children: [
                ["flag", "Flag"],
                ["config", "Config"],
              ]
            }
          ],

          "/dev/": [
            {
              title: "Development",
              children: [
                ["introduction", "Introduction"],
                ["design", "Design"],
              ]
            }
          ],
        },
      }
    },
  },
};
