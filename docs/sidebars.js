const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO Query",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        "index",
        "creating-data-sources",
        "data-source-reference",
        "creating-queries",
        "how-batching-works",
        "running-queries"
      ]
    }
  ]
};

module.exports = sidebars;
