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
        "creating-queries",
        "how-batching-works",
        "aspects",
        "running-queries"
      ]
    }
  ]
};

module.exports = sidebars;
