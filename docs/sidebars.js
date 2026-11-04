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
        "completed-request-map",
        "creating-queries",
        "how-batching-works",
        "running-queries",
        "error-handling"
      ]
    }
  ]
};

module.exports = sidebars;
