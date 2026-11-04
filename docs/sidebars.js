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
        "completed-request-map",
        "creating-queries",
        "how-batching-works",
        "aspects",
        "running-queries",
        "error-handling"
      ]
    }
  ]
};

module.exports = sidebars;
