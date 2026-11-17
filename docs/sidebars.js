const sidebars = {
  sidebar: [
    "index",
    "getting-started",
    {
      type: "category",
      label: "Reference",
      collapsed: false,
      items: [
        {
          type: "category",
          label: "Queries",
          collapsed: false,
          items: ["zquery", "creating-queries", "running-queries", "error-handling"]
        },
        {
          type: "category",
          label: "Data Sources and Requests",
          collapsed: false,
          items: [
            "request",
            "creating-data-sources",
            "data-source-reference",
            "completed-request-map",
            "query-failure"
          ]
        },
        {
          type: "category",
          label: "Batching, Caching and Aspects",
          collapsed: false,
          items: ["how-batching-works", "cache", "aspects", "described"]
        }
      ]
    },
    "tutorial-batching-database-lookups",
    {
      type: "category",
      label: "How-to Guides",
      collapsed: true,
      items: [
        "how-to-batch-n-plus-one-queries",
        "how-to-query-multiple-sources-in-parallel",
        "how-to-cache-query-results",
        "how-to-add-logging-and-metrics",
        "how-to-handle-missing-results",
        "how-to-time-out-slow-queries",
        "how-to-test-queries",
        "how-to-use-environment-in-data-sources"
      ]
    }
  ]
};

module.exports = sidebars;
