# ZIO Query

| CI | Release | Snapshot | Discord |
| --- | --- | --- | --- |
| [![Build Status][Badge-Circle]][Link-Circle] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Badge-Discord]][Link-Discord] |

# Summary

ZIO Query adds efficient pipelining, batching, and caching to any data source.

ZIO Query helps you dramatically reduce load on data sources and improve performance.

- **Pipelining**. ZIO Query detects parts of composite queries that can be combined together for fewer individual requests to the data source.
- **Batching**. ZIO Query detects parts of composite queries that can be executed in parallel without changing the semantics of the query.
- **Caching**. ZIO Query can transparently cache read queries to minimize the cost of fetching the same item repeatedly in the scope of a query.

Compared with Fetch, ZIO Query supports pipelining, supports response types that depend on request types, does not require higher-kinded types and implicits, supports ZIO environment and statically typed errors, and has no dependencies except for ZIO.

# Documentation
[ZIO Query Microsite](https://zio.github.io/zio-query/)

# Contributing
[Documentation for contributors](https://zio.github.io/zio-query/docs/about/about_contributing)

## Code of Conduct

See the [Code of Conduct](https://zio.github.io/zio-query/docs/about/about_coc)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].


# License
[License](LICENSE)

[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-query_2.12.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-query_2.12.svg "Sonatype Snapshots"
[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Badge-Circle]: https://circleci.com/gh/zio/zio-query.svg?style=svg "circleci"
[Link-Circle]: https://circleci.com/gh/zio/zio-query "circleci"
[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-query_2.12/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-query_2.12/ "Sonatype Snapshots"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"

