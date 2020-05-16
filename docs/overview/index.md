---
id: overview_index
title: "Summary"
---

A `ZQuery[R, E, A]` is a purely functional description of an effectual query that may contain requests from one or more data sources, requires an environment `R`, and may fail with an `E` or succeed with an `A`.

Requests that can be performed in parallel, as expressed by `zipWithPar` and combinators derived from it, will automatically be batched. Requests that must be performed sequentially, as expressed by `zipWith` and combinators derived from it, will automatically be pipelined. This allows for aggressive data source specific optimizations. Requests can also be deduplicated and cached.

```scala mdoc:invisible
import zio._
import zio.query._
```

This allows for writing queries in a high level, compositional style, with confidence that they will automatically be optimized. For example, consider the following query from a user service.

```scala mdoc:silent
lazy val getAllUserIds: ZQuery[Any, Nothing, List[Int]]    = ???
def getUserNameById(id: Int): ZQuery[Any, Nothing, String] = ???

lazy val userQuery: ZQuery[Any, Nothing, List[String]] = for {
  userIds   <- getAllUserIds
  userNames <- ZQuery.foreachPar(userIds)(getUserNameById)
} yield userNames
```

This would normally require N + 1 queries, one for `getAllUserIds` and one for each call to `getUserNameById`. In contrast, `ZQuery` will automatically optimize this to two queries, one for `userIds` and one for `userNames`.

## Installation

Include ZIO Query in your project by adding the following to your `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "0.2.0"
```