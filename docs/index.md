---
id: index
title: "Introduction to ZIO Query"
sidebar_label: "ZIO Query"
---

[ZIO Query](https://github.com/zio/zio-query) is a library for writing optimized queries to data sources in a high-level compositional style. It can add efficient pipelining, batching, and caching to any data source. ZIO Query helps us dramatically reduce load on data sources and improve performance.

@PROJECT_BADGES@

## Introduction

Some key features of ZIO Query:

- **Batching** — ZIO Query detects parts of composite queries that can be executed in parallel without changing the semantics of the query.
- **Pipelining** — ZIO Query detects parts of composite queries that can be combined together for fewer individual requests to the data source.
- **Caching** — ZIO Query can transparently cache read queries to minimize the cost of fetching the same item repeatedly in the scope of a query.

Compared with Fetch, ZIO Query supports response types that depend on request types, does not require higher-kinded types and implicits, supports ZIO environment and statically typed errors, and has no dependencies except for ZIO.

A `ZQuery[R, E, A]` is a purely functional description of an effectual query that may contain requests from one or more data sources, requires an environment `R`, and may fail with an `E` or succeed with an `A`.

Requests that can be performed in parallel, as expressed by `zipWithPar` and combinators derived from it, will automatically be batched. Requests that must be performed sequentially, as expressed by `zipWith` and combinators derived from it, will automatically be pipelined. This allows for aggressive data source specific optimizations. Requests can also be deduplicated and cached.

```scala mdoc:invisible
import zio._
import zio.query._
```

This allows for writing queries in a high level, compositional style, with confidence that they will automatically be optimized. For example, consider the following query from a user service.

Assume we have the following database access layer APIs:

```scala mdoc:silent
def getAllUserIds: ZIO[Any, Nothing, List[Int]] = {
  // Get all user IDs e.g. SELECT id FROM users
  ZIO.succeed(???)
}

def getUserNameById(id: Int): ZIO[Any, Nothing, String] = {
  // Get user by ID e.g. SELECT name FROM users WHERE id = $id
  ZIO.succeed(???)
}
```

We can get their corresponding usernames from the database by the following code snippet:

```scala mdoc:silent
val userNames = for {
  ids   <- getAllUserIds
  names <- ZIO.foreachPar(ids)(getUserNameById)
} yield names
```

It works, but this is not performant. It is going to query the underlying database _N + 1_ times, one for `getAllUserIds` and one for each call to `getUserNameById`.

In contrast, `ZQuery` will automatically optimize this to two queries, one for `userIds` and one for `userNames`:

```scala mdoc:silent:nest
lazy val getAllUserIds: ZQuery[Any, Nothing, List[Int]]    = ???
def getUserNameById(id: Int): ZQuery[Any, Nothing, String] = ???

lazy val userQuery: ZQuery[Any, Nothing, List[String]] = for {
  userIds   <- getAllUserIds
  userNames <- ZQuery.foreachPar(userIds)(getUserNameById)
} yield userNames
```

## Installation

In order to use this library, we need to add the following line in our `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

## Example

Here is an example of using ZIO Query, which optimizes multiple database queries by batching all of them in one query:

```scala mdoc:compile-only
import zio._
import zio.query._

object ZQueryExample extends ZIOAppDefault {
  case class GetUserName(id: Int) extends Request[Throwable, String]

  lazy val UserDataSource: DataSource.Batched[Any, GetUserName] =
    new DataSource.Batched[Any, GetUserName] {
      val identifier: String = "UserDataSource"

      def run(requests: Chunk[GetUserName])(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] = {
        val resultMap = CompletedRequestMap.empty
        requests.toList match {
          case request :: Nil =>
            val result: Task[String] = {
              // get user by ID e.g. SELECT name FROM users WHERE id = $id
              ZIO.succeed(???)
            }

            result.exit.map(resultMap.insert(request, _))

          case batch: Seq[GetUserName] =>
            val result: Task[List[(Int, String)]] = {
              // get multiple users at once e.g. SELECT id, name FROM users WHERE id IN ($ids)
              ZIO.succeed(???)
            }

            result.fold(
              err =>
                requests.foldLeft(resultMap) { case (map, req) =>
                  map.insert(req, Exit.fail(err))
                },
              _.foldLeft(resultMap) { case (map, (id, name)) =>
                map.insert(GetUserName(id), Exit.succeed(name))
              }
            )
        }
      }

    }

  def getUserNameById(id: Int): ZQuery[Any, Throwable, String] =
    ZQuery.fromRequest(GetUserName(id))(UserDataSource)

  val query: ZQuery[Any, Throwable, List[String]] =
    for {
      ids <- ZQuery.succeed(1 to 10)
      names <- ZQuery.foreachPar(ids)(id => getUserNameById(id)).map(_.toList)
    } yield (names)

  def run = query.run.tap(usernames => Console.printLine(s"Usernames: $usernames"))
}
```

## Resources

- [Wicked Fast API Calls with ZIO Query](https://www.youtube.com/watch?v=rUUxDXJMzJo) by Adam Fraser (July 2020) (https://www.youtube.com/watch?v=rUUxDXJMzJo)
