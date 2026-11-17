---
id: getting-started
title: "Getting Started"
description: "Install ZIO Query, then write and run your first request, data source and query in a few lines of Scala."
keywords:
  - "ZIO Query Installation"
  - "First Query"
  - "Data Source"
  - "ZQuery"
---

This page takes you from an empty `build.sbt` to a running query. You will add the dependency, define a request and a data source, build a `ZQuery` that uses them, and run it. It assumes you know basic Scala and have seen a `ZIO` effect before.

## Installation

Add the following line to your `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

ZIO Query depends on ZIO, which sbt downloads for you.

ZIO Query is published for Scala 2.12, Scala 2.13 and Scala 3, and for three platforms: the JVM, Scala.js and Scala Native. On Scala.js and Scala Native, use `%%%` instead of `%%` so that sbt picks the artifact for that platform:

```scala
libraryDependencies += "dev.zio" %%% "zio-query" % "@VERSION@"
```

## Your First Query

A query in ZIO Query is built from three parts. A **request** describes one question, such as "what is the name of user 1?". A **data source** knows how to answer requests of one type. A **query** (`ZQuery`) combines requests into a description of the work, which we run at the end.

All the code on this page compiles together, so each block uses the definitions of the blocks before it. We start with the imports:

```scala mdoc:silent
import zio._
import zio.query._
```

### 1. Define a Request

A request is a value that extends `Request[E, A]`, where `E` is the error the request can fail with and `A` is the type of its result. Define requests as case classes, because ZIO Query compares them with `equals` to avoid asking the same question twice:

```scala mdoc:silent
final case class GetUserName(id: Int) extends Request[Nothing, String]
```

`GetUserName` asks for the name of the user with a given id. Its error type is `Nothing`, which means this request cannot fail, and its result type is `String`.

### 2. Create a Data Source

A data source answers requests. To keep this example self-contained, ours reads from an in-memory `Map` instead of a database:

```scala mdoc:silent
val userNames: Map[Int, String] = Map(1 -> "Ada", 2 -> "Grace", 3 -> "Linus")

val userSource: DataSource[Any, GetUserName] =
  DataSource.fromFunction("UserSource")((request: GetUserName) => userNames(request.id))
```

`DataSource.fromFunction` takes a name and a function from a request to its result. The name identifies the data source, so it must be unique among your data sources. The type `DataSource[Any, GetUserName]` says that the data source needs no environment (`Any`) and answers `GetUserName` requests. A real data source would call a database or a service. [Creating Data Sources](./creating-data-sources.md) shows how to write one that receives a whole batch of requests at once, and [DataSource Reference](./data-source-reference.md) lists the other constructors.

### 3. Build a Query

`ZQuery.fromRequest` takes a request and the data source that answers it, and returns a `ZQuery`:

```scala mdoc:silent
def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(userSource)
```

The type `ZQuery[Any, Nothing, String]` reads as "a query that needs no environment, cannot fail, and produces a `String`". Nothing has been fetched yet, because a query is only a description.

Queries compose like effects. To fetch several users at once, use `ZQuery.foreachPar`:

```scala mdoc:silent
val query: ZQuery[Any, Nothing, List[String]] =
  ZQuery.foreachPar(List(1, 2, 3))(userName)
```

The `Par` in the name matters: it tells ZIO Query that the three lookups are independent, so it can hand all three requests to the data source together instead of one by one. [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains how each combinator affects the calls a data source receives.

### 4. Run the Query

`ZQuery#run` turns the query into a `ZIO` effect, and running that effect fetches the data:

```scala mdoc:invisible
def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())
```

```scala mdoc
exec(query.run)
```

Here `exec` is a small helper that these docs use to run an effect and print its result, so you can see the output. In your own program, run the effect from a `ZIOAppDefault`:

```scala mdoc:compile-only
object Main extends ZIOAppDefault {
  def run =
    query.run.flatMap(names => Console.printLine(names.mkString(", ")))
}
```

The `run` method of `Main` is a normal ZIO effect, so everything you know about ZIO applies to it: you can provide layers, add retries, or run it inside a larger program.

## What You Have Learned

You added ZIO Query to your build, described a lookup with a `Request`, answered it with a `DataSource`, combined lookups with `ZQuery.foreachPar`, and ran the result with `ZQuery#run`. The reason to use ZIO Query is what happens when the data source is a real database: the three lookups above reach the data source as one batch, and equal requests are answered only once.

## Next Steps

- To see that effect with real numbers, follow the [Tutorial: Batching Database Lookups with ZIO Query](./tutorial-batching-database-lookups.md). It counts database calls before and after batching.
- To write data sources for your own services, read [Creating Data Sources](./creating-data-sources.md) and the [DataSource Reference](./data-source-reference.md).
- To build larger queries, read [Creating Queries](./creating-queries.md).
- To run queries and control the cache, read [Running Queries and Caching](./running_queries.md).
- To handle failures, read [Error Handling](./error-handling.md).
