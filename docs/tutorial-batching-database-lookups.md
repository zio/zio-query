---
id: tutorial-batching-database-lookups
title: "Tutorial: Batching Database Lookups with ZIO Query"
description: "Learn how ZIO Query removes the N+1 query problem by batching, deduplicating and caching requests, using call counts."
keywords:
  - "N+1 Query Problem"
  - "Request Batching"
  - "Query Caching"
  - "ZQuery"
---

Welcome to this tutorial on batching database lookups with ZIO Query! It is written for Scala developers who know basic Scala and have seen a `ZIO` effect, for example `ZIO.succeed(42)`. You don't need any prior experience with ZIO Query, and you don't need a real database: we build a tiny in-memory one that counts how often we call it.

## Learning Objectives

By the end of this tutorial, you will understand:

- What the N+1 query problem is and how to measure it
- How to describe a lookup as a `Request` and answer it with a `DataSource`
- Why a `ZQuery` built with `ZQuery.foreachPar` reaches the database in one call instead of many
- How the per-run cache removes duplicate requests
- How a second request type and data source join the same query

We'll learn these concepts through:

1. A database that counts its calls
2. The N+1 problem
3. Describing a lookup as a request
4. A data source that batches
5. Running the query
6. Caching
7. A second request type

We recommend reading from top to bottom, because each section builds on the code of the previous one. Every block of code is compiled and run when this page is built, so the output you see is the real output.

## Background

Imagine a page that shows a list of users with their names. The usual way to write it is one query to get the ids of the users, followed by one query per id to get each name. With five users that is six queries, with a thousand users it is a thousand and one. This pattern is called the **N+1 problem**: the number of database round trips grows with the number of rows, even though the work could be done with a single query such as `SELECT id, name FROM users WHERE id IN (...)`.

Writing the batched query by hand makes code awkward, because every function that needs a user name must now take a list of ids. ZIO Query keeps the simple code, one lookup per user, and does the batching for you. You describe each lookup as a request, and when you combine lookups in parallel, ZIO Query collects all the requests that are ready and hands them to a data source in one go.

## 1. A Database That Counts Its Calls

To see the problem, we need something to measure. The following code defines a `Table` that looks rows up by id and records every call in a shared log. Think of each log entry as one round trip to the database:

```scala mdoc:silent
import zio._
import zio.query._

final class Table[A](name: String, rows: Map[Int, A], log: Ref[Chunk[String]]) {

  private def record(ids: Iterable[Int]): UIO[Unit] =
    log.update(_ :+ s"$name: ${ids.toList.sorted.mkString(", ")}")

  // One call to the database, for one id
  def find(id: Int): UIO[A] =
    record(List(id)) *> ZIO.succeed(rows(id))

  // One call to the database, for many ids
  def findAll(ids: Chunk[Int]): UIO[Map[Int, A]] =
    record(ids) *> ZIO.succeed(ids.toList.map(id => id -> rows(id)).toMap)
}
```

The code above:

- `Table[A]` — holds the rows of one table in a `Map`, from id to a value of type `A`.
- `record` — appends one entry to the log, naming the table and the ids that were asked for. We sort the ids so the output is stable.
- `find` — looks up one id and costs one call.
- `findAll` — looks up many ids and also costs only one call, like `WHERE id IN (...)` in SQL.

Next we create the log, one table of users, a helper that reads and clears the log, and a helper that runs an effect so this page can print its result:

```scala mdoc:silent
val log: Ref[Chunk[String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

val users: Table[String] =
  new Table("users", Map(1 -> "Ada", 2 -> "Grace", 3 -> "Linus", 4 -> "Margaret", 5 -> "Dennis"), log)

def takeCalls: UIO[List[String]] =
  log.getAndSet(Chunk.empty).map(_.toList.sorted)

def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())
```

The code above:

- `log` — a mutable counter shared by all tables. A `Ref` is ZIO's thread-safe mutable reference.
- `users` — five users. Every id we ask for in this tutorial exists, so the lookups never fail. [How to Handle Missing Results](./how-to-handle-missing-results.md) covers the ids that don't.
- `takeCalls` — returns the calls recorded so far and clears the log, so each experiment starts from zero. It sorts the entries so that the output is the same on every run.
- `exec` — runs an effect to completion and returns its result. We use it only to display results in this page. In a real program you run effects from a `ZIOAppDefault`.

## 2. The N+1 Problem

Let's write the straightforward version first. For each id we call `find`:

```scala mdoc:silent
def naiveNames(ids: List[Int]): UIO[List[String]] =
  ZIO.foreach(ids)(users.find)
```

Fetching the names of all five users gives the expected result:

```scala mdoc
exec(naiveNames(List(1, 2, 3, 4, 5)))
```

The result is correct. Now let's look at what the database saw:

```scala mdoc
val naiveCalls = exec(takeCalls)

naiveCalls.size
```

Notice that the database received five separate calls, one per user, so asking for N users costs N calls. Running the lookups in parallel with `ZIO.foreachPar` would not help, because they would still be five round trips, only overlapping in time.

## 3. Describing a Lookup as a Request

The fix starts with a change in how we think about a lookup. Instead of calling the database, we create a value that describes what we want, and we let ZIO Query decide how to get it. Such a value is a `Request`:

```scala mdoc:silent
final case class GetUserName(id: Int) extends Request[Nothing, String]
```

The code above:

- `GetUserName(id: Int)` — a case class, because ZIO Query compares requests with `equals` to detect duplicates.
- `Request[Nothing, String]` — the first type parameter is the error the request can fail with, and the second is its result. `Nothing` means this request cannot fail, and the result is a `String`.

A request does nothing by itself. It is a question without an answerer.

## 4. A Data Source That Batches

The answerer is a `DataSource`. A data source receives requests and returns their results. We create one with `DataSource.Batched.make`, whose function receives all the requests of one batch as a single `Chunk`, so we can answer them with one call to the database:

```scala mdoc:silent
val userSource: DataSource[Any, GetUserName] =
  DataSource.Batched.make[Any, GetUserName]("UserSource") { requests =>
    users.findAll(requests.map(_.id)).map { rows =>
      CompletedRequestMap.fromIterableWith(requests)(
        request => request,
        request => Exit.succeed(rows(request.id))
      )
    }
  }
```

The code above:

- `"UserSource"` — the identifier of the data source. It must be unique among your data sources.
- `requests` — a `Chunk[GetUserName]`, all the requests that can be answered together.
- `users.findAll(requests.map(_.id))` — extracts the ids and fetches all of them in one call.
- `CompletedRequestMap.fromIterableWith` — builds the answer, a map from each request to its result. The first function says which request an element is, and the second says which `Exit` it completed with. `Exit.succeed` wraps a successful result.

:::note
A data source must provide a result for every request it receives. If it leaves one out, the query dies with a `QueryFailure`. See [Completed Request Map](./completed-request-map.md) for the other ways to build the answer.
:::

Finally, we connect requests to the data source. `ZQuery.fromRequest` takes a request and the data source that answers it, and returns a `ZQuery`:

```scala mdoc:silent
def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(userSource)
```

The function `userName` looks just like the function that called the database directly. A `ZQuery[Any, Nothing, String]` is a description of a query that needs no environment, cannot fail, and produces a `String`. Nothing has been fetched yet.

## 5. Running the Query

Now we fetch the same five users, but with `ZQuery.foreachPar` instead of `ZIO.foreach`. The `Par` tells ZIO Query that the lookups are independent, so it can collect their requests into one batch:

```scala mdoc:silent
val batchedQuery: ZQuery[Any, Nothing, List[String]] =
  ZQuery.foreachPar(List(1, 2, 3, 4, 5))(userName)
```

We run a query with `ZQuery#run`, which turns it into a `ZIO` effect:

```scala mdoc
exec(batchedQuery.run)
```

The names are the same as before. Let's look at the database calls:

```scala mdoc
val batchedCalls = exec(takeCalls)

batchedCalls.size
```

The database received one call that asked for all five ids, instead of five calls that asked for one id each. Here is the comparison of the two versions:

| Version                           | Database calls | Ids per call |
|-----------------------------------|----------------|--------------|
| `ZIO.foreach` with `Table#find`   | 5              | 1            |
| `ZQuery.foreachPar` with batching | 1              | 5            |

The reduction grows with the number of users. With a thousand users, the first version makes a thousand calls and the second still makes one, with the sizes of batches controlled by [aspects](./aspects.md) if the database limits them.

:::tip
Batching applies to requests that are ready at the same time. Combinators such as `ZQuery.foreachPar`, `ZQuery.collectAllPar` and `ZQuery#zipPar` combine queries in parallel and batch their requests, while `ZQuery#flatMap` starts a new step for each query that depends on a previous result. [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains each combinator.
:::

## 6. Caching

ZIO Query also remembers the requests of a run. If the same request appears twice, the data source is asked only once. Let's fetch a list in which users 1 and 2 appear twice:

```scala mdoc
val withDuplicates = ZQuery.foreachPar(List(1, 2, 1, 2, 3))(userName)

exec(withDuplicates.run)

exec(takeCalls)
```

The result has five names, because we asked for five, but the database call contains only three ids: 1, 2 and 3. ZIO Query noticed that `GetUserName(1)` and `GetUserName(2)` were requested again and answered the duplicates from the cache. That is also why we defined the request as a case class.

To see the difference that the cache makes, we disable it with `ZQuery#uncached`:

```scala mdoc
exec(withDuplicates.uncached.run)

exec(takeCalls)
```

This time the data source receives all five requests, duplicates included. Caching is on by default.

The cache lives only as long as one run. Running the same query twice calls the database twice:

```scala mdoc
exec(batchedQuery.run)

exec(batchedQuery.run)

exec(takeCalls)
```

To keep results between runs, create a cache and pass it to `ZQuery#runCache`. Now the second run finds all five users in the cache, and the database is called only once in total:

```scala mdoc
val cache = exec(Cache.empty)

exec(batchedQuery.runCache(cache))

exec(batchedQuery.runCache(cache))

exec(takeCalls)
```

:::caution
The default cache never evicts entries, so a cache that lives as long as your application serves stale data and grows without bound. Share a cache only across the runs of one logical operation. [Running Queries and Caching](./running_queries.md) explains when to share a cache.
:::

## 7. A Second Request Type

Real queries read from several tables. To add one, we repeat the pattern of sections 3 and 4: a request type, a data source, and a function that builds a query. This time the request is the number of orders of a user:

```scala mdoc:silent
final case class GetOrderCount(id: Int) extends Request[Nothing, Int]

val orders: Table[Int] =
  new Table("orders", Map(1 -> 3, 2 -> 0, 3 -> 7, 4 -> 1, 5 -> 2), log)

val orderSource: DataSource[Any, GetOrderCount] =
  DataSource.Batched.make[Any, GetOrderCount]("OrderSource") { requests =>
    orders.findAll(requests.map(_.id)).map { rows =>
      CompletedRequestMap.fromIterableWith(requests)(
        request => request,
        request => Exit.succeed(rows(request.id))
      )
    }
  }

def orderCount(id: Int): ZQuery[Any, Nothing, Int] =
  ZQuery.fromRequest(GetOrderCount(id))(orderSource)
```

The code above is the same as before with different names: a second `Table` with order counts, a data source that answers `GetOrderCount` with one call to that table, and `orderCount`, which builds a query.

Now we combine both lookups for one user into a `Summary`, and build a summary for several users:

```scala mdoc:silent
final case class Summary(name: String, orderCount: Int)

def summary(id: Int): ZQuery[Any, Nothing, Summary] =
  userName(id).zipPar(orderCount(id)).map { case (name, count) => Summary(name, count) }

val summaries: ZQuery[Any, Nothing, List[Summary]] =
  ZQuery.foreachPar(List(1, 2, 3))(summary)
```

The code above:

- `userName(id).zipPar(orderCount(id))` — runs the two queries in parallel and pairs their results. Their requests belong to different data sources.
- `.map { ... }` — turns the pair into a `Summary`.
- `ZQuery.foreachPar(List(1, 2, 3))(summary)` — builds three summaries in parallel.

Let's run it:

```scala mdoc
exec(summaries.run)

exec(takeCalls)
```

Three summaries needed six lookups, three of users and three of orders. The database received two calls, one for each table. ZIO Query groups requests by data source, so each data source receives all its requests in a single batch, even though each `summary` was written as if it ran alone. This is the main idea of the library: write queries as simple compositions of small lookups, and let the structure of the query decide how many calls reach the data sources.

## What You've Learned

In this tutorial, you learned:

- The N+1 problem makes the number of database calls grow with the number of rows, and a counter in a fake database lets us measure it.
- A `Request` describes a lookup, a `DataSource` answers a batch of requests, and `ZQuery.fromRequest` connects them.
- Combining queries with `ZQuery.foreachPar` batches their requests, so five lookups became one database call.
- The cache of a run removes duplicate requests, `ZQuery#uncached` turns it off, and `ZQuery#runCache` shares it between runs.
- Requests of different types go to their own data sources, and each data source receives one batch.

You now have a solid foundation in how ZIO Query reduces database calls. As a next step, try the exercises below.

## Exercises

1. Change `batchedQuery` to fetch the ids `List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)` and predict the call before running it.
2. Replace `ZQuery.foreachPar` with `ZQuery.foreach` in `batchedQuery` and look at how the calls change. [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains the result.
3. Add `.batchN(2)` to `userSource`, which limits the number of requests in a batch, and count the calls for five users. See the [DataSource Reference](./data-source-reference.md).

## Where to Go Next

- **Ready to apply this to your own code?** Read [How to Batch N+1 Queries](./how-to-batch-n-plus-one-queries.md).
- **Want to know why the calls look the way they do?** Read [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md).
- **Want more control over the cache?** Read [Running Queries and Caching](./running_queries.md).
- **Want to write other kinds of data sources?** Read [Creating Data Sources](./creating-data-sources.md) and the [DataSource Reference](./data-source-reference.md).
- **Want to handle errors?** Read [Error Handling](./error-handling.md).
