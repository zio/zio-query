---
id: how-to-query-multiple-sources-in-parallel
title: "How to Query Multiple Sources in Parallel"
description: "Combine lookups against several data sources into one query that calls every source at the same time and batches the requests of each."
keywords:
  - "Parallel Queries"
  - "Multiple Data Sources"
  - "Request Batching"
  - "ZQuery"
---

In this guide, we will build a dashboard query that reads from three services (users, orders, and inventory) and calls all of them at the same time. By the end, each service receives exactly one call, however many rows the dashboard shows, and a failure in one service does not take the page down. We start with two sources combined with `ZQuery#zipPar`, grow to a whole table with `ZQuery.foreachPar`, and finish with recipes for failing and redundant sources.

## The Problem

A dashboard needs data from several services. Written with plain `ZIO`, the calls are easy to run in sequence by accident, and then the latencies of the services add up. Even when they run in parallel, nothing combines the lookups of one service into one call:

```scala mdoc:compile-only
import zio._

def fetchUserName(id: Int): Task[String] = ???
def fetchOrderCount(id: Int): Task[Int]  = ???
def fetchStock(sku: String): Task[Int]   = ???

def dashboardRow(id: Int): Task[(String, Int, Int)] =
  for {
    name   <- fetchUserName(id)
    orders <- fetchOrderCount(id)
    stock  <- fetchStock("widget")
  } yield (name, orders, stock)

val dashboard: Task[List[(String, Int, Int)]] =
  ZIO.foreach(List(1, 2, 3))(dashboardRow)
```

This code makes three sequential calls per row, so the total time is the sum of nine calls. Making the loop parallel with `ZIO.foreachPar` helps with time but not with load: each service still receives three separate calls, and the inventory service is asked for the same SKU three times.

## Prerequisites

Add ZIO Query to the build:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

We assume that you know what a request and a data source are, see [Creating Data Sources](./creating-data-sources.md). The table in [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) lists which combinators run requests in parallel.

## Setting Up the Example

The example has three request types, one for each service, and one data source for each request type. Each data source records the ids it receives in a log, so that we can count its calls. The inventory service rejects the SKU `gadget`, so that we can fail one source on purpose later:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUserName(id: Int)   extends Request[Nothing, String]
final case class GetOrderCount(id: Int) extends Request[Nothing, Int]
final case class GetStock(sku: String)  extends Request[String, Int]

val calls: Ref[Chunk[String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

def record(source: String, keys: Chunk[String]): UIO[Unit] =
  calls.update(_ :+ s"$source: ${keys.sorted.mkString("[", ", ", "]")}")

val users: DataSource[Any, GetUserName] =
  DataSource.fromFunctionBatchedZIO[Any, Nothing, GetUserName, String]("Users") { requests =>
    record("Users", requests.map(_.id.toString)).as(requests.map(request => s"user-${request.id}"))
  }

val orders: DataSource[Any, GetOrderCount] =
  DataSource.fromFunctionBatchedZIO[Any, Nothing, GetOrderCount, Int]("Orders") { requests =>
    record("Orders", requests.map(_.id.toString)).as(requests.map(request => request.id * 10))
  }

val inventory: DataSource[Any, GetStock] =
  DataSource.make[Any, GetStock]("Inventory") { batches =>
    record("Inventory", batches.flatten.map(_.sku)).as(
      CompletedRequestMap.fromIterableWith(batches.flatten)(
        request => request,
        request =>
          if (request.sku == "gadget") Exit.fail(s"unknown sku ${request.sku}")
          else Exit.succeed(request.sku.length)
      )
    )
  }

def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(users)

def orderCount(id: Int): ZQuery[Any, Nothing, Int] =
  ZQuery.fromRequest(GetOrderCount(id))(orders)

def stock(sku: String): ZQuery[Any, String, Int] =
  ZQuery.fromRequest(GetStock(sku))(inventory)

def exec[A](effect: UIO[A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

def runAndShow[A](query: ZQuery[Any, Nothing, A]): Unit = {
  val (result, received) = exec(query.run <*> calls.getAndSet(Chunk.empty))
  println(s"result: $result")
  received.sorted.foreach(println)
}
```

The log lines have the form `Source: [keys]`. Each line is one call to a data source, and the keys in brackets are the requests of that call.

## Combining Queries on Two Sources

To run two queries at the same time, combine them with `ZQuery#zipPar`, or its symbolic alias `<&>`. The result is a tuple of both results. A user and the number of their orders live in two services, so we ask both:

```scala mdoc
runAndShow(userName(1) <&> orderCount(1))
```

Each service receives one call. ZIO Query groups the requests of a query by data source, and it runs the groups of different data sources in parallel. The sequential combinator `ZQuery#zip` would send the second request only after the first one completes, and so it would make the page as slow as the sum of both services.

## Loading a Table from Several Sources

A dashboard shows many rows, and every row needs data from several sources. To build one row, we zip the two lookups with `ZQuery#zipWithPar`, which combines the results with a function. To build the table, we use `ZQuery.foreachPar`:

```scala mdoc:silent
final case class Row(name: String, orders: Int)

def row(id: Int): ZQuery[Any, Nothing, Row] =
  userName(id).zipWithPar(orderCount(id))(Row(_, _))

def table(ids: List[Int]): ZQuery[Any, Nothing, List[Row]] =
  ZQuery.foreachPar(ids)(row)
```

Even though the query for each row is written separately, the requests of all rows join the same batch for their data source:

```scala mdoc
runAndShow(table(List(1, 2, 3)))
```

Each service received one call with three requests, instead of three calls with one request each. The number of calls depends on the number of sequential steps in the query, and not on the number of rows, see [How to Batch N + 1 Queries](./how-to-batch-n-plus-one-queries.md).

## Keeping a Failing Source from Failing the Page

A failure in one branch of `ZQuery#zipPar` fails the whole query. For a dashboard that is often too strict: users and orders are more important than the stock level, and the page should still show them when the inventory service rejects a request. To turn the failure into a value, call `ZQuery#either` on the branch that may fail:

```scala mdoc:silent
def page(ids: List[Int], sku: String): ZQuery[Any, Nothing, (List[Row], Either[String, Int])] =
  table(ids) <&> stock(sku).either
```

The rows still arrive when the inventory source fails for the SKU, and the failure is part of the result:

```scala mdoc
runAndShow(page(List(1, 2), "gadget"))

runAndShow(page(List(1, 2), "widget"))
```

The query calls all three sources in both runs. The failure of one request does not change the results of the other requests in the same batch, see [Error Handling](./error-handling.md) for the other operators that recover from failures, for example `ZQuery#catchAll` with a default value.

## Verifying That the Sources Run at the Same Time

The log shows how many calls each source receives, but not that the calls overlap. To prove it, we use two data sources that each wait for the other to arrive before they answer. If the query ran them one after the other, the first call would wait forever. The helper `meeting` counts the arrivals in a `Ref` and completes when the count reaches two:

```scala mdoc:silent
val arrivals: Ref[Int] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val meeting: UIO[Unit] =
  arrivals.update(_ + 1) *> arrivals.get.repeatUntil(_ == 2).unit

val meetingUsers: DataSource[Any, GetUserName] =
  DataSource.fromFunctionBatchedZIO[Any, Nothing, GetUserName, String]("MeetingUsers") { requests =>
    meeting.as(requests.map(request => s"user-${request.id}"))
  }

val meetingOrders: DataSource[Any, GetOrderCount] =
  DataSource.fromFunctionBatchedZIO[Any, Nothing, GetOrderCount, Int]("MeetingOrders") { requests =>
    meeting.as(requests.map(request => request.id * 10))
  }
```

Because `ZQuery#zipPar` sends the two requests at the same time, both data sources are running when the other arrives, and the query completes:

```scala mdoc
exec(
  (ZQuery.fromRequest(GetUserName(1))(meetingUsers) <&> ZQuery.fromRequest(GetOrderCount(1))(meetingOrders)).run
)
```

With `ZQuery#zip` (`<*>`) in place of `<&>`, the second request exists only after the first one has completed, and the first data source would never see its partner.

## Choosing the Faster of Two Replicas

Redundant copies of a service are another reason to use several sources for the same request type. `DataSource#race` returns a data source that sends every batch to both sources, uses the results of the first to complete, and interrupts the other. In the following block, the second replica never answers, as `DataSource.never` does, so the first one wins:

```scala mdoc:silent
val replicated: DataSource[Any, GetUserName] =
  users.race(DataSource.never)
```

A query on the replicated source completes although one replica is silent:

```scala mdoc
exec(ZQuery.fromRequest(GetUserName(1))(replicated).run)
```

`DataSource#race` completes with whichever source answers first, even if some of its results are failures, so use it for sources that are interchangeable.

## Going Further

- To cap the number of fibers that a parallel query uses, call `ZQuery#withParallelism`, as described in [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md#limiting-parallelism).
- To bound the size of the batches a source receives, use `DataSource#batchN`, see [Aspects](./aspects.md#limiting-the-batch-size).
- To bound how long the page may take, see [How to Time Out Slow Queries](./how-to-time-out-slow-queries.md).
