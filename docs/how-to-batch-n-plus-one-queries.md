---
id: how-to-batch-n-plus-one-queries
title: "How to Batch N + 1 Queries"
description: "Replace one database call per row with a single batched call by describing lookups as requests and answering them with a batched data source."
keywords:
  - "N+1 Queries"
  - "Request Batching"
  - "Batched Data Source"
  - "ZQuery"
---

In this guide, we will fix an N + 1 query: code that loads a list of orders with one query and then loads the user of every order with one more query each. By the end, the same code issues exactly two database queries, no matter how many orders there are. We take three steps: describe each user lookup as a request, answer the requests with a data source that has no batching, and then switch to a data source that answers a whole batch with one `IN (...)` query.

## The Problem

A list of N orders needs N users, so the obvious implementation makes one query for the orders and N queries for the users: N + 1 calls in total. Each call has its own network round trip, and the cost grows with the size of the list instead of with the amount of work.

For example, consider loading orders together with the users who placed them, using plain `ZIO`:

```scala mdoc:compile-only
import zio._

final case class User(id: Int, name: String)
final case class Order(id: Int, userId: Int)

def selectOrders: Task[List[Order]] = ???
def selectUser(id: Int): Task[User] = ???

val ordersWithUsers: Task[List[(Order, User)]] =
  selectOrders.flatMap { orders =>
    ZIO.foreachPar(orders) { order =>
      selectUser(order.userId).map(user => (order, user))
    }
  }
```

The code is short and runs the user lookups in parallel, but the database still receives one query per order, and the same user is loaded again for every order they placed. The fix is not a smarter loop: the loop does not know that the lookups could be combined. ZIO Query makes the lookups visible as data, so that the library can combine them.

## Prerequisites

Add ZIO Query to the build:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

We assume that you know how to write a request type and a data source. [Creating Data Sources](./creating-data-sources.md) introduces both, and [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains why combining queries in parallel produces batches.

## Setting Up the Example

The examples use users and orders, and an in-memory `Database` that records every query it receives, so that we can count them. The orders belong to three users, and two of the users have placed two orders each:

```scala mdoc:silent
import zio._
import zio.query._

final case class User(id: Int, name: String)
final case class Order(id: Int, userId: Int)

object Database {
  val log: Ref[Chunk[String]] =
    Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

  private val users = Map(1 -> User(1, "Ada"), 2 -> User(2, "Grace"), 3 -> User(3, "Edsger"))

  private val orders = List(Order(100, 1), Order(101, 2), Order(102, 1), Order(103, 3), Order(104, 2))

  def selectOrders: UIO[List[Order]] =
    log.update(_ :+ "SELECT * FROM orders").as(orders)

  def selectUser(id: Int): UIO[User] =
    log.update(_ :+ s"SELECT * FROM users WHERE id = $id").as(users(id))

  def selectUsers(ids: Chunk[Int]): UIO[Chunk[User]] =
    log.update(_ :+ s"SELECT * FROM users WHERE id IN (${ids.sorted.mkString(", ")})").as(ids.collect(users))
}
```

The helper below runs a query and prints its result, followed by the queries that the database received. The queries are sorted, because the data sources in this guide may run them in parallel:

```scala mdoc:silent
def runAndShow(query: ZQuery[Any, Nothing, List[String]]): Unit = {
  val (result, queries) = Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(query.run <*> Database.log.getAndSet(Chunk.empty)).getOrThrowFiberFailure()
  }
  result.foreach(println)
  println(s"${queries.size} queries:")
  queries.sorted.foreach(println)
}
```

## Describing Lookups as Requests

The first change is to stop calling the database directly. A request is a plain value that describes what we want, here the user with a given id. Because it is a case class, two requests for the same user are equal:

```scala mdoc:silent
final case class GetUser(id: Int) extends Request[Nothing, User]
```

A `ZQuery` for a request needs a data source to answer it. The query does not depend on how the data source is implemented, so we write it once, taking the data source as a parameter. It loads the orders with one effect and then asks for the user of every order in parallel with `ZQuery.foreachPar`:

```scala mdoc:silent
def getUser(id: Int)(users: DataSource[Any, GetUser]): ZQuery[Any, Nothing, User] =
  ZQuery.fromRequest(GetUser(id))(users)

def ordersWithUserNames(users: DataSource[Any, GetUser]): ZQuery[Any, Nothing, List[String]] =
  ZQuery.fromZIO(Database.selectOrders).flatMap { orders =>
    ZQuery.foreachPar(orders) { order =>
      getUser(order.userId)(users).map(user => s"order ${order.id}: ${user.name}")
    }
  }
```

The `ZQuery.flatMap` is needed because the user ids come from the orders. It starts a second step, so the query has two steps: one for the orders and one for all of the users.

## Answering Requests One at a Time

To see what batching saves, we first answer the requests without it. `DataSource.fromFunctionZIO` calls our function once for each request:

```scala mdoc:silent
val oneByOne: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, Nothing, GetUser, User]("UsersOneByOne") { request =>
    Database.selectUser(request.id)
  }
```

Running the query with this data source shows the queries that the database receives:

```scala mdoc
runAndShow(ordersWithUserNames(oneByOne))
```

ZIO Query already removed the repeated lookups: the five orders belong to three users, so the database gets three user queries, not five. It still gets one for every distinct user, because the data source receives a batch of three requests and answers each of them with its own call.

## Answering a Batch with One Query

A data source that has a bulk operation can answer the whole batch at once. `DataSource.fromFunctionBatchedWithZIO` passes our function every request of a batch as a `Chunk`. A database does not promise to return rows in the order of the ids in `IN (...)`, so we also pass a second function that tells ZIO Query which request a result answers, here the `GetUser` for the user's id:

```scala mdoc:silent
val batched: DataSource[Any, GetUser] =
  DataSource.fromFunctionBatchedWithZIO[Any, Nothing, GetUser, User]("UsersBatched")(
    requests => Database.selectUsers(requests.map(_.id)),
    user => GetUser(user.id)
  )
```

The query does not change. We only pass a different data source:

```scala mdoc
runAndShow(ordersWithUserNames(batched))
```

The database now receives two queries: one for the orders and one with `IN (1, 2, 3)` for all users. Adding more orders or more users adds no queries, unless the batch exceeds a limit of the database, which the next section addresses.

:::warning
A data source must return a result for every request it receives. If a requested user has no row, `Database.selectUsers` returns fewer users than requests, and the query dies with a `QueryFailure` for the missing user. [How to Handle Missing Results](./how-to-handle-missing-results.md) shows how to answer such requests with an error or an empty result.
:::

## Limiting the Size of a Batch

Databases and APIs limit how many ids one call accepts. `DataSource#batchN` splits every batch into groups of at most `n` requests, and the data source receives the groups one after the other. To allow at most two ids per query:

```scala mdoc:silent
val limited: DataSource[Any, GetUser] = batched.batchN(2)
```

The three users are now loaded with two queries instead of one:

```scala mdoc
runAndShow(ordersWithUserNames(limited))
```

To apply the same limit to every data source that a query uses without touching the data sources, use the `QueryAspect.maxBatchSize` aspect described in [Aspects](./aspects.md#limiting-the-batch-size).

## Avoiding Accidental Sequencing

Batching only happens for requests that the query combines in parallel. The sequential combinators send the requests of each step to the data source as a separate batch, and `DataSource.fromFunctionBatchedWithZIO` runs one call for each of them. To load the same three users with `ZQuery.foreach` instead of `ZQuery.foreachPar`:

```scala mdoc
runAndShow(ZQuery.foreach(List(1, 2, 3))(id => getUser(id)(batched).map(_.name)))
```

The data source receives all three requests in one call, but as three batches that must run in order, so the database receives three queries again. Choose the combinator by what the query needs:

| Need                                                      | Use                                          |
|-----------------------------------------------------------|----------------------------------------------|
| Independent lookups, effects can run at the same time     | `ZQuery.foreachPar`, `ZQuery#zipPar`         |
| Independent lookups, effects must run one after the other | `ZQuery.foreachBatched`, `ZQuery#zipBatched` |
| Lookups where the next one needs the result of the last   | `ZQuery#flatMap` (one call per step)         |

Prefer the parallel variants, and use `ZQuery#flatMap` only for the step that really depends on an earlier result, as the example did when the user ids came from the orders.

## Going Further

- [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains the batches that a data source receives for every combinator.
- [DataSource Reference](./data-source-reference.md) lists the other `DataSource.fromFunction*` constructors, for example for APIs whose results arrive in request order.
- [How to Cache Query Results](./how-to-cache-query-results.md) shows how to avoid repeating the same lookup across several queries.
