---
id: creating-queries
title: "Creating Queries"
description: "Build a ZQuery from values, effects, Option, Either, requests, collections and the environment, with runnable examples."
keywords:
  - "Query Construction"
  - "ZQuery Constructors"
  - "Request Batching"
  - "ZQuery"
---

A `ZQuery[R, E, A]` is a description of work, and every query starts from a small constructor on the `ZQuery` companion object. This page shows how to create a query from each kind of source you are likely to have at hand: a plain value, an effect, an `Option` or an `Either`, a request to a data source, a collection, or the environment. For the complete list of constructors and operators, see the [ZQuery](./zquery.md) reference.

The following table maps what you have to the constructor that you need:

| You have                          | Use                                                           | Result type                           |
|-----------------------------------|---------------------------------------------------------------|---------------------------------------|
| A value                           | `ZQuery.succeed`, `ZQuery.succeedNow`                         | `ZQuery[Any, Nothing, A]`             |
| An error                          | `ZQuery.fail`, `ZQuery.failNow`                               | `ZQuery[Any, E, Nothing]`             |
| A `ZIO` effect                    | `ZQuery.fromZIO`                                              | `ZQuery[R, E, A]`                     |
| An `Option`                       | `ZQuery.fromOption`                                           | `ZQuery[Any, Option[Nothing], A]`     |
| An `Either`                       | `ZQuery.fromEither`                                           | `ZQuery[Any, E, A]`                   |
| A request and a data source       | `ZQuery.fromRequest`                                          | `ZQuery[R, E, B]`                     |
| Several requests                  | `ZQuery.fromRequests`, `ZQuery.fromRequestsWith`              | `ZQuery[R, E, Chunk[B]]` or `List[B]` |
| A collection of values or queries | `ZQuery.foreach`, `ZQuery.foreachPar`, `ZQuery.collectAllPar` | `ZQuery[R, E, Collection[B]]`         |
| A service in the environment      | `ZQuery.service`, `ZQuery.serviceWith`                        | `ZQuery[R, Nothing, A]`               |

## Setting Up the Examples

The examples use one request type and one data source. The data source records every call it receives, and `show` runs a query and prints its result together with those calls. `exec` runs a query and returns its result as an `Either`, so that failures are visible as values:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUserName(id: Int) extends Request[Nothing, String]

val calls: Ref[Chunk[String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("UserDataSource") { requests =>
    val shape = requests.map(_.map(_.id).mkString("[", ", ", "]")).mkString(" ")
    calls.update(_ :+ s"UserDataSource: $shape") *>
      ZIO.succeed(
        CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, r => Exit.succeed(s"user-${r.id}"))
      )
  }

def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(userDataSource)

def show[A](query: ZQuery[Any, Nothing, A]): Unit = {
  val (result, received) = Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(query.run <*> calls.getAndSet(Chunk.empty)).getOrThrowFiberFailure()
  }
  println(s"result: $result")
  received.foreach(println)
}

def exec[E, A](query: ZQuery[Any, E, A]): Either[E, A] =
  Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(query.run.either <* calls.set(Chunk.empty)).getOrThrowFiberFailure()
  }
```

In the output of `show`, each pair of square brackets is one batch of requests that the data source receives. See [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) for how batches are formed.

## From Pure Values

When the result is already known, `ZQuery.succeed` creates a query that succeeds with it, and `ZQuery.fail` creates a query that fails with an error. Both take their argument by name. `ZQuery.succeedNow` and `ZQuery.failNow` take it by value, which is slightly cheaper when the value is already computed:

```scala mdoc
exec(ZQuery.succeed(42))
exec(ZQuery.fail("not found"))
```

A query built from a value does not call any data source, so it never takes part in batching. Its use is to give a fixed result to code that expects a query, for example as the fallback of a query that can fail.

## From Effects

`ZQuery.fromZIO` lifts a `ZIO` effect into a query, so that a query can read the clock, call a service that is not a data source, or update a `Ref`. The effect runs each time the query runs:

```scala mdoc:silent
val counter: Ref[Int] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val next: ZQuery[Any, Nothing, Int] =
  ZQuery.fromZIO(counter.updateAndGet(_ + 1))
```

Running the query twice runs the effect twice:

```scala mdoc
exec(next)
exec(next)
```

:::note
An effect lifted with `ZQuery.fromZIO` is opaque to ZIO Query, even when it calls a database: it is never batched, pipelined or deduplicated. To get those optimizations, build the query from a request, as shown in [From Requests](#from-requests).
:::

## From Option and Either

`ZQuery.fromEither` fails with the `Left` and succeeds with the `Right`. `ZQuery.fromOption` succeeds with the `Some` and fails with `None` when the option is empty:

```scala mdoc
exec(ZQuery.fromEither(Right(1): Either[String, Int]))
exec(ZQuery.fromEither(Left("bad input"): Either[String, Int]))
exec(ZQuery.fromOption(Some("Ada")))
exec(ZQuery.fromOption(Option.empty[String]))
```

The error type of a query from an option is `Option[Nothing]`, which has the single value `None`. To report a more useful error, map it with `ZQuery#mapError`, or see [Optional Values](./zquery.md#optional-values) for the operators that extract an `Option` from the result of a query.

## From Requests

`ZQuery.fromRequest` takes a request and the data source that answers it. This is the constructor that makes a query optimizable: requests to the same data source are deduplicated, cached and batched when the query runs. [Creating Data Sources](./creating-data-sources.md) shows how to write a data source, and the `userName` helper from the setup is a query created this way:

```scala mdoc
show(userName(1))
```

To submit several requests at once, use `ZQuery.fromRequests`, which takes a `Chunk` or a `List` of requests and returns their results in the same order. `ZQuery.fromRequestsWith` takes values and a function that turns each value into a request:

```scala mdoc
show(ZQuery.fromRequests(List(GetUserName(1), GetUserName(2), GetUserName(3)))(userDataSource))
show(ZQuery.fromRequestsWith(Chunk(4, 5), (id: Int) => GetUserName(id))(userDataSource))
```

The data source receives all requests of each query in one batch. If you do not want a request to use the cache, create it with `ZQuery.fromRequestUncached`.

If the data source returns no result for a request, the query dies with a `QueryFailure`. See [Error Handling](./error-handling.md#queryfailure-a-request-without-a-result).

## From Collections

To run a query for each element of a collection, use `ZQuery.foreach`, `ZQuery.foreachPar` or `ZQuery.foreachBatched`. To run a collection of queries that already exist, use `ZQuery.collectAll`, `ZQuery.collectAllPar` or `ZQuery.collectAllBatched`. The suffix decides how the requests of the elements reach the data source.

With no suffix, the elements are evaluated one after the other and their requests are pipelined: the data source receives one call with one batch for each element:

```scala mdoc
show(ZQuery.foreach(List(1, 2, 3))(userName))
```

With `Par`, the elements are evaluated in parallel and their requests are batched into one batch. This is how ZIO Query avoids the N + 1 problem:

```scala mdoc
show(ZQuery.foreachPar(List(1, 2, 3))(userName))
```

`ZQuery.collectAllPar` does the same for queries that you have already built:

```scala mdoc
show(ZQuery.collectAllPar(List(userName(1), userName(2))))
```

These operators accept lists, sets, arrays and `NonEmptyChunk`, and return the same kind of collection. `ZQuery.foreach` and `ZQuery.collectAll` also accept an `Option`, and the `foreach` family also accepts a `Map`. [Collecting Queries](./zquery.md#collecting-queries) lists which forms exist for which collection.

## From the Environment

A query can read a service from the environment, which becomes part of its `R` type. `ZQuery.service` returns the service, and `ZQuery.serviceWith` applies a function to it:

```scala mdoc:silent
final case class Greeter(greeting: String)

val greeting: ZQuery[Greeter, Nothing, String] =
  ZQuery.serviceWith[Greeter](_.greeting)
```

A query can't run until its environment is provided. `ZQuery#provideEnvironment` supplies a value, together with a description of it that is used to build data source identifiers. The `?` syntax from `Described` adds the description:

```scala mdoc
import zio.query.Described._

exec(greeting.provideEnvironment(ZEnvironment(Greeter("Hello")) ? "Greeter(Hello)"))
```

[Providing the Environment](./zquery.md#providing-the-environment) lists the other ways to access and provide the environment, including layers.

## Next Steps

Once a query is built, combine it with other queries using the operators in [ZQuery](./zquery.md#combining-queries), and run it as described in [Running Queries and Caching](./running_queries.md). To recover from failures, convert errors to defects, or limit how long a query may run, see [Error Handling](./error-handling.md).
