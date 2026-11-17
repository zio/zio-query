---
id: zquery
title: "ZQuery"
description: "Reference for ZQuery[-R, +E, +A]: constructors, transformations, zip and collect operators, environment, resources and summaries."
keywords:
  - "Query Composition"
  - "Request Batching"
  - "Data Access"
  - "ZQuery"
---

A `ZQuery[-R, +E, +A]` is a description of a query: it needs an environment `R`, may fail with an error `E`, may succeed with a value `A`, and may contain requests to one or more data sources. Building a query does not run anything. You build it from small queries with the operators on this page, and ZIO Query decides how the requests inside it are batched, pipelined, deduplicated and cached when you run it.

This page lists the constructors on `ZQuery` and the operators on a `ZQuery` value, grouped by what they do. The topics that have their own pages are not repeated here:

- [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains which operators produce which calls to a data source.
- [Running Queries and Caching](./running_queries.md) covers `run`, `runCache`, `runLog`, `cached`, `uncached` and `memoize`.
- [Error Handling](./error-handling.md) covers recovering from failures, converting errors to defects, timeouts and `race`.
- [Aspects](./aspects.md) covers `@@` and `mapDataSources`.

For a task-oriented introduction to the constructors, see [Creating Queries](./creating-queries.md).

## The Type

`ZQuery` has the same three type parameters as `ZIO`, and the same variance:

```scala
final class ZQuery[-R, +E, +A]
```

`R` is contravariant, so a query that needs less can be used where a query that needs more is expected. `E` and `A` are covariant. Operators that combine two queries widen the types accordingly: the environment of the result is the intersection of both environments (`R1 <: R`), and the error type is the common supertype of both error types (`E1 >: E`).

The `zio.query` package object defines aliases for the most common shapes:

| Alias             | Expands to                  |
|-------------------|-----------------------------|
| `UQuery[+A]`      | `ZQuery[Any, Nothing, A]`   |
| `Query[+E, +A]`   | `ZQuery[Any, E, A]`         |
| `TaskQuery[+A]`   | `ZQuery[Any, Throwable, A]` |
| `URQuery[-R, +A]` | `ZQuery[R, Nothing, A]`     |
| `RQuery[-R, +A]`  | `ZQuery[R, Throwable, A]`   |

## Setting Up the Examples

The examples on this page use two request types and one data source for each. The data sources record the batches they receive, and `show` runs a query and prints its result together with those batches. `exec` runs a query and returns its result as an `Either`, so that failures are visible as values:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUserName(id: Int)   extends Request[Nothing, String]
final case class GetOrderCount(id: Int) extends Request[Nothing, Int]

val calls: Ref[Chunk[String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

def batches[A](requests: Chunk[Chunk[A]])(id: A => Int): String =
  requests.map(_.map(id).mkString("[", ", ", "]")).mkString(" ")

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("UserDataSource") { requests =>
    calls.update(_ :+ s"UserDataSource: ${batches(requests)(_.id)}") *>
      ZIO.succeed(
        CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, r => Exit.succeed(s"user-${r.id}"))
      )
  }

val orderDataSource: DataSource[Any, GetOrderCount] =
  DataSource.make[Any, GetOrderCount]("OrderDataSource") { requests =>
    calls.update(_ :+ s"OrderDataSource: ${batches(requests)(_.id)}") *>
      ZIO.succeed(
        CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, r => Exit.succeed(r.id * 10))
      )
  }

def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(userDataSource)

def orderCount(id: Int): ZQuery[Any, Nothing, Int] =
  ZQuery.fromRequest(GetOrderCount(id))(orderDataSource)

def show[A](query: ZQuery[Any, Nothing, A]): Unit = {
  val (result, received) = Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(query.run <*> calls.getAndSet(Chunk.empty)).getOrThrowFiberFailure()
  }
  println(s"result: $result")
  received.sorted.foreach(println)
}

def exec[E, A](query: ZQuery[Any, E, A]): Either[E, A] =
  Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(query.run.either <* calls.set(Chunk.empty)).getOrThrowFiberFailure()
  }
```

In the output of `show`, each pair of square brackets is one batch of requests that can be performed in parallel. Several pairs in one line are sequential batches of the same call to the data source.

## Constructing Queries

The constructors live on the `ZQuery` companion object. They are grouped below by the source of the value: plain values, effects, `Either` and `Option`, and requests to data sources. Environment accessors and resources have their own sections later on this page.

### Pure Values and Failures

The constructors below do not need an environment and do not talk to a data source. Most come in two forms: a lazy one that takes its argument by name, and an eager `...Now` one that takes it by value. The eager form is slightly cheaper, and it is the right choice when the value is already computed and creating it has no side effect.

| Constructor                     | Result type                             | Behavior                  |
|---------------------------------|-----------------------------------------|---------------------------|
| `succeed(value: => A)`          | `ZQuery[Any, Nothing, A]`               | Succeeds with the value   |
| `succeedNow(value: A)`          | `ZQuery[Any, Nothing, A]`               | Eager form of `succeed`   |
| `unit`                          | `ZQuery[Any, Nothing, Unit]`            | Succeeds with `()`        |
| `some(a: => A)`                 | `ZQuery[Any, Nothing, Option[A]]`       | Succeeds with `Some(a)`   |
| `none`                          | `ZQuery[Any, Nothing, Option[Nothing]]` | Succeeds with `None`      |
| `fail(error: => E)`             | `ZQuery[Any, E, Nothing]`               | Fails with the error      |
| `failNow(error: E)`             | `ZQuery[Any, E, Nothing]`               | Eager form of `fail`      |
| `failCause(cause: => Cause[E])` | `ZQuery[Any, E, Nothing]`               | Fails with the cause      |
| `failCauseNow(cause: Cause[E])` | `ZQuery[Any, E, Nothing]`               | Eager form of `failCause` |
| `die(t: => Throwable)`          | `ZQuery[Any, Nothing, Nothing]`         | Dies with the defect      |
| `never`                         | `ZQuery[Any, Nothing, Nothing]`         | Never completes           |

To run three of them and see their results:

```scala mdoc
exec(ZQuery.succeed(42))
exec(ZQuery.fail("not found"))
exec(ZQuery.some("Ada"))
```

`ZQuery.die` and `ZQuery.failCause` produce queries that fail with a defect or with a full `Cause`. See [Error Handling](./error-handling.md) for what a defect is and how to recover from it.

### From Effects

`ZQuery.fromZIO` turns a `ZIO` effect into a query. The effect is created when the query runs, and it runs once for every run of the query. The following definitions create a counter and a query that increments it:

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

| Constructor                                  | Result type       | Behavior                                     |
|----------------------------------------------|-------------------|----------------------------------------------|
| `fromZIO(effect: => ZIO[R, E, A])`           | `ZQuery[R, E, A]` | Runs the effect, creating it at run time     |
| `fromZIONow(effect: ZIO[R, E, A])`           | `ZQuery[R, E, A]` | Eager form of `fromZIO`, for library authors |
| `unwrap(zio: => ZIO[R, E, ZQuery[R, E, A]])` | `ZQuery[R, E, A]` | Runs the effect, then the query it returns   |
| `suspend(query: => ZQuery[R, E, A])`         | `ZQuery[R, E, A]` | Builds the query again for every run         |

`ZQuery.suspend` delays building the query itself. Its argument is evaluated again every time the query runs, which is what you want when constructing the query has a side effect or depends on mutable state. The next example builds two queries from a mutable counter: `eager` captures the counter once, when it is defined, while `deferred` reads it again for every run:

```scala mdoc:silent
val created = new java.util.concurrent.atomic.AtomicInteger(0)

val eager: ZQuery[Any, Nothing, Int]    = ZQuery.succeedNow(created.incrementAndGet())
val deferred: ZQuery[Any, Nothing, Int] = ZQuery.suspend(ZQuery.succeedNow(created.incrementAndGet()))
```

Running each query twice shows the difference:

```scala mdoc
exec(eager)
exec(eager)
exec(deferred)
exec(deferred)
```

`ZQuery.unwrap` is for a query that is itself produced by an effect, for example when the effect decides which query to run. It is `ZQuery.fromZIO` followed by `ZQuery#flatten`. In the following example the counter decides which query runs:

```scala mdoc
exec(ZQuery.unwrap(counter.get.map(n => if (n > 0) userName(n) else ZQuery.succeed("nobody"))))
```

### From Either and Option

`ZQuery.fromEither` fails with the `Left` and succeeds with the `Right`. `ZQuery.fromOption` succeeds with the `Some` and fails with `None` when the option is empty, so its error type is `Option[Nothing]`:

```scala mdoc
exec(ZQuery.fromEither(Right(1): Either[String, Int]))
exec(ZQuery.fromEither(Left("bad input"): Either[String, Int]))
exec(ZQuery.fromOption(Some(1)))
exec(ZQuery.fromOption(Option.empty[Int]))
```

Both constructors evaluate their argument lazily, each time the query runs. To turn the `None` into a more descriptive error, use `ZQuery#mapError`, or build the query with `ZQuery#someOrFail` as shown in [Optional Values](#optional-values).

### From Requests

The constructors that send requests to a data source are the way to get optimized queries: a request is deduplicated, cached, and batched with the other requests of the same data source. Requests that are not constructed with one of these methods are opaque to ZIO Query. See [Creating Data Sources](./creating-data-sources.md) for how to write a data source and [DataSource Reference](./data-source-reference.md) for the ready-made constructors.

| Constructor                                                                 | Result type              | Behavior                                      |
|-----------------------------------------------------------------------------|--------------------------|-----------------------------------------------|
| `fromRequest(request: => A)(dataSource: => DataSource[R, A])`               | `ZQuery[R, E, B]`        | One request; deduplicated, cached and batched |
| `fromRequestUncached(request: => A)(dataSource: => DataSource[R, A])`       | `ZQuery[R, E, B]`        | One request; skips the cache                  |
| `fromRequests(requests: Chunk[A])(dataSource: DataSource[R, A])`            | `ZQuery[R, E, Chunk[B]]` | Several requests in one batch                 |
| `fromRequests(requests: List[A])(dataSource: DataSource[R, A])`             | `ZQuery[R, E, List[B]]`  | Several requests in one batch                 |
| `fromRequestsWith(as: Chunk[In], f: In => A)(dataSource: DataSource[R, A])` | `ZQuery[R, E, Chunk[B]]` | Builds a request from each value              |
| `fromRequestsWith(as: List[In], f: In => A)(dataSource: DataSource[R, A])`  | `ZQuery[R, E, List[B]]`  | Builds a request from each value              |

If the data source returns no result for a request, the query dies with a `QueryFailure`. See [Error Handling](./error-handling.md#queryfailure-a-request-without-a-result).

`ZQuery.fromRequests` and `ZQuery.fromRequestsWith` submit several requests at once and return their results in the order of the requests, without the need for `ZQuery.foreachPar`:

```scala mdoc
show(ZQuery.fromRequests(List(GetUserName(1), GetUserName(2), GetUserName(3)))(userDataSource))
show(ZQuery.fromRequestsWith(Chunk(4, 5), (id: Int) => GetOrderCount(id))(orderDataSource))
```

## Transforming Queries

The operators in this section change the value or the error of a query, or extract one kind of value from another, without combining it with a second query.

### Mapping Values and Errors

| Operator                                            | Result type               | Behavior                            |
|-----------------------------------------------------|---------------------------|-------------------------------------|
| `map(f: A => B)`                                    | `ZQuery[R, E, B]`         | Transforms the value                |
| `mapError(f: E => E1)`                              | `ZQuery[R, E1, A]`        | Transforms the typed error          |
| `mapBoth(f: E => E1, g: A => B)`                    | `ZQuery[R, E1, B]`        | Transforms the error and the value  |
| `mapBothCause(f: Cause[E] => Cause[E1], g: A => B)` | `ZQuery[R, E1, B]`        | Transforms the cause and the value  |
| `mapErrorCause(h: Cause[E] => Cause[E2])`           | `ZQuery[R, E2, A]`        | Transforms the whole cause          |
| `mapZIO(f: A => ZIO[R1, E1, B])`                    | `ZQuery[R1, E1, B]`       | Transforms the value with an effect |
| `as(b: => B)`                                       | `ZQuery[R, E, B]`         | Replaces the value with a constant  |
| `asSomeError`                                       | `ZQuery[R, Option[E], A]` | Wraps the error in `Some`           |

`map` and `mapError` change one channel. `mapBoth` changes both in one step, and `mapBothCause` and `mapErrorCause` give you the full `Cause`, which preserves defects and the structure of parallel failures. `mapZIO` applies an effectful function, such as a call to a service that is not a data source:

```scala mdoc
exec(userName(1).map(_.toUpperCase))
exec(ZQuery.fail("not found").mapError(_.length))
exec(ZQuery.fail("not found").mapBoth(_.length, (_: Int) + 1))
exec(userName(2).mapZIO(name => ZIO.succeed(name.length)))
exec(userName(3).as("constant"))
exec(ZQuery.fail("not found").asSomeError)
```

`mapBoth`, `mapError` and the operators that depend on a failing query need an implicit `CanFail[E]`, so they do not compile on a query that cannot fail, such as `ZQuery.succeed(1)`.

### Chaining Queries

`flatMap` runs a query that depends on the result of the previous one. Because the second query is only known after the first completes, its requests cannot be batched with the requests of the first one: they go in a later step. For queries that do not depend on each other, prefer the `zip` family from [Combining Queries](#combining-queries). `flatten` removes one level of nesting from a `ZQuery[R, E, ZQuery[R1, E1, B]]`:

```scala mdoc
show(userName(1).flatMap(name => orderCount(name.length)))
exec(ZQuery.succeed(ZQuery.succeed(1)).flatten)
```

### Optional Values

A query that produces an `Option` is easy to work with once the `Option` is moved to where it matters. `ZQuery#some` moves the `None` into the error channel, `ZQuery#someOrElse`, `ZQuery#someOrElseZIO` and `ZQuery#someOrFail` replace it, and `ZQuery#unoption` moves an `Option` in the error channel into the value channel, which is the inverse of `ZQuery#some`.

| Operator                                    | Requires         | Result type                |
|---------------------------------------------|------------------|----------------------------|
| `some`                                      | `A = Option[B]`  | `ZQuery[R, Option[E], B]`  |
| `someOrElse(default: => B)`                 | `A = Option[B]`  | `ZQuery[R, E, B]`          |
| `someOrElseZIO(default: ZQuery[R1, E1, B])` | `A = Option[B]`  | `ZQuery[R1, E1, B]`        |
| `someOrFail(e: => E1)`                      | `A = Option[B]`  | `ZQuery[R, E1, B]`         |
| `unoption`                                  | `E = Option[E1]` | `ZQuery[R, E1, Option[A]]` |

`ZQuery#someOrElseZIO` takes a `ZQuery`, not a `ZIO`, despite its name, and it takes it by value, so the default query is constructed even when it is not used. To apply each operator to a small query:

```scala mdoc
exec(ZQuery.succeed(Option(1)).some)
exec(ZQuery.succeed(Option.empty[Int]).some)
exec(ZQuery.succeed(Option.empty[Int]).someOrElse(0))
exec(ZQuery.succeed(Option.empty[String]).someOrElseZIO(userName(7)))
exec(ZQuery.succeed(Option.empty[Int]).someOrFail("missing"))
exec(ZQuery.fail(Option.empty[String]).unoption)
```

`optional` has a different purpose from the operators above, despite the similar name. It does not look at the value: it converts a `QueryFailure`, that is, a request that the data source did not answer, into `None`. See [Error Handling](./error-handling.md#queryfailure-a-request-without-a-result).

### Working With Either

`ZQuery#absolve` is the inverse of `either`: it moves the `Left` of an `Either` value into the error channel. `ZQuery#left` and `ZQuery#right` zoom in on one side of an `Either` value and move the other side to the error channel, and `ZQuery#unleft` and `ZQuery#unright` are their inverses.

| Operator                                           | Requires               | Result type                   |
|----------------------------------------------------|------------------------|-------------------------------|
| `absolve`                                          | `A = Either[E1, B]`    | `ZQuery[R, E1, B]`            |
| `ZQuery.absolve(v: => ZQuery[R, E, Either[E, A]])` | Any query of that type | `ZQuery[R, E, A]`             |
| `left`                                             | `A = Either[B, C]`     | `ZQuery[R, Either[E, C], B]`  |
| `right`                                            | `A = Either[B, C]`     | `ZQuery[R, Either[B, E], C]`  |
| `unleft`                                           | `E = Either[E1, B]`    | `ZQuery[R, E1, Either[A, B]]` |
| `unright`                                          | `E = Either[B, E1]`    | `ZQuery[R, E1, Either[B, A]]` |

To apply the operators to small queries:

```scala mdoc
exec(ZQuery.succeed(Left("oops"): Either[String, Int]).absolve)
exec(ZQuery.succeed(Right(1): Either[String, Int]).right)
exec(ZQuery.succeed(Left("oops"): Either[String, Int]).right)
exec(ZQuery.fail(Right(2): Either[String, Int]).unleft)
```

The operators that recover from errors, such as `catchAll`, `fold`, `either` and `sandbox`, are covered in [Error Handling](./error-handling.md#recovering-from-errors).

## Combining Queries

Two independent queries can be combined in three ways, and the way matters, because it determines how their requests reach the data sources. The names follow `ZIO`: no suffix means sequential, `Par` means parallel, and `Batched` means batched.

| Result           | Sequential       | Parallel            | Batched                 |
|------------------|------------------|---------------------|-------------------------|
| Tuple of both    | `zip`, `<*>`     | `zipPar`, `<&>`     | `zipBatched`, `<~>`     |
| Left value       | `zipLeft`, `<*`  | `zipParLeft`, `<&`  | `zipBatchedLeft`, `<~`  |
| Right value      | `zipRight`, `*>` | `zipParRight`, `&>` | `zipBatchedRight`, `~>` |
| Function of both | `zipWith`        | `zipWithPar`        | `zipWithBatched`        |

Each form has a `Left` and a `Right` variant that keeps the result of one side only. Every operator except the `zipWith` ones also has a symbolic alias, listed after the name in the table. The ampersand marks the parallel forms and the tilde marks the batched forms.

The three forms differ in how the two sides are evaluated and in what the data source sees:

- The sequential forms evaluate the left side first, then the right side. The requests are pipelined: the data source receives them in one call, as separate batches that must run in order. If the left side fails, the right side is not evaluated.
- The parallel forms evaluate both sides concurrently, on separate fibers. The requests are batched: the data source receives them in one batch. If both sides fail, the causes are combined with `Cause.Both`.
- The batched forms put the requests of both sides in one batch, like the parallel forms, but they evaluate the two sides one after the other, without forking a fiber for each.

To see the difference in the calls, combine two user lookups three ways:

```scala mdoc
show(userName(1).zip(userName(2)))
show(userName(1).zipPar(userName(2)))
show(userName(1).zipBatched(userName(2)))
```

The sequential form sends two batches of one request each, in one call. The other two send one batch of two requests. Requests to different data sources are batched per data source, so combining a user and an order lookup gives each data source one call:

```scala mdoc
show(userName(1) <&> orderCount(1))
```

The left and right variants combine the queries in the same way and discard one result:

```scala mdoc
show(userName(1) <& orderCount(1))
show(userName(1) &> orderCount(1))
```

The tuple results of `zip`, `zipPar` and `zipBatched` are flattened, so chaining them produces one tuple, not nested ones:

```scala mdoc
exec(ZQuery.succeed(1).zip(ZQuery.succeed("a")).zip(ZQuery.succeed(true)))
```

`zipWith`, `zipWithPar` and `zipWithBatched` combine the two results with a function instead of building a tuple:

```scala mdoc
exec(ZQuery.succeed(6).zipWith(ZQuery.succeed(7))(_ * _))
```

## Collecting Queries

To run a query for each element of a collection, or to run a collection of queries, use `ZQuery.foreach` or `collectAll`. `collectAll(queries)` is `foreach(queries)(identity)`. Both families come in the same three forms as the `zip` operators: sequential, `Par` and `Batched`. `ZQuery.foreachPar` is the fix for the N + 1 problem: all requests of the collection are sent to the data source in one batch.

| Methods                                            | Parameters                                     | Result type                                      |
|----------------------------------------------------|------------------------------------------------|--------------------------------------------------|
| `foreach`, `foreachPar`, `foreachBatched`          | `(as: Collection[A])(f: A => ZQuery[R, E, B])` | `ZQuery[R, E, Collection[B]]`                    |
| `collectAll`, `collectAllPar`, `collectAllBatched` | `(as: Collection[ZQuery[R, E, A]])`            | `ZQuery[R, E, Collection[A]]`                    |
| `partitionQuery`, `partitionQueryPar`              | `(as: Iterable[A])(f: A => ZQuery[R, E, B])`   | `ZQuery[R, Nothing, (Iterable[E], Iterable[B])]` |

The sequential and batched forms evaluate the elements one after the other. The parallel forms evaluate them concurrently, and when several fail, they combine the causes. All forms return the results in the order of the input. The following examples use the same lookups with each form:

```scala mdoc
show(ZQuery.foreach(List(1, 2, 3))(userName))
show(ZQuery.foreachPar(List(1, 2, 3))(userName))
show(ZQuery.foreachBatched(List(1, 2, 3))(userName))
show(ZQuery.collectAllPar(List(userName(1), userName(2))))
```

The first form has three batches in one call: the lookups are pipelined. The last three form one batch each.

Besides lists, the operators accept the following inputs and return the same kind of collection: `Set`, `Array` (which needs a `ClassTag` for the result), `NonEmptyChunk`, and any other `Iterable` that has a `BuildFrom` instance, such as `Vector`, `Chunk` or `List`. `ZQuery.foreach` and its siblings on a `Map` take a function from a key and a value to a pair, and return a `Map`:

```scala mdoc
show(ZQuery.foreachPar(Map(1 -> "a", 2 -> "b")) { (id, tag) => userName(id).map(name => (tag, name)) })
show(ZQuery.foreach(Option(3))(userName))
```

`ZQuery.partitionQuery` and `ZQuery.partitionQueryPar` run a query for each element and split the outcome into the errors and the successes, instead of failing on the first error. The result query cannot fail:

```scala mdoc:silent
def checkedUser(id: Int): ZQuery[Any, String, String] =
  if (id % 2 == 0) ZQuery.fail(s"user $id is not allowed") else userName(id)
```

Partitioning four ids gives the failures and the successes as two collections:

```scala mdoc
exec(ZQuery.partitionQuery(List(1, 2, 3, 4))(checkedUser))
```

:::note
`ZQuery` has no `mergeAll` or `reduceAll`. To combine the results of a collection with an operator, collect them with `ZQuery.foreachPar` or `ZQuery.collectAllPar` and fold the resulting collection with a `map`.
:::

## Providing the Environment

A query that needs an environment can't run until you provide it. The operators below remove all or part of the requirement. Each takes its argument wrapped in `Described`, a value paired with a `String` description. The description is used to build the identifier of data sources whose environment is provided, so that equal values have equal identifiers and their requests can still be batched together. The `?` syntax from `Described` adds a description to any value. See [Aspects](./aspects.md#describing-values-with-described) for the rules.

| Operator                     | Argument                                         | Result type         |
|------------------------------|--------------------------------------------------|---------------------|
| `provideEnvironment`         | `Described[ZEnvironment[R]]`                     | `ZQuery[Any, E, A]` |
| `provideSomeEnvironment[R0]` | `Described[ZEnvironment[R0] => ZEnvironment[R]]` | `ZQuery[R0, E, A]`  |
| `provideLayer[E1 >: E, R0]`  | `Described[ZLayer[R0, E1, R]]`                   | `ZQuery[R0, E1, A]` |
| `provideSomeLayer[R0]`       | `Described[ZLayer[R0, E1, R1]]`                  | `ZQuery[R0, E1, A]` |

To read the environment, use the accessors on `ZQuery`:

| Constructor                 | Argument                                | Result type                           |
|-----------------------------|-----------------------------------------|---------------------------------------|
| `environment[R]`            | None                                    | `ZQuery[R, Nothing, ZEnvironment[R]]` |
| `environmentWith[R]`        | `f: ZEnvironment[R] => A`               | `ZQuery[R, Nothing, A]`               |
| `environmentWithQuery[R]`   | `f: ZEnvironment[R] => ZQuery[R, E, A]` | `ZQuery[R, E, A]`                     |
| `environmentWithZIO[R]`     | `f: ZEnvironment[R] => ZIO[R, E, A]`    | `ZQuery[R, E, A]`                     |
| `service[R]`                | None                                    | `ZQuery[R, Nothing, R]`               |
| `serviceWith[R]`            | `f: R => A`                             | `ZQuery[R, Nothing, A]`               |
| `serviceWithQuery[Service]` | `f: Service => ZQuery[R, E, A]`         | `ZQuery[R with Service, E, A]`        |
| `serviceWithZIO[Service]`   | `f: Service => ZIO[R, E, A]`            | `ZQuery[R with Service, E, A]`        |

The following example reads a service from the environment with `ZQuery.serviceWith` and `ZQuery.serviceWithQuery`. `Prefix` is a service that is looked up by its type:

```scala mdoc:silent
import zio.query.Described._

final case class Prefix(value: String)

val greeting: ZQuery[Prefix, Nothing, String] =
  ZQuery.serviceWith[Prefix](_.value + ", ")
    .zipWith(ZQuery.serviceWithQuery[Prefix](p => userName(p.value.length)))(_ + _)

val prefixLayer: Described[ULayer[Prefix]] =
  ZLayer.succeed(Prefix("Hello")) ? "Prefix(Hello)"
```

Providing the `Prefix` as an environment or as a layer gives the same result:

```scala mdoc
exec(greeting.provideEnvironment(ZEnvironment(Prefix("Hello")) ? "Prefix(Hello)"))
exec(greeting.provideLayer(prefixLayer))
```

`ZQuery#provideSomeLayer` and `ZQuery#provideSomeEnvironment` provide part of the environment and leave the rest. The type argument of `ZQuery#provideSomeLayer` is the part that remains. A query that needs a `Prefix` and an `Int`, read with `ZQuery.service`, can have its `Prefix` provided with `provideSomeLayer[Int]`, which gives a query that needs an `Int`:

```scala mdoc:silent
val needsBoth: ZQuery[Prefix with Int, Nothing, String] =
  for {
    prefix <- ZQuery.service[Prefix]
    n      <- ZQuery.service[Int]
  } yield s"${prefix.value} $n"

val needsInt: ZQuery[Int, Nothing, String] =
  needsBoth.provideSomeLayer[Int](prefixLayer)
```

Providing the remaining `Int` makes the query runnable:

```scala mdoc
exec(needsInt.provideEnvironment(ZEnvironment(42) ? "42"))
```

For the data sources that need an environment, see [DataSource Reference](./data-source-reference.md).

## Resources and Finalizers

A query can acquire a resource before it starts and release it after it completes, whether it succeeds, fails or is interrupted. `ZQuery.acquireReleaseWith` takes three argument lists: the effect that acquires the resource, a function that releases it, and a function that builds the query that uses it. `ZQuery.acquireReleaseExitWith` is the same, except that the release function also receives the `Exit` of the query, so it can behave differently on success and on failure. The resource is acquired and released by `ZIO` effects, not queries.

| Operator                        | Arguments                                                                                             | Result type         |
|---------------------------------|-------------------------------------------------------------------------------------------------------|---------------------|
| `ZQuery.acquireReleaseWith`     | `(acquire: => ZIO[R, E, A])(release: A => URIO[R1, Any])(use: A => ZQuery[R1, E1, B])`                | `ZQuery[R1, E1, B]` |
| `ZQuery.acquireReleaseExitWith` | `(acquire: => ZIO[R, E, A])(release: (A, Exit[E1, B]) => URIO[R1, Any])(use: A => ZQuery[R1, E1, B])` | `ZQuery[R1, E1, B]` |
| `ensuring`                      | `(finalizer: => ZIO[R1, Nothing, Any])`                                                               | `ZQuery[R1, E, A]`  |

The following example records what happens in a `Ref`. The release function runs after the query that uses the resource, and it runs on failure too:

```scala mdoc:silent
val events: Ref[Chunk[String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[String]))

def record(event: String): UIO[Unit] = events.update(_ :+ event)

def withConnection[E, A](use: String => ZQuery[Any, E, A]): ZQuery[Any, E, A] =
  ZQuery.acquireReleaseWith(record("open").as("connection"))(_ => record("close"))(use)
```

Running the query with a successful and a failing `use` function shows that the resource is released both times:

```scala mdoc
exec(withConnection(connection => ZQuery.succeed(s"used $connection")))
exec(withConnection(_ => ZQuery.fail("boom")))
exec(ZQuery.fromZIO(events.getAndSet(Chunk.empty)))
```

`ZQuery.acquireReleaseExitWith` needs the types of the `Exit` in the release function written out, because the compiler can't infer them from the function:

```scala mdoc:silent
def withLoggedExit(use: String => ZQuery[Any, String, Int]): ZQuery[Any, String, Int] =
  ZQuery.acquireReleaseExitWith(ZIO.succeed("connection"))(
    (_: String, exit: Exit[String, Int]) => record(s"released with success = ${exit.isSuccess}")
  )(use)
```

Running it with a successful and a failing query shows the `Exit` that the release function receives:

```scala mdoc
exec(withLoggedExit(_ => ZQuery.succeed(1)))
exec(withLoggedExit(_ => ZQuery.fail("boom")))
exec(ZQuery.fromZIO(events.getAndSet(Chunk.empty)))
```

`ensuring` attaches a finalizer to an existing query, when there is no resource to acquire. The finalizer is a `ZIO` effect that cannot fail. It runs only if the query starts executing, so a query that is never run does not run its finalizer:

```scala mdoc:silent
val finalized: ZQuery[Any, Nothing, String] =
  userName(1).ensuring(record("finalizer ran"))
```

Running the query runs its finalizer:

```scala mdoc
exec(finalized)
exec(ZQuery.fromZIO(events.getAndSet(Chunk.empty)))
```

## Summarizing Queries

`summarized` runs an effect before the query and again after it, and combines the two results with a function. It returns the summary together with the value of the query. This is the way to measure what a query did, for example how many calls it made to the data sources. `timed` is `summarized` with the clock, and returns the duration:

| Operator     | Parameters                                   | Result type                   |
|--------------|----------------------------------------------|-------------------------------|
| `summarized` | `(summary0: ZIO[R1, E1, B])(f: (B, B) => C)` | `ZQuery[R1, E1, (C, A)]`      |
| `timed`      | None                                         | `ZQuery[R, E, (Duration, A)]` |

The first argument of `summarized` is a `ZIO` effect, and the second is the function that combines the value measured before with the value measured after. The following query counts how many calls the data sources received while it was running:

```scala mdoc:silent
val callCount: UIO[Int] = calls.get.map(_.size)
```

Comparing a parallel query with a sequential one shows the difference in the number of calls:

```scala mdoc
exec(ZQuery.foreachPar(List(1, 2, 3))(userName).summarized(callCount)((before, after) => after - before))
exec(userName(1).flatMap(_ => userName(2)).summarized(callCount)((before, after) => after - before))
```

Fetching three users with `ZQuery.foreachPar` made one call to the data source. Fetching two users where the second lookup follows the first with `flatMap` made two calls, because each step of the query is a separate call.

## Related Operators

The remaining operators of `ZQuery` are covered on other pages:

| Operators                                                                                                                                              | What they do                                  | Page                                                                               |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------|------------------------------------------------------------------------------------|
| `run`, `runCache`, `runLog`                                                                                                                            | Convert a query to a `ZIO` effect             | [Running Queries and Caching](./running_queries.md)                                |
| `cached`, `uncached`, `memoize`                                                                                                                        | Control caching and memoization               | [Running Queries and Caching](./running_queries.md#disabling-and-enabling-caching) |
| `catchAll`, `catchAllCause`, `catchAllZIO`, `catchAllCauseZIO`, `fold`, `foldQuery`, `foldZIO`, `foldCauseQuery`, `foldCauseZIO`, `either`, `optional` | Recover from failures                         | [Error Handling](./error-handling.md#recovering-from-errors)                       |
| `sandbox`, `sandboxWith`, `ZQuery.unsandbox`                                                                                                           | Work with the full `Cause`                    | [Error Handling](./error-handling.md#working-with-the-full-cause)                  |
| `orDie`, `orDieWith`, `refineOrDie`, `refineOrDieWith`, `unrefine`, `unrefineTo`, `unrefineWith`                                                       | Convert between errors and defects            | [Error Handling](./error-handling.md#converting-between-errors-and-defects)        |
| `timeout`, `timeoutFail`, `timeoutFailCause`, `timeoutTo`, `race`                                                                                      | Bound the time of a query or race two queries | [Error Handling](./error-handling.md#timeouts)                                     |
| `@@`, `mapDataSources`                                                                                                                                 | Apply aspects to a query or its data sources  | [Aspects](./aspects.md#query-aspects)                                              |
| `withParallelism`, `withParallelismUnbounded`                                                                                                          | Limit the fibers a query uses                 | [How Batching Works](./how-batching-works.md#limiting-parallelism)                 |

## Summary

The constructors on `ZQuery` create queries from values, failures, effects, `Either`, `Option`, requests, resources and the environment. The instance operators transform the result and error of a query, combine two queries in one of three forms (sequential, parallel or batched), and collect many queries with `ZQuery.foreach` and `collectAll`. For the three forms, the suffix of the name tells you how requests reach the data source: no suffix pipelines them, `Par` batches them and runs the two sides concurrently, and `Batched` batches them without forking. To run the query, continue with [Running Queries and Caching](./running_queries.md).
