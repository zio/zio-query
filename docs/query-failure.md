---
id: query-failure
title: "QueryFailure"
description: "Reference for QueryFailure: the defect that a query dies with when a data source returns no result for a request, and how to detect it."
keywords:
  - "Query Failure"
  - "Missing Request Result"
  - "Data Source Defect"
  - "QueryFailure"
---

A `QueryFailure` is the defect that a query dies with when a data source does not return a result for a request that it received. It names the data source and the request, so it points at the place where the bug is. This page describes what a `QueryFailure` carries, which situations produce one, how to detect it and how to fix the data source. [Error Handling](./error-handling.md#queryfailure-a-request-without-a-result) shows it next to the other kinds of failures of a query.

## The Type

`QueryFailure` is a case class that extends `Throwable`:

```scala
final case class QueryFailure(dataSource: DataSource[Nothing, Nothing], request: Request[_, _])
    extends Throwable(null, null, true, false) {
  override def getMessage: String =
    s"Data source ${dataSource.identifier} did not complete request ${request.toString}."
}
```

The class has these properties:

- `dataSource` is the data source that received the request. Its type is `DataSource[Nothing, Nothing]`, so any data source fits, and the useful member is `DataSource#identifier`.
- `request` is the request that the data source left unanswered.
- The message names both. It is built when `getMessage` is called, so the exception has no message of its own.
- The last two arguments of the `Throwable` constructor enable suppression and disable the stack trace. A `QueryFailure` has no stack trace, which keeps it cheap to create. The place to look is the data source, which the message names.
- As a case class it has structural equality: two failures are equal if their data sources have equal identifiers and their requests are equal.

## Setting Up an Example

The examples on this page use one request type and a data source that forgets to answer the request for user 99. The helper `run` runs a query and returns its `Exit`, and `queryFailures` and `defectMessages` pick the defects out of it:

```scala mdoc:silent
import zio._
import zio.query._
import zio.query.Described._

final case class GetUserName(id: Int) extends Request[Nothing, String]

val forgetful: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("ForgetfulDataSource") { requests =>
    ZIO.succeed(
      CompletedRequestMap.fromIterableWith(requests.flatten.filter(_.id != 99))(r => r, r => Exit.succeed(s"user-${r.id}"))
    )
  }

def user(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(forgetful)

def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

def run[E, A](query: ZQuery[Any, E, A]): Exit[E, A] =
  exec(query.run.exit)

def queryFailures(exit: Exit[Any, Any]): List[QueryFailure] =
  exit match {
    case Exit.Failure(cause) => cause.defects.collect { case failure: QueryFailure => failure }
    case Exit.Success(_)     => Nil
  }

def defectMessages(exit: Exit[Any, Any]): List[String] =
  exit match {
    case Exit.Failure(cause) => cause.defects.map(_.getMessage)
    case Exit.Success(_)     => Nil
  }
```

## When a Query Dies With It

A data source returns a `CompletedRequestMap`, and after it returns, the runtime looks up the result of every request that it sent. If the map has no entry for a request, the runtime completes that request with `Exit.die(QueryFailure(dataSource, request))`. The check is made for each request on its own, so one forgotten request does not change the results of the others in the same batch. In the following query, the request for user 99 dies and the other two succeed:

```scala mdoc
exec(ZQuery.foreachPar(List(1, 99, 2))(id => user(id).sandbox.fold(_ => "no result", identity)).run)
```

The query of the forgotten request dies with the failure. The failure carries the data source and the request, and its message names both:

```scala mdoc:silent
val failure: QueryFailure = queryFailures(run(user(99))).head
```

```scala mdoc
failure.dataSource.identifier

failure.request

println(failure.getMessage)
```

The following situations produce a `QueryFailure`:

| Cause                                      | Example                                         | Fix                                          |
|--------------------------------------------|-------------------------------------------------|----------------------------------------------|
| The data source skips a request            | `fromFunctionOption` returns `None`             | Return a result for every request            |
| A batched function returns too few results | `fromFunctionBatched` returns a shorter `Chunk` | Return one result per request, in order      |
| The map key differs from the request       | `contramap` converts to an unequal request      | Key the map by the request that was received |

The first two are cases of a data source that is missing a result, and the `Option` constructors of `DataSource` leave a request unanswered on purpose, see the [DataSource Reference](./data-source-reference.md). The third needs more care, because the data source answers every request it receives but the answer is stored under a different key from the one that the query waits for. `DataSource#contramap`, `DataSource#contramapZIO` and `DataSource#eitherWith` return the map of the original data source as is, so a conversion that produces an unequal request fails every request:

```scala mdoc:silent
val complete: DataSource[Any, GetUserName] =
  DataSource.fromFunction[GetUserName, String]("complete")(request => s"user-${request.id}")

val shifted: DataSource[Any, GetUserName] =
  complete.contramap(((request: GetUserName) => GetUserName(request.id + 1000)) ? "shift ids")
```

The data source `complete` answers `GetUserName(1001)`, but the query waits for `GetUserName(1)`:

```scala mdoc
defectMessages(run(ZQuery.fromRequest(GetUserName(1))(shifted)))
```

The cache plays no part here. Whether or not caching is on, the lookup in the `CompletedRequestMap` is by request equality, see [Request](./request.md#equality-and-caching).

### What Does Not Produce It

A data source that dies or fails as a whole does not produce a `QueryFailure`. If the effect returned by `runAll` dies, the runtime completes every request of that call with the cause of the defect, and the defect is the original one:

```scala mdoc:silent
val exploding: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("ExplodingDataSource")(_ => ZIO.die(new RuntimeException("boom")))
```

```scala mdoc
defectMessages(run(ZQuery.fromRequest(GetUserName(1))(exploding)))

queryFailures(run(ZQuery.fromRequest(GetUserName(1))(exploding)))
```

A request that is completed with `Exit.fail` or `Exit.die` is a result like any other, and the query fails with that error or defect.

## Detecting It

A `QueryFailure` is a defect, so it is not part of the error type of the query, and operators that handle typed errors, such as `catchAll`, do not see it. It is also not thrown as an exception into your code: it is an element of the `Cause` of the failed query. There are three ways to find it:

- Run the query with `ZQuery#run`, take its `exit` and inspect the `Cause`, as the `queryFailures` helper above does with `Cause#defects`. This is the way to assert on it in a test, together with the data source and the request it carries.
- Inside a query, `ZQuery#sandbox` moves the whole `Cause` into the error channel, where `fold`, `catchAll` and the other operators can inspect it.
- `ZQuery#optional` converts exactly this failure to a value: the query returns `Some` if all data sources returned results and `None` if a `QueryFailure` occurred. Other defects and typed errors still fail the query.

The second and the third look like this:

```scala mdoc:silent
def noResult[A](query: ZQuery[Any, Nothing, A]): ZQuery[Any, Nothing, Boolean] =
  query.sandbox.fold(_.defects.exists(_.isInstanceOf[QueryFailure]), _ => false)
```

```scala mdoc
exec(noResult(user(99)).run)

exec(noResult(user(1)).run)

exec(user(99).optional.run)

exec(user(1).optional.run)
```

## Handling It

A `QueryFailure` reports a bug in a data source, not a condition that a caller is expected to handle. The right response is to fix the data source. Use `optional` only when a data source deliberately omits results, for example when it was built with `DataSource.fromFunctionOption`, and the caller wants a `None` instead of a failure. When a missing result is a normal outcome, model it in the request instead, with a result type of `Option[A]` or an error type for "not found". [How to Handle Missing Results](./how-to-handle-missing-results.md) compares the options.

The checklist for a data source that produces `QueryFailure`:

1. The `CompletedRequestMap` has an entry for every request in `runAll`, including duplicates of one request.
2. Each key of the map is equal to the request that was received, which a case class guarantees as long as the data source stores the request itself and not a rebuilt one.
3. Any `contramap`, `contramapZIO` or `eitherWith` returns a request that equals its input, see [Described](./described.md) for how these combinators build the data source.
4. A batched function returns exactly one result per request and in the same order.

Constructors that build the map from the requests, such as `CompletedRequestMap.fromIterableWith`, make the first two points easier to get right. See [CompletedRequestMap](./completed-request-map.md).
