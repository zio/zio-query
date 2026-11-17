---
id: how-to-handle-missing-results
title: "How to Handle Missing Results"
description: "Choose between Option results, typed errors and QueryFailure when a lookup finds nothing, and keep one missing id from failing a whole batch of queries."
keywords:
  - "Missing Results"
  - "Query Failure"
  - "Optional Lookup"
  - "ZIO Query"
---

A lookup by id often finds nothing: the user was deleted, the key never existed, the cache holds no entry. This guide shows three ways to represent that in ZIO Query, how to recover when a data source leaves a request unanswered, and how to keep one missing id from discarding the results of the other requests in the same query.

The three ways differ in where the "nothing" lives:

| Approach                              | A missing id becomes     | Error type of the query |
|---------------------------------------|--------------------------|-------------------------|
| `Option` in the request's result type | `None`                   | `Nothing`               |
| A typed error in the request          | A failure `UserNotFound` | `UserNotFound`          |
| A data source that skips the request  | A `QueryFailure` defect  | `Nothing`               |

Prefer the first two: they make "not found" an ordinary, expected outcome. The third is for data sources that cannot tell you about the gap, and it needs `optional` to be usable. For background on how the other failures behave, see [Error Handling](./error-handling.md).

## Setting Up an Example

The examples use a small in-memory table of users and two helpers: `runSync` runs a query that cannot fail, and `describe` runs any query and renders its outcome in one line.

```scala mdoc:silent
import zio._
import zio.query._

final case class User(id: Int, name: String)

val table: Map[Int, User] = Map(1 -> User(1, "Ada"), 2 -> User(2, "Grace"))

def runSync[A](query: ZQuery[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(query.run).getOrThrowFiberFailure())

def describe[E, A](query: ZQuery[Any, E, A]): String =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(query.run.exit).getOrThrowFiberFailure()) match {
    case Exit.Success(a) => s"success: $a"
    case Exit.Failure(c) =>
      c.failureOption
        .map(e => s"failure: $e")
        .orElse(c.dieOption.map(t => s"defect: ${t.getClass.getSimpleName}: ${t.getMessage}"))
        .getOrElse("interrupted")
  }
```

## Return an Option from the Request

The simplest representation is to make "not found" part of the answer. Declare the request with `Option[User]` as its result type, and have the data source return `None` for ids it does not know:

```scala mdoc:silent
final case class FindUser(id: Int) extends Request[Nothing, Option[User]]

val findUsers: DataSource[Any, FindUser] =
  DataSource.fromFunction[FindUser, Option[User]]("FindUsers")(request => table.get(request.id))

def findUser(id: Int): ZQuery[Any, Nothing, Option[User]] =
  ZQuery.fromRequest(FindUser(id))(findUsers)
```

Every request has an answer, so nothing can die with a `QueryFailure`, and the query cannot fail. A missing id is just a `None` in the result, and the other requests are unaffected:

```scala mdoc
runSync(ZQuery.foreachPar(List(1, 99, 2))(findUser))
```

To fall back to a default, map over the option:

```scala mdoc
runSync(findUser(99).map(_.getOrElse(User(0, "guest"))))
```

Use this approach when a missing result is a normal case that callers handle with `map`, `getOrElse` or pattern matching, and when they do not need to know why the result is missing.

## Fail with a Typed Error

When callers should treat "not found" as a failure, put the error in the request type. Then `UserNotFound` appears in the error type of every query that uses the request, and the compiler reminds callers to handle it:

```scala mdoc:silent
final case class UserNotFound(id: Int)

final case class GetUser(id: Int) extends Request[UserNotFound, User]

val getUsers: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, UserNotFound, GetUser, User]("GetUsers") { request =>
    ZIO.fromOption(table.get(request.id)).orElseFail(UserNotFound(request.id))
  }

def getUser(id: Int): ZQuery[Any, UserNotFound, User] =
  ZQuery.fromRequest(GetUser(id))(getUsers)
```

A failing function fails only its own request. Recover from it with the usual operators, such as `either` to see the failure as a value, or `catchAll` to substitute another query:

```scala mdoc
describe(getUser(99))
describe(getUser(99).either)
describe(getUser(99).catchAll(error => ZQuery.succeed(User(error.id, "guest"))))
```

When you combine several lookups, the first failure fails the whole `foreachPar`. Apply `either` to each lookup if the other results should survive:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 99, 2))(id => getUser(id).either))
```

The failing request does not prevent its batch from being sent: all three requests above still reach the data source together. See [Error Handling](./error-handling.md) for the full set of recovery operators.

## Recover from a Data Source That Skips Requests

Some data sources are built so that a request may stay unanswered: `DataSource.fromFunctionOption`, `DataSource.fromFunctionOptionZIO`, `DataSource.fromFunctionBatchedOption` and `DataSource.fromFunctionBatchedOptionZIO` take a function that returns an `Option`, and a `None` means "no result for this request". A hand-written data source skips a request in the same way when it leaves it out of the `CompletedRequestMap`.

A request without a result does not fail with a typed error. The query dies with a `QueryFailure`, a defect, so the error type of the query stays `Nothing`:

```scala mdoc:silent
final case class LookUp(id: Int) extends Request[Nothing, User]

val skipping: DataSource[Any, LookUp] =
  DataSource.fromFunctionOption[LookUp, User]("SkippingUsers")(request => table.get(request.id))

def lookUp(id: Int): ZQuery[Any, Nothing, User] =
  ZQuery.fromRequest(LookUp(id))(skipping)
```

The message of the defect names the data source and the request:

```scala mdoc
describe(lookUp(1))
describe(lookUp(99))
```

### Convert the Missing Result to None

`optional` turns the query into one that returns `Some` of the result, or `None` if a data source did not answer a request. It recovers from `QueryFailure` only: typed errors and other defects still fail the query.

```scala mdoc
describe(lookUp(99).optional)
describe(lookUp(2).optional)
```

### Apply optional to Each Lookup

`optional` looks at the whole query it is applied to. If you apply it after combining several lookups, one missing id turns the entire result into `None`, and the users that were found are lost:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 99, 2))(lookUp).optional)
```

Apply it to each lookup instead. The requests are still sent as one batch, and each position in the result tells you whether that id was found:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 99, 2))(id => lookUp(id).optional))
```

To keep only the users that exist, flatten the result:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 99, 2))(id => lookUp(id).optional).map(_.flatten))
```

### Find Out Which Request Was Missing

A `QueryFailure` carries the `dataSource` that failed to answer and the `request` it received. To read them, recover with `catchAllCause` and look for the defect in the `Cause`:

```scala mdoc
describe(
  lookUp(99).catchAllCause { cause =>
    ZQuery.succeed(cause.defects.collect { case failure: QueryFailure => failure.request })
  }
)
```

:::warning[Do Not Use optional to Hide a Bug]
A `QueryFailure` from a data source that is supposed to answer every request is a bug in that data source, such as a batch function that returns a shorter chunk than it received. Converting it to `None` makes the bug look like missing data. Use `optional` only with data sources that are meant to skip requests.
:::

## Leave Gaps in a Batched Data Source

A data source created with `DataSource.fromFunctionBatched` must return one result per request, in request order. If your backend returns fewer rows than requested, as a bulk `SELECT ... WHERE id IN (...)` does for unknown ids, use `DataSource.fromFunctionBatchedOption` and return `None` at the positions without a row:

```scala mdoc:silent
val bulk: DataSource[Any, LookUp] =
  DataSource.fromFunctionBatchedOption[LookUp, User]("BulkUsers") { requests =>
    requests.map(request => table.get(request.id))
  }
```

The result is the same as with the single-request constructor: a request with a `None` dies with a `QueryFailure`, which `optional` turns into `None`.

```scala mdoc
describe(
  ZQuery.foreachPar(List(1, 99, 2))(id => ZQuery.fromRequest(LookUp(id))(bulk).optional)
)
```

If the backend returns rows in an arbitrary order, use `DataSource.fromFunctionBatchedWith` instead, which matches results to requests by a key rather than by position. Both constructors are described in [DataSource Reference](./data-source-reference.md), and [CompletedRequestMap](./completed-request-map.md) shows how to build the result map by hand, including how to complete only some of the requests of a batch.

## Summary

- Model "not found" in the request: `Request[Nothing, Option[A]]` for an ordinary absence, `Request[NotFound, A]` when callers must handle it.
- A data source that skips a request makes the query die with a `QueryFailure`.
- `optional` converts that defect to `None`. Apply it to each lookup, not to the combined query.
- Inspect the `Cause` with `catchAllCause` to find which request was skipped.
