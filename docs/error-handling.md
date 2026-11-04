---
id: error-handling
title: "Error Handling"
---

A `ZQuery[R, E, A]` can fail with a typed error `E`, die with a defect, or be interrupted, just like a `ZIO` effect. This page shows where those failures come from when a query talks to a data source, what happens to the other requests in the same batch, and which `ZQuery` operators recover from each kind of failure, convert between them, or bound the time a query may take.

## How Errors Flow

Every request type declares its own error type, as in `Request[E, A]`. When a data source completes a request, it stores an `Exit[E, A]` for that request in the `CompletedRequestMap`, so each request succeeds or fails on its own. `ZQuery.fromRequest` turns that `Exit` into the result of the query, which is why the error type of a request becomes the error type of the query.

The effect returned by `DataSource#runAll` has the error type `Nothing`: a data source reports problems for individual requests through the `CompletedRequestMap`, not by failing as a whole. Three situations can therefore make a query fail:

| Situation                                              | Result in the query                      |
| ------------------------------------------------------ | ---------------------------------------- |
| The data source stores `Exit.fail(e)` for a request    | A failure of type `E`                    |
| The data source stores `Exit.die(t)` for a request     | A defect                                 |
| The data source returns no result for a request        | A defect: a [`QueryFailure`](#queryfailure-a-request-without-a-result) |

## Setting Up an Example

The examples on this page use one request type and three data sources. The request type fails with a `String` error. The first data source answers ids `1` and `2`, fails id `3` with a typed error, and fails id `4` with a defect. The second data source forgets to answer id `99`. The third counts how many times it is called.

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUser(id: Int) extends Request[String, String]

val userDataSource: DataSource[Any, GetUser] =
  DataSource.make[Any, GetUser]("UserDataSource") { requests =>
    ZIO.succeed(
      CompletedRequestMap.fromIterableWith(requests.flatten)(
        r => r,
        r =>
          r.id match {
            case 3     => Exit.fail("user 3 not found")
            case 4     => Exit.die(new RuntimeException("connection reset"))
            case other => Exit.succeed(s"user-$other")
          }
      )
    )
  }

val forgetfulDataSource: DataSource[Any, GetUser] =
  DataSource.make[Any, GetUser]("ForgetfulDataSource") { requests =>
    ZIO.succeed(
      CompletedRequestMap.fromIterableWith(requests.flatten.filter(_.id != 99))(r => r, r => Exit.succeed(s"user-${r.id}"))
    )
  }

val calls: Ref[Int] = Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val countingDataSource: DataSource[Any, GetUser] =
  DataSource.make[Any, GetUser]("CountingDataSource") { requests =>
    calls.update(_ + 1) *> ZIO.succeed(
      CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, _ => Exit.fail("service unavailable"))
    )
  }

def user(id: Int): ZQuery[Any, String, String] =
  ZQuery.fromRequest(GetUser(id))(userDataSource)

def run[E, A](query: ZQuery[Any, E, A]): Exit[E, A] =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(query.run.exit).getOrThrowFiberFailure())

def describe[E, A](query: ZQuery[Any, E, A]): String =
  run(query) match {
    case Exit.Success(a) => s"success: $a"
    case Exit.Failure(c) =>
      c.failureOption
        .map(e => s"failure: $e")
        .orElse(c.dieOption.map(t => s"defect: ${t.getClass.getSimpleName}: ${t.getMessage}"))
        .getOrElse("interrupted")
  }
```

The `describe` helper runs a query and renders its outcome in one line, so that the output below stays short.

## Typed Errors and Defects

A request that the data source answers with `Exit.fail` fails the query with that error, and `Exit.die` makes the query die:

```scala mdoc
describe(user(1))
describe(user(3))
describe(user(4))
```

### A Failing Request Does Not Fail Its Batch

The requests in a batch are completed one by one, so a failing request does not change the results of the other requests in the same batch. To see this, we combine three requests with `ZQuery.foreachPar`, which sends them to the data source in one batch, and turn each result into an `Either` with `either`:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 2, 3))(id => user(id).either))
```

Each request has its own result: ids `1` and `2` succeeded and id `3` failed. Without `either`, the first failure fails the whole `foreachPar` query:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 2, 3))(user))
```

## QueryFailure: A Request Without a Result

A data source must provide a result for every request it receives. If it leaves a request out of the `CompletedRequestMap`, the query dies with a `QueryFailure`. The defect carries the data source and the request that was not completed, and its message names both:

```scala mdoc
val forgotten = ZQuery.fromRequest(GetUser(99))(forgetfulDataSource)

describe(forgotten)
```

A `QueryFailure` is a defect, not a typed error, so it does not appear in the error type of the query and `catchAll` does not recover from it. It signals a bug in the data source, not a condition that a caller is expected to handle.

The only operator that is aware of `QueryFailure` is `optional`. It converts a query to one that returns `Some` if the data sources returned results for all requests and `None` otherwise. Typed errors and other defects still fail the query:

```scala mdoc
describe(forgotten.optional)
describe(user(1).optional)
describe(user(3).optional)
```

:::note
`DataSource.fromFunctionOption` and `DataSource.fromFunctionOptionZIO` construct data sources that may not provide results for all requests. Use `optional` on queries built from them.
:::

## Recovering from Errors

The operators below follow the naming of `ZIO`. They differ in what they recover from: the `catchAll` family and the `fold`/`foldQuery`/`foldZIO` family handle typed errors only, while the `*Cause` variants also see defects.

| Operator                                  | Handles                  | Recovers with                  |
| ----------------------------------------- | ------------------------ | ------------------------------ |
| `catchAll`                                | Typed errors             | A `ZQuery`                     |
| `catchAllZIO`                             | Typed errors             | A `ZIO` effect                 |
| `catchAllCause`                           | The whole `Cause`        | A `ZQuery`                     |
| `catchAllCauseZIO`                        | The whole `Cause`        | A `ZIO` effect                 |
| `fold`                                    | Typed errors and success | A pure value                   |
| `foldQuery`                               | Typed errors and success | A `ZQuery`                     |
| `foldZIO`                                 | Typed errors and success | A `ZIO` effect                 |
| `foldCauseQuery`                          | The whole `Cause` and success | A `ZQuery`                |
| `foldCauseZIO`                            | The whole `Cause` and success | A `ZIO` effect            |
| `either`                                  | Typed errors and success | `Either[E, A]`, cannot fail    |
| `optional`                                | `QueryFailure`           | `Option[A]`                    |

### Recovering with a Fallback Query

`catchAll` takes a function from the error to a new query. The fallback query is a full query: it can issue requests of its own, and those requests are batched with the rest of the query:

```scala mdoc
describe(user(3).catchAll(_ => user(1)))
```

`catchAll` handles only typed errors. A defect passes through unchanged:

```scala mdoc
describe(user(4).catchAll(_ => user(1)))
```

`catchAllCause` receives the whole `Cause`, so it can also recover from defects:

```scala mdoc
describe(user(4).catchAllCause(_ => user(1)))
```

To recover with an effect instead of a query, use `catchAllZIO` or `catchAllCauseZIO`:

```scala mdoc
describe(user(3).catchAllZIO(error => ZIO.succeed(s"fallback after: $error")))
```

### Handling Both Outcomes

`fold` handles failure and success in one step and produces a query that cannot fail. `foldQuery` and `foldZIO` do the same with a query or an effect, and `foldCauseQuery` and `foldCauseZIO` receive the whole `Cause` for the failure case:

```scala mdoc
describe(user(3).fold(error => s"failed: $error", name => s"found: $name"))
describe(user(4).foldCauseQuery(cause => ZQuery.succeed(s"died: ${cause.isDie}"), name => ZQuery.succeed(name)))
```

`either` is the shortest way to make a failure visible as a value, as in the batch example above.

## Working with the Full Cause

`sandbox` moves the whole `Cause` into the error channel, so that typed errors and defects can be handled in one place with the ordinary operators. `unsandbox` is the inverse:

```scala mdoc
val sandboxed: ZQuery[Any, Cause[String], String] = user(4).sandbox

describe(sandboxed.mapError(cause => s"was a defect: ${cause.isDie}"))
describe(ZQuery.unsandbox(sandboxed))
```

`sandboxWith` applies a function to the sandboxed query and unsandboxes the result. It is useful for partial recovery from both errors and defects at once:

```scala mdoc
describe(user(4).sandboxWith[Any, String, String](_.catchAll(_ => ZQuery.succeed("recovered"))))
```

## Converting Between Errors and Defects

`mapError` and `mapErrorCause` transform the error or the whole `Cause` without changing success values. They do not prevent batching of the requests in the query.

`orDie` turns a typed error into a defect. It requires the error type to be a `Throwable`; `orDieWith` takes a function that converts the error:

```scala mdoc
describe(user(3).orDieWith(message => new IllegalStateException(message)))
```

`refineOrDie` keeps only the errors matched by a partial function and turns all others into defects. `refineOrDieWith` takes a function that converts the unmatched error to a `Throwable`:

```scala mdoc
val refined: ZQuery[Any, Int, String] =
  user(3).refineOrDieWith { case message if message.endsWith("not found") => 404 }(new RuntimeException(_))

describe(refined)
```

`unrefine`, `unrefineTo` and `unrefineWith` go the other way: they move some defects of the query into the error channel.

## Timeouts

`timeout` returns `None` if the query does not complete within the given duration, and `Some` of the result otherwise. `timeoutFail` and `timeoutFailCause` fail with a given error or `Cause` instead, and `timeoutTo` produces a default value:

```scala mdoc
describe(ZQuery.never.timeout(100.millis))
describe(ZQuery.never.timeoutFail("took too long")(100.millis))
describe(user(1).timeout(1.second))
```

A timeout also prevents later requests of a timed-out query from being executed. The test suite `ZQuerySpec` checks that a query that times out while sleeping never sends its next request to the data source.

### Racing Two Queries

`race` runs two queries and returns the result of the first one that succeeds, interrupting the other. If both fail, the failure `Cause` of the result combines the two:

```scala mdoc
describe(user(3).race(user(2)))
```

## Retrying

`ZQuery` has no `retry` operator. Because every call to `run` starts with its own cache, you can retry a whole query by retrying the effect that `run` returns:

```scala mdoc:silent
val retried: ZIO[Any, String, String] =
  ZQuery.fromRequest(GetUser(1))(countingDataSource).run.retry(Schedule.recurs(2))
```

```scala mdoc
Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(retried.either).getOrThrowFiberFailure())

Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(calls.get).getOrThrowFiberFailure())
```

The data source was called three times: the first attempt and two retries.

:::warning
Within a single `run`, the result of a request is cached, including a failed result. Re-running the same request inside a query, for example with `catchAll(_ => sameQuery)`, returns the cached failure and does not call the data source again. To retry a request inside one run, use `uncached` on the retried part or retry the whole `run` as above.
:::

## Errors and the Environment

A query that requires an environment fails like any other query when providing that environment fails. `provideLayer` builds the layer when the query runs and fails the query with the error of the layer if building it fails. The layer has to be wrapped in a `Described` value, which `?` creates from a description:

```scala mdoc
import Described._

val failingLayer: ZLayer[Any, String, Int] = ZLayer.fromZIO(ZIO.fail("no connection"))

val needsInt: ZQuery[Int, Nothing, Int] = ZQuery.service[Int]

describe(needsInt.provideLayer(failingLayer ? "failingLayer"))
```

`provideEnvironment` and `provideSomeEnvironment` also take `Described` values and eliminate the requirement without any chance of failure.
