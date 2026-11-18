---
id: how-to-time-out-slow-queries
title: "How to Time Out Slow Queries"
description: "Bound the time a ZQuery may take with timeout, timeoutTo and timeoutFail, and learn why a slow data source call needs its own timeout or a timeout on run."
keywords:
  - "Query Timeout"
  - "Slow Data Source"
  - "Timeout Fallback"
  - "ZIO Query"
---

A query that waits on a slow service holds up everything that depends on it. This guide shows how to put an upper bound on the time a query may take, how to choose what the query produces when the bound is exceeded, and, most importantly, how to bound a data source call that is already in flight, which the `timeout` operators on `ZQuery` do not interrupt.

The examples use real time with short durations, so each block finishes in well under a second and prints the same result on every run. To test timeouts without waiting at all, see [How to Test Queries](./how-to-test-queries.md).

## Setting Up an Example

The examples use one request type, two helpers that run a query and render its outcome in one line, and a data source that counts the calls it receives:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUser(id: Int) extends Request[String, String]

def runZIO[E, A](effect: ZIO[Any, E, A]): Exit[E, A] =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect.exit).getOrThrowFiberFailure())

def render[E, A](exit: Exit[E, A]): String =
  exit match {
    case Exit.Success(a) => s"success: $a"
    case Exit.Failure(c) => s"failure: ${c.failureOption.getOrElse(c.prettyPrint)}"
  }

def describe[E, A](query: ZQuery[Any, E, A]): String =
  render(runZIO(query.run))

val calls: Ref[Int] = Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val userDataSource: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, String, GetUser, String]("UserDataSource") { request =>
    calls.update(_ + 1).as(s"user-${request.id}")
  }

val slowQuery: ZQuery[Any, Nothing, String] =
  ZQuery.fromZIO(ZIO.sleep(1.second).as("done"))
```

`slowQuery` takes a second to complete. The timeouts below are much shorter, so they fire first.

## Return None When the Time Is Up

`timeout` returns a query of `Option[A]`: `Some` of the result if the query completes within the duration, and `None` if it does not. The slow query is cut short after 50 milliseconds, and a quick query is unaffected:

```scala mdoc
describe(slowQuery.timeout(50.millis))
describe(ZQuery.succeed("done").timeout(1.second))
```

The duration is counted from the moment the query starts running. When the time is up, the query is interrupted, so the work it was doing stops.

## Choose What a Timeout Produces

The four operators below differ only in what the query does when the time is up. Note that `timeoutTo` takes its arguments in three steps: the default value, the function that converts the successful result, and the duration.

| Operator           | On timeout           | On success     |
|--------------------|----------------------|----------------|
| `timeout`          | `None`               | `Some(result)` |
| `timeoutTo`        | The default value    | `f(result)`    |
| `timeoutFail`      | Fails with an error  | The result     |
| `timeoutFailCause` | Fails with a `Cause` | The result     |

### Fall Back to a Default Value

`timeoutTo` produces a default value instead of `None`, so the type of the query stays `A` and callers need no `Option` handling. Pass `identity` to keep the successful result as is:

```scala mdoc
describe(slowQuery.timeoutTo("default")(identity)(50.millis))
```

### Fail with an Error

`timeoutFail` fails the query with the error you give it. Choose an error type with a case for the timeout, so callers can tell a timeout from other failures. `timeoutFailCause` takes a whole `Cause`, which is useful to fail with a defect:

```scala mdoc
describe(slowQuery.timeoutFail("took too long")(50.millis))
describe(slowQuery.timeoutFailCause(Cause.fail("took too long"))(50.millis))
```

## Stop Later Requests from Being Sent

A timeout also stops the rest of the query. The query below sleeps for a second before it sends a request to the data source. With a timeout of 50 milliseconds, the query is interrupted while it sleeps, so the request is never sent:

```scala mdoc
describe(
  (ZQuery.fromZIO(ZIO.sleep(1.second)) *> ZQuery.fromRequest(GetUser(1))(userDataSource)).timeout(50.millis)
)

runZIO(calls.get)
```

The counter shows that the data source received no call. A timeout therefore saves work for every step that has not started yet.

## Bound a Slow Data Source Call

The timeout of a query does not interrupt a data source call that has already started. ZIO Query runs a batch of requests to completion before it resumes the query, so a query that is waiting for a slow data source keeps waiting, even after its timeout has elapsed. The next block uses a data source that takes 400 milliseconds to answer, and a timeout of only 50 milliseconds. The query still runs for more than 300 milliseconds:

```scala mdoc:silent
val slowDataSource: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, String, GetUser, String]("SlowDataSource") { request =>
    ZIO.sleep(400.millis).as(s"user-${request.id}")
  }

val slowRequest: ZQuery[Any, String, String] =
  ZQuery.fromRequest(GetUser(1))(slowDataSource)
```

```scala mdoc
runZIO(slowRequest.timeout(50.millis).timed.map { case (elapsed, _) => elapsed > 300.millis }.run)
```

With a data source that never answers, such as `DataSource.never`, the query never completes at all, and `timeout` does not help. The reason is that `timeout` races the timer against the steps of the query, while the data source call runs between two steps. Which result a timed-out query produces once a slow data source finally answers is not something to depend on. If a hanging data source can block your application, use one of the two techniques below.

### Time Out Inside the Data Source

Put the timeout in the effect that calls the backend. This is the best place: the data source decides which error a request gets, and the other data sources of the query are not affected. A request that times out fails with a typed error, and the rest of the query carries on:

```scala mdoc:silent
val boundedDataSource: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, String, GetUser, String]("BoundedDataSource") { request =>
    ZIO.never.timeoutFail(s"user ${request.id} timed out")(50.millis)
  }
```

`ZIO.never` stands for a backend that never answers:

```scala mdoc
describe(ZQuery.fromRequest(GetUser(1))(boundedDataSource))
```

A data source built with `DataSource.Batched.make` applies one timeout to the whole batch. When the time is up, `CompletedRequestMap.fail` completes every request of the batch with the same error:

```scala mdoc:silent
val boundedBatch: DataSource[Any, GetUser] =
  DataSource.Batched.make[Any, GetUser]("BoundedBatch") { requests =>
    ZIO.never
      .timeout(50.millis)
      .map(_ => CompletedRequestMap.fail(requests, "batch timed out"))
  }
```

Here `ZIO.never.timeout` always yields `None`, which stands for a backend that did not answer in time. In a real data source, the effect that calls the backend replaces `ZIO.never`, and its `Some` result is turned into a `CompletedRequestMap`. All requests of the batch fail together:

```scala mdoc
describe(ZQuery.foreachPar(List(1, 2))(id => ZQuery.fromRequest(GetUser(id))(boundedBatch)))
```

### Time Out the Whole Run

If you only need a hard limit on the whole execution, apply the timeout to the effect that `run` returns. Interrupting a fiber that is running a query also interrupts the data source call that it is waiting for, so this works even when the data source never answers:

```scala mdoc
render(
  runZIO(ZQuery.fromRequest(GetUser(1))(DataSource.never).run.timeoutFail("took too long")(50.millis))
)
```

The trade-off is that the limit covers the entire run, so you lose the partial results of a query that combines many requests. It cannot bound one part of a query on its own. Use the timeout inside the data source when each backend needs its own limit.

:::tip[Which Timeout to Use]
Use `timeout` on a query to bound work that is part of the query itself, such as effects created with `ZQuery.fromZIO`. Use a timeout in the data source to bound a backend call. Use a timeout on `run` as a last line of defense for the whole execution.
:::

## Retry After a Timeout

`ZQuery` has no `retry` operator. To try again after a timeout, retry the effect that `run` returns. Each run starts with its own cache, so the retried requests reach the data source again. See [Error Handling](./error-handling.md#retrying) for an example, and [Running Queries and Caching](./running_queries.md) for how the cache is scoped to a run.

## Summary

- `timeout`, `timeoutTo`, `timeoutFail` and `timeoutFailCause` decide what the query produces when the time is up.
- A timeout interrupts the work of the query and prevents its later requests from being sent.
- A data source call that is already running is not interrupted by the timeout of the query.
- Bound a backend call with a timeout inside the data source, or bound the whole execution with a timeout on `run`.
