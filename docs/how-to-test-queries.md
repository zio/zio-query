---
id: how-to-test-queries
title: "How to Test Queries"
description: "Test ZQuery code with in-memory stub data sources: assert results, count calls, verify batching and caching, and check failures without a real backend."
keywords:
  - "Testing Queries"
  - "Stub Data Source"
  - "Batching Assertions"
  - "ZIO Query"
---

A query talks to the outside world only through its data sources, so a test can replace the real backend with an in-memory data source and check two things: the result of the query, and what reached the data source. The second one is specific to ZIO Query: batching and caching change how many calls the backend receives, and a test is the place to pin that down.

This guide builds a stub data source that records every batch it receives, then uses it to test results, batching, caching and failures. It uses only ZIO and ZIO Query, with plain Scala `assert`, so that every block runs when this page is built. The same stub works in any test framework: with `zio-test`, the effect that `ZQuery#run` returns is the test, and no `Unsafe` runner is needed.

## Setting Up the Stub

The examples use one request type and an in-memory table of users:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUser(id: Int) extends Request[String, String]

val table: Map[Int, String] = Map(1 -> "Ada", 2 -> "Grace", 3 -> "Edsger")
```

A stub data source needs two parts: the answers, which come from the table, and a record of the calls. `stubUsers` creates both. It returns the data source together with an effect that reads the recorded batches, where each batch is the list of ids the data source received in one call:

```scala mdoc:silent
def stubUsers(users: Map[Int, String]): UIO[(DataSource[Any, GetUser], UIO[Chunk[Chunk[Int]]])] =
  Ref.make(Chunk.empty[Chunk[Int]]).map { batches =>
    def answer(request: GetUser): Exit[String, String] =
      users.get(request.id) match {
        case Some(name) => Exit.succeed(name)
        case None       => Exit.fail(s"user ${request.id} not found")
      }

    val dataSource = DataSource.Batched.make[Any, GetUser]("StubUsers") { requests =>
      batches.update(_ :+ requests.map(_.id)).as(
        CompletedRequestMap.fromIterableWith(requests)(request => request, answer)
      )
    }

    (dataSource, batches.get)
  }
```

`DataSource.Batched.make` hands the function all the requests that can run in parallel, so each call to it is one batch. The stub answers an unknown id with a typed error, like a real backend that reports "not found". See [Creating Data Sources](./creating-data-sources.md) and [DataSource Reference](./data-source-reference.md) for the other constructors. `DataSource.fromFunction` is enough when you do not need to record calls.

Next, `observe` runs a query against a fresh stub and returns two things: a one-line rendering of the outcome, and the recorded batches as plain lists. A fresh stub for every call keeps the tests independent of each other:

```scala mdoc:silent
def observe[E, A](
  users: Map[Int, String]
)(build: DataSource[Any, GetUser] => ZQuery[Any, E, A]): (String, List[List[Int]]) =
  Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe
      .run(
        stubUsers(users).flatMap { case (dataSource, batches) =>
          build(dataSource).run.exit.zip(batches)
        }
      )
      .getOrThrowFiberFailure()
  } match {
    case (exit, batches) =>
      val outcome = exit match {
        case Exit.Success(a) => s"success: $a"
        case Exit.Failure(c) =>
          c.failureOption
            .map(e => s"failure: $e")
            .orElse(c.dieOption.map(t => s"defect: ${t.getMessage}"))
            .getOrElse("interrupted")
      }
      (outcome, batches.map(_.toList).toList)
  }

def user(dataSource: DataSource[Any, GetUser])(id: Int): ZQuery[Any, String, String] =
  ZQuery.fromRequest(GetUser(id))(dataSource)
```

## Assert the Result

The simplest test checks the result of a query. Here the query looks up two users in parallel:

```scala mdoc
observe(table)(dataSource => ZQuery.foreachPar(List(1, 2))(user(dataSource)))
```

The first element is the outcome, and the second is the list of batches. The rest of this guide asserts on both.

## Assert That Requests Are Batched

Batching is the reason to use ZIO Query, and it is easy to lose by accident: replacing `foreachPar` with `foreach` in a refactoring keeps the result and turns one call into several. A test with the recording stub catches that. Compare the two combinators on the same ids:

```scala mdoc
observe(table)(dataSource => ZQuery.foreachPar(List(1, 2, 3))(user(dataSource)))
observe(table)(dataSource => ZQuery.foreach(List(1, 2, 3))(user(dataSource)))
```

The parallel query reaches the data source in one batch of three ids. The sequential query sends three batches of one id each, because every request waits for the previous one to finish. [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains the rules.

To turn this into a test, assert on the recorded batches. A failing `assert` throws, which also fails the page build, so the blocks in this guide are checked every time the documentation is built:

```scala mdoc:silent
val (outcome, batches) =
  observe(table)(dataSource => ZQuery.foreachPar(List(1, 2, 3))(user(dataSource)))

assert(outcome == "success: List(Ada, Grace, Edsger)")
assert(batches == List(List(1, 2, 3)))
```

## Assert That Requests Are Cached

Within one run, a request that was already answered does not reach the data source again. The first query below asks for the same user twice in parallel, the second asks for it twice in sequence:

```scala mdoc
observe(table)(dataSource => ZQuery.foreachPar(List(1, 1, 2))(user(dataSource)))
observe(table)(dataSource => user(dataSource)(1) *> user(dataSource)(1))
```

Both queries send each id to the data source once. The first one removes the duplicate from the batch, and the second one answers the repeated request from the cache. To check that a request is sent every time, make it uncached, as in [Running Queries and Caching](./running_queries.md):

```scala mdoc
observe(table)(dataSource => ZQuery.foreachPar(List(1, 1, 2))(user(dataSource)).uncached)
```

## Assert on Failures

A stub that answers `Exit.fail` for an unknown id lets you test how your query reacts to a failing backend. The failure of one request fails the whole combined query, and the batch still reached the data source:

```scala mdoc
observe(table)(dataSource => ZQuery.foreachPar(List(1, 99, 2))(user(dataSource)))
```

To test a recovery, apply it in the query under test and assert on the outcome. Here a missing user is replaced by a default value:

```scala mdoc
observe(table)(dataSource => user(dataSource)(99).catchAll(_ => ZQuery.succeed("guest")))
```

Use a table with fewer entries, or an empty one, to test the "not found" paths without changing the stub. [Error Handling](./error-handling.md) lists the operators to recover from errors and defects, and [How to Handle Missing Results](./how-to-handle-missing-results.md) shows how to test a data source that skips requests.

## Inspect the Cache After a Run

`ZQuery#runLog` returns the cache of the run together with the result. Asking the cache for a request tells you whether the query submitted it, which is useful when the data source is not a stub that you control:

```scala mdoc:silent
val inspection: UIO[(Boolean, Boolean)] =
  for {
    dataSource <- stubUsers(table).map(_._1)
    logged     <- user(dataSource)(1).runLog.orDieWith(message => new RuntimeException(message))
    seen       <- logged._1.get[String, String](GetUser(1)).isSuccess
    unseen     <- logged._1.get[String, String](GetUser(2)).isSuccess
  } yield (seen, unseen)
```

Running it shows that the cache holds the request that the query submitted and no other:

```scala mdoc
Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(inspection).getOrThrowFiberFailure())
```

`Cache#get` succeeds when the request is in the cache and fails otherwise. See [Inspecting a Run With runLog](./running_queries.md#inspecting-a-run-with-runlog) for how to read the stored result.

## Test Environments and Timeouts

A data source that needs an environment can be tested by providing a stub environment: see [How to Use Environment in Data Sources](./how-to-use-environment-in-data-sources.md). To test a timeout without waiting for it, run the query in a fiber under `zio-test` and advance `TestClock`, in the same way as you would test any ZIO effect that sleeps. The timeout operators of `ZQuery` use the clock of the environment, so they follow `TestClock`. [How to Time Out Slow Queries](./how-to-time-out-slow-queries.md) lists them.

## Summary

- Replace the backend with an in-memory data source, built with `DataSource.Batched.make` when you need to see each batch.
- Record the ids of every call in a `Ref`, and assert on the list of batches to pin down batching and caching.
- Build the stub per test, so tests do not share state.
- Test failure handling with a stub that answers `Exit.fail`.
