---
id: how-to-add-logging-and-metrics
title: "How to Add Logging and Metrics"
description: "Observe queries and data sources with aspects: log every call, wrap a whole query, and count calls, batch sizes and durations as ZIO metrics."
keywords:
  - "Query Observability"
  - "Logging and Metrics"
  - "Data Source Aspects"
  - "DataSourceAspect"
  - "QueryAspect"
---

In this guide, we will add logging and metrics to queries without changing the queries or the data sources. By the end, every call to a data source is logged, a whole query can be logged as one unit, and ZIO metrics count the calls, the sizes of the batches and the duration of every data source. We use aspects for all of it: a `DataSourceAspect` wraps a data source, and a `QueryAspect` wraps a query or applies a `DataSourceAspect` to every data source of a query.

## The Problem

The first idea for observing a lookup is to wrap the function that performs it. With plain `ZIO`, that means a logging and timing wrapper around each lookup function:

```scala mdoc:compile-only
import zio._

def fetchUser(id: Int): Task[String] = ???

def observedFetchUser(id: Int): Task[String] =
  ZIO.logInfo(s"fetching user $id") *>
    fetchUser(id).timed.tap { case (duration, _) =>
      ZIO.logInfo(s"fetched user $id in ${duration.toMillis} ms")
    }.map(_._2)
```

Every lookup function needs its own copy of this wrapper. The wrapper also observes the wrong unit: with ZIO Query, many lookups are combined into one call to a data source, so one line per lookup says nothing about how many calls the backend received or how large they were.

## Prerequisites

Add ZIO Query to the build:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

We assume that you know what an aspect is, see [Aspects](./aspects.md), which also explains `Described`. The aspects that wrap data sources run once for each call that the data source receives. [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains how requests become calls.

## Setting Up the Example

The example has one request type and one data source. Aspects wrap the data source, and the queries stay as they are:

```scala mdoc:silent
import zio._
import zio.metrics._
import zio.query._
import zio.query.Described._

final case class GetUserName(id: Int) extends Request[Nothing, String]

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.fromFunctionBatched[GetUserName, String]("UserDataSource") { requests =>
    requests.map(request => s"user-${request.id}")
  }

def userNameFrom(dataSource: DataSource[Any, GetUserName])(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(dataSource)

def userName(id: Int): ZQuery[Any, Nothing, String] =
  userNameFrom(userDataSource)(id)

def exec[A](effect: UIO[A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())
```

To show the log messages on this page, the helper below runs a query with a logger that collects the messages, in place of the default console logger. In an application, configure your usual ZIO logger and do not write this helper:

```scala mdoc:silent
val collected = new java.util.concurrent.ConcurrentLinkedQueue[String]()

val collector: ZLogger[String, Unit] =
  ZLogger.simple[String, Unit] { message =>
    collected.add(message)
    ()
  }

def runLogged[A](query: ZQuery[Any, Nothing, A]): Unit = {
  val result = exec(query.run.provideLayer(Runtime.removeDefaultLoggers >>> Runtime.addLogger(collector)))
  println(s"result: $result")
  while (!collected.isEmpty) println(s"log: ${collected.poll()}")
}
```

## Logging Every Call to a Data Source

`DataSourceAspect.around` runs an effect before and an effect after each call to a data source. The effect that runs first returns a value, which the second effect receives. Both effects are `Described`, because the description becomes part of the identifier of the wrapped data source, and neither of them can fail. The aspect below logs a message before and after the call. The name that we pass in appears in both messages and in both descriptions, so wrapped data sources get distinct identifiers:

```scala mdoc:silent
def logCalls(name: String): DataSourceAspect[Any] =
  DataSourceAspect.around(ZIO.logInfo(s"calling $name") ? s"log start of $name")(
    ((_: Unit) => ZIO.logInfo(s"called $name")) ? s"log end of $name"
  )
```

We apply an aspect to a data source with `DataSource#@@`. A query that loads three users in parallel sends one call, and so the messages appear once:

```scala mdoc:silent
val loggedDataSource: DataSource[Any, GetUserName] =
  userDataSource @@ logCalls("UserDataSource")
```

To run a query that loads three users through the wrapped data source:

```scala mdoc
runLogged(ZQuery.foreachPar(List(1, 2, 3))(userNameFrom(loggedDataSource)))
```

The messages are logged once for the call, not once for each of the three requests. They have the INFO level, so they reach whatever logger the application configured.

:::note
The two effects of `DataSourceAspect.around` do not receive the requests of the call. To log or count what a call contains, write your own aspect, as in [Counting Calls, Batches and Durations](#counting-calls-batches-and-durations).
:::

## Logging the Calls of All Data Sources in a Query

Wrapping every data source by hand is tedious when a query uses several. `QueryAspect.aroundDataSource` takes the same two effects as `DataSourceAspect.around` and applies them to every data source that the query uses. We apply a query aspect with `ZQuery#@@`:

```scala mdoc:silent
val logAllCalls: QueryAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
  QueryAspect.aroundDataSource(ZIO.logInfo("calling a data source") ? "log call start")(
    ((_: Unit) => ZIO.logInfo("called a data source")) ? "log call end"
  )
```

The query itself is the one we already have:

```scala mdoc
runLogged(ZQuery.foreachPar(List(1, 2, 3))(userName) @@ logAllCalls)
```

To apply any other `DataSourceAspect` to all data sources of a query, wrap it with `QueryAspect.fromDataSourceAspect`.

## Logging a Whole Query

`QueryAspect.around` wraps the query itself, not the calls to data sources. It runs an effect before the query and an effect after it, and the second one runs also when the query fails or is interrupted. The effects are plain effects with no `Described` wrapper. To log when a query starts and finishes:

```scala mdoc:silent
val logQuery: QueryAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
  QueryAspect.around(ZIO.logInfo("query started"))(_ => ZIO.logInfo("query finished"))
```

Aspects combine with `>>>`, and the left aspect takes effect first. The next query logs both the query and its data source calls:

```scala mdoc
runLogged(ZQuery.foreachPar(List(1, 2, 3))(userName) @@ (logQuery >>> logAllCalls))
```

The data source messages are between the query messages, because the calls happen while the query runs. To measure a query's duration instead of logging it, call `ZQuery#timed`, which returns the elapsed time together with the result.

## Counting Calls, Batches and Durations

Metrics need to know what each call contains: how many requests, in how many batches. `DataSourceAspect` is a trait with a single method, so we can write an aspect that sees the requests. We start with three metrics. Metrics are global and are identified by name and tags, and we tag each one with the identifier of the data source, so that one definition serves every data source:

```scala mdoc:silent
val dataSourceCalls: Metric.Counter[Long] =
  Metric.counter("zio_query_data_source_calls")

val dataSourceBatchSize: Metric.Histogram[Double] =
  Metric.histogram("zio_query_batch_size", MetricKeyType.Histogram.Boundaries.linear(1.0, 1.0, 10))

val dataSourceMillis: Metric.Histogram[Double] =
  Metric.histogram("zio_query_call_millis", MetricKeyType.Histogram.Boundaries.exponential(1.0, 2.0, 12))
```

The aspect creates a data source that records the three metrics around each call to the original data source. A call is one invocation of `DataSource#runAll`, and it receives a `Chunk` of batches. The aspect records the size of every batch, and the time that the whole call takes. It also gives the new data source its own identifier, as `DataSourceAspect.around` does:

```scala mdoc:silent
val metered: DataSourceAspect[Any] = new DataSourceAspect[Any] {
  def apply[R, A](dataSource: DataSource[R, A]): DataSource[R, A] =
    new DataSource[R, A] {
      val identifier = s"${dataSource.identifier} @@ metered"

      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] = {
        val calls  = dataSourceCalls.tagged("data_source", dataSource.identifier)
        val sizes  = dataSourceBatchSize.tagged("data_source", dataSource.identifier)
        val millis = dataSourceMillis.tagged("data_source", dataSource.identifier)
        for {
          _     <- calls.increment
          _     <- ZIO.foreachDiscard(requests)(batch => sizes.update(batch.size.toDouble))
          timed <- dataSource.runAll(requests).timed
          _     <- millis.update(timed._1.toMillis.toDouble)
        } yield timed._2
      }
    }
}
```

We apply the aspect to a whole query with `QueryAspect.fromDataSourceAspect`, so that every data source of the query is measured without a change to the queries. To run one query that loads three users in parallel and another that loads two users one after the other:

```scala mdoc:silent
val measured = QueryAspect.fromDataSourceAspect(metered)

exec((ZQuery.foreachPar(List(1, 2, 3))(userName) @@ measured).run.unit)

exec((ZQuery.foreach(List(4, 5))(userName) @@ measured).run.unit)
```

The first query sends one batch of three requests. The second one sends two batches of one request each in a single call, as described in [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md#pipelining). We read the metrics for the data source with `Metric#value`:

```scala mdoc:silent
val counted = exec(dataSourceCalls.tagged("data_source", "UserDataSource").value)

val sizes = exec(dataSourceBatchSize.tagged("data_source", "UserDataSource").value)

val durations = exec(dataSourceMillis.tagged("data_source", "UserDataSource").value)
```

Printing the counts, the sum of the batch sizes and the largest batch gives:

```scala mdoc
println(s"calls: ${counted.count}, batches: ${sizes.count}, requests: ${sizes.sum}, largest batch: ${sizes.max}")

println(s"measured calls: ${durations.count}")
```

The data source received two calls, which contained three batches and five requests. The durations histogram holds one observation for each call, and its buckets show the latency distribution. In an application, a metrics connector reads the same metrics, so no more code is needed to export them.

:::warning
An aspect changes the identifier of the data source, and requests are batched together only when their data sources have equal identifiers. Apply an aspect to every use of a data source, or to none. Requests that go to `userDataSource` and requests that go to `userDataSource @@ metered` are sent in separate calls.
:::

## Choosing Where to Apply the Aspect

The aspects differ in what they wrap and in what they can see. The following table lists them:

| Aspect                                          | Wraps                       | Sees requests         |
|-------------------------------------------------|-----------------------------|-----------------------|
| `DataSourceAspect.around`                       | One data source             | No                    |
| `QueryAspect.aroundDataSource`                  | All data sources of a query | No                    |
| `QueryAspect.around`                            | A whole query               | No                    |
| A custom `DataSourceAspect`, as `metered` above | One data source             | Yes                   |
| `QueryAspect.fromDataSourceAspect(aspect)`      | All data sources of a query | Only if `aspect` does |

## Going Further

- [Aspects](./aspects.md) lists every aspect that ships with ZIO Query, including `QueryAspect.maxBatchSize`, which composes with the aspects of this guide.
- [How to Time Out Slow Queries](./how-to-time-out-slow-queries.md) bounds the duration that the metrics of this guide measure.
- [Error Handling](./error-handling.md) explains the failures that a query can report, which are worth counting in the same aspect.
