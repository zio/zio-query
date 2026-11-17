---
id: how-to-cache-query-results
title: "How to Cache Query Results"
description: "Control how long query results are reused: share a cache across queries, bypass it for fresh reads, invalidate entries, and expire them."
keywords:
  - "Query Caching"
  - "Request Deduplication"
  - "Cache Invalidation"
  - "Cache"
---

In this guide, we will decide how long the results of a query are reused. We start with what ZIO Query caches without any configuration, then share one cache between several queries that handle the same incoming request, bypass the cache for reads that must be fresh, remove entries after a write, retry a request whose failure was cached, and finally write a cache whose entries expire after a time to live.

## The Problem

Several parts of a program often need the same data, and fetching it again for each of them costs a round trip. The usual remedy is a hand-rolled cache around the lookup function:

```scala mdoc:compile-only
import zio._

final case class User(id: Int, name: String)

def fetchUser(id: Int): Task[User] = ???

def cachedFetchUser(cache: Ref[Map[Int, User]])(id: Int): Task[User] =
  cache.get.flatMap { users =>
    users.get(id) match {
      case Some(user) => ZIO.succeed(user)
      case None       => fetchUser(id).tap(user => cache.update(_ + (id -> user)))
    }
  }
```

This cache has three weaknesses. Two fibers that ask for the same user at the same time both miss and both call `fetchUser`, because only finished results are stored. Nothing groups the lookups that miss into one batched call. And every lookup function needs its own copy of the cache code. ZIO Query keeps one cache keyed by request and stores a promise for every request as soon as it is submitted, so concurrent duplicates wait for the same result and misses are still batched.

## Prerequisites

Add ZIO Query to the build:

```scala
libraryDependencies += "dev.zio" %% "zio-query" % "@VERSION@"
```

We assume that you know how to build a query from a request, see [Creating Data Sources](./creating-data-sources.md). [Running Queries and Caching](./running_queries.md) explains the cache in detail. This guide uses it to solve concrete problems.

## Setting Up the Example

The example has one request type and one data source over an in-memory table of user names. The table, a switch that makes the data source fail, and a counter of data source calls are `Ref`s, so that the examples can change them and observe how often the data source runs:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUserName(id: Int) extends Request[String, String]

val names: Ref[Map[Int, String]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Map(1 -> "Ada", 2 -> "Grace", 3 -> "Edsger")))

val unavailable: Ref[Boolean] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(false))

val calls: Ref[Int] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("UserDataSource") { batches =>
    for {
      _       <- calls.update(_ + 1)
      down    <- unavailable.get
      current <- names.get
    } yield CompletedRequestMap.fromIterableWith(batches.flatten)(
      request => request,
      request =>
        if (down) Exit.fail("user service unavailable")
        else Exit.succeed(current.getOrElse(request.id, "unknown"))
    )
  }

def userName(id: Int): ZQuery[Any, String, String] =
  ZQuery.fromRequest(GetUserName(id))(userDataSource)

def exec[A](effect: UIO[A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

def freshCache: Cache = exec(Cache.empty)

def run[A](query: ZQuery[Any, String, A], cache: Cache): Unit = {
  val (result, made) = exec(query.runCache(cache).either <*> calls.getAndSet(0))
  println(s"result: $result, data source calls: $made")
}
```

The run helper executes a query with the given cache and prints its result together with the number of calls that the data source received during the run.

## Deduplicating Within One Run

Nothing needs to be configured to deduplicate the requests of one query. `ZQuery#run` creates a new cache for every run, and a request that equals an earlier one is answered from it. The requests must be case classes, because the cache compares them with `equals`. To ask for user 1 twice in one query:

```scala mdoc
run(ZQuery.foreachPar(List(1, 1, 2))(userName), freshCache)
```

The data source runs once, with two requests. Every run starts with an empty cache, so the second run of the same query would call the data source again. [Running Queries and Caching](./running_queries.md#running-a-query) shows that case.

## Sharing a Cache Between Queries

Programs often run several queries for one unit of work, for example the queries that build one HTTP response. By default, each of them fetches the same data again. To reuse results across the queries, create one `Cache` for the unit of work with `Cache.empty` and run every query with `ZQuery#runCache`:

```scala mdoc:silent
val requestCache: Cache = freshCache
```

The first query fetches users 1 and 2. The second query needs users 2 and 3 and fetches only user 3. The third query needs data that the cache already holds and does not call the data source at all:

```scala mdoc
run(userName(1) <&> userName(2), requestCache)

run(userName(2) <&> userName(3), requestCache)

run(userName(1) <&> userName(3), requestCache)
```

Create the cache where the unit of work starts, for example when a request arrives, and drop it when the unit of work ends. The default cache never evicts entries and has no size limit, so one cache for the lifetime of the application keeps stale results and grows with every distinct request. The sections below show how to remove entries and how to make them expire.

## Reading Fresh Data

Some requests must see the latest data, for example a read that follows a write in the same unit of work. To bypass the cache for a whole query, call `ZQuery#uncached`. To bypass it for a single request, build the query with `ZQuery.fromRequestUncached`:

```scala mdoc:silent
def freshUserName(id: Int): ZQuery[Any, String, String] =
  ZQuery.fromRequestUncached(GetUserName(id))(userDataSource)
```

Both queries below ask for user 1, which `requestCache` already holds. They still call the data source:

```scala mdoc
run(userName(1).uncached, requestCache)

run(freshUserName(1), requestCache)
```

An uncached request is neither looked up in the cache nor stored in it, so the cached value stays as it was. The requests of the same query that are not wrapped stay cached.

## Invalidating Entries After a Write

When the data changes, the cached result is stale. To drop it, call `Cache#remove` with the request. The next equal request goes to the data source. The following block renames user 1 and then reads the user through `requestCache`, first before and then after the removal:

```scala mdoc:silent
val rename: UIO[Unit] = names.update(_ + (1 -> "Ada Lovelace"))
```

To compare the cached read with a read after the removal, we run the following steps in order:

```scala mdoc
exec(rename)

run(userName(1), requestCache)

exec(requestCache.remove(GetUserName(1)))

run(userName(1), requestCache)
```

The first read returns the old name and does not call the data source. After `Cache#remove`, the second read calls the data source and returns the new name. Remove the entries of every request that the write affects, and do it after the write succeeds.

## Retrying a Request After a Failure

The cache also stores failures. A request that failed once keeps failing for every query that shares the cache, even after the service recovers. The following block makes the data source fail, runs a query, restores the service, and runs the query again with the same cache:

```scala mdoc:silent
val outageCache: Cache = freshCache
```

The following steps make the service fail, restore it, and run the query each time with the same cache:

```scala mdoc
exec(unavailable.set(true))

run(userName(3), outageCache)

exec(unavailable.set(false))

run(userName(3), outageCache)
```

The second run does not call the data source, because the failure of the first run is in the cache. To retry, remove the request first:

```scala mdoc
exec(outageCache.remove(GetUserName(3)))

run(userName(3), outageCache)
```

Applications that share a cache and retry failed queries should remove the failed requests before the retry, or use a fresh cache for every attempt.

## Expiring Entries After a Time

The default cache has no expiry. To keep results for a limited time, implement the `Cache` trait. The runtime calls `Cache#lookup` for every request. It must return a `Left` with a new `Promise` when the request is missing, which the runtime completes with the result, and a `Right` with the stored `Promise` when the request is present. The cache below stores the time at which it stored each entry, and treats an entry that is older than the time to live as missing:

```scala mdoc:silent
final class ExpiringCache(ttlMillis: Long, now: UIO[Long]) extends Cache {

  private val entries = new java.util.concurrent.ConcurrentHashMap[Request[_, _], (Long, Promise[_, _])]()

  def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Promise[E, A]] =
    now.flatMap { time =>
      entries.get(request) match {
        case (createdAt, promise) if time - createdAt < ttlMillis => ZIO.succeed(promise.asInstanceOf[Promise[E, A]])
        case _                                                    => ZIO.fail(())
      }
    }

  def lookup[E, A, B](request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Either[Promise[E, B], Promise[E, B]]] =
    now.flatMap { time =>
      Promise.make[E, B].map { fresh =>
        val entry = entries.compute(
          ev(request),
          (_, old) => if (old != null && time - old._1 < ttlMillis) old else (time, fresh)
        )
        val promise = entry._2.asInstanceOf[Promise[E, B]]
        if (promise eq fresh) Left(fresh) else Right(promise)
      }
    }

  def put[E, A](request: Request[E, A], result: Promise[E, A])(implicit trace: Trace): UIO[Unit] =
    now.flatMap(time => ZIO.succeed(entries.put(request, (time, result))).unit)

  def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit] =
    ZIO.succeed(entries.remove(request)).unit
}
```

In production, pass `Clock.currentTime(TimeUnit.MILLISECONDS)` as the clock. To keep the example deterministic, we use a `Ref` that we advance by hand. With a time to live of 60 seconds, the entry is reused after 30 seconds and fetched again after 90 seconds:

```scala mdoc:silent
val clock: Ref[Long] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0L))

val expiring: Cache = new ExpiringCache(ttlMillis = 60000L, now = clock.get)
```

To run the same query at three points in time:

```scala mdoc
run(userName(2), expiring)

exec(clock.set(30000L))

run(userName(2), expiring)

exec(clock.set(90000L))

run(userName(2), expiring)
```

This cache replaces an expired entry only when the same request is looked up again. Entries of requests that are never repeated stay in memory. For a long-lived cache, add a periodic cleanup, or prefer one cache per unit of work as in the previous sections.

## Going Further

- [Running Queries and Caching](./running_queries.md) describes `ZQuery#runLog`, which returns the cache of a run, and `ZQuery#memoize`, which caches the result of a whole query instead of its requests.
- [How Batching, Pipelining, and Parallelism Work](./how-batching-works.md) explains how deduplication interacts with batches.
- [How to Test Queries](./how-to-test-queries.md) shows how to test queries against in-memory data sources.
