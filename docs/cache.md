---
id: cache
title: "Cache"
description: "Reference for the Cache trait: how requests map to promises, the get, lookup, put and remove methods, and how to write a custom cache."
keywords:
  - "Query Cache"
  - "Request Deduplication"
  - "Custom Cache"
  - "Cache"
---

A `Cache` is the structure that ZIO Query uses to remember the requests of a run. It maps each request to a `Promise` that holds the result of that request, which is how a run avoids sending the same request to a data source twice. [Running Queries and Caching](./running_queries.md) explains when a cache is created and how `ZQuery#run`, `ZQuery#runCache` and `ZQuery#runLog` use it. This page is the reference for the `Cache` type itself: what it stores, what its four methods do, and how to implement your own.

## The Cache Trait

A `Cache` has four methods. Each one is keyed by a request, and the values are promises that the runtime completes when a data source answers:

```scala
trait Cache {
  def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Promise[E, A]]
  def lookup[E, A, B](request: A)(implicit ev: A <:< Request[E, B], trace: Trace): UIO[Either[Promise[E, B], Promise[E, B]]]
  def put[E, A](request: Request[E, A], result: Promise[E, A])(implicit trace: Trace): UIO[Unit]
  def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit]
}
```

The following table compares the methods:

| Method                 | Returns                                     | Changes the cache                              |
|------------------------|---------------------------------------------|------------------------------------------------|
| `get(request)`         | `IO[Unit, Promise[E, A]]`                   | No                                             |
| `lookup(request)`      | `UIO[Either[Promise[E, B], Promise[E, B]]]` | Inserts a new promise if the request is absent |
| `put(request, result)` | `UIO[Unit]`                                 | Stores the promise, replacing an existing one  |
| `remove(request)`      | `UIO[Unit]`                                 | Removes the entry for the request              |

The cache compares requests with `equals` and `hashCode`, so the requests must be case classes or case objects, see [Request](./request.md#equality-and-caching).

## Creating a Cache

`Cache.empty` returns an effect that creates an empty cache. `Cache.empty(expectedNumOfElements)` does the same and sizes the internal map so that it does not need to grow while it holds that many entries:

```scala
object Cache {
  def empty(implicit trace: Trace): UIO[Cache]
  def empty(expectedNumOfElements: Int)(implicit trace: Trace): UIO[Cache]
}
```

The cache that `Cache.empty` creates is backed by a concurrent hash map. It has no size limit, no eviction and no expiry: an entry stays until `Cache#remove` drops it or the cache itself is garbage collected.

## Setting Up an Example

To see what the cache holds, we need a data source that reports its calls. The following code defines a request type, a data source that prints the batches it receives, a query for one user and a helper that runs an effect to completion:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUserName(id: Int) extends Request[Nothing, String]

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("UserDataSource") { requests =>
    ZIO.succeed {
      println(s"UserDataSource received: ${requests.map(_.map(_.id).mkString("[", ", ", "]")).mkString(" ")}")
      CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, r => Exit.succeed(s"user-${r.id}"))
    }
  }

def userName(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetUserName(id))(userDataSource)

def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())
```

## Reading and Removing Entries

`Cache#get` looks up a request without changing the cache. It fails with the unit value if the request is not in the cache, and otherwise succeeds with the `Promise` that holds the result. The compiler cannot infer the error and result types of a request from a case class, so we give them explicitly. In the following block we run a query with a new cache, and then read the cache:

```scala mdoc:silent
val cache: Cache = exec(Cache.empty)
```

```scala mdoc
exec(userName(1).runCache(cache))

exec(cache.get[Nothing, String](GetUserName(1)).flatMap(_.await).orDieWith(_ => new NoSuchElementException))

exec(cache.get[Nothing, String](GetUserName(2)).isSuccess)
```

The entry for user 1 exists after the run, and the one for user 2 does not. `Cache#get` is a read: it never adds a missing request. Use it to inspect a cache that `ZQuery#runLog` returned.

`Cache#remove` drops the entry for a request. The next equal request is sent to the data source again:

```scala mdoc
exec(cache.remove(GetUserName(1)))

exec(userName(1).runCache(cache))
```

## Looking Up a Request

`Cache#lookup` is the method that the runtime calls for every request. It either finds an entry or inserts one, in a single atomic step, and the shape of the result tells which of the two happened:

- `Left(promise)` means that the request was not in the cache. The cache has stored a new, empty promise for it, and the caller owns that promise: it has to complete it with the result.
- `Right(promise)` means that the request was already in the cache. The promise holds the result, or will hold it when the first caller completes it.

The runtime sends a request to the data source only when `lookup` returns a `Left`, and completes the promise with the answer of the data source. A request whose `lookup` returns a `Right` waits on the existing promise instead of reaching the data source. This is the deduplication described in [Running Queries and Caching](./running_queries.md).

As with `Cache#get`, the compiler cannot infer the type parameters, so we spell them out: the error type, the request type and the result type. We can play the role of the runtime ourselves. The first lookup of user 5 is a miss, so we complete the promise with a value of our choice. The second lookup is a hit and reads that value:

```scala mdoc
exec(cache.lookup[Nothing, GetUserName, String](GetUserName(5)).flatMap {
  case Left(promise) => promise.succeed("prefilled").as("miss, completed the promise")
  case Right(_)      => ZIO.succeed("hit")
})

exec(cache.lookup[Nothing, GetUserName, String](GetUserName(5)).flatMap {
  case Left(_)        => ZIO.succeed("miss")
  case Right(promise) => promise.await.map(value => s"hit, the result is $value")
})
```

A query that runs with this cache finds the promise too, so it gets the prefilled result and never calls the data source. In the output there is no line from the data source:

```scala mdoc
exec(userName(5).runCache(cache))
```

## Seeding a Cache

`Cache#put` stores a promise for a request, replacing any entry that is already there. Together with a promise that is already completed, it lets you provide a result up front, for example to start a test from a known state or to warm a cache with data that you already have:

```scala mdoc:silent
val seeded: Cache =
  exec(for {
    cache   <- Cache.empty
    promise <- Promise.make[Nothing, String]
    _       <- promise.succeed("seeded user")
    _       <- cache.put(GetUserName(7), promise)
  } yield cache)
```

A query that runs with the seeded cache answers user 7 from the cache and sends only user 8 to the data source:

```scala mdoc
exec(userName(7).zipPar(userName(8)).runCache(seeded))
```

The promise stored by `Cache#put` can be completed with a failure as well, with `Promise#fail`, `Promise#die` or `Promise#done`. A request that was answered with a failure is cached as a failure.

## Writing a Custom Cache

To change how requests are stored, for example to add eviction, expiry or statistics, implement `Cache` and pass the instance to `ZQuery#runCache`. The runtime calls `Cache#lookup` and no other method of the trait, so `lookup` has to follow the contract. (The cache from `Cache.empty` is special-cased internally for speed, but it behaves the same way.)

- It must insert a new promise when the request is absent and return it as a `Left`, so that the next equal request finds the entry and gets a `Right`.
- It must be safe to call from many fibers at the same time, because the requests of a query are looked up concurrently. Do the check and the insert as one atomic step.
- It must use the same promise for later lookups until the entry is removed, because the runtime completes only the promise that it received in the `Left`.

The runtime also calls `lookup` for results that a data source returns for requests it did not receive, and completes the promise that the call returns. This is how extra results from a data source end up in the cache.

The following cache wraps another cache and counts how many lookups found an entry and how many did not. It delegates everything to the cache that it wraps, so it follows the contract by construction:

```scala mdoc:silent
final class CountingCache(underlying: Cache, hits: Ref[Int], misses: Ref[Int]) extends Cache {

  def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Promise[E, A]] =
    underlying.get(request)

  def lookup[E, A, B](request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Either[Promise[E, B], Promise[E, B]]] =
    underlying.lookup[E, A, B](request).tap {
      case Left(_)  => misses.update(_ + 1)
      case Right(_) => hits.update(_ + 1)
    }

  def put[E, A](request: Request[E, A], result: Promise[E, A])(implicit trace: Trace): UIO[Unit] =
    underlying.put(request, result)

  def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit] =
    underlying.remove(request)
}

val (counting, hits, misses) =
  exec(for {
    underlying <- Cache.empty
    hits       <- Ref.make(0)
    misses     <- Ref.make(0)
  } yield (new CountingCache(underlying, hits, misses), hits, misses))
```

The next query asks for user 1 twice and for user 2 once. Two of the three lookups are misses and the data source receives them as one batch. The other one is a hit:

```scala mdoc
exec(ZQuery.foreachPar(List(1, 2, 1))(userName).runCache(counting))

exec(hits.get.zip(misses.get))
```

## Sharing and Lifetime

A cache lives as long as something references it, so a cache that you share between runs keeps all its results, including failed ones, for that whole time. Create one cache for the runs that make up one logical operation, such as the queries that handle a single incoming request. Do not keep one for the lifetime of an application unless a custom implementation bounds its size and the age of its entries. [Running Queries and Caching](./running_queries.md#when-to-share-a-cache) discusses this trade-off, and [How to Cache Query Results](./how-to-cache-query-results.md) shows the patterns in practice.
