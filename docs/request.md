---
id: request
title: "Request"
description: "Reference for Request[E, A]: how to model requests as case classes and ADTs, how equality drives caching, and how results are typed."
keywords:
  - "Query Requests"
  - "Request Equality"
  - "Request ADT"
  - "Request"
---

A `Request[E, A]` describes one piece of data that a query wants from a [data source](./creating-data-sources.md): it fails with an `E` or succeeds with an `A`. A request is plain data and says nothing about how to fetch the value. This page shows how to define requests, how to group them into a hierarchy that one data source serves, and why the equality of a request decides whether ZIO Query sends it to the data source once or many times.

## The Request Trait

`Request` is a trait with two type parameters and no members:

```scala
trait Request[E, A]
```

`E` is the error type that the data source may complete the request with, and `A` is the type of the successful result. A request type is almost always a case class or case object, because its fields are the parameters of the lookup and ZIO Query compares requests with `equals`.

## Defining Requests

The examples on this page use the following request types. `GetUser` can fail with a `String`, and `CountUsers` cannot fail, which we express with `Nothing`:

```scala mdoc:silent
import zio._
import zio.query._

final case class GetUser(id: Int) extends Request[String, String]

case object CountUsers extends Request[Nothing, Int]
```

A request is attached to a data source only when we build a query. `ZQuery.fromRequest` takes the request and the data source, and requires evidence of type `A <:< Request[E, B]`, from which the compiler infers the error type `E` and the result type `B` of the query. The following data source answers `GetUser` and fails for ids that are not positive:

```scala mdoc:silent
val users: DataSource[Any, GetUser] =
  DataSource.fromFunctionZIO[Any, String, GetUser, String]("users") { request =>
    if (request.id > 0) ZIO.succeed(s"user-${request.id}")
    else ZIO.fail(s"invalid id ${request.id}")
  }

def user(id: Int): ZQuery[Any, String, String] =
  ZQuery.fromRequest(GetUser(id))(users)

def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

def attempt[E, A](query: ZQuery[Any, E, A]): Either[E, A] =
  exec(query.run.either)
```

The type `ZQuery[Any, String, String]` comes straight from `Request[String, String]`. The data source completes each request with an `Exit[E, A]` of the same types, so a successful request produces a value and a failing request produces a typed error:

```scala mdoc
attempt(user(1))

attempt(user(0))
```

The type parameters of `Request` are checked by the compiler where results are stored. `CompletedRequestMap.single(request, exit)` accepts a `Request[E, A]` only together with an `Exit[E, A]`. The [CompletedRequestMap](./completed-request-map.md) page lists all the constructors.

## Request Hierarchies

One data source often serves several kinds of requests with different result types, for example a user service that returns all ids and the name of one user. Model them as a sealed trait that is parameterized by the result type, and let each case fix that type:

```scala mdoc:silent
sealed trait UserRequest[A] extends Request[Nothing, A]

case object GetAllIds                 extends UserRequest[List[Int]]
final case class GetNameById(id: Int) extends UserRequest[String]
```

The data source is typed on `UserRequest[_]`, so it receives every request of the hierarchy. For each request it matches on the case and stores a result of the type that the case promises. Because `CompletedRequestMap.single` checks the types, the compiler rejects a `GetNameById` entry that has a `List[Int]` result:

```scala mdoc:silent
val userRequests: DataSource[Any, UserRequest[_]] =
  DataSource.Batched.make[Any, UserRequest[_]]("userRequests") { requests =>
    ZIO.succeed(
      CompletedRequestMap.combine(requests.map {
        case GetAllIds       => CompletedRequestMap.single(GetAllIds, Exit.succeed(List(1, 2, 3)))
        case GetNameById(id) => CompletedRequestMap.single(GetNameById(id), Exit.succeed(s"user-$id"))
      })
    )
  }
```

Queries built from the hierarchy get the result type of the request they send. `DataSource` is contravariant in its request type, so a `DataSource[Any, UserRequest[_]]` can be used wherever a data source for one of the subtypes is needed:

```scala mdoc:silent
val allIds: ZQuery[Any, Nothing, List[Int]] =
  ZQuery.fromRequest(GetAllIds)(userRequests)

def nameById(id: Int): ZQuery[Any, Nothing, String] =
  ZQuery.fromRequest(GetNameById(id))(userRequests)
```

```scala mdoc
exec(allIds.zip(nameById(2)).run)
```

Use a hierarchy when the requests share a backend. When two requests need different backends, define them as separate types and separate data sources, and combine the data sources only if needed with `DataSource#eitherWith`, see the [DataSource Reference](./data-source-reference.md).

## Equality and Caching

ZIO Query identifies a request by `equals` and `hashCode`. Two things depend on that:

- The cache of a run is keyed by the request. A request that is equal to an earlier one in the same run is answered from the cache and does not reach the data source again. See [Running Queries and Caching](./running_queries.md) for how the cache behaves.
- A data source returns a `CompletedRequestMap`, and the query looks up the result of each request in it by equality. The key in the map must be equal to the request that the query submitted.

Case classes and case objects get structural equality for free, which is why requests are defined that way. A request that is a plain class compares by reference, so two requests with the same fields are different keys. To see the difference, we define two requests with the same field, one as a case class and one as a plain class. The data source below records the size of every batch it receives:

```scala mdoc:silent
final case class ByValue(id: Int)    extends Request[Nothing, String]
final class ByReference(val id: Int) extends Request[Nothing, String]

val batchSizes: Ref[Chunk[Int]] =
  Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[Int]))

val counting: DataSource[Any, Request[Nothing, String]] =
  DataSource.Batched.make[Any, Request[Nothing, String]]("counting") { requests =>
    batchSizes.update(_ :+ requests.size) *>
      ZIO.succeed(CompletedRequestMap.fromIterableWith(requests)(r => r, _ => Exit.succeed("ok")))
  }
```

Two requests with the same `id` reach the data source once when the request is a case class:

```scala mdoc
exec(ZQuery.foreachPar(List(1, 1))(id => ZQuery.fromRequest(ByValue(id))(counting)).run)

exec(batchSizes.getAndSet(Chunk.empty))
```

The plain class has no structural equality, so both requests are sent and the data source does the work twice:

```scala mdoc
exec(ZQuery.foreachPar(List(1, 1))(id => ZQuery.fromRequest(new ByReference(id))(counting)).run)

exec(batchSizes.getAndSet(Chunk.empty))
```

The same rule applies to the fields of a case class. Every field of the first parameter list takes part in `equals`, so a field that differs between otherwise identical requests, such as a timestamp, a random value or a function, prevents deduplication. Keep such values out of the request. If the data source needs one of them anyway, put it in a second parameter list, which `equals` and `hashCode` ignore:

```scala mdoc:compile-only
final case class GetUserTraced(id: Int)(val traceId: String) extends Request[Nothing, String]
```

With this definition `GetUserTraced(1)("a")` and `GetUserTraced(1)("b")` are the same request, and the data source receives the instance of the first one that was submitted.

:::warning[Keys must stay equal]
If a data source stores a result under a key that is not equal to the request it received, the query dies with a [`QueryFailure`](./query-failure.md). Return the request as it was received, or a request that is equal to it.
:::

## Choosing Result Types

A request that asks for something that may not exist can express it in the result type, with `Request[Nothing, Option[User]]`, or in the error type, with `Request[NotFound, User]`. Both are visible to the caller in the type of the query. Do not leave a request out of the `CompletedRequestMap` to signal "not found": that is a bug in the data source and dies with a `QueryFailure`. [How to Handle Missing Results](./how-to-handle-missing-results.md) compares the options.

## Summary

| To...                                      | Do this                                                            |
|--------------------------------------------|--------------------------------------------------------------------|
| Get deduplication and caching of a request | Define it as a case class or case object                           |
| Serve several result types from one source | Use a sealed trait parameterized by the result type                |
| Express a failure of the lookup            | Choose an error type `E` and complete the request with `Exit.fail` |
| Carry data that must not affect identity   | Put it in a second parameter list of the case class                |
