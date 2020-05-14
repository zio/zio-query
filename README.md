# ZIO Query

| CI | Release | Snapshot | Discord |
| --- | --- | --- | --- |
| [![Build Status][Badge-Circle]][Link-Circle] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Badge-Discord]][Link-Discord] |

# Summary

A `ZQuery[R, E, A]` is a purely functional description of an effectual query that may contain requests to one or more data sources. Similarly to `ZIO[R, E, A]`, it requires an environment `R`, may fail with an `E` or succeed with an `A`. All requests that do not need to be performed sequentially will automatically be batched, allowing for aggressive data source specific optimizations. Requests will also automatically be deduplicated and cached.

This allows for writing queries in a high level, compositional style, with confidence that they will automatically be optimized. For example, consider the following query from a user service.

```scala
val getAllUserIds: ZQuery[Any, Nothing, List[Int]] = ???
def getUserNameById(id: Int): ZQuery[Any, Nothing, String] = ???

for {
  userIds   <- getAllUserIds
  userNames <- ZQuery.foreachPar(userIds)(getUserNameById)
} yield userNames
```

This would normally require N + 1 queries, one for `getAllUserIds` and one for each call to `getUserNameById`. In contrast, `ZQuery` will automatically optimize this to two queries, one for `userIds` and one for `userNames`, assuming an implementation of the user service that supports batching.

## Building a DataSource

To build a `ZQuery` that executes a request, you first need to build a `DataSource`. A `DataSource[R, E, A]` defines how to execute requests of type `A` and it requires 2 things:

- an `identifier` that uniquely identifies the data source (requests from _different_ data sources will _not_ be batched together)
- a effectful function `run` from an `Iterable` of requests to a `Map` of requests and results

Let's consider `getUserNameById` from the previous example. We need to define a corresponding request type that extends `Request` for a given response type:

```scala
case class GetUserName(id: Int) extends Request[Throwable, String]
```

Now let's build the corresponding `DataSource`. We need to implement the following functions:

```scala
val UserDataSource = new DataSource[Any, GetUserName] {
  override val identifier: String = ???
  override def run(requests: Iterable[GetUserName]): ZIO[Any, Throwable, CompletedRequestMap] = ???
}
```

We will use "UserDataSource" as our identifier. This name should not be reused for other data sources.

```scala
override val identifier: String = "UserDataSource"
```

We will define two different behaviors depending on whether we receive a single request or multiple requests at once.
For each request, we need to insert into the result map a value of type `Either` (`Left` for an error and `Right` for a success).

```scala
override def run(requests: Iterable[GetUserName]): ZIO[Any, Nothing, CompletedRequestMap] = {
  val resultMap = CompletedRequestMap.empty
  requests.toList match {
    case request :: Nil =>
      // get user by ID e.g. SELECT name FROM users WHERE id = $id
      val result: Task[String] = ???
      result.either.map(resultMap.insert(request))
    case batch =>
      // get multiple users at once e.g. SELECT id, name FROM users WHERE id IN ($ids)
      val result: Task[List[(Int, String)]] = ???
      result.fold(
        err => requests.foldLeft(resultMap) { case (map, req) => map.insert(req)(Left(err)) },
        _.foldLeft(resultMap) { case (map, (id, name)) => map.insert(GetUserName(id))(Right(name)) }
      )
  }
}
```

Now to build a `ZQuery` from it, we can use `ZQuery.fromRequest` and just pass the request and the data source:

```scala
def getUserNameById(id: Int): ZQuery[Any, Throwable, String] =
  ZQuery.fromRequest(GetUserName(id))(UserDataSource)
```

To run a `ZQuery`, simply use `ZQuery#run` which will return a `ZIO[R, E, A]`.

## ZQuery constructors and operators

There are several ways to create a `ZQuery`. We've seen `ZQuery.fromRequest`, but you can also:

- create from a pure value with `ZQuery.succeed`
- create from an effect value with `ZQuery.fromEffect`
- create from multiple queries with `ZQuery.collectAllPar` and `ZQuery.foreachPar` and their sequential equivalents `ZQuery.collectAll` and `ZQuery.foreach`

If you have a `ZQuery` object, you can use:

- `map` and `mapError` to modify the returned result or error
- `flatMap` or `zip` to combine it with other `ZQuery` objects
- `provide` and `provideSome` to eliminate some of the `R` requirements

There are several ways to run a `ZQuery`:

- `runCache` runs the query using a given pre-populated cache. This can be useful for deterministically "replaying" a query without executing any new requests.
- `runLog` runs the query and returns its result along with the cache containing a complete log of all requests executed and their results. This can be useful for logging or analysis of query execution.
- `run` runs the query and returns its result.

# Contributing
[Documentation for contributors](https://zio.github.io/zio-query/docs/about/about_contributing)

## Code of Conduct

See the [Code of Conduct](https://zio.github.io/zio-query/docs/about/about_coc)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].


# License
[License](LICENSE)

[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-query_2.12.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-query_2.12.svg "Sonatype Snapshots"
[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Badge-Circle]: https://circleci.com/gh/zio/zio-query.svg?style=svg "circleci"
[Link-Circle]: https://circleci.com/gh/zio/zio-query "circleci"
[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-query_2.12/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-query_2.12/ "Sonatype Snapshots"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"

