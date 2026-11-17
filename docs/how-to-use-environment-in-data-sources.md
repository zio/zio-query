---
id: how-to-use-environment-in-data-sources
title: "How to Use Environment in Data Sources"
description: "Write data sources that use ZIO services such as a database, and provide them with run, provideLayer, provideEnvironment or a self-contained data source."
keywords:
  - "Data Source Environment"
  - "ZIO Services"
  - "Provide Layer"
  - "ZIO Query"
---

A real data source rarely stands alone: it needs a database pool, an HTTP client or a configuration. ZIO Query models those dependencies with the environment type `R` of `DataSource[R, A]`, the same way ZIO does. This guide shows how to write a data source that reads a service from the environment, how the requirement reaches the query, and the three places where you can provide it: when you run the query, on the query, and on the data source.

## Setting Up an Example

The examples use a `Database` service backed by an in-memory table. It records the ids of every call, so we can see how queries are batched. The data source does not create the service: it asks for it with `ZIO.serviceWithZIO`, which puts `Database` in the environment of the data source:

```scala mdoc:silent
import zio._
import zio.query._

final case class Database(rows: Map[Int, String], calls: Ref[Chunk[List[Int]]]) {
  def names(ids: Chunk[Int]): UIO[Map[Int, String]] =
    calls.update(_ :+ ids.toList).as(rows.filter { case (id, _) => ids.contains(id) })
}

final case class GetUser(id: Int) extends Request[Nothing, Option[String]]

val users: DataSource[Database, GetUser] =
  DataSource.Batched.make[Database, GetUser]("Users") { requests =>
    ZIO.serviceWithZIO[Database](_.names(requests.map(_.id))).map { found =>
      CompletedRequestMap.fromIterableWith(requests)(request => request, request => Exit.succeed(found.get(request.id)))
    }
  }

def userName(id: Int): ZQuery[Database, Nothing, Option[String]] =
  ZQuery.fromRequest(GetUser(id))(users)
```

`DataSource.Batched.make` is described in [DataSource Reference](./data-source-reference.md). What matters here is the type: `users` is a `DataSource[Database, GetUser]`, so it cannot run before a `Database` is provided.

The remaining setup creates a `Database` and a layer for it, and two helpers. `exec` runs an effect, and `show` runs a query and prints the result together with the batches that reached the database, ordered by their smallest id:

```scala mdoc:silent
val calls: Ref[Chunk[List[Int]]] = Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(Chunk.empty[List[Int]]))

val database: Database = Database(Map(1 -> "Ada", 2 -> "Grace"), calls)

val databaseLayer: ULayer[Database] = ZLayer.succeed(database)

def exec[A](effect: UIO[A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

def show[A](effect: UIO[A]): Unit = {
  println(s"result: ${exec(effect)}")
  println(s"batches: ${exec(calls.getAndSet(Chunk.empty)).sortBy(_.min)}")
}
```

## The Requirement Reaches the Query

A query needs every service that its data sources need. `userName` is a `ZQuery[Database, Nothing, Option[String]]`, and a query that combines it with another data source that needs `Clock` or `Random` needs `Database with Clock with Random`. A query with an unprovided requirement cannot be run: `run` returns a `ZIO[Database, Nothing, ...]`, and that effect cannot be run until the environment is provided.

You can also read a service directly in a query, without a data source, with `ZQuery.service`, `ZQuery.serviceWith` and `ZQuery.serviceWithZIO`. Those add the service to the requirement of the query in the same way.

## Provide the Environment When Running

The simplest solution is to leave the query alone and provide the environment to the effect that `run` returns, with the usual ZIO operators:

```scala mdoc
show(ZQuery.foreachPar(List(1, 2, 3))(userName).run.provide(databaseLayer))
```

This is the right choice for most applications: the environment is provided once, at the edge of the program, with the layers that the rest of the application already uses. The three ids reached the database as a single batch, as they would without the environment. Nothing else is needed, and no `Described` value is involved.

## Provide the Environment on the Query

Provide the environment on the query when only part of a larger query needs it, or when a function must return a query that needs no environment. `ZQuery` has four methods for it:

| Method                   | Takes                                            | Leaves              |
|--------------------------|--------------------------------------------------|---------------------|
| `provideEnvironment`     | `Described[ZEnvironment[R]]`                     | `ZQuery[Any, E, A]` |
| `provideLayer`           | `Described[ZLayer[R0, E1, R]]`                   | `ZQuery[R0, E1, A]` |
| `provideSomeEnvironment` | `Described[ZEnvironment[R0] => ZEnvironment[R]]` | `ZQuery[R0, E, A]`  |
| `provideSomeLayer[R0]`   | `Described[ZLayer[R0, E1, R1]]`                  | `ZQuery[R0, E1, A]` |

Every method takes a `Described` value: the value together with a `String` description. Create one with `Described(value, "description")` or, after `import zio.query.Described._`, with `value ? "description"`. See [Aspects](./aspects.md#describing-values-with-described) for what `Described` is.

### Provide a Whole Environment

`provideEnvironment` takes a `ZEnvironment` and removes the whole requirement:

```scala mdoc
import zio.query.Described._

show(userName(1).provideEnvironment(ZEnvironment(database) ? "database").run)
```

`provideLayer` builds the layer when the query runs, and fails the query with the error of the layer if building it fails. It suits a layer that only wraps values, such as configuration:

```scala mdoc
show(userName(2).provideLayer(databaseLayer ? "databaseLayer").run)
```

:::warning[Do Not Use provideLayer for Resources]
`provideLayer` and `provideSomeLayer` build the layer again each time the query runs, and they close the scope of the layer after the first step of the query, before the data sources of that step have run. A service created with `ZLayer.scoped` or `ZIO.acquireRelease`, such as a connection pool, is therefore already released when the data source uses it. Provide such a layer once with `run.provide`, as above, or build the resource yourself and pass it with `provideEnvironment`.
:::

### Provide Part of the Environment

`provideSomeLayer` and `provideSomeEnvironment` provide some services and leave the others. The following query reads a `Config` service and needs a `Database` for the lookup, so its environment is `Config with Database`:

```scala mdoc:silent
final case class Config(prefix: String)

val greeting: ZQuery[Config with Database, Nothing, String] =
  for {
    config <- ZQuery.service[Config]
    name   <- userName(1)
  } yield s"${config.prefix}${name.getOrElse("nobody")}"
```

`provideSomeLayer[R0]` takes the type of the environment that remains, and a layer for the rest. Here the layer provides the `Database` and the query still needs `Config`:

```scala mdoc:silent
val needsConfig: ZQuery[Config, Nothing, String] =
  greeting.provideSomeLayer[Config](databaseLayer ? "databaseLayer")
```

The remaining requirement is provided when the query is run:

```scala mdoc
show(needsConfig.run.provide(ZLayer.succeed(Config("Hello, "))))
```

`provideSomeEnvironment` is the more general form: it takes a function from the smaller environment to the one the query needs, so it can also derive the service from the environment that remains:

```scala mdoc:silent
val alsoNeedsConfig: ZQuery[Config, Nothing, String] =
  greeting.provideSomeEnvironment[Config](
    Described((env: ZEnvironment[Config]) => env ++ ZEnvironment(database), "add database")
  )
```

### Descriptions Decide Whether Requests Are Batched

ZIO Query batches the requests that go to equal data sources. When you provide an environment to a query, each of its data sources is wrapped, and the description becomes part of the identifier of the wrapped data source. Two data sources are equal only if their identifiers are. So the same description means the same environment, and the requests are batched together:

```scala mdoc
show(
  userName(1)
    .provideEnvironment(ZEnvironment(database) ? "database")
    .zipPar(userName(2).provideEnvironment(ZEnvironment(database) ? "database"))
    .run
)
```

A different description makes the data sources different, so the requests cannot share a batch, even though the environment is the same value:

```scala mdoc
show(
  userName(1)
    .provideEnvironment(ZEnvironment(database) ? "first")
    .zipPar(userName(2).provideEnvironment(ZEnvironment(database) ? "second"))
    .run
)
```

The database received two calls with one id each. To avoid this, give equal environments the same description, or provide the environment once on the combined query, or when running it.

:::note[Use Different Descriptions for Different Environments]
The opposite mistake is worse: two different environments with the same description are treated as one. Requests meant for two tenants could then be sent to the data source of the first tenant. Describe each distinct environment with a distinct string.
:::

## Provide the Environment on the Data Source

If a function returns a data source, or a library exposes one, you may not want callers to deal with the environment at all. `DataSource#provideEnvironment` and `DataSource#provideSomeEnvironment` remove the requirement from the data source itself, and take `Described` values in the same way:

```scala mdoc:silent
val readyUsers: DataSource[Any, GetUser] =
  users.provideEnvironment(ZEnvironment(database) ? "database")

def readyUserName(id: Int): ZQuery[Any, Nothing, Option[String]] =
  ZQuery.fromRequest(GetUser(id))(readyUsers)
```

Queries built with `readyUsers` need no environment, so they run without `provide`:

```scala mdoc
show(ZQuery.foreachPar(List(1, 2))(readyUserName).run)
```

The identifier of the new data source is derived from the original one and the description, so all queries that use `readyUsers` share its batches. `provideSomeEnvironment` removes only part of the requirement, in the same way as the query method above. See [DataSource Reference](./data-source-reference.md#providing-the-environment) for its signature.

## Choosing Where to Provide

| Where                 | Use it when                                                       | Needs `Described` |
|-----------------------|-------------------------------------------------------------------|-------------------|
| `run.provide(...)`    | The application provides all services once, at its edge           | No                |
| `ZQuery#provide*`     | Part of a query needs a different service                         | Yes               |
| `DataSource#provide*` | A function or library hands out a data source with no requirement | Yes               |

In all three cases the data source stays the same: it declares what it needs in its type, and the caller decides how to satisfy it. A test can then provide a stub service instead, as shown in [How to Test Queries](./how-to-test-queries.md).
