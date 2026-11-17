---
id: described
title: "Described"
description: "Reference for Described[A]: a value paired with a description, how to build it, and how descriptions form data source identifiers."
keywords:
  - "Described Values"
  - "Data Source Identifier"
  - "Aspect Description"
  - "Described"
---

A `Described[A]` is a value of type `A` together with a `String` that describes it. ZIO Query asks for one wherever it needs to compare two functions or effects, which Scala cannot do: the description stands in for the value. This page explains how to build a `Described`, which APIs require one, how the description ends up in the identifier of a data source, and how to write descriptions that keep batching correct. The short introduction in [Aspects](./aspects.md#describing-values-with-described) shows `Described` in the context of aspects.

## The Type

`Described` is a case class with two fields. It is covariant in the type of the value:

```scala
final case class Described[+A](value: A, description: String)
```

The scaladoc states the contract: the description may be used to generate a hash associated with the value, so values that are equal should have the same description, and values that are not equal should have different descriptions. ZIO Query cannot check this, so keeping the description and the value in sync is the job of the caller.

## Building a Described Value

There are two ways to build one. The constructor takes the value and the description, and the `?` syntax adds a description to any value. The syntax is an implicit class in the companion object, `Described.AnySyntax`, so it needs an import:

```scala mdoc:silent
import zio._
import zio.query._
import zio.query.Described._
```

Both forms below build the same value. `value` and `description` read the two fields back:

```scala mdoc
val explicit: Described[String] = Described("hello", "greeting")

val withSyntax: Described[String] = "hello" ? "greeting"

explicit == withSyntax

withSyntax.value

withSyntax.description
```

When the value is a lambda, put it in parentheses and annotate the parameter types, because the compiler does not infer a type for a lambda that is the receiver of `?`:

```scala mdoc:compile-only
val double: Described[Int => Int] = ((n: Int) => n * 2) ? "double"
```

## Where Described Is Required

Every API in the following table takes a `Described` value and derives the identifier of the data source that it returns from the description. `<id>` stands for the identifier of the data source that the method is called on:

| API                                 | Takes                                                                      | Identifier of the result                          |
|-------------------------------------|----------------------------------------------------------------------------|---------------------------------------------------|
| `DataSource#contramap`              | `Described[B => A]`                                                        | `<id>.contramap(<description>)`                   |
| `DataSource#contramapZIO`           | `Described[B => ZIO[R1, Nothing, A]]`                                      | `<id>.contramapZIO(<description>)`                |
| `DataSource#eitherWith`             | `Described[C => Either[A, B]]`                                             | `<id>.eitherWith(<other id>)(<description>)`      |
| `DataSource#provideSomeEnvironment` | `Described[ZEnvironment[R0] => ZEnvironment[R]]`                           | `<id>.provideSomeEnvironment(<description>)`      |
| `DataSource#provideEnvironment`     | `Described[ZEnvironment[R]]`                                               | `<id>.provideSomeEnvironment(_ => <description>)` |
| `DataSourceAspect.around`           | `Described[ZIO[R, Nothing, A]]` and `Described[A => ZIO[R, Nothing, Any]]` | `<id> @@ around(<before>)(<after>)`               |

`QueryAspect.aroundDataSource` takes the same two arguments as `DataSourceAspect.around` and applies it to every data source in a query. The query-level methods `ZQuery#provideEnvironment`, `ZQuery#provideSomeEnvironment` and `ZQuery#provideLayer` also take a `Described` value. They pass it on to `DataSource#provideSomeEnvironment` for every data source that the query uses, and `ZQuery#provideLayer` uses the description of the layer as the description of the environment that it builds. For a query that requires an `Int`, providing the environment looks like this:

```scala mdoc:silent
def exec[A](effect: ZIO[Any, Nothing, A]): A =
  Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

val needsInt: ZQuery[Int, Nothing, Int] = ZQuery.service[Int]
```

```scala mdoc
exec(needsInt.provideEnvironment(ZEnvironment(5) ? "five").run)
```

The `?` call is the only place where the description is written. The first five rows of the table are described further in the [DataSource Reference](./data-source-reference.md), and `DataSourceAspect.around` in [Aspects](./aspects.md#running-code-around-a-data-source).

## How Descriptions Become Identifiers

The identifier of a data source is a `String`, and two data sources are equal when their identifiers are equal. A combinator builds the identifier of its result by appending its own name and the description to the identifier of the original, as in the table above. We can see that with a data source that counts how often it is called:

```scala mdoc:silent
final case class GetUserName(id: Int) extends Request[Nothing, String]

val calls: Ref[Int] = Unsafe.unsafe(implicit unsafe => Ref.unsafe.make(0))

val userDataSource: DataSource[Any, GetUserName] =
  DataSource.make[Any, GetUserName]("UserDataSource") { requests =>
    calls.update(_ + 1) *>
      ZIO.succeed(CompletedRequestMap.fromIterableWith(requests.flatten)(r => r, r => Exit.succeed(s"user-${r.id}")))
  }

def viaIdentity(description: String): DataSource[Any, GetUserName] =
  userDataSource.contramap(((request: GetUserName) => request) ? description)
```

The conversion in `viaIdentity` returns its input, so it is safe to use with `contramap`. Its identifier contains the description:

```scala mdoc
viaIdentity("identity").identifier
```

Requests to equal data sources are batched together, and requests to data sources that are not equal are not. The helper below runs a query that sends one request to each of two data sources and returns the number of calls that the underlying data source received:

```scala mdoc:silent
def callsFor(first: DataSource[Any, GetUserName], second: DataSource[Any, GetUserName]): Int = {
  val query = ZQuery.fromRequest(GetUserName(1))(first).zipPar(ZQuery.fromRequest(GetUserName(2))(second))
  exec(calls.set(0) *> query.run *> calls.get)
}
```

Two data sources with the same description are equal, so their requests form one batch. With different descriptions they are different data sources, so each gets its own call:

```scala mdoc
callsFor(viaIdentity("identity"), viaIdentity("identity"))

callsFor(viaIdentity("identity"), viaIdentity("other"))
```

## Writing Good Descriptions

The description is the only thing that separates two data sources built from different functions. A wrong description does not fail at compile time or at runtime, so follow these rules:

- **Use the same description for the same function.** If two copies of a data source are built from equal functions but described differently, they are never batched together. That costs performance and not correctness.
- **Use a different description for a different function.** If two different functions share a description, their data sources are equal, the requests of both are merged into one batch, and only one of the two data sources receives that batch. The requests of the other one are answered by the wrong function.
- **Put every captured value into the description.** A function that closes over a value is a different function for every value. The following data source needs a `Prefix` from its environment, and the description of the environment contains the prefix:

```scala mdoc:silent
final case class Prefix(value: String)

val prefixed: DataSource[Prefix, GetUserName] =
  DataSource.fromFunctionZIO[Prefix, Nothing, GetUserName, String]("prefixed") { request =>
    ZIO.serviceWith[Prefix](prefix => s"${prefix.value}${request.id}")
  }

def withPrefix(value: String): DataSource[Any, GetUserName] =
  prefixed.provideEnvironment(ZEnvironment(Prefix(value)) ? s"Prefix($value)")
```

Providing the same prefix twice gives equal data sources, and different prefixes give different ones:

```scala mdoc
withPrefix("user-").identifier

withPrefix("user-") == withPrefix("user-")

withPrefix("user-") == withPrefix("account-")
```

:::warning[Described does not change the request]
`DataSource#contramap`, `DataSource#contramapZIO` and `DataSource#eitherWith` return the results of the converted requests unchanged, and the query looks up the result of the request it submitted. A conversion therefore has to produce a request that is equal to the original, or the query dies with a [`QueryFailure`](./query-failure.md). The description cannot fix this: it affects the identifier of the data source and nothing else.
:::
