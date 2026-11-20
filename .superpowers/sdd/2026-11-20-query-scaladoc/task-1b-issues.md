# Task 1b: library bugs found outside ZQuery.scala

All found on 2026-11-20 at 4e04bed1b (= v0.7.9 in `zio-query/shared/src/main`). Every reproducer is a
self-contained scala-cli program (`scala-cli run --server=false <dir>`) on Scala 2.13.18 against
`dev.zio::zio-query:0.7.9`; the output is pasted from a real run. Line numbers refer to the base commit.

## 1. `DataSource#contramap`, `contramapZIO` and `eitherWith` key results by the converted request

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** the three combinators convert each request of the new type into a request of the
  underlying data source, run the underlying data source, and return its `CompletedRequestMap` unchanged.
  That map is keyed by the converted requests, but the query looks up the request it submitted. Unless the
  conversion returns a request equal to its input, every query through the combined data source dies with a
  `QueryFailure`, after the underlying data source has done the work. So the combinators work only for
  conversions that do nothing (identity, or an upcast in `eitherWith`).
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._
  import zio.query.Described._

  object Main extends ZIOAppDefault {
    final case class GetUserName(id: Int) extends Request[Nothing, String]
    final case class UserById(id: Int)    extends Request[Nothing, String]

    val userNames: DataSource[Any, GetUserName] =
      DataSource.fromFunctionBatched[GetUserName, String]("UserNames") { requests =>
        println(s"UserNames received ${requests.mkString(", ")}")
        requests.map(request => s"user-${request.id}")
      }

    val byId: DataSource[Any, UserById] =
      userNames.contramap(((request: UserById) => GetUserName(request.id)) ? "toGetUserName")

    val byIdZIO: DataSource[Any, UserById] =
      userNames.contramapZIO(((request: UserById) => ZIO.succeed(GetUserName(request.id))) ? "toGetUserNameZIO")

    val routed: DataSource[Any, UserById] =
      userNames.eitherWith(userNames)(((request: UserById) => Left(GetUserName(request.id))) ? "left")

    def show(label: String, ds: DataSource[Any, UserById]) =
      ZQuery.fromRequest(UserById(1))(ds).run.exit.flatMap(exit => Console.printLine(s"$label: $exit").orDie)

    val run = show("contramap", byId) *> show("contramapZIO", byIdZIO) *> show("eitherWith", routed)
  }
  // UserNames received GetUserName(1)
  // contramap: Failure(Die(zio.query.QueryFailure: Data source UserNames.contramap(toGetUserName) did not complete request UserById(1).,StackTrace.none))
  // UserNames received GetUserName(1)
  // contramapZIO: Failure(Die(zio.query.QueryFailure: Data source UserNames.contramapZIO(toGetUserNameZIO) did not complete request UserById(1).,StackTrace.none))
  // UserNames received GetUserName(1)
  // eitherWith: Failure(Die(zio.query.QueryFailure: Data source UserNames.eitherWith(UserNames)(left) did not complete request UserById(1).,StackTrace.none))
  ```

- **Expected behaviour:** each query succeeds with `user-1`: the result of the converted request is
  returned for the request that the query submitted, as the Scaladoc ("executes requests of type `B` using
  the specified function to transform `B` requests into requests that this data source can execute")
  promises.
- **Root cause:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:85`
  (`self.runAll(requests.map(_.map(f.value)))`), `:99` (`ZIO.foreach(requests)(ZIO.foreachPar(_)(f.value)).flatMap(self.runAll)`)
  and `:119-120` (`reqs.partitionMap(f.value)` then `self.runAll(Chunk.single(as)) <&> that.runAll(Chunk.single(bs))`)
  pass the underlying map through without re-keying it. The lookup that fails is
  `zio-query/shared/src/main/scala/zio/query/internal/BlockedRequests.scala:128` / `:134`, and `:295` turns
  the miss into `Exit.die(QueryFailure(dataSource, req))`. As a side effect, the unrequested entries are
  stored in the cache as leftovers (`BlockedRequests.scala` `cacheLeftovers`).
- **Impact:** the three combinators cannot be used for their stated purpose (adapting a data source to a new
  request type, or routing one request type to two data sources through a conversion). Every query through
  them dies, and only at run time. `docs/data-source-reference.md` and `docs/aspects.md` already warn about it.
- **Test gap:** `grep -rn 'contramap\|eitherWith' zio-query/shared/src/test` finds nothing; no test covers the
  three combinators.
- **Suggested fix:** keep the pairs `(original, converted)` and re-key the map: for each original request
  `b`, look up `f(b)` in the returned map and store the result under `b` (for `eitherWith`, likewise for the
  `Left` and `Right` sides). `contramapZIO` needs the converted requests zipped with the originals, which
  `ZIO.foreachPar` preserves by position.

## 2. `CompletedRequestMap#requests` throws `ClassCastException` for most maps

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** `requests` casts `map.keySet` to `Set[Request[_, _]]`, which is `scala.collection.immutable.Set`.
  For maps backed by a `mutable.HashMap` (every constructor except `empty`, `single`, `apply` with at most one
  entry, and the deprecated `++`, `insert` and `insertOption`, the last only when it adds an entry),
  `keySet` is a `collection.Set` view, and the cast fails. Checked on Scala 2.12.21, 2.13.18 and 3.3.9 on the
  JVM: same result on all three. On Scala.js a development build fails with an `UndefinedBehaviorError`, and a
  release build does not check the cast (Scala.js 1.22.0, scala-cli 1.19.0: fastLink
  `throws org.scalajs.linker.runtime.UndefinedBehaviorError: java.lang.ClassCastException: scala.collection.MapOps$KeySet cannot be cast to scala.collection.immutable.Set`;
  `--js-mode release`: `requests returned: Set(G(1), G(2)) size 2 contains true`). The JVM message on 2.12 is
  `scala.collection.mutable.HashMap$$anon$1 cannot be cast ...`.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main {
    final case class GetUserName(id: Int) extends Request[Nothing, String]

    def attempt(label: String)(map: => CompletedRequestMap): Unit =
      println(s"$label: " + (try map.requests.toString catch { case t: Throwable => s"throws ${t.getClass.getName}" }))

    def main(args: Array[String]): Unit = {
      attempt("single")(CompletedRequestMap.single(GetUserName(1), Exit.succeed("Ann")))
      attempt("apply with 2 entries")(
        CompletedRequestMap(GetUserName(1) -> Exit.succeed("Ann"), GetUserName(2) -> Exit.succeed("Bob"))
      )
      attempt("fromIterable")(CompletedRequestMap.fromIterable(List(GetUserName(1) -> Exit.succeed("Ann"))))
      attempt("combine")(CompletedRequestMap.combine(List(CompletedRequestMap.empty)))
    }
  }
  // single: Set(GetUserName(1))
  // apply with 2 entries: throws java.lang.ClassCastException
  // fromIterable: throws java.lang.ClassCastException
  // combine: throws java.lang.ClassCastException
  ```

  The full message on 2.13.18: `class scala.collection.MapOps$KeySet cannot be cast to class
  scala.collection.immutable.Set`.
- **Expected behaviour:** `requests` returns the set of keys for every map on every platform, e.g.
  `Set(GetUserName(1), GetUserName(2))`.
- **Root cause:** `zio-query/shared/src/main/scala/zio/query/CompletedRequestMap.scala:101`
  (`map.keySet.asInstanceOf[Set[Request[_, _]]]`) with `Mutable` at `:255-256` holding a
  `mutable.HashMap`. The cast was safe before `CompletedRequestMap` gained the mutable implementation
  (PR #470, "Optimize `CompletedRequestMap`").
- **Impact:** any user code that inspects a map with `requests` (for example a test of a data source, or a
  data source that checks which requests it answered) crashes, including on maps returned by the built-in
  `DataSource.fromFunction*` data sources.
- **Test gap:** `grep -rn '\.requests\b' zio-query/shared/src/test` finds no call to `CompletedRequestMap#requests`.
- **Suggested fix:** return `map.keySet.toSet.asInstanceOf[Set[Request[_, _]]]` (or build an immutable set
  from the keys), keeping the cheap path for `Immutable`.

## 3. `DataSource#eitherWith` calls both data sources for every batch, even with no requests

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** for each batch, `eitherWith` calls `self.runAll(Chunk.single(as)) <&> that.runAll(Chunk.single(bs))`
  even when `as` or `bs` is empty. A data source built with `DataSource.make` (or a hand-written `runAll`)
  then receives `Chunk(Chunk())`, and the query waits for that call too: if the idle side is slow, every
  query through the combined data source is slow, and if it never answers, the query never completes.
  (`DataSource.Batched` data sources return early on an empty batch, so the `fromFunction*` constructors are
  not called.)
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._
  import zio.query.Described._

  object Main extends ZIOAppDefault {
    sealed trait UserRequest                extends Request[Nothing, String]
    final case class ByCache(id: Int)      extends UserRequest
    final case class ByDatabase(id: Int)   extends UserRequest

    val cache: DataSource[Any, ByCache] =
      DataSource.fromFunction[ByCache, String]("Cache")(request => s"cached-${request.id}")

    val database: DataSource[Any, ByDatabase] =
      DataSource.make[Any, ByDatabase]("Database") { batches =>
        Console.printLine(s"Database received $batches").orDie.as(
          CompletedRequestMap.fromIterableWith(batches.flatten)(r => r, r => Exit.succeed(s"stored-${r.id}"))
        )
      }

    def byType(request: UserRequest): Either[ByCache, ByDatabase] =
      request match {
        case r: ByCache    => Left(r)
        case r: ByDatabase => Right(r)
      }

    val routed: DataSource[Any, UserRequest] =
      cache.eitherWith(database)(Described(byType _, "byType"))

    // Routes every request to the left; the right side never answers.
    val leftOrNever: DataSource[Any, ByCache] =
      cache.eitherWith(DataSource.never)(Described((request: ByCache) => Left(request): Either[ByCache, Nothing], "left"))

    val run =
      ZQuery.fromRequest(ByCache(1))(routed).run.flatMap(result => Console.printLine(s"result: $result")) *>
        ZQuery.fromRequest(ByCache(1))(leftOrNever).run.timeout(2.seconds).flatMap(result => Console.printLine(s"leftOrNever: $result"))
  }
  // Database received Chunk(Chunk())
  // result: cached-1
  // leftOrNever: None
  ```

- **Expected behaviour:** a side with no requests in a batch is not called: `Database received` is not
  printed, and `leftOrNever` succeeds with `Some(cached-1)`.
- **Root cause:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:119-120`: the `<&>` runs both
  sides unconditionally.
- **Impact:** extra calls with empty batches (a backend call such as `SELECT ... WHERE id IN ()` can fail),
  and the latency of the combined data source is the latency of the slower side even for requests that only
  go to the faster one.
- **Test gap:** `grep -rn 'eitherWith' zio-query/shared/src/test` finds nothing.
- **Suggested fix:** call a side only when its chunk is non-empty, e.g.
  `(if (as.isEmpty) Exit.succeed(CompletedRequestMap.empty) else self.runAll(Chunk.single(as))) <&> (...)`.
