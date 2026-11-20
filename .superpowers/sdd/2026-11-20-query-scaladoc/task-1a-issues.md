# Task 1a: library bugs found in ZQuery.scala

All found on 2026-11-20 at 4e04bed1b (= v0.7.9 in `zio-query/shared/src/main`). Every reproducer is a
self-contained scala-cli program (`scala-cli run --server=false <dir>`) on Scala 2.13.18 against
`dev.zio::zio-query:0.7.9`; the output is pasted from a real run. Line numbers refer to the base commit.

## 1. `ZQuery#memoize` does not memoize queries that make requests

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** `memoize` stores the first step of the query. For a query that makes a request, that step is
  "blocked on these requests, then continue with this". Every use of the memoized query returns the same
  blocked step, so the request is sent once per use (even with caching enabled) and the work after it runs
  once per use.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    final case class GetUserName(id: Int) extends Request[Nothing, String]

    val userNames: DataSource[Any, GetUserName] =
      DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
        println(s"UserNames called with ${requests.mkString(", ")}")
        requests.map(request => s"user-${request.id}")
      }

    val run =
      for {
        counter <- Ref.make(0)
        query    = ZQuery.fromRequest(GetUserName(1))(userNames).mapZIO(name => counter.update(_ + 1).as(name))
        memo    <- query.memoize
        result  <- (memo <&> memo).run
        count   <- counter.get
        _       <- Console.printLine(s"result: $result, mapZIO ran $count times")
      } yield ()
  }
  // UserNames called with GetUserName(1), GetUserName(1)
  // result: (user-1,user-1), mapZIO ran 2 times
  ```

  The same happens with `zip` (two pipelined batches), with `flatMap` (two calls), across two runs of the
  memoized query, and with a `flatMap(... ZQuery.fromZIO ...)` continuation (probes in
  `/tmp/claude-1000/query-scaladoc/probes-1a/P2Memoize.scala`).
- **Expected:** the data source receives `GetUserName(1)` once and `mapZIO` runs once, as the Scaladoc
  ("computed at-most-once") and `docs/running_queries.md` ("a query that computes at most once") promise.
- **Root cause:** `ZQuery.unsafe.memoize`, `ZQuery.scala:1749-1755`, stores `query.step`'s `Result`. When it
  is `Result.Blocked(br, continue)`, every reader gets the same `BlockedRequests` (re-submitted by
  `BlockedRequests.run`, which does not check whether the promise is already done) and the same `Continue`
  (re-evaluated: `Continue.Get(io)` reruns `io`, `Continue.Effect(query)` reruns `query`).
- **Impact:** queries built from requests, the main use of ZIO Query, are not memoized: duplicate data source
  calls and repeated side effects after the request.
- **Test gap:** `ZQuerySpec` suite "memoize" (`ZQuerySpec.scala:338-374`) memoizes only
  `ZQuery.fromZIO(...)` queries, which complete in one step. No test memoizes a query built with
  `fromRequest`.
- **Suggested fix:** memoize the final result, not the first step. For example, keep a `Promise[E, A]` and a
  "started" flag: the first user runs `query` and completes the promise with its exit (`foldCauseQuery`),
  later users return `Result.blocked(BlockedRequests.empty, Continue.get(promise.await))`, or
  `Result.fromExit` when the promise is already done.

## 2. `ZQuery#timeout` does not bound calls to data sources

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** a query whose request is slow is not timed out: the run waits for the data source and then
  returns `Some(result)`, after the timeout has long elapsed. `timeoutFail`, `timeoutFailCause` and
  `timeoutTo` behave the same (they are built on `TimeoutTo`).
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    final case class GetReport(id: Int) extends Request[Nothing, String]

    val slowReports: DataSource[Any, GetReport] =
      DataSource.fromFunctionZIO("SlowReports") { (request: GetReport) =>
        ZIO.sleep(2.seconds).as(s"report-${request.id}")
      }

    val run =
      for {
        start  <- Clock.nanoTime
        result <- ZQuery.fromRequest(GetReport(1))(slowReports).timeout(100.millis).run
        end    <- Clock.nanoTime
        _      <- Console.printLine(s"result: $result after ${(end - start) / 1000000} ms")
      } yield ()
  }
  // result: Some(report-1) after 2068 ms
  ```

  With `timeoutFail("too slow")(200.millis)` the query succeeds with the value after 2 s (probe
  `P3Timeout.scala`).
- **Expected:** `None` after about 100 ms (and the slow call interrupted or abandoned).
- **Root cause:** `ZQuery.TimeoutTo#apply`, `ZQuery.scala:1805-1835`, races only the query's *steps* against
  the sleep fiber. When a step is `Result.Blocked` (`ZQuery.scala:1820-1823`), it returns the blocked
  requests unchanged to the caller, and the runner executes them outside the race
  (`runToZIO`, `ZQuery.scala:640-650`: `br.run *> run(c)`). The continuation then races an already
  completed promise against the already completed sleep fiber, which usually returns the request's result.
- **Impact:** `timeout` cannot protect against slow data sources, which is the main reason to use it on a
  query. `docs/error-handling.md` and `docs/how-to-time-out-slow-queries.md` describe timeouts on queries.
  It also fails to stop the query's later requests: when the timeout elapses during a request, the
  continuation races an already finished timer fiber once the request completes, and often wins. Probe
  (`P8Fix1.scala`): `fromRequest(GetPost(1))(slowPosts /* 500 ms */).flatMap(_ => userName(2))` with
  `.timeout(100.millis)`, 10 runs, `(result, was GetUserName(2) sent)`: `(None, false) -> 5`,
  `(Some(user-2), true) -> 4`, `(None, true) -> 1`. A timeout that elapses during a `ZQuery.fromZIO` step does
  stop later requests (the case the test covers).
- **Test gap:** suite "timeout" (`ZQuerySpec.scala:237-252`) only times out `ZQuery.never` (no request) and
  `ZQuery.fromZIO(ZIO.sleep(2.seconds)) *> neverQuery` (the timeout fires during the `fromZIO` step, before
  the request is made). No test times out a query while its request is running.
- **Suggested fix:** make the request phase part of the race, for example by applying a
  `DataSourceAspect` to the blocked requests (`br.mapDataSources`) whose `runAll` races the original
  `runAll` against `fiber.await`, completing the unfinished requests with the timeout result. This changes
  the data source identifiers, so these requests are no longer batched with requests from outside the timed
  query; that trade-off should be documented.

## 3. `ZQuery#race` does not race data source requests

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** when both sides make requests, the side whose first step finishes first has its requests run
  to completion before the other side's requests are sent. A slow request can therefore win over a fast one,
  and the run takes as long as the slow request. The winner is decided by fiber scheduling, so the result
  changes from run to run.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    final case class GetSlow(id: Int) extends Request[Nothing, String]
    final case class GetFast(id: Int) extends Request[Nothing, String]

    val slow: DataSource[Any, GetSlow] =
      DataSource.fromFunctionZIO("Slow")((r: GetSlow) => ZIO.sleep(2.seconds).as(s"slow-${r.id}"))
    val fast: DataSource[Any, GetFast] =
      DataSource.fromFunctionZIO("Fast")((r: GetFast) => ZIO.succeed(s"fast-${r.id}"))

    val once =
      for {
        start  <- Clock.nanoTime
        result <- ZQuery.fromRequest(GetSlow(1))(slow).race(ZQuery.fromRequest(GetFast(1))(fast)).run
        end    <- Clock.nanoTime
        _      <- Console.printLine(s"result: $result after ${(end - start) / 1000000} ms")
      } yield ()

    val run = once.repeatN(5)
  }
  // result: fast-1 after 57 ms
  // result: slow-1 after 2005 ms
  // result: fast-1 after 1 ms
  // result: slow-1 after 2005 ms
  // result: slow-1 after 2002 ms
  // result: fast-1 after 1 ms
  ```

  A request also wins over a plain value in some runs: `userName(1).race(ZQuery.succeed("plain"))` returned
  `user-1` in 2 of 5 runs (probe `P3Timeout.scala`).
- **Expected:** `fast-1` every time, after a few milliseconds, with the slow request interrupted.
- **Root cause:** `ZQuery#race`, `ZQuery.scala:578-608`. `coordinate` handles a `Result.Blocked` from the
  first side to finish its step by returning that side's blocked requests with a continuation that races
  again (`ZQuery.scala:589-595`). The other side keeps running in its fiber, but its own blocked requests are
  only seen after the first side's requests have been executed by the runner (`runToZIO`,
  `ZQuery.scala:643`), so the two sides' requests never run at the same time.
- **Impact:** `race` cannot be used to pick the faster of two data sources (use `DataSource#race` for that),
  and its result depends on scheduling.
- **Test gap:** suite "race" (`ZQuerySpec.scala:275-290`) races `ZQuery.never` against `ZQuery.succeed` and
  two `ZQuery.fromZIO` queries; neither side makes a request. "data sources can be raced" tests
  `DataSource#race`, not `ZQuery#race`.
- **Suggested fix:** run each side as its own request loop when racing (each side executes its blocked
  requests in its own fiber, sharing the current cache), and race those two effects with `ZIO#raceWith`.
  This gives up batching the raced requests with requests outside the race, which is inherent to racing.

## 4. `ZQuery#provideLayer` closes the layer's scope before the data sources run

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** a scoped layer given to `provideLayer` (or `provideSomeLayer`) is released when the first step
  of the query completes. For a query that makes requests, that is before the data sources run, so the data
  sources receive services whose resources are already released.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    final class Connection {
      @volatile var open = true
    }
    final case class GetRow(id: Int) extends Request[Nothing, String]

    val rows: DataSource[Connection, GetRow] =
      DataSource.fromFunctionZIO("Rows") { (r: GetRow) =>
        ZIO.serviceWith[Connection](c => s"row-${r.id} (connection open: ${c.open})")
      }

    val connectionLayer: ZLayer[Any, Nothing, Connection] =
      ZLayer.scoped(
        ZIO.acquireRelease(Console.printLine("open").orDie.as(new Connection))(c =>
          Console.printLine("close").orDie *> ZIO.succeed(c.open = false)
        )
      )

    val run =
      ZQuery
        .fromRequest(GetRow(1))(rows)
        .provideLayer(Described(connectionLayer, "connectionLayer"))
        .run
        .flatMap(result => Console.printLine(s"result: $result"))
  }
  // open
  // close
  // result: row-1 (connection open: false)
  ```

  `provideSomeLayer[Any]` gives the same order (probe `P4Misc.scala`). A query made only of
  `ZQuery.fromZIO` steps is not affected: there the body runs between "open" and "close".
- **Expected:** `open`, the data source call, then `close`; the result says `connection open: true`.
- **Root cause:** `ZQuery#provideLayer`, `ZQuery.scala:539-549`, wraps only `self.provideEnvironment(...).step`
  in `ZIO.scoped`. The step returns `Result.Blocked` as soon as the query needs a data source, `ZIO.scoped`
  then closes the scope, and the blocked requests and the continuation (which captured the built
  environment) run later.
- **Impact:** any scoped resource (connection pool, client, file handle) provided to a query through a layer
  is closed before the data sources use it.
- **Test gap:** no test in `ZQuerySpec.scala` calls `ZQuery#provideLayer` or `provideSomeLayer` (the
  `.provideLayer(Cache.live)` at line 331 is on the ZIO test, not on a query).
- **Suggested fix:** tie the layer's scope to the whole query, for example
  `ZQuery.acquireReleaseExitWith(Scope.make)((scope, exit) => scope.close(exit))(scope =>
  ZQuery.fromZIO(layer.value.build.provideSomeEnvironment[R0](_.add(scope)))...)`, so the scope closes when
  the query completes, like other resources acquired by a query.

## 5. `acquireReleaseWith`, `acquireReleaseExitWith` and `ensuring` leak when `timeout` or `race` interrupts the query

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9).
- **Summary:** when the query that uses a resource is interrupted by `timeout` or as the loser of `race`, and
  the run as a whole still succeeds, the release (or finalizer) never runs.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    val run =
      for {
        open   <- Ref.make(0)
        query   = ZQuery.acquireReleaseWith(open.update(_ + 1))(_ => open.update(_ - 1)) { _ =>
                    ZQuery.fromZIO(ZIO.sleep(1.second).as("done"))
                  }
        result <- query.timeout(100.millis).run
        _      <- ZIO.sleep(2.seconds)
        count  <- open.get
        _      <- Console.printLine(s"timeout: result $result, resources still open: $count")
        result2 <- query.race(ZQuery.fromZIO(ZIO.sleep(100.millis).as("winner"))).run
        _      <- ZIO.sleep(2.seconds)
        count2 <- open.get
        _      <- Console.printLine(s"race: result $result2, resources still open: $count2")
      } yield ()
  }
  // timeout: result None, resources still open: 1
  // race: result winner, resources still open: 2
  ```

  `ZQuery#ensuring` (built on the same code) does not run its finalizer in the same two cases. When the run
  fails afterwards, or the run's fiber is interrupted from outside, the release does run (probe
  `P5Release.scala`).
- **Expected:** `resources still open: 0` in both lines: release runs on success, failure or interruption,
  as the Scaladoc says.
- **Root cause:** `ZQuery.ReleaseExit#apply`, `ZQuery.scala:1915-1956`. The release runs either in
  `use(a).foldCauseZIO(...)` (`ZQuery.scala:1938-1951`), which is skipped when the use query's fiber is
  interrupted, or in the finalizer added to the run's `QueryScope` (`ZQuery.scala:1928-1935`), which only
  acts on `Exit.Failure` of the whole run and does nothing on `Exit.Success`.
- **Impact:** resource leaks (connections, locks, counters) whenever a resource-using query is combined with
  `timeout` or `race`.
- **Test gap:** test "acquireReleaseWith" (`ZQuerySpec.scala:316-331`) interrupts the whole run's fiber,
  so the run's exit is a failure and the scope finalizer releases. No test interrupts the use query inside a
  run that succeeds.
- **Suggested fix:** in the scope finalizer, release on any exit when the resource has not been released yet
  (`case _ => release(a, ...).when(ref.getAndSet(false))`), so a resource is at the latest released when the
  run ends; or release on interruption of the use step directly (`onInterrupt` on each step of `use(a)`).

## 6. `ZQuery#memoize` does not keep a defect raised by `ZQuery.die`, `orDie` and similar combinators

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9). Reported by review A (I4), re-run by Task 1a.
- **Summary:** a memoized query that dies through `ZQuery.die`, `orDie`, or another combinator that turns a
  cause without typed failures into a failed step is not memoized: each use runs it again. A defect raised
  inside an effect lifted with `ZQuery.fromZIO` is memoized, as are typed failures.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    def twice[E, A](label: String, query: ZQuery[Any, E, A]) =
      for {
        memo <- query.memoize
        _    <- memo.sandbox.either.run
        _    <- memo.sandbox.either.run
        _    <- Console.printLine(s"-- $label done")
      } yield ()

    val run =
      twice("orDie", ZQuery.fromZIO(Console.printLine("computing").orDie *> ZIO.fail(new Exception("boom"))).orDie) *>
        twice("ZQuery.die", ZQuery.fromZIO(Console.printLine("computing").orDie) *> ZQuery.die(new Exception("boom"))) *>
        twice("ZIO.die in fromZIO", ZQuery.fromZIO(Console.printLine("computing").orDie *> ZIO.die(new Exception("boom"))))
  }
  // computing
  // computing
  // -- orDie done
  // computing
  // computing
  // -- ZQuery.die done
  // computing
  // -- ZIO.die in fromZIO done
  ```

  `ZQuery.fromZIO(...).flatMap(_ => ZQuery.die(...))` also computes twice (`P8Fix1.scala`).
- **Expected:** "computing" once for each of the three queries.
- **Root cause:** `ZQuery.unsafe.memoize`, `ZQuery.scala:1749-1755`, stores the result inside
  `ref.modifyZIO` only when `query.step` succeeds. `ZQuery.die` is `ZQuery(ZIO.die(t))`
  (`ZQuery.scala:1231-1232`), and `Result.failExit` (`internal/Result.scala:106-108`) turns a cause with no typed
  failure into a failed step; `orDie` goes through `catchAll` and `FailCauseNone`
  (`ZQuery.scala:209`, `1961-1962`), which also fails the step. A failed step makes `modifyZIO` fail, so
  nothing is stored. `ZQuery.fromZIO` keeps a defect inside `Result.Fail` (`foldCause(Result.fail, ...)`), which
  is stored.
- **Impact:** an expensive query that dies is recomputed on every use of its memoized value, with its side
  effects repeated.
- **Test gap:** test "defects are memoized" (`ZQuerySpec.scala:362-373`) dies inside
  `ZQuery.fromZIO(... ZIO.die(...))`, the one path that is stored. No test memoizes `ZQuery.die` or `orDie`.
  The test shows that memoizing defects is intended.
- **Suggested fix:** store a failed step too: `query.step.exit.map(exit => (exit, Some(exit)))` inside
  `modifyZIO`, and replay the stored `Exit` (`ZIO.done`) on later uses. This fits with the fix for bug 1.

## 7. `ZQuery#mapError` drops a defect that comes with a typed failure

- **Found at:** 2026-11-20, 4e04bed1b (= v0.7.9). Found by Task 1a while checking review A's M3.
- **Summary:** when a query fails with a cause that holds both a typed failure and a defect, `mapError`
  returns only the mapped typed failure and drops the defect. `ZIO#mapError` keeps the defect. When the cause
  is the result of a request (the `Continue.Get` path), `ZQuery#mapError` keeps it too, so the behaviour
  depends on where the failure comes from.
- **Reproducer:**

  ```scala
  //> using scala 2.13.18
  //> using dep dev.zio::zio-query:0.7.9

  import zio._
  import zio.query._

  object Main extends ZIOAppDefault {
    val defect = new RuntimeException("defect")

    val query: ZQuery[Any, String, Int] = ZQuery.fail("error") <&> ZQuery.fromZIO(ZIO.die(defect))
    val effect: ZIO[Any, String, Int]   = ZIO.fail("error") <&> ZIO.die(defect)

    def show(label: String, exit: Exit[String, Int]) =
      Console.printLine(s"$label: failures=${exit.causeOption.map(_.failures)} defects=${exit.causeOption.map(_.defects.map(_.getMessage))}")

    val run =
      for {
        q <- query.mapError(_ + "!").run.exit
        z <- effect.mapError(_ + "!").exit
        _ <- show("ZQuery#mapError", q)
        _ <- show("ZIO#mapError   ", z)
      } yield ()
  }
  // ZQuery#mapError: failures=Some(List(error!)) defects=Some(List())
  // ZIO#mapError   : failures=Some(List(error!)) defects=Some(List(defect))
  ```

  A request whose data source completes it with `Exit.failCause(Cause.fail("e") && Cause.die(ex))` keeps the
  defect through `mapError` (`P10MapErr.scala`: `Both(Fail(e!), Die(...))`).
- **Expected:** `defects=Some(List(defect))`, as with `ZIO#mapError`, whatever the source of the failure.
- **Root cause:** `ZQuery#mapError`, `ZQuery.scala:471-478`: line 475 maps
  `e.failureOrCause.fold(e => Result.failExit(Cause.fail(f(e))), Exit.failCause)`, which keeps only the first
  typed failure. `Continue.Get#mapError` uses `io.mapError(f)` (ZIO's), which maps the whole cause.
- **Impact:** defects are silently lost when an error type is mapped, so a bug in a query can disappear from
  logs and `Cause` reports. `catchAll`, `catchAllZIO`, `either`, `fold` and `mapBoth` drop such a defect too,
  but the corresponding ZIO operators do the same, so those are not reported as bugs.
- **Test gap:** test "mapError does not prevent batching" (`ZQuerySpec.scala:20-28`) checks batching only; no
  test maps the error of a query whose cause holds a defect.
- **Suggested fix:** implement it as `mapErrorCause(_.map(f))` (as `ZIO#mapError` does), so the whole cause
  is kept.
