# Task 1 review A: fact-check of `ZQuery.scala`

Scope: `zio-query/shared/src/main/scala/zio/query/ZQuery.scala` at HEAD (`git diff 4e04bed1b HEAD`). Line numbers
are HEAD lines unless marked "base". Probes: `/tmp/claude-1000/query-scaladoc/rev-a/p/` (Scala 2.13.18,
`dev.zio::zio-query:0.7.9`; `Common.scala` defines `userNamesB` (a `fromFunctionBatchedZIO` data source that logs
each function call), `userNamesM` and `postsM` (`DataSource.make`, which log each `runAll` call with its batches)).
Bug reproducers: `/tmp/claude-1000/query-scaladoc/rev-a/bug<n>-<scala version>/`.

Nothing security-relevant was found.

## Important

### I1. Header: the pipelining bullet over-states what pipelining does (lines 42-46)

Current text:

```
 *   - Requests combined with `zipWith` or combinators derived from it (`zip`,
 *     `*>`, `foreach`, ...) are pipelined: a data source receives its requests
 *     in one call, as batches that must be performed one after another. A
 *     `flatMap` on the left side ends the pipeline, because the next requests
 *     depend on its result.
```

What is wrong, with evidence (probe `Batching.scala`, `Pipe.scala`):

1. "a data source receives its requests in one call" is true only of `runAll`. Every `DataSource.fromFunction*`
   constructor builds a `DataSource.Batched` (DataSource.scala:240-380 at base), whose `runAll` calls the user's
   function once per batch (`DataSource.Batched#runAll`, DataSource.scala:198-219 at base). The examples on this
   very page count calls to a `fromFunctionBatched` function, so a reader will read "one call" that way, and it is
   false: `nB(1) *> nB(2)` gives `UserNames fn(1)` then `UserNames fn(2)`; `ZQuery.foreach(List(1, 2, 3))(nB)`
   gives three function calls. With `DataSource.make` the same query gives one `runAll([1] [2])`.
2. Pipelining needs every side to use that one data source. `BlockedRequests.merge`
   (internal/BlockedRequests.scala) merges a round into the previous one only when both rounds hold exactly one
   data source and it is the same. `(nM(1) <&> pM(1)) *> nM(2)` gives `UserNamesM runAll([1])`,
   `PostsM runAll([1])`, `UserNamesM runAll([2])`: two calls to `UserNamesM`.
3. `flatMap` is not the only thing that ends the pipeline. `Continue.Get#catchAll`, `catchAllCause`, `foldQuery`
   and `foldCauseQuery` return a `Continue.Effect`, and `TimeoutTo`/`race` always return `Continue.effect(...)`;
   `zipWith` (line 1189-1190) does not pipeline an `Effect`. Probes: `nM(1).optional *> nM(2)` and
   `nM(1).sandbox *> nM(2)` each give two `runAll` calls; `nM(1).map(identity).mapZIO(ZIO.succeed(_)) *> nM(2)` gives
   one `runAll([1] [2])`.

Replacement (checked against the source and the probes above):

```
 *   - Requests combined with `zipWith` or combinators derived from it (`zip`,
 *     `*>`, `foreach`, ...) are pipelined when the requests of both sides go to
 *     the same single data source: it receives them in one call to `runAll`, as
 *     batches that must be performed one after another. A data source built
 *     with one of the `DataSource.fromFunction` constructors, which are
 *     `DataSource.Batched`, still calls its function once for each batch. A
 *     `flatMap` after the requests of the left side ends the pipeline, and so
 *     do `catchAll`, `foldQuery`, `optional`, `sandbox`, `timeout` and other
 *     combinators that continue with a query.
```

### I2. `zipWith`: same over-statement (lines 1178-1182)

Current text: "Requests composed with `zipWith` or combinators derived from it are pipelined: when both queries make
requests to the same data source, it receives them in one call, as batches performed one after another. A `flatMap`
in this query, between its requests and its end, prevents this, and the requests of the specified query are then
sent in a later call."

Same three problems and the same evidence as I1 ("when both queries make requests to the same data source" is also
true of `(nM(1) <&> pM(1)) *> nM(2)`, which is not pipelined).

Replacement:

```
   * Requests composed with `zipWith` or combinators derived from it are
   * pipelined when the requests of both queries go to the same single data
   * source: it receives them in one call to `runAll`, as batches performed one
   * after another, and a data source built with one of the
   * `DataSource.fromFunction` constructors then calls its function once for
   * each batch. A `flatMap` in this query after its requests prevents this, and
   * so do `catchAll`, `foldQuery`, `optional`, `sandbox`, `timeout` and other
   * combinators that continue with a query; the requests of the specified query
   * are then sent in a later call.
```

### I3. `timeout`: "A timeout does prevent requests that the query would make later from being sent" is false after a slow request (line 971-972)

Evidence (probe `Misc.scala`): `q2 = fromRequest(GetPost(1))(slowPosts /* 500 ms */).flatMap(_ => nB(2))`,
`q2.timeout(100.millis).run`, 10 runs. Result `(result, was GetUserName(2) sent)`:
`(Some(user-2), true) -> 8, (None, true) -> 1, (None, false) -> 1`. After the slow request completes, `TimeoutTo`
races the continuation's step against an already finished timer fiber (`ZQuery.scala:2161-2183`), and the
continuation usually wins and returns `Blocked` with the next request. The sentence holds only when the timeout
elapses during a step (the test "prevents subsequent requests to data sources from being executed",
ZQuerySpec.scala:245-251, times out during a `ZQuery.fromZIO(ZIO.sleep(...))` step).

Replacement for lines 968-972:

```
   * The timeout applies to the steps of the query, such as effects lifted with
   * `ZQuery.fromZIO`, not to the calls to data sources: a request that is
   * already sent runs to completion, and the query can then return `Some` of
   * its result after the timeout has elapsed. This is a known bug. A timeout
   * that elapses during a step stops the query before it sends its later
   * requests; a timeout that elapses during a request does not, and the query
   * usually goes on to send its next requests once that request completes.
```

The same consequence belongs in bug entry 2 of `task-1a-issues.md` (impact section).

### I4. `memoize`: "later uses reuse it, including a failure" is false for most defects (lines 456-459)

Evidence (probe `Misc.scala`, two uses of the memoized query, each followed by `.sandbox.either.run`):

| Query | "computing" printed |
|---|---|
| `ZQuery.fromZIO(rec(...) *> ZIO.fail(new Exception("boom"))).orDie` | 2 times |
| `ZQuery.fromZIO(rec(...)) *> ZQuery.die(new Exception("boom"))` | 2 times |
| `ZQuery.fromZIO(rec(...) *> ZIO.die(new Exception("boom")))` | 1 time |
| `ZQuery.fromZIO(rec(...) *> ZIO.fail("e"))` | 1 time |

Root cause: `ZQuery.unsafe.memoize` (lines 2060-2066) stores the result inside `ref.modifyZIO` only when
`query.step` succeeds. `ZQuery.die` is `ZQuery(ZIO.die(t))` (line 1430), and `Result.failExit`
(internal/Result.scala) turns a cause with no typed failure into a failed step, so `orDie`, `catchAll`, `either`,
`mapZIO` and the other combinators that go through it produce a failed step that is never stored. Only a defect
that stays inside `Result.Fail` (as with `fromZIO(ZIO.die(...))`, the case the test "defects are memoized",
ZQuerySpec.scala:362-373, covers) is memoized. The test shows that memoizing defects is intended, so this is a bug,
a sibling of bug 1, and should be added to `task-1a-issues.md` (reproducer: the first two rows above).

Replacement for lines 456-459:

```
   * Returns an effect that, when evaluated, returns a memoized version of this
   * query: the first use of the memoized query computes its result, and later
   * uses reuse it, including a typed failure or a defect raised by an effect
   * lifted with `ZQuery.fromZIO`. A query that dies in other ways, for example
   * through `ZQuery.die` or `orDie`, is not memoized and each use runs it
   * again: this is a known bug. Creating the memoized query does not run
   * anything.
```

## Minor

### M1. Header and `fromRequest`: "a request equal to one already made in the same run" (lines 47-48, 1769-1771)

A request made with caching disabled is not stored, so an equal request made later with caching enabled is sent
again. Probe `Unc.scala`: `nM(1).uncached.flatMap(_ => nM(1)).run` and
`ZQuery.fromRequestUncached(GetUserName(1))(userNamesM).flatMap(_ => nM(1)).run` each give two `runAll([1])`
calls (`uncachedResult`, lines 1914-1923, never touches the cache; `cacheLeftovers` runs only when `currentCache`
is set). Replacements:

- lines 47-48: "While caching is enabled, which is the default, a request equal to one already made with caching
  enabled in the same run is not sent again, and its result is reused."
- lines 1769-1771: "While caching is enabled, which is the default, a request equal to one already made with
  caching enabled in the same run is answered from the cache."

### M2. `asExitMaybe`: `fromZIONow` of a failed `Exit` is not always known (lines 200-205)

Probe: `ZQuery.fromZIONow(Exit.die(ex)).asExitMaybe` = `None`; `fromZIONow(Exit.fail("e"))` = `Some(Failure(...))`;
`failCauseNow(Cause.fail("e") && Cause.die(ex))` = `Some(...)`. `Result.failExit` keeps the result only when
`cause.isFailure`. Replacement for lines 202 and 204-205: "...and [[ZQuery.fromZIONow]] applied to an
[[zio.Exit]] that succeeds or holds a typed failure, and for `map` of such a query. It is not known for lazy
constructors such as `ZQuery.succeed`, for `ZQuery.failCauseNow` or `ZQuery.fromZIONow` with a cause that holds
no typed failure, or for queries that make requests."

### M3. `catchAll`, `catchAllZIO`, `either`: a defect that comes with a typed failure is dropped (lines 246-247, 297-298, 306-309)

Probe `Mixed.scala`: `mixed = ZQuery.fail("e") <&> ZQuery.fromZIO(ZIO.die(...))`; `mixed.catchAll(_ =>
ZQuery.succeed(-1)).run` = `Success(-1)`; `mixed.either.run` = `Success(Left(e))`. `failureOrCause` returns the
first typed failure and discards the rest of the cause. Same semantics as ZIO, so Minor. Suggested additions:

- `catchAll` (after line 247) and `catchAllZIO` (after line 298): "When the cause holds both a typed failure and a
  defect, `h` receives the typed failure and the defect is dropped."
- `either` lines 308-309: "Defects still fail it, unless the cause also holds a typed failure, which then becomes
  the `Left`."

### M4. `fold` (unchanged, lines 355-357): "yield a query that does not fail"

Probe: `ZQuery.fromZIO[Any, String, Int](ZIO.die(...)).fold(_ => "failed", _ => "ok").run` = `Failure(Die(...))`
(line 367 re-fails non-typed causes). The author fixed the identical wording in `either`. Replacement: "Folds over
the failed or successful result of this query to yield a query that has no typed failures, but succeeds with the
value returned by the left or right function passed to `fold`. Defects still fail it."

### M5. `acquireReleaseExitWith`: the release does not always receive the query's `Exit` (lines 1279-1280, 1283-1286)

When the release runs from the run's scope, it receives `Exit.failCause(cause.stripFailures)` of the whole run
(lines 2353-2357). Probe: a use query interrupted by `timeout`, then `*> ZQuery.fail("later")`: the release prints
`release got Failure(Empty)`. Replacement for lines 1283-1286: "If the query is interrupted, the release runs when
the whole run ends with a failure or an interruption, and receives the run's cause with its typed failures removed
(which can be empty) instead of the query's `Exit`. When `timeout` or `race` interrupts the query and the run still
succeeds, the release does not run: this is a known bug."

### M6. `currentScope`: every finalizer runs at the end of every run (lines 2282-2285)

`QueryScope` and `addFinalizerExit` are public, and `closeAndExitWith` (internal/QueryScope.scala) runs every
finalizer with the run's `Exit`, success included. Probe: `ZQuery.fromZIO(ZQuery.currentScope.getWith(
_.addFinalizerExit(e => ...)))` on a successful run prints `user finalizer ran with Success(ok)`. Replacement:
"The scope of the current run, which holds the finalizers that `acquireReleaseWith` and `ensuring` register.
`runCache` sets a new scope for each run and, at the end of the run, runs each finalizer in it with the run's
`Exit`; the finalizers that `acquireReleaseWith` and `ensuring` register release only when that `Exit` is a
failure or an interruption."

### M7. `foreachBatched`: "or when parallel fibers are not wanted" (lines 1569-1570)

`BlockedRequests.run` calls the data sources of one round with `ZIO.foreachParDiscard`. Probe `Pipe.scala`:
`foreachBatched` over a request to each of two 300 ms data sources takes 390 ms, not 600 ms. Replacement: "Use it
when the order of those effects matters or when the queries should not run on parallel fibers; calls to different
data sources in the same round still run in parallel, as for any query."

### M8. `TimeoutTo#apply` (lines 2154-2155)

"if it completes within `duration`" repeats the claim that bug 2 breaks, without the caveat that `timeoutTo` has.
Replacement: "Applies `f` to the result of the query, or returns the default value if `duration` elapses first.
Like `ZQuery#timeout`, it does not bound calls to data sources."

## Confirmed

Example result comments (probe `Examples.scala`, 2.13.18, each data source function logging its calls): header
(`UserIds` once, then `UserNames fn(1,2,3)`, `List(user-1, user-2, user-3)`); `run` (`(user-1,user-1)`, one call
with `GetUserName(1)`, a second evaluation calls again); `runCache` (one call); `runLog` (`user-2`, one call
`fn(1,2)`); `foreachBatched` ("loading 1/2/3" in order, then `fn(1,2,3)`); `foreachPar` (`fn(1,2,3)`,
`List(user-1, user-2, user-2, user-3)`); `fromRequest` (`fn(1,2)`, `(user-1,user-2)`); `memoize` ("computing"
twice vs once, both within one run and across two runs); `environmentWith` and `serviceWith` (8080).

New or changed claims confirmed against source or probe: `asExitMaybe` (other cases), `catchAllCause`,
`catchAllCauseZIO`, `ensuring` (finalizer runs when the run fails after a `timeout` interruption; does not when it
succeeds), `memoize` request caveat, `optional` (`QueryFailure` only gives `None`; with a typed failure it fails),
`orDie`, `orDieWith`, `provideLayer`, `provideSomeLayer`, `race` (request vs `ZQuery.succeed("plain")`: request
won 7 of 20), `run`, `runCache`, `runLog`, `sandboxWith`, `timeout` (except I3), `timeoutTo`,
`withParallelismUnbounded`, header batching bullet (`(nM(1).flatMap(_ => nM(3))) <&> nM(2)` gives `[1,2]` then
`[3]`), the `foreach`/`foreachBatched`/`foreachPar` overload notes, `fromRequests*` "in one batch",
`fromZIONow`, `none`, `service`, `serviceWith*`, `environmentWith*`, `unsandbox`, `absolve` (object), the
`*PartiallyApplied`, `ProvideSomeLayer`, `TimeoutTo`, `Acquire`/`Release`/`AcquireExit`/`ReleaseExit` class docs,
`cachingEnabled` ("not read": only its definition matches in `zio-query`), `currentCache`.

Unchanged comments sampled (all true): `@@`, `&>`, `<&`, `<&>`, `*>`, `<*`, `<*>`, `~>`, `<~`, `<~>`, `absolve`
(class), `as`, `asSomeError`, `cached`, `flatMap`, `flatten`, `foldCauseQuery`, `foldCauseZIO`, `foldQuery`,
`foldZIO`, `left`, `map`, `mapBoth`, `mapBothCause`, `mapDataSources`, `mapError`, `mapErrorCause`, `mapZIO`,
`provideEnvironment`, `provideSomeEnvironment`, `refineOrDie`, `refineOrDieWith`, `right`, `sandbox`, `some`,
`someOrElse`, `someOrElseZIO`, `someOrFail`, `summarized`, `timed`, `timeoutFail`, `timeoutFailCause`, `uncached`,
`unleft`, `unoption`, `unrefine*`, `unright`, `withParallelism`, `zip*`, `zipWithBatched`, `zipWithPar`,
`collectAll*` (13), `die`, `environment`, `fail`, `failNow`, `failCause`, `failCauseNow`, `foreach` (Collection,
Option), `fromEither`, `fromOption`, `fromRequestUncached`, `fromZIO`, `never`, `partitionQuery`,
`partitionQueryPar`, `some`, `succeed`, `succeedNow`, `suspend`, `unit`, `object unsafe`, `unwrap`, private `apply`,
`partitionMap`, `CachedResult`, `foreachSequentialOuter`. Only `fold` is imprecise (M4).

Bug entries in `task-1a-issues.md` (re-run exactly on 2.13.18 and 3.3.9; base line citations, test-gap claims and
root causes checked):

1. `memoize`: confirmed on both versions (`GetUserName(1), GetUserName(1)`, `mapZIO ran 2 times`); `zip`
   (`runAll([1] [1])`), `flatMap` and two-run variants also confirmed. Citations 1749-1755 correct. Test gap
   correct (ZQuerySpec.scala:338-374 memoizes only `fromZIO` queries). Sibling found: I4.
2. `timeout`: confirmed (`Some(report-1)` after 2081 ms on 2.13, 2097 ms on 3.3); `timeoutFail` variant
   confirmed (succeeds with the value after 2045 ms). Citations 1805-1835, 1820-1823, 640-650 correct. Test gap
   correct (237-252). Add the I3 consequence to its impact.
3. `race`: confirmed; outcome is nondeterministic as stated (2.13: 1 of 6 slow; 3.3: 3 of 6 slow, plus one
   `fast-1 after 2004 ms`). Citations 578-608, 589-595, 643 correct. Test gap correct (275-290).
4. `provideLayer`: confirmed on both versions (`open`, `close`, `connection open: false`). Citation 539-549
   correct. Test gap correct (only `.provideLayer(Cache.live)` on the ZIO test at line 331).
5. release leak: confirmed on both versions (`resources still open: 1`, then `2`); the "run fails afterwards"
   case releases (probe `Misc.scala`). Citations 1915-1956, 1928-1935, 1938-1951 correct. Test gap correct
   (316-331 interrupts the whole run).

Rules: no ledger, tracker, "in this file", "above" or "below" references in the new text; no `^`, `<h1>`-`<h6>`,
`@throws`, `/*` or unescaped `$` in new text; no new `[[...]]` to an overloaded member (new links: `[[Cache]]`, a
trait plus companion like the existing `[[DataSource]]`, and three `@see` URLs). `@see` URLs:
`how-batching-works`, `running-queries`, `creating-queries` each return 301 to the trailing-slash URL, which returns
200 (the rules forbid the trailing slash in the source, so this is expected). The Haxl paper URL returns 200.
