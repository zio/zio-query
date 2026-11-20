# Task 1a report: `ZQuery.scala`

File: `zio-query/shared/src/main/scala/zio/query/ZQuery.scala`. Base 4e04bed1b (= v0.7.9). Probes:
`/tmp/claude-1000/query-scaladoc/probes-1a/` (Scala 2.13.18, `dev.zio::zio-query:0.7.9`; `P1Batching`,
`P2Memoize`, `P3Timeout`, `P4Misc`, `P5Release`, `P6Examples`, `P7Zip`, and `issues/*` reproducers).

## Commits

1. `b3f1583` docs(query): fact-check ZQuery Scaladoc and fix false or imprecise comments
2. `c0b28da` docs(query): add overviews and compiled examples to ZQuery entry points
3. `9ad089e` docs(query): document the undocumented ZQuery members
4. (this report and `task-1a-issues.md`) docs(query): add the Task 1a report and issues

The three file commits were built as cumulative subsets of the final formatted file (hunks applied to the
base), each committed with `git commit -- <path>`; the last one equals the `scala-cli fmt` output.

## Counts

- Existing `/**` blocks checked: **154 of 154** (private and `CachedResult` ones included), plus the 4 line
  comments (`// NOTE: Running a ZQuery requires up to 3 FiberRefs`, `// Cheap and common ...`,
  `// 2-pass to pre-size arrays ...`, `// Patches for Scala 2.12`), which are true.
- Existing blocks changed: **49**: **13 false**, **19 imprecise**, **3 broken examples**, **8 style**, and
  **6 tier-1 expansions** of comments that were true.
- Unchanged and verified true: 105 (for example `flatMap`, `cached`, `uncached`, `zipPar`, `zipBatched`,
  `foreach`, `collectAll*`, `partitionQuery*`, `fromRequestUncached`, `fromZIONow`, `summarized`,
  `withParallelism`, `refineOrDie`, `unrefine`, `succeed`, `suspend`, `unwrap`).

## Coverage (ZQuery.scala, `list-members.py`)

| | Members | Documented |
|---|---:|---:|
| Before | 182 | 150 (82%) |
| After | 182 | 182 (100%) |

Whole module at the time of the check (with Task 1b's work in the tree): 279 / 279.

## Example check

`snippets-scala-cli.py --dep dev.zio::zio-query:0.7.9 --imports "zio._; zio.query._"` on ZQuery.scala:

```
10 blocks in 1 files
Scala 2.12.21: OK
Scala 2.13.18: OK
Scala 3.3.9: OK
```

Blocks: `ZQuery` header, `memoize`, `run`, `runCache`, `runLog`, `environmentWith`, `foreachBatched`,
`foreachPar`, `fromRequest`, `serviceWith`. Result comments come from `P6Examples.scala` (same code with a
`println` in each data source function to count calls).

Other checks: `comments-only.py 4e04bed1b zio-query` = "16 files, 0 with code changes". No `.scala-build`
left under `zio-query`.

## Fact-check table (every existing comment changed)

| Member | Old claim | Kind | Evidence |
|---|---|---|---|
| `ZQuery` (class header) | Example `val getAllUserIds = ???` + bare `for`; "`zipWith` ... will automatically be pipelined"; "optimize this to two queries" | broken example, imprecise | The example had no data source and was not a query value. The "two queries" claim is true: P1 "header" and P6 show `UserIds` 1 call + `UserNames` 1 call with `[1, 2, 3]`. Pipelining fails after a `flatMap` on the left (P1 "zip with flatMap on left": 2 calls). Rewritten with a compiled example, a bullet list of batching / pipelining / dedup, and `@see how-batching-works`. |
| `asExitMaybe` | "applies to `succeedNow` and other xNow constructors. The only exception is `fromZIONow` ..." | imprecise | P4: `failCauseNow(Cause.die(..))` gives `None` (`Result.failExit` returns `Exit.Failure` for non-failure causes, `internal/Result.scala`); `succeedNow(1).map(_ + 1)` gives `Some`; `fromZIONow(Exit.succeed(1))` gives `Some`, `fromZIONow(ZIO.succeed(1))` `None`; `unit`, `none`, `succeed` give `None`. |
| `catchAll` | "Recovers from all errors." | imprecise | `ZQuery.scala:209` folds `failureOrCause`, defects go to `FailCauseNone`; P4 "catchAll on defect" dies. |
| `catchAllCause` | "Recovers from all errors with provided Cause." | imprecise | Handles failures and defects (P4 "catchAllCause on defect" = `Success(true)`); wording made precise. |
| `catchAllCauseZIO` | "Recovers from all errors and maps the provided Cause to a ZIO effect" | style | Same behaviour as `catchAllCause` with a `ZIO` handler (`ZQuery.scala:234-246`, test "catches defects"). |
| `catchAllZIO` | "Catches all errors and maps them to a ZIO effect" | imprecise | `ZQuery.scala:254`; test "doesn't catch defects". |
| `either` | "The resulting query cannot fail" | imprecise | P4 "either on defect" dies; `fold` only folds `failureOrCause`. |
| `ensuring` | "the specified query will be executed immediately after this query completes" | false | The finalizer is a `ZIO`, not a query (`ZQuery.scala:269`). P5: when `timeout` interrupts the query and the run succeeds, the finalizer never runs (issue 5). |
| `memoize` | "computed at-most-once"; example with 4-space indentation and `query.map(i => Foo(ZQuery.succeed(i +1), ...))` | false, style | P2: a memoized `fromRequest(...).mapZIO(...)` used twice sends `GetUserName(1)` twice in one batch and runs `mapZIO` twice; same with `zip`, `flatMap` and across runs (issue 1). For `fromZIO` the effect runs once (P2, P6). Example replaced with a compiled one whose comments come from P6 ("computing" twice vs once). |
| `optional` | "returns `Some` if data sources return results for all requests received and `None` otherwise" | imprecise | `ZQuery.scala:515` strips only `QueryFailure` defects. P4: typed failure, other defects, and `QueryFailure` combined with a typed failure still fail. |
| `orDie` | "dies if a query failure occurs" | imprecise | "query failure" reads as `QueryFailure`; the method turns the typed error into a defect (`orDieWith(ev1)`, P4 "orDie"). |
| `orDieWith` | "dies if a query failure occurs" | imprecise | Same as `orDie` (`catchAll(e => ZQuery.die(f(e)))`). |
| `provideLayer` | "which translates it to another level" | imprecise | Text had no meaning. P4: a scoped layer prints "layer open", "layer close", then the data source call (issue 4); described in the comment. |
| `provideSomeLayer` | "providing one part using the specified layer" | imprecise | Same scope problem (P4 "provideSomeLayer"); a pointer to `provideLayer` added. |
| `race` | "returning the result of the first to complete successfully and safely interrupting the other" | false | P3: slow request vs fast request returns `slow-22` after 2002 ms in 1 of 3 runs; request vs `ZQuery.succeed("plain")` returns the request in 2 of 5 (issue 3). Fail vs fail gives `Both(e2, e1)`, fail vs late success gives the success: kept and stated. |
| `run` | "Returns an effect that models executing this query." | tier-1 expansion | True. Added: new empty cache per evaluation (P1 "same request across two runs": 2 calls), example, `@see running-queries`. |
| `runCache` | "with the specified cache" | tier-1 expansion | True. P1 "runCache shared": second run makes no call; example from P6. |
| `runLog` | "returning the query result along with the cache" | tier-1 expansion | True (`ZQuery.scala:716-720`). P1: the returned cache answers `GetUserName(7)` and has no `GetUserName(9)`; example from P6. |
| `sandboxWith` | "Allows recovery ... from errors and defects alike, as in:" | style | Sentence ended with "as in:" and no example; now says what it does (`ZQuery.unsandbox(f(self.sandbox))`). |
| `timeout` | "Returns an effect that will timeout this query" | false | It returns a `ZQuery`. P3: a 2 s request with `.timeout(200.millis)` returns `Some(slow-1)` after 2047 ms (issue 2). "sleep 1s *> request" times out at 203 ms without calling the data source: stated. |
| `timeoutTo` | (no caveat) | imprecise | Same mechanism as `timeout` (`TimeoutTo`, `ZQuery.scala:1805-1835`); one-sentence caveat added. `timeoutFail` / `timeoutFailCause` say "the same as `timeout`" and were left as is. |
| `withParallelismUnbounded` | "Sets the parallelism ... to the specified maximum number of fibers." | false | Takes no number; sets `ZIO.Parallelism` to `None` (`ZQuery.scala:919`). P4: 4 sleeps of 200 ms take 204 ms unbounded vs 807 ms with `withParallelism(1)`. |
| `zipWith` | "Requests composed with `zipWith` ... will automatically be pipelined." | imprecise | P1: `flatMap` on the left gives 2 calls; P7: two different data sources get separate calls in order; same data source gets `[1] [2]` in one call. |
| `ZQuery.acquireReleaseExitWith` | "releases it ... whether by success, failure, or interruption" | false | P5 / issue 5: interrupted by `timeout` or `race` in a successful run, release never runs. Interrupting the run's fiber from outside does release (P5). |
| `ZQuery.acquireReleaseWith` | same | false | Same evidence. |
| `ZQuery.environmentWith` | "Accesses the environment of the effect." + `effect.access(_.config.portNumber)` | broken example | No `effect`, no `access`. New example compiled on 3 versions; P6 returns 8080. |
| `ZQuery.environmentWithQuery` | "Effectfully accesses the environment of the effect." | imprecise | It builds a query (`environment[R].flatMap(f)`). |
| `ZQuery.environmentWithZIO` | "Effectfully accesses the environment of the effect." | imprecise | It builds a `ZIO` effect, wrapped in a query. |
| `ZQuery.foreach` (Set) | "If you do not need the results, see `foreach_`" | false | No `foreach_` exists in ZIO Query (`grep -rn foreach_ zio-query` finds only these comments). |
| `ZQuery.foreach` (Array) | same | false | same |
| `ZQuery.foreach` (Map) | same | false | same |
| `ZQuery.foreach` (NonEmptyChunk) | same | false | same |
| `ZQuery.foreachBatched` (Array) | "For a sequential version of this method, see `foreach`." | imprecise | `foreachBatched` is sequential too (P4: 4 sleeps take 807 ms); the difference is batching vs pipelining (P1: `[1, 2, 3]` vs `[1] [2] [3]`). |
| `ZQuery.foreachBatched` (Map) | same | imprecise | same |
| `ZQuery.foreachBatched` (NonEmptyChunk) | same | imprecise | same |
| `ZQuery.foreachBatched` (Collection) | "batching requests to data sources" | tier-1 expansion | True. Example from P6: "loading 1/2/3" in order, then one `UserNames` call with 1, 2, 3. |
| `ZQuery.foreachPar` (Collection) | "executed in parallel and will be batched" | tier-1 expansion | True. Example from P6: `List(1, 2, 2, 3)` gives one call with 1, 2, 3. |
| `ZQuery.fromRequest` | "Queries will die with a `QueryFailure` ..." | tier-1 expansion | True (P4 "missing without optional"). Example from P6; `@see creating-queries`. |
| `ZQuery.fromRequests` (Chunk) | "Queries will die with a QueryFailure when run the data source does not provide ..." | style | Grammar; "in one batch" added (P1 "fromRequests": one batch). |
| `ZQuery.fromRequests` (List) | same | style | same |
| `ZQuery.fromRequestsWith` (Chunk) | same | style | same (P1 "fromRequestsWith dup": `[1, 2]`). |
| `ZQuery.fromRequestsWith` (List) | same | style | same |
| `ZQuery.fromZIONow` | "Note that this is method is meant ..." | style | Typo. The claim itself is true (P4: argument evaluated once, at definition). |
| `ZQuery.none` | "Constructs a query that succeds with the empty value." | style | Typo; it is a `val`, not a constructor. |
| `ZQuery.service` | "Accesses the whole environment of the query." | false | `ZIO.serviceWith[R](Result.done)` returns the service `R`, not the `ZEnvironment` (P4 "service" = `hello`). |
| `ZQuery.serviceWith` | "Accesses the environment of the effect." + `effect.access(...)` | broken example | Same broken example; new example compiled; P6 returns 8080. |
| `ZQuery.serviceWithQuery` | "Effectfully accesses the environment of the effect." | imprecise | Builds a query from the service. |
| `ZQuery.serviceWithZIO` | "Effectfully accesses the environment of the effect." | imprecise | Builds a `ZIO` effect from the service. |
| `ZQuery.unsandbox` | "Terminates with exceptions on the `Left` side of the `Either` error ... extracts the contained `IO[E, A]`" | false | No `Either` is involved: `mapErrorCause(_.flatten)` (`ZQuery.scala:1766`); P4 `unsandbox(fail(Cause.fail("inner")))` = `Fail(inner)`. |

## New documentation (32 undocumented members)

`asExitOrElse`, `object ZQuery` (overview + `@see creating-queries`), `ZQuery.absolve`,
`foreachBatched` (Set), `unsafe.memoize`, the six `*PartiallyApplied` classes and their `apply`s,
`ProvideSomeLayer` and `apply`, `TimeoutTo` and `apply`, `Acquire`/`Release`/`AcquireExit`/`ReleaseExit`
and their `apply`s, `cachingEnabled` (deprecated, not read), `currentCache`, `currentScope`. Claims checked
against the source: `currentCache` is `None` outside a run and inside `uncached` (`ZQuery.scala:1891`, test
"disabling caching is reentrant safe"); `currentScope` finalizers act only on a failed run
(`ZQuery.scala:1928-1935`, P5).

## Deviations

- Tier 1 for `ZQuery.scala` covers the class header, `fromRequest`, `foreachBatched`, `foreachPar`, `run`,
  `runCache` and `runLog` as the brief lists; `object ZQuery` got an overview and a `@see` but no example.
- `@tparam` tags added to the class header (not asked, cheap and true).
- Links: only the live pages `how-batching-works`, `running-queries`, `creating-queries`. New links to
  members use backticks (`ZQuery#either`-style) to avoid Scaladoc link resolution on overloads. Existing
  `[[...]]` links that I did not rewrite were kept as they were.
- The three file commits were made by writing cumulative intermediate versions of the file to the working
  tree and committing each with `-- <path>` (no interactive staging available).

## Concerns

- `docs/` pages repeat some of the behaviours found buggy here: `running_queries.md` says `memoize` "computes
  at most once"; `error-handling.md` describes `race` as returning "the first one that succeeds";
  `zquery.md` and `error-handling.md` say resources are released on interruption. `docs/` is out of scope
  for this PR; the controller may want a follow-up.
- `how-batching-works.md` says `collectAllPar(List(userName(1).zip(userName(2)), userName(3).zip(userName(4))))`
  puts all four requests in one batch; P1 shows one call with two pipelined batches `[1, 3] [4, 2]`
  (with `DataSource.make`). Not in my file; noted for the docs.
- The five bugs (see `task-1a-issues.md`) are described in the Scaladoc as "a known bug" in plain text, with
  no tracker reference. Nothing security-relevant was found.
- I did not run sbt (`doc` with fatal warnings is the controller's gate). Risky Scaladoc syntax was avoided:
  no new `[[...]]` to overloaded members, no `$` without `\`, no `^`, no `/*` in text.

DONE

## Fix round 1 (review A, ruling R6)

Probes: `/tmp/claude-1000/query-scaladoc/probes-1a/P8Fix1.scala`, `P9Mixed.scala`, `P10MapErr.scala` (Scala
2.13.18, `dev.zio::zio-query:0.7.9`). The issue reproducers moved to
`/tmp/claude-1000/query-scaladoc/probes-1a-issues/` (they clashed as `object Main` when compiled with the probes).
Every reviewer replacement was re-probed before use.

Commits:

1. `5fe55c6` docs(query): state when ZQuery pipelines requests and what fromFunction data sources receive (I1, I2, M7)
2. `e7b73fb` docs(query): describe what a ZQuery timeout does to later requests (I3, M8)
3. `6669c35` docs(query): say which failures ZQuery#memoize keeps (I4)
4. `269e6bb` docs(query): describe how ZQuery error operators treat defects and known results (M2, M3, M4, mapError, mapBoth)
5. `59426fd` docs(query): note when ZQuery#mapError keeps a defect (mapError refinement)
6. `08b9a6d` docs(query): limit the ZQuery deduplication claims to requests made with caching enabled (M1)
7. `ec1805c` docs(query): describe the release Exit and the run scope finalizers (M5, M6)
8. `49743d1` docs(query): use the trailing-slash zio.dev URLs in ZQuery @see links
9. (this section and `task-1a-issues.md`) docs(query): add the Task 1a fix round 1 report and issues

| Finding | Action | Evidence |
|---|---|---|
| I1 header pipelining bullet | Applied the reviewer's text. | P8: `nB(1) *> nB(2)` gives `NamesB fn(1)`, `NamesB fn(2)`; `foreach` gives 3 function calls; `(nM(1) <&> oM(1)) *> nM(2)` gives 3 `runAll` calls; `optional`, `sandbox`, `catchAll`, `timeout` before `*>` each give 2 calls; `map.mapZIO` and `either` keep `[1] [2]` in one call (so `either` is not in the list). `Continue.Get#foldQuery` returns `Effect` (source). |
| I2 `zipWith` | Applied the reviewer's text. | Same as I1. |
| I3 `timeout` later requests | Applied, with "can go on" instead of "usually goes on". | P8, 10 runs: `(None,false) -> 5`, `(Some(user-2),true) -> 4`, `(None,true) -> 1` (reviewer: 8 of 10 sent). Added to the impact of bug 2. |
| I4 `memoize` defects | Applied the reviewer's text; new bug 6. | P8: `orDie` x2, `ZQuery.die` x2, `flatMap(_ => ZQuery.die)` x2, `fromZIO(ZIO.die)` x1, `fromZIO(ZIO.fail)` x1. Reproducer re-run (`probes-1a-issues/memoizedie`). |
| M1 caching "already made" | Applied in the header and `fromRequest`; same fix in `run` ("equal requests made with caching enabled"). | P8: `nM(1).uncached.flatMap(_ => nM(1))` and `fromRequestUncached(...).flatMap(_ => nM(1))` each give 2 `runAll([1])`. |
| M2 `asExitMaybe` | Applied the reviewer's text. | P8: `fromZIONow(Exit.die)` = `None`, `fromZIONow(Exit.fail)` = `Some`, `failCauseNow(fail && die)` = `Some`. |
| M3 `catchAll`, `catchAllZIO`, `either` | Described the real behaviour (typed failure goes to `h` / `Left`, defect dropped). Not a bug: ZIO does the same. | P9: ZQuery and ZIO `catchAll` both `Success(-1)`, `either` both `Success(Left(e))`, `fold` both `Success(-1)`. Source: `failureOrCause` in `catchAll` (base 209), `fold` (base 313). |
| M3 follow-up: `mapError`, `mapBoth` | `mapError`: described, new bug 7 (ZIO keeps the defect, ZQuery drops it except on the request path). `mapBoth`: one sentence; ZIO drops it too, not a bug. | P9: ZQuery `mapError` `Fail(e!)` vs ZIO `Both(Fail(e!), Die)`; `mapBoth` both `Fail(e!)`. P10: a request whose result holds both keeps `Die` through `mapError` (`Continue.Get#mapError` uses ZIO's). |
| M4 `fold` | Applied ("has no typed failures ... Defects still fail it"), plus the mixed-cause case. | P9: `fold` on `fromZIO(ZIO.die)` = `Failure(Die)`; mixed = `Success(-1)`. |
| M5 `acquireReleaseExitWith` Exit | Applied the reviewer's text. | P8: release prints `release got Failure(Empty)` after a `timeout` interruption and a later `ZQuery.fail`. Source base 1930: `Exit.failCause(cause.stripFailures)`. |
| M6 `currentScope` | Applied the reviewer's text. | P8: a user finalizer added with `addFinalizerExit` runs with `Success(ok)` on a successful run. |
| M7 `foreachBatched` "parallel fibers" | Applied the reviewer's text. | P8: `foreachBatched` over one request to each of two 300 ms data sources takes 304 ms. |
| M8 `TimeoutTo#apply` | Applied the reviewer's text. | Same evidence as bug 2; P8 `timeoutTo` on a 500 ms request returned the default this time, so the outcome varies. |
| `@see` URLs | All 8 links now end with `/`. | `curl`: each bare URL 301, each trailing-slash URL 200. |

Checks after the round:

- Official checker `python3 -I ~/.claude/skills/scaladoc-rollout/scripts/snippets-scala-cli.py . --version 0.7.9
  --base 4e04bed1b`: 16 examples in 5 files, PASS on 2.12.21, 2.13.18 and 3.3.9.
- `comments-only.py 4e04bed1b zio-query`: 16 files, 0 with code changes.
- `scala-cli fmt` run on ZQuery.scala after each group; no `.scala-build` left under `zio-query`.
- No other `.scala` file touched.

DONE
