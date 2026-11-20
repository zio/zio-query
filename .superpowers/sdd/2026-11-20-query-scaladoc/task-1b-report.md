# Task 1b report: Scaladoc outside ZQuery.scala

Scope: every `.scala` file under `zio-query/shared/src/main` except `ZQuery.scala`. Base 4e04bed1b (= v0.7.9).
Probes: `/tmp/claude-1000/query-scaladoc/probes-1b/` (`p1` contramap, `p2` combinators/aspects, `p3`
CompletedRequestMap/Cache, `p4` race with a dying `runAll`, `requests` on 3 Scala versions, `examples` = every
`{{{ }}}` block run with a `main`, `issue1..3` = the reproducers). All on Scala 2.13.18 unless noted.

## Commits

| Commit | Files |
|---|---|
| `92c5d79` docs(query): fact-check and complete the DataSource Scaladoc | `DataSource.scala` |
| `4022d81` docs(query): fact-check and complete the CompletedRequestMap Scaladoc | `CompletedRequestMap.scala` |
| `3bd8afb` docs(query): document Cache, Request, QueryFailure, Described and the query type aliases | `Cache.scala`, `Request.scala`, `QueryFailure.scala`, `Described.scala`, `package.scala` |
| `173f54d` docs(query): fix the DataSourceAspect and QueryAspect Scaladoc | `DataSourceAspect.scala`, `QueryAspect.scala` |
| `3868f34` docs(query): fact-check the internal Scaladoc | `internal/BlockedRequest.scala`, `BlockedRequests.scala`, `Continue.scala`, `QueryScope.scala`, `Result.scala`, `Sequential.scala` |
| (this commit) docs(query): add the Task 1b report and issues | this file, `task-1b-issues.md` |

## Counts

- Existing comments checked: **123** (117 `/**` blocks + 6 `//` line comments) in 19 files, version folders
  included (`scala-2.12/UtilsVersionSpecific.scala` has the only comment there; it is true).
- Existing comments changed: **61**: **10 false**, **18 imprecise**, 7 style, 26 enriched (true but
  incomplete: a fact, example or `@see` added, old text kept). 0 broken examples (the one existing example,
  `Request`, compiled; it was extended).
- The 6 line comments (`// TODO` in `Cache.scala`, `// Required for Scala 3`, three in `BlockedRequests.run`,
  one in `scala-2.12`) are all true and unchanged. `Parallel.scala` and most of `Continue`, `Result`,
  `BlockedRequests` and `Sequential` are true and unchanged.
- Undocumented public members documented: **29 of 29**.

## Fact-check table (existing comments changed)

Evidence: `src:N` = line of `DataSource.scala` at the base commit, `File.scala:N` = line of another file at the base; `pN` = probe output in `probes-1b/pN/`.

| # | Member | Old claim | Kind | Evidence |
|---|---|---|---|---|
| 1 | `DataSource` (trait) | "parameterized on a subtype of `Request[A]`", "not strictly necessarily as long as the data source can map the request type to a `Request[A]`", "place the results into the `CompletedRequestsMap` one of the constructors" | false | `Request` has two type params (`Request.scala:33`); mapping to another request is exactly what fails (issue 1, `p1`); no type `CompletedRequestsMap`. Rewritten, example added (run in `examples`: `UserNames received [2, 1, 3]`, `List(user-1, user-2, user-1, user-3)`), `@see data-source-reference` |
| 2 | `DataSource#identifier` | "The data source's identifier." | enriched | `equals`/`hashCode`/`toString` use it (`src:129-136,183`); `p2`: `users.batchN(2) == users.batchN(2)` true |
| 3 | `DataSource#runAll` | outer/inner chunks | enriched | added "must contain a result for every request" (`BlockedRequests.scala:295`) |
| 4 | `DataSource#batchN` | "executes at most `n` requests in parallel" | imprecise | `src:70-73`: groups become consecutive batches in one `runAll`; `p2`: `users[1,2] users[3,4] users[5]` (Batched, sequential), `mk[1,2][3,4][5]` (one call); `batchN(0)` dies `IllegalArgumentException`; identifier `users.batchN(2)` |
| 5 | `DataSource#contramap` | "executes requests of type `B` using the specified function to transform `B` requests" | false | `src:85` returns the map keyed by `f(b)`; `p1`: `UserById(1)` dies `QueryFailure ... did not complete request UserById(1)` after `users[1]`; identity works. Known bug, issue 1 |
| 6 | `DataSource#contramapZIO` | same, effectual | false | `src:99`; `p1`: dies `QueryFailure`, `users[1]` called once. Issue 1 |
| 7 | `DataSource#eitherWith` | "executes requests of type `C` using the specified function to transform `C` requests" | false | `src:119-120`; `p2`: converting `f` dies `QueryFailure` (`cache[2] dbMake[1]` called); subtype routing works (`cache[1,3] dbMake[2]`); only-left batch still calls `dbMake[]`; `issue3`: `eitherWith(DataSource.never)` never completes. Issues 1 and 3 |
| 8 | `DataSource#provideEnvironment` | "Provides ... required environment." | enriched | identifier `prefixed.provideSomeEnvironment(_ => prefix)` (`p2`) |
| 9 | `DataSource#provideSomeEnvironment` | "part of its required environment" | enriched | `src:153` |
| 10 | `DataSource#query` | "Submits a request to this datasource and returns the result." | imprecise | returns a `ZQuery` (`src:160`), nothing is submitted until it runs |
| 11 | `DataSource#queryAll` | "Submits a Chunk ... returns the results in the same order" | imprecise | returns a `ZQuery` (`src:169`); missing period |
| 12 | `DataSource#race` | "results from the first data source to complete" | imprecise | `p2`: a source whose results are all `Die` (failed map) wins (`DIED boom`); `p4`: a source whose `runAll` dies loses (`Success(slow-1)`) |
| 13 | `DataSource.Batched.make` | "function taking a collection of requests" | enriched | called once per parallel batch (`src:195-224`, `src:229-233`) |
| 14 | `DataSource.fromFunction` | "from a pure function" | enriched | once per request (`src:240-246`) |
| 15 | `DataSource.fromFunctionBatched` | same size, by index | enriched | `p2`: shorter result → `QueryFailure` for the tail, longer → extras ignored; example (run: `UserNames received 3 requests`, `List(user-1, user-2, user-3)`), `@see creating-data-sources` |
| 16 | `DataSource.fromFunctionBatchedOption` | same size, optional | enriched | `p2`: `None` → `QueryFailure` |
| 17 | `DataSource.fromFunctionBatchedOptionZIO` | same, effectual | enriched | `src:282-283`: failure → every request fails with the cause; `None` dropped by `fromIterableOption` |
| 18 | `DataSource.fromFunctionBatchedWith` | "returns a list of results of the same size", "associate each result with the corresponding effect" | false | `src:319-322`: `fromWith(bs, bs)(g, ...)`, so size is free and `g` maps a result to its *request*; `p2`: reversed order works, missing result → `QueryFailure` for that request only, wrong `g` → `QueryFailure`. Example added (run: `List(User(1,user-1), User(2,user-2), User(3,user-3))`) |
| 19 | `DataSource.fromFunctionBatchedWithZIO` | same | false | same code; `p2`: failed effect → both requests `Left(down for 2)` |
| 20 | `DataSource.fromFunctionBatchedZIO` | same size, by index | enriched | `src:339-342` (`fromSuccesses` truncates); `p2`: failure → `Left(db down)` for both, one call `bz[2]` |
| 21 | `DataSource.fromFunctionZIO` | "from an effectual function" | enriched | `src:356-357` `foreachPar`; `p2`: `fz(1) fz(0)`, `Right(user-1), Left(bad 0)` |
| 22 | `DataSource.fromFunctionOption` | "may not provide results for all requests" | imprecise | reads as harmless; `p2`: `None` → query dies `QueryFailure` |
| 23 | `DataSource.fromFunctionOptionZIO` | same, effectual | imprecise | `src:380`, same as 22 |
| 24 | `DataSource.make` | "function taking a collection of requests" | enriched | it is `runAll` (`src:388-392`), receives `Chunk[Chunk[A]]` |
| 25 | `DataSource.never` | "never executes requests" | imprecise | `runAll` is `ZIO.never` (`src:402`): the query hangs; `p2`: `never.race(fast)` → `Success(fast-1)` |
| 26 | `CompletedRequestMap` (class) | universally quantified mapping | enriched | example (run: `Some(Success(Ann))`, `Some(Failure(Fail(no user 2,StackTrace.none)))`, `None`), `@see completed-request-map`; blank line between comment and class removed so the comment attaches |
| 27 | `CompletedRequestMap#++` | "Appends the specified result" | false | it merges two maps, that map wins (`CompletedRequestMap.scala:49`; `p3`: `++ that wins: Some(Success(Anna))`) |
| 28 | `CompletedRequestMap#insert` | "Appends the specified result" | imprecise | returns a new map, replaces an existing entry, original unchanged (`p3`) |
| 29 | `CompletedRequestMap#insertOption` | "Appends the specified optional result" | imprecise | `None` returns `self` (`CompletedRequestMap.scala:88`; `p3`: `eq` true) |
| 30 | `CompletedRequestMap#requests` | "Collects all requests in a set." | false | throws `ClassCastException` for every mutable-backed map, on 2.12.21, 2.13.18, 3.3.9 (`probes-1b/requests`). Known bug, issue 2 |
| 31 | `CompletedRequestMap.combine` | "left-associated ... overriden by the last map" | imprecise | wording and typo; `p3`: last wins (`Anna`) |
| 32 | `CompletedRequestMap.die` | (no period) | style | |
| 33 | `CompletedRequestMap.fail` | (no period) | style | |
| 34 | `CompletedRequestMap.failCause` | (no period) | style | |
| 35 | `CompletedRequestMap.fromIterable` | "from the specified results" | enriched | duplicates: last kept (`p3`: `Some(Success(y))`) |
| 36 | `CompletedRequestMap.fromIterableOption` | "from the specified optional results" | enriched | `None` left out (`CompletedRequestMap.scala:181`; `p3`) |
| 37 | `CompletedRequestMap.fromIterableWith` | "Constructs a completed requests map an iterable of A" | style | grammar |
| 38 | `CompletedRequestMap.single` | (no period) | style | |
| 39 | `CompletedRequestMap.unsafe` | "do not check ... same size" | enriched | pairs by position, stops at the shorter (`CompletedRequestMap.scala:238`; `p3`: 3 requests, 2 results → 2 entries) |
| 40 | `Cache` (trait) | "used internally ... deduplication and caching" | enriched | example (run: `(user-1,user-1,1)`), `@see running-queries`; `run` uses a new cache per run (`ZQuery.scala:656`; `p3`: two runs → 2 calls) |
| 41 | `Cache#lookup` | Left/Right | enriched | stores the new promise on a miss (`Cache.scala:95`; `p3`: `isLeft` true then false); the default cache goes through `lookupUnsafe` (`ZQuery.scala:1610`) |
| 42 | `Cache#put` | "Inserts" | enriched | replaces (`ConcurrentHashMap.put`, `Cache.scala:100`) |
| 43 | `Request` | "request from a data source for a value of type `A`" | enriched | example extended with a data source and queries (run: `(List(1, 2),user-1)`), `@see creating-data-sources` |
| 44 | `QueryFailure` | "keeps track of details relevant to query failures" | imprecise | it is the defect for a missing result (`BlockedRequests.scala:295`); `p2`: message `Data source users did not complete request GetUserName(9).`, stack length 0 |
| 45 | `Described` | hash / description contract | enriched | description goes into identifiers (`p2`: `users.contramap(toGetUserName)`) |
| 46 | `DataSourceAspect` | "aspect that can be weaved into queries" | imprecise | applies to data sources (`DataSource#@@`), and to queries only through `mapDataSources`/`fromDataSourceAspect` |
| 47 | `DataSourceAspect#andThen` | sequential composition | enriched | order: `that(self(dataSource))` (`DataSourceAspect.scala:47`) |
| 48 | `DataSourceAspect.around` | before/after | enriched | `p2`: `before users[1,2,3] after 1` once per `runAll`; identifier `users @@ around(before)(after)` |
| 49 | `DataSourceAspect.maxBatchSize` | "at most `n` requests in parallel" | imprecise | it is `batchN` (`DataSourceAspect.scala:76`), see 4 |
| 50 | `QueryAspect` | "transforming one data source into another" | false | it transforms queries (`QueryAspect.scala:31`) |
| 51 | `QueryAspect#andThen` | sequential composition | enriched | `that(self(query))` |
| 52 | `QueryAspect.around` | before/after a query | enriched | `p2`: `qbefore ... qafter 1` once per query |
| 53 | `QueryAspect.aroundDataSource` | "executes requests between two effects" | imprecise | once per data-source call, per data source; `p2`: two data sources → `before before ... after 1 after 1` |
| 54 | `QueryAspect.fromDataSourceAspect` | "executes requests with the specified data source aspect" | enriched | applies it to every data source (`mapDataSources`) |
| 55 | `QueryAspect.maxBatchSize` | "at most `n` requests in parallel" | imprecise | `p2`: `users[1,2] users[3]` |
| 56 | `BlockedRequests#run` | "submitting requests to each data source in parallel" | imprecise | `BlockedRequests.scala:108-109`: rounds run with `foreachDiscard`, data sources within a round with `foreachParDiscard` |
| 57 | `Continue#zipWithPar` | "in parallel" | imprecise | only `Effect`+`Effect` uses `zipWithPar`; `Get` cases use `zipWith` (`Continue.scala:254,353`) |
| 58 | `Continue#zipWithBatched` | "batching requests to data sources" | imprecise | same shape (`Continue.scala:262,361`) |
| 59 | `Continue.apply` | "from a request, a data source, and a `Promise`" | false | takes only a `Promise` (`Continue.scala:156`) |
| 60 | `Result` | "may either by done" | style | typo |
| 61 | `Sequential#toIterable` | "batches requests" | style | grammar |

## New documentation (the 29 undocumented members)

- Tier 2: `object DataSource`, `DataSource.Batched#run`, `#runAll`, `object DataSource.Batched`,
  `object CompletedRequestMap`, `CompletedRequestMap.apply`, `.empty`, `trait UnsafeApi`,
  `UnsafeApi.fromExits`, `.fromSuccesses`, `.fromWith`, `object Cache`, `object DataSourceAspect`,
  `object QueryAspect`, `object Described`, `Described.AnySyntax`, `AnySyntax.?`, the five aliases in
  `package.scala`, and the class `CompletedRequestMap` (its existing comment now attaches).
- Tier 3 one-liners: `BlockedRequest.Failure`, `.Success`, `.request`, `.result`, `QueryScope.addFinalizerExit`,
  `QueryScope.closeAndExitWith` (LIFO order from `ref.updateAndGet(f :: _)`, `QueryScope.scala:29,38`).
- Tier 1 (overview + compiled example + live `@see`): `DataSource`, `DataSource.fromFunctionBatched`,
  `Request`, `CompletedRequestMap`, `Cache`. `fromFunctionBatchedWith` also got an example, because its
  matching rule was the one documented wrongly. The other siblings are tier 2 with the facts from `p2`.

## Coverage (my files, `list-members.py ... all`)

| File | Before | After |
|---|---:|---:|
| DataSource.scala | 27/31 | 31/31 |
| CompletedRequestMap.scala | 16/24 | 24/24 |
| QueryAspect.scala | 8/9 | 9/9 |
| Cache.scala | 7/8 | 8/8 |
| DataSourceAspect.scala | 6/7 | 7/7 |
| package.scala | 0/5 | 5/5 |
| Described.scala | 1/4 | 4/4 |
| internal/BlockedRequest.scala | 0/4 | 4/4 |
| internal/QueryScope.scala | 1/3 | 3/3 |
| QueryFailure.scala, Request.scala | 2/2 | 2/2 |
| **Total** | **68/97 (70%)** | **97/97 (100%)** |

`list-members.py zio-query undoc` lists 0 gaps outside `ZQuery.scala`. Whole module after both tasks:
279/279 (100%).

## Checks

```
$ python3 -I .../snippets-scala-cli.py --dep dev.zio::zio-query:0.7.9 --imports "zio._; zio.query._" \
    --scala 2.12.21,2.13.18,3.3.9 --out /tmp/claude-1000/query-scaladoc/snip-1b <my 15 changed files>
6 blocks in 4 files
Scala 2.12.21: OK  (sources in /tmp/claude-1000/query-scaladoc/snip-1b/v2.12.21)
Scala 2.13.18: OK  (sources in /tmp/claude-1000/query-scaladoc/snip-1b/v2.13.18)
Scala 3.3.9: OK  (sources in /tmp/claude-1000/query-scaladoc/snip-1b/v3.3.9)

$ python3 -I .../comments-only.py 4e04bed1b zio-query
16 files, 0 with code changes
```

Each changed file was formatted with `scala-cli fmt --scalafmt-conf .scalafmt.conf <file>` (never
`ZQuery.scala`), and `.scala-build` folders under `zio-query/` were removed. sbt was not run.

## Deviations

- `package.scala`: the five aliases lost their alignment padding (`type RQuery[-R, +A]  =` → one space),
  because a comment now separates them and scalafmt no longer aligns them. Whitespace only;
  `comments-only.py` accepts it.
- `CompletedRequestMap.scala`: removed the blank line between the class comment and `sealed abstract class`,
  so the existing comment attaches to the class (the coverage tool counted the class undocumented).
  Whitespace only.
- The fact-check table has a fifth kind, "enriched", for true comments that only gained facts, so the
  false/imprecise counts are not inflated.
- Version folders: unchanged (only one comment, true).

## Concerns

- New Scaladoc links: `[[DataSource$]]` (DataSource header) is new; it follows the existing
  `[[CompletedRequestMap$]]` pattern. Other type references use backticks to avoid class/companion
  ambiguity. The 2.12/2.13 `doc` gates (controller) are the real check.
- `docs/completed-request-map.md` lists `requests` as working; it throws for most maps (issue 2). Docs are out
  of scope for this PR.
- `DataSource.never` + `ZQuery#timeout` does not complete (the timeout does not bound data-source calls,
  Task 1a's bug); the `never` Scaladoc says only that the query does not complete and that it loses a `race`.
- `Cache.empty(n)` sizes the map with `ceil(n / 0.75)` and `ConcurrentHashMap(int)` adds its own load-factor
  headroom, so it over-allocates about 2x. Not a correctness bug; not filed, and the Scaladoc claim ("without
  the need ... to be resized") stays true.
- The batch order in the `DataSource` example varies (`[2, 1, 3]` in the run), so the comment says "such as".

## Bugs (details in `task-1b-issues.md`)

1. `contramap`, `contramapZIO`, `eitherWith` return results keyed by the converted request, so queries die with
   `QueryFailure` unless the conversion is the identity.
2. `CompletedRequestMap#requests` throws `ClassCastException` for every mutable-backed map (all constructors
   but `empty`, `single`, one-entry `apply`, `++`, `insert`), on all three Scala versions.
3. `eitherWith` calls both sides for every batch, even a side with no requests: empty batches reach
   `DataSource.make` sources, and the query waits for the idle side (hangs with `DataSource.never`).

DONE_WITH_CONCERNS
