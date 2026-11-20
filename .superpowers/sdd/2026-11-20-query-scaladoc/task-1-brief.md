# Task 1: fact-check and complete the zio-query Scaladoc

Two writers work in parallel on disjoint files. Both follow `global-constraints.md`.

## Task 1a: `zio-query/shared/src/main/scala/zio/query/ZQuery.scala` (2,218 lines)

- Existing: 154 `/**` blocks. Undocumented public members (32): `asExitOrElse`, `object ZQuery`, `absolve`,
  `foreachBatched`, `unsafe.memoize`, the `*PartiallyApplied` classes and their `apply`s, `ProvideSomeLayer`,
  `TimeoutTo`, `Acquire`, `Release`, `AcquireExit`, `ReleaseExit`, `cachingEnabled`, `currentCache`,
  `currentScope`. Full list: `grep ZQuery.scala inventory-before.tsv | grep UNDOC`.
- Tier 1: the `ZQuery` trait overview (fix its example so it compiles and is honest about how many batches
  run), `fromRequest`, `foreachBatched`, `foreachPar`, `run`, `runCache`, `runLog`.
- Known broken: the `environmentWith` and `serviceWith` examples use `effect.access(...)`, which does not exist.
- Suspects to probe: the "two queries" claim in the header, `memoize` (example indentation and the
  "at-most-once" claim), `foreachPar` vs `foreach` vs `foreachBatched` batching, `zipPar`/`zipBatched`,
  `cached`/`uncached`, `timeout*`, `race*`, `summarized`, `ensuring`, `acquireRelease*`, `fromZIO` vs
  `fromZIONow`, `either`/`option`/`optional`, `orDie*`, `refine*`, `catch*`, `provide*`, `sandbox`,
  `maxBatchSize`, `@@`.
- Reference pages to mine for examples and claims (compare, do not trust): `docs/zquery.md`,
  `docs/creating-queries.md`, `docs/running_queries.md`, `docs/error-handling.md`, `docs/how-batching-works.md`.

## Task 1b: every other file

`DataSource.scala`, `CompletedRequestMap.scala`, `Cache.scala`, `DataSourceAspect.scala`,
`QueryAspect.scala`, `Described.scala`, `Request.scala`, `QueryFailure.scala`, `package.scala`, and
`internal/*.scala` (tier 3). The version folders (`scala-2.12/`, `scala-2.13/`, `scala-3/`) have no Scaladoc;
leave them unless a comment there is false.

- Undocumented public members (29): `grep -v ZQuery.scala inventory-before.tsv | grep UNDOC`.
- Tier 1: `DataSource` (trait overview: its example must show a batched data source end to end),
  `DataSource.fromFunctionBatched` (and decide for its siblings), `Request`, `CompletedRequestMap`
  (overview + example), `Cache` (overview + example).
- Known wrong (verify yourself, then describe the real behaviour): `DataSource.contramap`, `contramapZIO`
  and `fromFunctionBatchedWith` (and its `ZIO`/`Option` siblings). Also check `eitherWith`, `batchN` (its
  `identifier`), `race`, `provideEnvironment`/`provideSomeEnvironment`, `mapDataSource`-style aspects, the
  `DataSource` header (it says `Request[A]` with one type parameter, and "`CompletedRequestsMap`").
- Reference pages: `docs/data-source-reference.md`, `docs/creating-data-sources.md`,
  `docs/completed-request-map.md`, `docs/cache.md`, `docs/request.md`, `docs/aspects.md`, `docs/described.md`,
  `docs/query-failure.md`.

## Output (each task, `<n>` = `1a` or `1b`)

1. Commits on `query-scaladoc-pilot`, explicit paths, comments only.
2. `task-<n>-report.md`: commits; a **fact-check table of every existing comment you changed** (member,
   old claim, kind = false / imprecise / broken example / style, evidence = source line or probe output);
   the count of existing comments checked and how many were false or imprecise; coverage before and after
   for your files; example check output for the 3 versions; deviations; concerns. End with DONE,
   DONE_WITH_CONCERNS or BLOCKED.
3. `task-<n>-issues.md`: each library bug in this shape: title; found at (2026-11-20, 4e04bed1b = v0.7.9);
   summary; self-contained scala-cli reproducer with its real output pasted as `// ...` comments; expected
   behaviour; root cause with `file:line` at the base; impact; test gap (grep the tests and read the
   assertions before claiming a gap); suggested fix. Write "none" if there are none.
