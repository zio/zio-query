# Task 1 review B: fact-check of the Scaladoc outside ZQuery.scala

Scope: `DataSource`, `CompletedRequestMap`, `Cache`, `DataSourceAspect`, `QueryAspect`, `Described`, `Request`,
`QueryFailure`, `package.scala`, `internal/*`, at HEAD (`3f439db`), against base `4e04bed1b` (= v0.7.9).
Probes are in `/tmp/claude-1000/query-scaladoc/rev-b/` (`ds` = every DataSource combinator and aspect, `crm-<ver>` =
CompletedRequestMap on 2.12.21/2.13.18/3.3.9, `crm-js` = Scala.js, `cache`, `hm-<ver>` = cost of the deprecated
`insert`, `ex-<ver>` = every `{{{ }}}` example run on three versions, `issue<n>-<ver>` = the bug reproducers copied
from the issues file, `doc212`/`doc213` = scaladoc runs on the HEAD sources). All probes use
`dev.zio::zio-query:0.7.9` unless noted.

Counts: Important 2, Minor 6.

## Important

### I-1. The deprecated `++`, `insert` and `insertOption` do not copy every entry on each call

- **Where:** `zio-query/shared/src/main/scala/zio/query/CompletedRequestMap.scala:69`, `:94`, `:107` (three copies
  of "degradation: each call copies every entry into a new map").
- **What is wrong:** the code is `immutable.HashMap.from(map) ++ that.map` / `immutable.HashMap.from(map).updated(...)`.
  When this map is already backed by an immutable `HashMap` (built with `empty`, `single`, `apply` with at most one
  entry, or an earlier `++`/`insert`), `immutable.HashMap.from` returns the same map, and nothing is copied. Only a
  map backed by a mutable `HashMap` (`fromIterable`, `combine`, the `unsafe` constructors, data-source output, ...)
  is copied in full. `insertOption` with a `None` copies nothing at all (it returns `self`). So "each call" is false
  for the usual deprecated pattern, a fold of `insert` calls starting from `empty`.
- **Evidence:** `hm-2.13.18` and `hm-2.12.21`: `HashMap.from(hm) eq hm: true` on both. 20000 chained `insert`
  calls from `empty`: 31 ms (2.13) / 21 ms (2.12); 200 `insert` calls on a 20000-entry `fromIterable` map: 1642 ms
  / 1799 ms. `crm-*`: `insertOption None eq self: true`.
- **Replacement (all three places):** replace `degradation: each call copies every entry into a new map.` with
  `degradation: a call can copy every entry of this map into a new map.` (keep the following "Prefer ..." sentence
  as it is).

### I-2. `DataSource.never` does not "always lose" a race

- **Where:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:554-555`.
- **What is wrong:** "Combined with another data source by `race`, it always loses." `race` returns the first side
  to succeed; a side whose `runAll` dies loses and the race waits for the other side. If the other data source's
  `runAll` dies, the race waits for `never` forever: `never` does not win, but the query does not complete either,
  so "always loses" is false for that input (rule: over-broad word not proved for every input).
- **Evidence:** `ds` probe: `race never vs fast => Success(fast-1)`; `race dying vs never (2s timeout) =>
  Success(None)` (the 2-second timeout fired). Source: `DataSource.scala:253` at HEAD (`self.runAll(requests).race(that.runAll(requests))`), base `:180`.
- **Replacement:** `Combined with another data source by `race`, it never wins: the race returns the results of the
  other data source, and does not complete if the other data source's `runAll` dies. Useful in tests.`

## Minor

### M-1. `CompletedRequestMap#requests`: zero-entry `apply`, empty data-source output, and platforms

- **Where:** `zio-query/shared/src/main/scala/zio/query/CompletedRequestMap.scala:127-132`.
- **What is wrong:** (a) `apply` with zero entries returns `empty` and works, but the comment lists only "`apply`
  with one entry". (b) "the maps returned by the `DataSource.fromFunction*` data sources" throw only for a non-empty
  batch; with no requests `Batched.runAll` returns `CompletedRequestMap.empty`. (c) The `ClassCastException` is the
  JVM behaviour. On Scala.js (the library is published for JS) a development build fails with
  `org.scalajs.linker.runtime.UndefinedBehaviorError`, and a release build does not check the cast and returns the
  keys.
- **Evidence:** `crm-*` on 2.12.21, 2.13.18, 3.3.9: `apply0: Set()`, `fromFunction ds output 0 req: Set()`,
  `fromFunction ds output 1 req: throws java.lang.ClassCastException`. `crm-js` (Scala.js 1.22, run with scala-cli
  1.19.0): fastLink `throws org.scalajs.linker.runtime.UndefinedBehaviorError: java.lang.ClassCastException: ...`;
  `--js-mode release`: `requests returned: Set(G(1), G(2)) size 2 contains true`. Scala Native not probed.
- **Replacement:**
  ```
   * It works for maps built with `empty`, `single`, `apply` with at most one
   * entry, or the deprecated `++` and `insert`. For maps built with the other
   * constructors, such as `apply` with two or more entries, `combine`,
   * `fromIterable`, `fromIterableWith`, `fail` or the `unsafe` ones, which
   * include the maps that the `DataSource.fromFunction*` data sources return
   * for a non-empty batch, the cast fails: on the JVM it throws a
   * `ClassCastException`. This is a known bug.
  ```

### M-2. `DataSource.fromFunctionZIO`: "fails only the query for that request" reads as the enclosing query

- **Where:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:498-499`.
- **What is wrong:** a reader can take "the query" to be the query they run. A query that combines several requests
  (for example `ZQuery.foreachPar`) fails as a whole when one of them fails; what is isolated is the request's
  result. The sibling `fromFunctionOptionZIO` already says it better ("a failure fails only that request").
- **Evidence:** `ds` probe: per-request handling gives `1:OK u-1, 0:ERR bad 0, 2:OK u-2`; the plain
  `ZQuery.foreachPar(List(1, 0, 2))(...)` gives `Failure(Fail(bad 0, ...))`. Source: base `DataSource.scala:356-357`
  (`foreachPar(requests)(a => f(a).exit)` then `unsafe.fromExits`).
- **Replacement:** `request. The calls for one batch run in parallel, and a failure of one call fails only that
  request: the other requests of the batch still get their results.`

### M-3. `DataSource.Batched` (unchanged comment) says it does not optimize sequential batches

- **Where:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:270-272`.
- **What is wrong:** `Batched.runAll` leaves out, from the second batch on, requests that an earlier batch has
  already completed, which is an optimization of sequential batches. The new `runAll` comment (`:283-286`) says so,
  and the trait comment now contradicts it.
- **Evidence:** base `DataSource.scala:211` (`reqs.filterNot(crm.contains)`); `ds` probe
  `par(batched("dd").batchN(1), List(1, 1, 2)).uncached` prints `dd[1]` then `dd[2]` for the batches `[1] [1] [2]`.
- **Replacement:**
  ```
   * A data source that executes requests that can be performed in parallel in
   * batches, one batch after another. Apart from leaving out requests that an
   * earlier batch has already completed, it does not further optimize batches
   * of requests that must be performed sequentially.
  ```

### M-4. `DataSource.Batched.make`: "called once per batch"

- **Where:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:319-321`.
- **What is wrong:** the function is not called for an empty batch, or for a batch whose requests were all
  completed by an earlier batch (same code as M-3), and it receives the batch without those requests.
- **Evidence:** base `DataSource.scala:203` (`if (reqs0.nonEmpty) run(reqs0)`) and `:211-212`; `ds` probe above.
- **Replacement:** `and returning a `CompletedRequestMap`. The function is called once per non-empty batch of
  requests that can be performed in parallel, after requests that an earlier batch has already completed are left
  out, and `name` becomes the identifier.`

### M-5. `Continue` (unchanged internal comments): a `Get` is not limited to pure functions

- **Where:** `zio-query/shared/src/main/scala/zio/query/internal/Continue.scala:26-27` and `:169-170`.
- **What is wrong:** "a `Get` that merely gets the result of a blocked request (potentially transforming it with
  pure functions)". `Get#mapZIO`, `Get#foldZIO` and `Get#foldCauseZIO` keep a `Get` while adding a ZIO effect
  (`Get(io.flatMap(f))`, `Get(io.foldZIO(...))`); only the operations that take a `ZQuery` turn it into an `Effect`.
  Tier 3, so low priority.
- **Evidence:** `Continue.scala` at HEAD: `Get#mapZIO` returns `Get(io.flatMap(f))`; `Get#mapQuery` returns
  `Effect(ZQuery.fromZIONow(io).flatMap(f))`.
- **Replacement:** in both places, `(potentially transforming it with pure functions)` becomes `(potentially
  transforming it with functions or ZIO effects, but not with queries)`.

### M-6. Bug entry 2 overstates the set of failing maps and is JVM-only

- **Where:** `task-1b-issues.md`, entry 2, "Summary" and "Expected behaviour".
- **What is wrong:** the list of working constructors leaves out `apply` with zero entries (it returns `empty`) and
  `insertOption` (it returns an immutable map unless it is given `None`, in which case it returns the receiver),
  and the entry does not say that the `ClassCastException` is the JVM behaviour (see M-1(c)). The bug itself is
  real and confirmed.
- **Replacement for the parenthesis in the Summary:** `(every constructor except `empty`, `single`, `apply` with at
  most one entry, and the deprecated `++`, `insert` and `insertOption`, the last only when it adds an entry)`, and
  add after "Checked on Scala 2.12.21, 2.13.18 and 3.3.9": `on the JVM. On Scala.js a development build fails with
  an `UndefinedBehaviorError`, and a release build does not check the cast.`

## Confirmed

Bug entries (reproducers copied verbatim from `task-1b-issues.md`, run on 2.13.18 and 3.3.9, output identical to
the pasted output on both):

- **Entry 1 (contramap / contramapZIO / eitherWith keyed by the converted request): confirmed.** Real bug: the
  Scaladoc and `docs/` describe the combinators as adapters; nothing re-keys the map. Citations correct at base
  (`DataSource.scala:85`, `:99`, `:119-120`; `BlockedRequests.scala:128`, `:134`, `:295`; leftovers cached by
  `cacheLeftovers`). Test gap confirmed: `grep -rn 'contramap\|eitherWith' zio-query/shared/src/test` is empty. The
  `docs/` warnings exist (`aspects.md:91`, `data-source-reference.md:271`). Identity `contramap` works (`ds`:
  `contramap identity => Success(ci-1)`). No sibling has the same flaw: `race`, `batchN`, `provideSomeEnvironment`
  and `DataSourceAspect.around` pass requests through unchanged.
- **Entry 2 (`CompletedRequestMap#requests` ClassCastException): confirmed** on 2.12.21, 2.13.18, 3.3.9 (JVM); see
  M-6 for precision. Citations correct (`CompletedRequestMap.scala:101`, `:255-256`). History correct: before
  `c5ed2a3` (#470) the class held only an immutable `HashMap`. Test gap confirmed (the only `requests` hit in
  `ZQuerySpec.scala:518` is a local variable). Message on 2.12 is `scala.collection.mutable.HashMap$$anon$1 cannot be
  cast ...`, on 2.13/3 `scala.collection.MapOps$KeySet cannot be cast ...`.
- **Entry 3 (eitherWith calls both sides for every batch): confirmed.** `Database received Chunk(Chunk())`,
  `leftOrNever: None` on both versions. Citation `DataSource.scala:119-120` correct; `Batched.runAll` returns early
  on an empty batch (base `:203`). Test gap confirmed. Only `eitherWith` builds empty inner batches (`Parallel`
  creates a builder only on `addOne`, `batchN`'s `grouped` drops empty chunks).

New or changed Scaladoc, checked against the source and the `ds`, `crm-*`, `cache` probes:

- `DataSource` trait: equal identifiers share a batch (`Same[1,2]`, the second instance is never called; `a==b
  true`); four lookups with a duplicate reach the data source as one batch of three; `fromRequest` evidence.
- `identifier`, `runAll`, `batchN` (`bt[1,2] bt[3,4] bt[5]`; `mk [1,2][3,4][5]` in one call; `batchN(0)` and
  `maxBatchSize(0)` die with `IllegalArgumentException`; identifier `bt.batchN(2)`), `contramap`, `contramapZIO`,
  `eitherWith`, `provideEnvironment` (identifier `pe.provideSomeEnvironment(_ => env)`), `provideSomeEnvironment`,
  `query`, `queryAll` (`Chunk(qa2-3,qa2-1,qa2-2)`), `race` (failed map wins: `Fail(boom)`; dying `runAll` loses:
  `Success(slow-1)`).
- `Batched#run`, `Batched#runAll`, `fromFunction`, `fromFunctionBatched` (fewer → `QueryFailure` for `P(2)`, extra
  ignored), `fromFunctionBatchedOption`, `fromFunctionBatchedOptionZIO` (both requests `down`),
  `fromFunctionBatchedWith` (reversed and truncated: `P(1)` → `QueryFailure`, 2 and 3 OK),
  `fromFunctionBatchedWithZIO`, `fromFunctionBatchedZIO`, `fromFunctionZIO` (three 200 ms calls in 211 ms),
  `fromFunctionOption`, `fromFunctionOptionZIO` (`err2` isolated, `None` → `QueryFailure`), `make`, `never` (query
  does not complete).
- `CompletedRequestMap`: class text, `++` (that wins), `insert` (replaces, original unchanged), `insertOption`
  (`None` returns `self`), `apply`, `empty`, `combine` (last wins), `die`/`fail`/`failCause`, `fromIterable` (last
  duplicate kept), `fromIterableOption` (`None` left out), `fromIterableWith`, `single`, `unsafe` (stops at the
  shorter chunk), `UnsafeApi` members.
- `Cache`: two `run`s make two calls; `runCache` with one cache makes one; `lookup` is `Left` then `Right` with the
  same promise; `put` replaces; `remove` empties (`get` fails with `()`); the library never calls `remove`
  (grep); `lookupUnsafe` for `Cache.Default` (ZQuery `cachedResult`).
- `DataSourceAspect` (applies through `mapDataSources` to later rounds too: `r1[0] r2[1,2] r2[3]`), `andThen` order
  (`z @@ around(A)(A) @@ around(B)(B)`), `around` (once per `runAll`, identifier `ar @@ around(b)(a)`),
  `maxBatchSize`; `QueryAspect`, `andThen`, `around` (once per query), `aroundDataSource` (once per data source:
  `qb qb ... qa 1 qa 1`), `fromDataSourceAspect`, `maxBatchSize`.
- `Described`, `AnySyntax`, `?`; `Request` (cache keyed by request only, `ZQuery.scala` `cachedResult`);
  `QueryFailure` (message, `writableStackTrace = false`, raised at `BlockedRequests.scala:297` at HEAD);
  the five aliases in `package.scala`.
- Internal: `BlockedRequest` members, `BlockedRequests#run` (rounds with `foreachDiscard`, data sources with
  `foreachParDiscard`), `Continue#zipWithPar`/`zipWithBatched`/`apply`, `QueryScope` (LIFO, `NoOp` ignores),
  `Result`, `Sequential` typo fixes.

Examples: all six `{{{ }}}` blocks run on 2.12.21, 2.13.18 and 3.3.9 (`ex-<ver>`); every stated output matches:
`UserNames received [2, 1, 3]` once and `[1, 2, 3]` in other runs ("such as" is right), `List(user-1, user-2,
user-1, user-3)`, `UserNames received 3 requests`, `List(User(1,user-1), User(2,user-2), User(3,user-3))`,
`Some(Success(Ann))` / `Some(Failure(Fail(no user 2,StackTrace.none)))` / `None`, `(user-1,user-1,1)`,
`(List(1, 2),user-1)`.

Unchanged comments sampled (24) and found true, except M-3 and M-5: `DataSource#@@`, the `runAll` paragraph of the
trait comment, `race` first sentence, `contramap` first sentence, `CompletedRequestMap` first paragraph (type
guarantee holds through the typed constructors), `contains`, `isEmpty`, `lookup`, `combine` first sentence,
`die`/`fail`/`failCause` first sentences, `fromIterableWith` `@param`s, `unsafe` first paragraph, `Cache` first
sentence, `Cache#get` (fails with `()`), `Cache#remove`, `Cache.empty`, `Cache.empty(n)` (over-allocates, never
resizes for `n` entries: true), `DataSourceAspect#apply`/`>>>`, `QueryAspect#apply`/`>>>`, `BlockedRequest` class,
`BlockedRequests` class, `&&`, `++`, `mapDataSources`, `merge` (pipelines only one data source on both sides:
`seqHead.size == 1 && parallel.size == 1`), `Parallel`, `Sequential#++`, `Result` and its constructors,
`scala-2.12/UtilsVersionSpecific` line comment.

Rules:

- No ledger, issue-tracker, task, "above", "below" or "in this file" reference in the added lines.
- `@see` links: `data-source-reference`, `completed-request-map`, `creating-data-sources`, `running-queries`,
  `aspects`; all five are on the allowed list. `curl -s -o /dev/null -w '%{http_code}'` gives `301` for each (to
  the same URL with a trailing slash), and `200` with `-L`. The rules' own format has no trailing slash, so this
  is expected, not a finding.
- No unescaped `$name`, `^`, `/*`, `<h1>`..`<h6>` or `@throws` in the added lines; no `[[link]]` to an overloaded
  member.
- `[[DataSource$]]` resolves: the 2.13.18 and 2.12.21 scaladoc of the HEAD sources link it to `DataSource$.html`
  (like the existing `[[CompletedRequestMap$]]`; the link text renders as `DataSource$`). Both scaladoc runs give
  the same warnings as the base sources (the existing `zio.Scope` link in `QueryScope` and a `zio.Exit` link in
  `ZQuery.scala`, which are unresolved without API mappings), so the PR adds no doc warning in these files.

Security: nothing security-relevant found.
