# Task 1b re-review 2 (fix round 1, non-ZQuery files)

Diff: `git diff 3f439db HEAD -- zio-query ':!zio-query/shared/src/main/scala/zio/query/ZQuery.scala'` (214 lines).
Probes: `/tmp/claude-1000/query-scaladoc/rerev-b/` (`p` = race/never, fromFunctionZIO, Batched, `requests` on 2.13.18;
`hm` = copy cost; `js` = Scala.js, run with `scala-cli --cli-version 1.19.0`, the installed 1.14.0 cannot link Scala.js 1.22).
All against `dev.zio::zio-query:0.7.9`.

Verdict: APPROVE. Important 0, Minor 2 (both optional wording, no claim is false), Confirmed below.

## Important

None.

## Minor

### m-1. `requests`: "for a non-empty batch" under-states which `fromFunction*` maps fail (optional)
- **Where:** `zio-query/shared/src/main/scala/zio/query/CompletedRequestMap.scala:130-134`.
- **Evidence:** `DataSource.Batched#runAll` with two or more batches always returns a `Mutable` map, even when every
  batch is empty. Probe: `fromFunctionOption.runAll(Chunk(Chunk.empty, Chunk.empty)).requests` throws
  `ClassCastException`; with no batch, or one empty batch, it returns `Set()`. The comment's sentence ("the maps
  that the `DataSource.fromFunction*` data sources return for a non-empty batch") is true, only incomplete for this
  corner. Not a false claim.
- **Checked replacement (only if the author wants exactness):** `include the maps that the DataSource.fromFunction*
  data sources return, except for no batch or a single empty batch,` (same line wrapping left to scalafmt).

### m-2. `fromFunctionZIO`: "a failure of one call" does not cover a function that throws (optional)
- **Where:** `zio-query/shared/src/main/scala/zio/query/DataSource.scala:499-501`.
- **Evidence:** failures and defects inside the returned effect are isolated per request (`fz crm`: `u-1`,
  `Failure(Fail(bad))`, `u-2`; `fzDie crm`: `Failure(Die(d))` for 0 only). If `f` itself throws before returning an
  effect (`r => throw ...`), `runAll` dies as a whole (`fzThrow`: `Failure(Both(Empty,Die(t)))`). "A failure of one
  call" is read as a failure of the effect, so the text is not false; the current text is acceptable.
- **Checked replacement (optional):** `and a failure of the effect for one request fails only that request: the
  other requests of the batch still get their results.`

## Confirmed

Finding by finding:

- **I-1 (copy cost), resolved.** `CompletedRequestMap.scala:69`, `:94`, `:107` all read "a call can copy every entry of
  this map into a new map". Probe `hm` (2.13.18): `immutable.HashMap.from(h) eq h: true`; 20000 chained `insert`s from
  `empty` 208 ms; 200 `insert`s on a 20000-entry `fromIterable` map 1817 ms; 200 `insert`s on a 20000-entry
  immutable-backed map 1 ms. `insertOption None` on an empty map returns the empty map (probe `p`). "can" is true for both
  cases. The "Prefer ..." sentences are kept.
- **I-2 (`never`), resolved.** `DataSource.scala:556-560`. Probe `p`: `never.race(fast)` and `fast.race(never)` both
  `Some(Success(fast-1))`; dying vs `never` in either order `None` (2 s timeout). "never wins ... returns the
  results of the other data source, and does not complete if the other data source's `runAll` dies" is literally true.
  `runAll` is `ZIO.never` (source).
- **M-1 (`requests`), resolved.** `CompletedRequestMap.scala:127-137`. Probe `p` 2.13.18: `empty`, `single`, `apply0`
  (`Set()`), `apply1`, `++`, `insert` on a mutable map (`Set(R(1), R(2))`) work; `apply2`, `fromFunctionOption` with
  one request, `fromFunctionBatchedWith` with one request throw; `fromFunctionOption` with one empty batch or no batch
  returns `Set()`. Source: `apply` matches `Seq()` -> `empty`, `Seq(x)` -> `single`. Scala.js (probe `js`, 1.22, scala-cli
  1.19.0): development link `throws UndefinedBehaviorError: java.lang.ClassCastException: ...MapOps$KeySet cannot be
  cast ...`; `--js-mode release`: `Set(G(1), G(2))`. "development build" / "release build" are the Scala.js terms
  (fastLink / fullLink; `--js-mode release` runs fullLink). Scala Native is not mentioned in the comment and is not
  claimed. `insertOption` is correctly left out of the working list (`None` on a mutable map throws).
- **M-2 (`fromFunctionZIO`), resolved.** `DataSource.scala:499-501`; probes above; plain `foreachBatched` of the same
  requests gives `Failure(Fail(bad,...))` for the query, so the new wording "only that request" is right.
- **M-3 (`Batched`), resolved.** `DataSource.scala:269-274`. Probe `p`: `runAll([1,2],[2,3],[1],[])` calls `run` with
  `[1,2]` then `[3]` ("one batch after another", "leaving out requests that an earlier batch has already completed").
  Within one batch duplicates are kept (`[R(1),R(1)]` reaches `run` once as written), which the text does not deny.
- **M-4 (`Batched.make`), resolved.** `DataSource.scala:320-324`. Probe: no call for `Chunk(Chunk())`, `Chunk()` or two
  empty batches; the third batch of the probe above (`[1]`) is skipped; `[3]` arrives without `2`. "once per non-empty
  batch ... after requests that an earlier batch has already completed are left out" holds.
- **M-5 (`Continue`), resolved.** `internal/Continue.scala:26-30` and `:169-172`. Source: `Get#mapZIO`, `foldZIO`,
  `foldCauseZIO` return `Get(io...)` (`:298-314`, `:339`); `Get#foldQuery`/`mapQuery` return `Effect(...)`
  (`:308`, `:334`); `Effect` overrides return `Effect(...)`. "functions or ZIO effects, but not with queries" is true.
- **M-6 (bug entry 2), resolved.** Constructor list matches the source and probe `p` (`empty`, `single`, `apply` with at
  most one entry, `++`, `insert` immutable; `insertOption` immutable only when it adds an entry; `None` on a mutable
  map throws). JVM-only wording and the Scala.js outputs reproduced exactly (probe `js`, both modes). "same result on all
  three" (2.12.21, 2.13.18, 3.3.9) was verified by review B; the 2.12 message is as before. The entry says Scala Native
  nothing, so no unproved claim.
- **R7 `@see` trailing slash:** all 7 `@see` URLs outside `ZQuery.scala` end with `/` (`grep`); no other change in the
  diff: the only edits are the ones named above. No ledger, "above", "below" or "in this file" text in the added lines (0
  grep hits). No unescaped `$`, `^`, `/*`, `@throws` added.

Ruling scope: nothing outside R7 changed. The diff touches Cache, CompletedRequestMap, DataSource, DataSourceAspect,
QueryAspect, Request, Continue, and the ledger (issues, report); `Described`, `QueryFailure`, `package.scala`,
`BlockedRequest*`, `QueryScope`, `Result`, `Sequential` are untouched.

`python3 -I ~/.claude/skills/scaladoc-rollout/scripts/comments-only.py 4e04bed1b --all`: `files=28 code=0
not_allowed=12 directives=0`; the 12 not_allowed are the ledger files under `.superpowers/sdd/...` (status A), as
expected. All 16 Scala files `OK`.

Security: nothing security-relevant found.
