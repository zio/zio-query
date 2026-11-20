# Task 1a re-review 2 (fix round 1): `ZQuery.scala`

Scope: `git diff 3f439db HEAD -- zio-query/shared/src/main/scala/zio/query/ZQuery.scala`, HEAD line numbers.
Probes: `/tmp/claude-1000/query-scaladoc/rerev-a/` (`probe/`, `probe2/`, `bug6-*`, `bug7-*`, `p9/`), Scala 2.13.18 and
3.3.9, `dev.zio::zio-query:0.7.9`. Nothing security-relevant found.

Verdict: NEEDS_FIXES (0 Important, 1 Minor; the Minor is an over-broad claim, and ruling R6 fixes all findings).

## Important

None.

## Minor

### M1. `memoize` (line 471-473): "including ... a defect raised by an effect lifted with `ZQuery.fromZIO`" is over-broad

Current text: "later uses reuse it, including a typed failure or a defect raised by an effect lifted with
`ZQuery.fromZIO`. A query that dies in other ways, for example through `ZQuery.die` or `orDie`, is not memoized".

Evidence (probe `probe/Main.scala`, number of times the effect ran over two uses of the memoized query):

| Query | computed |
|---|---|
| `fromZIO(ZIO.die(ex))` | 1 |
| `fromZIO(ZIO.die(ex)).map(identity)` | 1 |
| `fromZIO(ZIO.die(ex))` typed `ZQuery[Any, String, _]` then `.mapError(identity)` | 2 |
| same, then `.either` | 2 |
| same, then `.orDie` | 2 |

The defect is still raised by a `fromZIO` effect, but `mapError`, `either`, `catchAll` and the other combinators that go
through `Result.failExit` turn a cause with no typed failure into a failed step, which `memoize` does not store
(same root cause as bug 6). So "a defect raised by an effect lifted with `ZQuery.fromZIO`" holds only while the
defect is not passed through such a combinator. The sentence "dies in other ways, for example through `ZQuery.die`
or `orDie`" does not exclude `mapError`/`either`, so a reader of the doc could expect them to be memoized.

Replacement (checked against the table):

```
   * Returns an effect that, when evaluated, returns a memoized version of this
   * query: the first use of the memoized query computes its result, and later
   * uses reuse it, including a typed failure, and a defect raised by an effect
   * lifted with `ZQuery.fromZIO` when no combinator that handles failures, such
   * as `mapError`, `either` or `orDie`, is applied to it afterwards. A query
   * that dies in other ways, for example through `ZQuery.die`, is not memoized
   * and each use runs it again: this is a known bug. Creating the memoized query
   * does not run anything.
```

Also add `mapError`, `either` and `catchAll` over a defect to the reproducer note of bug 6 (its summary already says
"or another combinator that turns a cause without typed failures into a failed step", so only the example list changes).

## Confirmed

1. Finding resolution. I1, I2 (header and `zipWith`), I3 (`timeout`), I4 (`memoize`, see M1 for the residue), M1
   (header, `run`, `fromRequest`), M2, M3 (`catchAll`, `catchAllZIO`, `either`), M4 (`fold`), M5, M6, M7, M8 and the
   eight `@see` trailing-slash URLs are all applied. Additions beyond the findings (`mapError` paragraph, `mapBoth`
   sentence, `run` wording) are the M3 follow-up (mixed typed failure plus defect) and the M1 wording applied to
   `run`; they stay inside the ruling's subject. The diff touches comments only.
2. Claims probed on 2.13.18 (`probe2/Main.scala`, `probe/Main.scala`):
   - Pipelining: `n *> n` and `foreach` give one `runAll`; `(n <&> p) *> n` gives `NM runAll([1])`, `PM`, `NM runAll([2])`;
     `flatMap`, `catchAll`, `foldQuery`, `optional`, `sandbox`, `timeout` before `*> n` each give two `runAll` calls;
     `either` and `mapZIO` keep `runAll([1] [2])` (so neither is wrongly listed). `fromFunctionBatched`,
     `fromFunctionZIO` and `fromFunctionBatchedZIO` call their function once per request or batch (`fn(1) | fn(2)`);
     all `fromFunction*` constructors build a `DataSource.Batched` (DataSource.scala 326-534).
   - `foreachBatched` over one request to each of two 300 ms data sources: 307-310 ms, so "calls to different data
     sources in the same round still run in parallel" is true.
   - `mapError` with a typed failure plus a defect: drops the defect (`mixedQ`, `fromZIONow(Exit.failCause(fail && die))`,
     and a failure after a request `flatMap`); keeps it when the cause is the result of a request (also after
     `.map(identity)`); ZIO's `mapError` keeps it. `mapBoth`, `catchAll`, `catchAllZIO`, `either`, `fold` match ZIO
     (ZIO baseline built with `ZIO.failCause(Cause.fail && Cause.die)`, because `ZIO.fail <&> ZIO.die` is
     racy: it sometimes loses the typed failure). `fold` over a defect-only query fails with the defect.
   - `asExitMaybe`: `fromZIONow(Exit.die)` None, `fromZIONow(Exit.fail)` Some, `fromZIONow(Exit.succeed)` Some,
     `failCauseNow(die)` None, `failCauseNow(fail && die)` Some, interrupt-only `fromZIONow` None, `failNow` Some,
     `succeed` None, `succeedNow(1).map` Some.
   - `acquireReleaseExitWith`: with `timeout` interrupting the use query and a later `ZQuery.fail("later")`, the release
     receives `Failure` with an empty cause (typed failure removed); with a later `ZQuery.die` or a `fail && die`
     cause it receives the defect only; with a later success it does not run. Direct failures of the use query
     reach the release with the full cause (outside the new sentence, which says "if the query is interrupted").
   - `currentScope`: a finalizer added with `addFinalizerExit` runs with `Success` on a successful run and
     with `Failure` on a failed run; `acquireReleaseWith` finalizers match `Exit.Failure` only (ZQuery.scala 2385-2392).
   - `memoize`: `fromZIO` fail 1, `fromZIO` die 1, `fromZIO(fail && die)` 1, `fromZIO(...) *> ZQuery.fail` 1,
     `fromZIO(...) *> ZQuery.die` 2, `orDie` 2 (typed failure or defect source).
   - `timeout`: the author's 10-run table (5/4/1) and the reviewer's (1/8/1) both show the outcome varies, which
     is what the new wording ("does not reliably do so", "can go on") says; the in-step case is test 245-251.
3. Bug 6 re-run on 2.13.18 and 3.3.9: identical output on both (`computing` x2 for `orDie`, x2 for `ZQuery.die`,
   x1 for `ZIO.die` inside `fromZIO`). Base 4e04bed1b citations: `memoize` 1749-1755 (`ref.modifyZIO` at 1751),
   `die` 1231-1232, `catchAll` fold at 209, `FailCauseNone` 1961-1962, `orDieWith` 533-534 (`catchAll(e => ZQuery.die(..))`),
   `Result.failExit` 106-108: all correct. Test gap correct (`ZQuerySpec.scala` memoize suite 338-374 uses only
   `fromZIO` queries; no `ZQuery.die`/`orDie` memoize test). Real bug: test "defects are memoized" (362-373) shows
   the intent.
4. Bug 7 re-run on 2.13.18 and 3.3.9: identical output
   (`ZQuery#mapError: failures=List(error!) defects=List()` vs `ZIO#mapError: ... defects=List(defect)`).
   Base citations: `mapError` 471-478, line 475, `Continue.Get#mapError` = `io.mapError(f)`
   (Continue.scala 324-325), `Continue.Effect#mapError` = `query.mapError(f)` (225-226), `Result.failExit`
   106-108: correct. Test gap correct (only `mapError(identity)` batching test at 20-28). Real bug: ZIO's own
   `mapError` keeps the defect on the same cause (probe `p9`, `probe`), and `ZQuery#mapErrorCause` and the
   request path keep it, so the behaviour is inconsistent inside ZQuery.
5. Checks. `comments-only.py 4e04bed1b --all`: `files=28 code=0 not_allowed=12 directives=0` (the 12 not_allowed are
   ledger files). `snippets-scala-cli.py . --version 0.7.9 --base 4e04bed1b`: 16 examples in 5 files, PASS on 2.12.21,
   2.13.18 and 3.3.9. `git status`: only the two review findings files untracked (before this file).
6. Rules: no new `all`/`every`/`never`/`always` in the new text; "only" and "in parallel" are verified above; no
   ledger references, no `^`, `<h1>`, `@throws`, `/*` or `$`; no new `[[ ]]` to an overloaded member. All eight `@see`
   URLs in `ZQuery.scala` (99, 780, 817, 897, 1291, 1626, 1715, 1828) end in `/`. The live HTTP check was not repeated:
   the network was flaky from here (curl returned 000 for two of the three pages), and the previous reviewer and the
   author both recorded 301 without the slash and 200 with it.
