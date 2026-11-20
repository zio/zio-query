# zio-query Scaladoc pilot (ZIO-205): plan

This folder is a working ledger. It is removed before the PR is opened.

## Problem

zio-query's public API is 78% documented (218 of 279 members, by the zio-blocks coverage script), but nobody
has checked whether the existing comments are true. Two are known to be wrong (`DataSource.contramap*`,
`fromFunctionBatchedWith`), and 2 of the 5 `{{{ }}}` examples do not compile (`effect.access(...)` in
`ZQuery.environmentWith` and `ZQuery.serviceWith`: there is no `effect` and no `access`).

## Goal

One comment-only PR on `series/2.x` that:
1. fact-checks every existing Scaladoc comment (268 `/**` blocks, internals included) against the code;
2. documents the 61 undocumented public members by tier;
3. gives each tier-1 entry point a `{{{ }}}` example that compiles on 2.12.21, 2.13.18 and 3.3.9.

## Facts from the survey (Phase 0)

- Base `origin/series/2.x` = 4e04bed1b. `git diff v0.7.9 HEAD -- zio-query/shared/src/main` is empty, so every
  probe can use the published `dev.zio::zio-query:0.7.9`.
- One cross-project (`zioQueryJVM/JS/Native`), all sources under `zio-query/shared/src/main`. Version folders
  `scala-2.12/`, `scala-2.13/`, `scala-3/` hold only `private[query] object UtilsVersionSpecific` (no Scaladoc).
- Scala versions: 2.12.21, 2.13.18, 3.3.9 (`build.sbt`). Scala 3 `doc` is switched off by zio-sbt
  (`Compile / doc / sources := Nil`), so only the 2.12 and 2.13 doc tools see the comments.
- scalafmt 3.11.5, `docstrings.style = Asterisk`, `wrapMaxColumn = 80`.
- CI: `sbt lint`, `+publishLocal` (runs `doc`), docs build. No test reads source text.
- Reference pages in `docs/` are newer than zio.dev. Live (HTTP 200) today: `data-source-reference`,
  `completed-request-map`, `aspects`, `creating-data-sources`, `creating-queries`, `error-handling`,
  `how-batching-works`, `running-queries`. `zquery`, `cache`, `request`, `described` and `query-failure`
  return 404 until the next docs publish, so `@see` links use only the live pages.

## Coverage inventory (before)

| File | Members | Documented |
|---|---:|---:|
| ZQuery.scala | 182 | 150 |
| DataSource.scala | 31 | 27 |
| CompletedRequestMap.scala | 24 | 16 |
| QueryAspect.scala | 9 | 8 |
| Cache.scala | 8 | 7 |
| DataSourceAspect.scala | 7 | 6 |
| package.scala | 5 | 0 |
| Described.scala | 4 | 1 |
| internal/BlockedRequest.scala | 4 | 0 |
| internal/QueryScope.scala | 3 | 1 |
| QueryFailure.scala, Request.scala | 2 | 2 |
| **Total** | **279** | **218 (78%)** |

The full member list is `inventory-before.tsv` (file:line, owner.member, DOC/UNDOC).

## Tiers

- **Tier 1** (overview, one compiled example, `@see` a live zio.dev page): `ZQuery` (type and the main
  constructors `fromRequest`, `foreachBatched`/`foreachPar`, `run`/`runCache`), `DataSource` (type and
  `fromFunctionBatched`/`make`-style constructors), `Request`, `CompletedRequestMap`, `Cache`.
- **Tier 2**: every other public member, 1 to 3 sentences.
- **Tier 3**: `internal` and `private[query]` code: one line or nothing; existing comments still fact-checked.

## Existing comments to fact-check

All 268 `/**` blocks in `zio-query/shared/src/main/scala/zio/query/**`. Known suspects:
`DataSource.contramap`, `contramapZIO`, `eitherWith`, `fromFunctionBatchedWith*`, `batchN` (identifier text),
the `ZQuery` header example's "two queries" claim, `memoize`, `environmentWith`/`serviceWith` examples,
and any "parallel", "cached", "deduplicated" or "at most once" claim.

## Steps

1. Ledger, plan and tooling (this commit). Scratch tools outside the repo, in `/tmp/claude-1000/query-scaladoc/tools/`:
   coverage (copied from zio-blocks), `list-members.py`, `comments-only.py`, `snippets-scala-cli.py` (new).
2. Two Opus writers in parallel, on disjoint files, scala-cli probes only (no sbt):
   - Task 1a: `ZQuery.scala`;
   - Task 1b: every other file.
3. Controller checks: comments-only, coverage, snippets on 3 versions.
4. Gates (controller, one sbt at a time, `-J-Xmx3g`): `sbt fmt`, `sbt lint`, `++2.12.21 zioQueryJVM/doc`,
   `++2.13.18 zioQueryJVM/doc`, `++3.3.9 zioQueryJVM/compile`.
5. Fresh-context Opus fact-check review of the whole diff; fix round; scoped Sonnet re-review.
6. Remove this folder, re-run comments-only `--all`, push to `khajavi`, open the PR, report `[PR]`.
