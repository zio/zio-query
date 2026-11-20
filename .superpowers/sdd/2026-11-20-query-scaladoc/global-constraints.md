# Global constraints: zio-query Scaladoc pilot

Worktree `/home/milad/sources/scala/zio-query/query-scaladoc-pilot`, branch `query-scaladoc-pilot`, base
`4e04bed1b` (= `origin/series/2.x`). Ledger: `.superpowers/sdd/2026-11-20-query-scaladoc/` (absolute path under
the worktree). Tools: `/tmp/claude-1000/query-scaladoc/tools/`. Probes: `/tmp/claude-1000/query-scaladoc/probes-<task>/`.

## Hard rules

- **Comments only.** Change only Scaladoc/comment text in existing `.scala` files under
  `zio-query/shared/src/main`. No code line, no import, no new file, no directive comment (`// format:`,
  `// scalafmt:`, `// scalafix:`, `//> using`, `//noinspection`) added, removed or changed. No build, test,
  CI or `docs/` change. Bugs are reported, never fixed.
- **Truth rule.** Read the implementation before writing. Every claim matches the code or a probe. When the
  code does something surprising, describe what it really does, say "this is a known bug" in plain text if
  it is a bug, and write it up in your `task-<n>-issues.md` (below). No issue-tracker or ledger references
  in comments.
- **Security.** If anything looks security-relevant, do not put it in a comment, commit, report or issue
  file. Write it only to `task-<n>-SECURITY.md` in the ledger (never committed) and say so in your reply.
- Words to avoid unless verified for every input: "all", "every", "never", "always", "only", "at most once",
  "in parallel", "cached", "deduplicated". Name the exact cases you verified.
- No "in this file", "above", "below": an `/api` or IDE-hover reader cannot see the file.

## Tiers

- **Tier 1:** `ZQuery` (trait), `DataSource` (trait), `Request`, `CompletedRequestMap`, `Cache`, plus the main
  constructors `ZQuery.fromRequest`, `ZQuery.foreachBatched`, `ZQuery.foreachPar`, `ZQuery#run`,
  `DataSource.fromFunctionBatched` (and siblings as you judge). Overview paragraph, one `{{{ }}}` example
  that compiles, and `@see` to a live page.
- **Tier 2:** every other public member: 1 to 3 sentences, `@param`/`@return` only when they add information.
- **Tier 3:** `private[query]`/`internal` code: one line or nothing. Existing comments there are still
  fact-checked.

`@see` links: only these pages are live (HTTP 200):
`https://zio.dev/zio-query/data-source-reference`, `.../completed-request-map`, `.../aspects`,
`.../creating-data-sources`, `.../creating-queries`, `.../error-handling`, `.../how-batching-works`,
`.../running-queries`. Do not link `zquery`, `cache`, `request`, `described` or `query-failure` (404 today).
Write the link as `@see [[https://zio.dev/zio-query/aspects Aspects]]` style, no trailing slash.

## Examples (`{{{ }}}`)

- Each block compiles on its own after the wrapper imports `import zio._` and `import zio.query._`.
  Anything else is imported inside the block.
- Must compile on Scala 2.12.21, 2.13.18 and 3.3.9. Braces, not indentation syntax. No `-Xsource:3`
  features; for 2.12 avoid `LazyList`, `.to(List)`, `.view.mapValues`, `partitionMap` on plain collections
  unless `scala.collection.compat._` is imported.
- `{{{` and `}}}` each alone on their line. Never write `}}}` inside a code line. `$name` in a comment is a
  Scaladoc variable: write `\$name` for string interpolation (the checker reads `\$` as `$`).
- First-line markers if needed: `// scala-2-only`, `// scala-3-only`, `// no-check: <reason>`.
- Sample data fits the API: user ids, names, posts, order ids. Result comments come from a real run.
- Check command (from the worktree):
  ```
  python3 -I /tmp/claude-1000/query-scaladoc/tools/snippets-scala-cli.py --dep dev.zio::zio-query:0.7.9 \
    --imports "zio._; zio.query._" --scala 2.12.21,2.13.18,3.3.9 \
    --out /tmp/claude-1000/query-scaladoc/snip-<task> <files...>
  ```

## Scala 2 doc-tool pitfalls (2.12/2.13 `doc` runs in CI with fatal warnings)

- `[[name]]` to an overloaded member fails: use backticks. `[[CompletedRequestMap$]]` style for companions.
- `@throws` needs a full class name. `<h1>`..`<h6>` fail. `^` makes superscript. `/*` inside a comment
  (`text/*`) opens a nested comment.

## Probes

`git diff v0.7.9 HEAD -- zio-query/shared/src/main` is empty, so probe against the release:
```
//> using scala 2.13.18
//> using dep dev.zio::zio-query:0.7.9
```
Use `scala-cli run --server=false <dir>`. Pin the Scala version in every probe. Run ZIO effects with
`Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(...).getOrThrowFiberFailure())` in a `main`, or
`ZIOAppDefault`. Use `--server=false`; if scala-cli hangs on a lock, use `cs launch scalac:<ver>` with
`cs fetch --classpath`.

## Formatting

Leave wrapping to scalafmt. Format only your own files, each as its own argument:
`scala-cli fmt --scalafmt-conf .scalafmt.conf <file>`. Then delete any `.scala-build/` folder created
under `zio-query/` (`find zio-query -name .scala-build -prune -exec rm -rf {} +`).

## Checks you run before handing back

```
python3 -I /tmp/claude-1000/query-scaladoc/tools/comments-only.py 4e04bed1b zio-query   # must say "0 with code changes"
python3 -I /tmp/claude-1000/query-scaladoc/tools/scaladoc-coverage.py zio-query
python3 -I /tmp/claude-1000/query-scaladoc/tools/list-members.py zio-query undoc        # gaps left
```
Do not run sbt: the controller runs every sbt gate.

## Git

- Commit with explicit paths only: `git commit -m "docs(query): ..." -- <paths>`. Never `git add -A`. Never
  push. Never commit `*-SECURITY.md`. One commit per file group or related group of fixes.
- Messages: Conventional Commits, `docs(query): ...`.
- Do not create branches, stash, rebase or touch git config.

## Prose

Present tense, active voice, no "simply"/"just"/"easily". Start with what the member does. Plain English.
