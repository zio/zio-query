/*
 * Copyright 2019-2023 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.query

import zio._
import zio.query.ZQuery.disabledCache
import zio.query.internal._

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.switch
import scala.collection.compat.{BuildFrom => _, _}
import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

/**
 * A `ZQuery[R, E, A]` is a purely functional description of an effectual query
 * that may contain requests from one or more data sources, requires an
 * environment `R`, and may fail with an `E` or succeed with an `A`.
 *
 * Building a query does not run anything: `run`, `runCache` and `runLog` turn
 * it into a `ZIO` effect. Requests enter a query through `ZQuery.fromRequest`
 * or one of its variants, and ZIO Query looks at how queries are combined to
 * decide how to send those requests to their data sources:
 *
 *   - Requests combined with `zipWithPar`, `zipWithBatched` or combinators
 *     derived from them (`zipPar`, `<&>`, `foreachPar`, `foreachBatched`, ...)
 *     are batched: each data source receives them together, in one batch.
 *   - Requests combined with `zipWith` or combinators derived from it (`zip`,
 *     `*>`, `foreach`, ...) are pipelined when the requests of both sides go to
 *     the same single data source: it receives them in one call to `runAll`, as
 *     batches that must be performed one after another. A data source built
 *     with one of the `DataSource.fromFunction` constructors, which are
 *     `DataSource.Batched`, still calls its function once for each batch. A
 *     `flatMap` after the requests of the left side ends the pipeline, and so
 *     do `catchAll`, `foldQuery`, `optional`, `sandbox`, `timeout` and other
 *     combinators that continue with a query.
 *   - While caching is enabled, which is the default, a request equal to one
 *     already made in the same run is not sent again, and its result is reused.
 *
 * This allows for writing queries in a high level, compositional style, with
 * confidence that they will be optimized. For example, the following query
 * fetches all user ids, then the name of each user:
 *
 * {{{
 * import zio._
 *
 * case object GetAllUserIds extends Request[Nothing, List[Int]]
 * final case class GetUserName(id: Int) extends Request[Nothing, String]
 *
 * val userIds: DataSource[Any, GetAllUserIds.type] =
 *   DataSource.fromFunction("UserIds")((_: GetAllUserIds.type) => List(1, 2, 3))
 *
 * val userNames: DataSource[Any, GetUserName] =
 *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
 *     requests.map(request => s"user-\${request.id}")
 *   }
 *
 * val getAllUserIds: UQuery[List[Int]] =
 *   ZQuery.fromRequest(GetAllUserIds)(userIds)
 *
 * def getUserName(id: Int): UQuery[String] =
 *   ZQuery.fromRequest(GetUserName(id))(userNames)
 *
 * val query: UQuery[List[String]] =
 *   for {
 *     ids   <- getAllUserIds
 *     names <- ZQuery.foreachPar(ids)(getUserName)
 *   } yield names
 * // query.run succeeds with List(user-1, user-2, user-3)
 * }}}
 *
 * Written with plain effects, this would take N + 1 calls: one for the ids and
 * one for each name. Run as a query, it takes two calls: `UserIds` receives
 * `GetAllUserIds`, then `UserNames` receives the three `GetUserName` requests
 * in one batch. What the data source does with a batch is up to it:
 * `DataSource.fromFunctionBatched` passes the whole batch to one function call,
 * while `DataSource.fromFunction` calls its function once for each request.
 *
 * Based on "There is no Fork: an Abstraction for Efficient, Concurrent, and
 * Concise Data Access" by Simon Marlow, Louis Brandy, Jonathan Coens, and Jon
 * Purdy. [[http://simonmar.github.io/bib/papers/haxl-icfp14.pdf]]
 *
 * @see
 *   [[https://zio.dev/zio-query/how-batching-works How Batching Works]]
 * @tparam R
 *   the environment the query requires
 * @tparam E
 *   the type of error the query may fail with
 * @tparam A
 *   the type of value the query succeeds with
 */
final class ZQuery[-R, +E, +A] private (private val step: ZIO[R, Nothing, Result[R, E, A]]) { self =>

  /**
   * Syntax for adding aspects.
   */
  def @@[LowerR <: UpperR, UpperR <: R, LowerE >: E, UpperE >: LowerE, LowerA >: A, UpperA >: LowerA](
    aspect: => QueryAspect[LowerR, UpperR, LowerE, UpperE, LowerA, UpperA]
  )(implicit trace: Trace): ZQuery[UpperR, LowerE, LowerA] =
    ZQuery.suspend(aspect(self))

  /**
   * A symbolic alias for `zipParRight`.
   */
  def &>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    zipParRight(that)

  /**
   * A symbolic alias for `zipParLeft`.
   */
  def <&[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipParLeft(that)

  /**
   * A symbolic alias for `zipPar`.
   */
  def <&>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipPar(that)

  /**
   * A symbolic alias for `zipRight`.
   */
  def *>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    zipRight(that)

  /**
   * A symbolic alias for `zipLeft`.
   */
  def <*[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipLeft(that)

  /**
   * A symbolic alias for `zip`.
   */
  def <*>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zip(that)

  /**
   * A symbolic alias for `zipBatchedRight`.
   */
  def ~>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    zipBatchedRight(that)

  /**
   * A symbolic alias for `zipBatchedLeft`.
   */
  def <~[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipBatchedLeft(that)

  /**
   * A symbolic alias for `zipBatched`.
   */
  def <~>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipBatched(that)

  /**
   * Returns a query which submerges the error case of `Either` into the error
   * channel of the query
   *
   * The inverse of [[ZQuery.either]]
   */
  def absolve[E1 >: E, B](implicit ev: A IsSubtypeOfOutput Either[E1, B], trace: Trace): ZQuery[R, E1, B] =
    self.flatMap {
      ev(_) match {
        case Right(b) => ZQuery.succeedNow(b)
        case Left(e)  => ZQuery.failNow(e)
      }
    }

  /**
   * Maps the success value of this query to the specified constant value.
   */
  def as[B](b: => B)(implicit trace: Trace): ZQuery[R, E, B] =
    map(_ => b)

  /**
   * Extracts the result of this query as an `Exit` without running it, if the
   * result is already known and computing it has no side effects. Returns
   * `None` otherwise.
   *
   * The result is known for queries built with [[ZQuery.succeedNow]],
   * `ZQuery.failNow`, `ZQuery.failCauseNow` with a cause that contains a typed
   * failure, and [[ZQuery.fromZIONow]] applied to an [[zio.Exit]] that succeeds
   * or holds a typed failure, and for `map` of such a query. It is not known
   * for lazy constructors such as `ZQuery.succeed`, for `ZQuery.failCauseNow`
   * or `ZQuery.fromZIONow` with a cause that holds no typed failure, or for
   * queries that make requests.
   */
  def asExitMaybe: Option[Exit[E, A]] =
    Option(asExitOrElse(null))

  /**
   * Like `asExitMaybe`, but returns `default` instead of `None` when the result
   * of this query is not known without running it.
   */
  def asExitOrElse[E1 >: E, A1 >: A](default: Exit[E1, A1]): Exit[E1, A1] =
    self.step match {
      case Exit.Success(Result.Done(a: A))        => Exit.succeed(a)
      case Exit.Success(Result.Fail(e: Cause[E])) => Exit.failCause(e)
      case _                                      => default
    }

  /**
   * Lifts the error channel into a `Some` value for composition with other
   * optional queries
   */
  def asSomeError(implicit trace: Trace): ZQuery[R, Option[E], A] =
    mapError(Some(_))

  /**
   * Enables caching for this query. Note that caching is enabled by default so
   * this will only be effective to enable caching in part of a larger query in
   * which caching has been disabled.
   *
   * @see
   *   [[memoize]] for memoizing the result of a single query
   */
  def cached(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.unwrap(ZQuery.disabledCache.get.map {
      case None => self
      case s    =>
        val acq = ZQuery.disabledCache.set(None) *> ZQuery.currentCache.set(s)
        val rel = ZQuery.disabledCache.set(s) *> ZQuery.currentCache.set(None)
        ZQuery.acquireReleaseWith(acq)(_ => rel)(_ => self)
    })

  /**
   * Recovers from all typed failures of this query with the query returned by
   * `h`. Defects are not caught: use `catchAllCause` to recover from them. When
   * the cause holds both a typed failure and a defect, `h` receives the typed
   * failure and the defect is dropped, as with `ZIO#catchAll`.
   */
  def catchAll[R1 <: R, E2, A1 >: A](
    h: E => ZQuery[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R1, E2, A1] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.catchAll(h))
        case Result.Fail(e)        => e.failureOrCause.fold(h, ZQuery.FailCauseNone).step
        case done                  => Exit.succeed(done.widen)
      }
    }

  /**
   * Recovers from typed failures and defects of this query, passing the full
   * `Cause` to `h`.
   */
  def catchAllCause[R1 <: R, E2, A1 >: A](h: Cause[E] => ZQuery[R1, E2, A1])(implicit
    trace: Trace
  ): ZQuery[R1, E2, A1] =
    ZQuery {
      step.foldCauseZIO(
        h(_).step,
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.catchAllCause(h))
          case Result.Fail(e)        => h(e).step
          case done                  => Exit.succeed(done.widen)
        }
      )
    }

  /**
   * Recovers from typed failures and defects of this query, passing the full
   * `Cause` to `h`, which returns a `ZIO` effect.
   */
  def catchAllCauseZIO[R1 <: R, E2, A1 >: A](h: Cause[E] => ZIO[R1, E2, A1])(implicit
    trace: Trace
  ): ZQuery[R1, E2, A1] =
    ZQuery {
      step.foldCauseZIO(
        h(_).foldCauseZIO(Result.failExit, Result.doneExit),
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.foldCauseZIO(h, Exit.succeed))
          case Result.Fail(e)        => h(e).foldCauseZIO(Result.failExit, Result.doneExit)
          case done                  => Exit.succeed(done.widen)
        }
      )
    }

  /**
   * Recovers from all typed failures of this query with the `ZIO` effect
   * returned by `h`. Defects are not caught. When the cause holds both a typed
   * failure and a defect, `h` receives the typed failure and the defect is
   * dropped.
   */
  def catchAllZIO[R1 <: R, E2, A1 >: A](
    h: E => ZIO[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R1, E2, A1] =
    catchAllCauseZIO(_.failureOrCause.fold(h, Exit.failCause))

  /**
   * Returns a query whose failure and success have been lifted into an
   * `Either`. The resulting query has no typed failures, because the failure
   * case has been exposed as part of the `Either` success case. Defects still
   * fail it, unless the cause also holds a typed failure, which then becomes
   * the `Left` while the defect is dropped.
   */
  def either(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, Either[E, A]] =
    fold(Left(_), Right(_))

  /**
   * Ensures that if this query starts executing, the specified `ZIO` effect
   * runs after this query completes, whether by success or failure.
   *
   * If this query is interrupted, the finalizer runs when the whole run ends
   * with a failure or an interruption. When `timeout` or `race` interrupts this
   * query and the run still succeeds, the finalizer does not run: this is a
   * known bug.
   */
  def ensuring[R1 <: R](finalizer: => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZQuery[R1, E, A] =
    ZQuery.acquireReleaseWith(ZIO.unit)(_ => finalizer)(_ => self)

  /**
   * Returns a query that models execution of this query, followed by passing
   * its result to the specified function that returns a query. Requests
   * composed with `flatMap` or combinators derived from it will be executed
   * sequentially and will not be pipelined, though deduplication and caching of
   * requests may still be applied.
   */
  def flatMap[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.mapQuery(f))
        case Result.Done(a)        => f(a).step
        case e                     => Exit.succeed(e.widen)
      }
    }

  /**
   * Returns a query that performs the outer query first, followed by the inner
   * query, yielding the value of the inner query.
   *
   * This method can be used to "flatten" nested queries.
   */
  def flatten[R1 <: R, E1 >: E, B](implicit
    ev: A IsSubtypeOfOutput ZQuery[R1, E1, B],
    trace: Trace
  ): ZQuery[R1, E1, B] =
    flatMap(ev)

  /**
   * Folds over the failed or successful result of this query to yield a query
   * that has no typed failures, but succeeds with the value returned by the
   * left or right function passed to `fold`. Defects still fail it, unless the
   * cause also holds a typed failure, which is then folded while the defect is
   * dropped.
   */
  def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZQuery[R, Nothing, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.fold(failure, success))
        case Result.Done(a)        => Result.doneExit(success(a))
        case Result.Fail(e)        => e.failureOrCause.fold(e => Result.doneExit(failure(e)), Exit.failCause)
      }
    }

  /**
   * A more powerful version of `foldQuery` that allows recovering from any type
   * of failure except interruptions.
   */
  def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.foldCauseZIO(
        failure(_).step,
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.foldCauseQuery(failure, success))
          case Result.Done(a)        => success(a).step
          case Result.Fail(e)        => failure(e).step
        }
      )
    }

  /**
   * A more powerful version of `foldZIO` that allows recovering from any type
   * of failure except interruptions.
   */
  def foldCauseZIO[R1 <: R, E1, B](
    failure: Cause[E] => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.foldCauseZIO(
        failure(_).foldCauseZIO(Result.failExit, Result.doneExit),
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.foldCauseZIO(failure, success))
          case Result.Done(a)        => success(a).foldCauseZIO(Result.failExit, Result.doneExit)
          case Result.Fail(e)        => failure(e).foldCauseZIO(Result.failExit, Result.doneExit)
        }
      )
    }

  /**
   * Recovers from errors by accepting one query to execute for the case of an
   * error, and one query to execute for the case of success.
   */
  def foldQuery[R1 <: R, E1, B](failure: E => ZQuery[R1, E1, B], success: A => ZQuery[R1, E1, B])(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.foldQuery(failure, success))
        case Result.Done(a)        => success(a).step
        case Result.Fail(e)        => e.failureOrCause.fold(failure, ZQuery.FailCauseNone).step
      }
    }

  /**
   * Recovers from errors by accepting one ZIO effect to execute for the case of
   * an error, and one ZIO effect to execute for the case of success.
   */
  def foldZIO[R1 <: R, E1, B](
    failure: E => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.foldZIO(failure, success))
        case Result.Done(a)        => success(a).foldCauseZIO(Result.failExit, Result.doneExit)
        case Result.Fail(e)        =>
          e.failureOrCause.fold(failure(_).foldCauseZIO(Result.failExit, Result.doneExit), Exit.failCause)
      }
    }

  /**
   * "Zooms in" on the value in the `Left` side of an `Either`, moving the
   * possibility that the value is a `Right` to the error channel.
   */
  def left[B, C](implicit
    ev: A IsSubtypeOfOutput Either[B, C],
    trace: Trace
  ): ZQuery[R, Either[E, C], B] =
    self.foldQuery(
      e => ZQuery.failNow(Left(e)),
      a => ev(a).fold(b => ZQuery.succeedNow(b), c => ZQuery.failNow(Right(c)))
    )

  /**
   * Returns an effect that, when evaluated, returns a memoized version of this
   * query: the first use of the memoized query computes its result, and later
   * uses reuse it, including a typed failure or a defect raised by an effect
   * lifted with `ZQuery.fromZIO`. A query that dies in other ways, for example
   * through `ZQuery.die` or `orDie`, is not memoized and each use runs it
   * again: this is a known bug. Creating the memoized query does not run
   * anything.
   *
   * This differs from caching, which stores the results of requests to a
   * [[DataSource]]. `memoize` also covers work that is not a request, such as
   * an effect lifted with `ZQuery.fromZIO`, which then runs once. It is useful
   * when one query value is used in several places:
   *
   * {{{
   * import zio._
   *
   * final case class Pair(x: UQuery[Int], y: UQuery[Int])
   *
   * val query: UQuery[Int] =
   *   ZQuery.fromZIO(Console.printLine("computing").orDie.as(10))
   *
   * // prints "computing" twice when both x and y run
   * val recomputed: Pair = Pair(query.map(_ + 1), query.map(_ + 2))
   *
   * // prints "computing" once when both x and y run
   * val memoized: UIO[Pair] =
   *   query.memoize.map(q => Pair(q.map(_ + 1), q.map(_ + 2)))
   * }}}
   *
   * The memoized value is the first step of the query. For a query that makes
   * requests to a data source, that step is "waiting for these requests", so
   * each use sends the requests again, even with caching enabled, and reruns
   * the work after them: this is a known bug.
   */
  def memoize(implicit trace: Trace): UIO[ZQuery[R, E, A]] =
    ZIO.succeed(ZQuery.unsafe.memoize(self)(Unsafe, trace))

  /**
   * Maps the specified function over the successful result of this query.
   */
  def map[B](f: A => B)(implicit trace: Trace): ZQuery[R, E, B] =
    ZQuery(step.map(_.map(f)))

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`. When the cause holds both a typed
   * failure and a defect, the defect is dropped, as with `ZIO#mapBoth`.
   */
  def mapBoth[E1, B](f: E => E1, g: A => B)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, B] =
    mapBothCause(_.failureOrCause.fold(c => Cause.fail(f(c)), ZIO.identityFn), g)

  /**
   * Returns a query whose failure cause and success channels have been mapped
   * by the specified pair of functions, `f` and `g`.
   */
  def mapBothCause[E1, B](f: Cause[E] => Cause[E1], g: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZQuery[R, E1, B] =
    ZQuery {
      step.foldCauseZIO(
        e => Result.failExit(f(e)),
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.mapBothCause(f, g))
          case Result.Done(a)        => Result.doneExit(g(a))
          case Result.Fail(e)        => Result.failExit(f(e))
        }
      )
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  def mapDataSources[R1 <: R](f: => DataSourceAspect[R1])(implicit trace: Trace): ZQuery[R1, E, A] =
    ZQuery(step.map(_.mapDataSources(f)))

  /**
   * Maps the specified function over the failed result of this query.
   *
   * When the cause holds both a typed failure and a defect, the result fails
   * with the mapped typed failure only, and the defect is dropped, unless the
   * cause is the result of a request, which keeps the defect as `ZIO#mapError`
   * does. Dropping the defect is a known bug.
   */
  def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, A] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.mapError(f))
        case Result.Fail(e)        => e.failureOrCause.fold(e => Result.failExit(Cause.fail(f(e))), Exit.failCause)
        case done                  => Exit.succeed(done.widen)
      }
    }

  /**
   * Returns a query with its full cause of failure mapped using the specified
   * function. This can be used to transform errors while preserving the
   * original structure of `Cause`.
   */
  def mapErrorCause[E2](h: Cause[E] => Cause[E2])(implicit trace: Trace): ZQuery[R, E2, A] =
    ZQuery {
      step.foldCauseZIO(
        e => Result.failExit(h(e)),
        {
          case Result.Blocked(br, c) => Result.blockedExit(br, c.mapErrorCause(h))
          case Result.Fail(e)        => Result.failExit(h(e))
          case done                  => Exit.succeed(done.widen)
        }
      )
    }

  /**
   * Maps the specified effectual function over the result of this query.
   */
  def mapZIO[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.mapZIO(f))
        case Result.Done(a)        => f(a).foldCauseZIO(Result.failExit, Result.doneExit)
        case f                     => Exit.succeed(f.widen)
      }
    }

  /**
   * Converts this query to one that returns `None` when it dies only because a
   * data source did not return a result for a request (a `QueryFailure`), and
   * `Some` of the result when it succeeds. Typed failures and other defects
   * still fail the query, without the `QueryFailure` defects.
   */
  def optional(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    foldCauseQuery(
      _.stripSomeDefects { case _: QueryFailure => () }.fold[ZQuery[R, E, Option[A]]](ZQuery.none)(ZQuery.failCause(_)),
      ZQuery.some(_)
    )

  /**
   * Converts this query to one that dies with its error, instead of failing,
   * when the error type is a `Throwable`.
   */
  def orDie(implicit
    ev1: E IsSubtypeOfError Throwable,
    ev2: CanFail[E],
    trace: Trace
  ): ZQuery[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Converts this query to one that dies instead of failing, using the
   * specified function to map the error to a `Throwable`.
   */
  def orDieWith(f: E => Throwable)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, A] =
    catchAll(e => ZQuery.die(f(e)))

  /**
   * Provides this query with its required environment, built from the specified
   * layer, so that it then requires the layer's input `R0`.
   *
   * The layer is built when the query starts. Its scope closes when the first
   * step of the query completes, which for a query that makes requests is
   * before the data sources run: the data sources receive the services, but a
   * scoped layer has already released them. This is a known bug.
   */
  def provideLayer[E1 >: E, R0](
    layer: => Described[ZLayer[R0, E1, R]]
  )(implicit trace: Trace): ZQuery[R0, E1, A] =
    ZQuery {
      ZIO.scoped[R0] {
        layer.value.build.exit.flatMap {
          case Exit.Failure(e) => Result.failExit(e)
          case Exit.Success(r) => self.provideEnvironment(Described(r, layer.description)).step
        }
      }
    }

  /**
   * Provides this query with its required environment.
   */
  def provideEnvironment(
    r: => Described[ZEnvironment[R]]
  )(implicit trace: Trace): ZQuery[Any, E, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Splits the environment into two parts, providing one part using the
   * specified layer and leaving the remainder `R0`. The layer's scope closes as
   * described for `provideLayer`.
   */
  def provideSomeLayer[R0]: ZQuery.ProvideSomeLayer[R0, R, E, A] =
    new ZQuery.ProvideSomeLayer(self)

  /**
   * Provides this query with part of its required environment.
   */
  def provideSomeEnvironment[R0](
    f: => Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): ZQuery[R0, E, A] =
    ZQuery(step.map(_.provideSomeEnvironment(f)).provideSomeEnvironment(f.value))

  /**
   * Races this query with the specified query, returning the result of the
   * first to succeed and interrupting the other. If one query fails, the race
   * waits for the other; if both fail, the cause combines both failures.
   *
   * The race applies to the steps of the two queries, not to their requests:
   * when one query stops to wait for data sources, its requests run to
   * completion before the race goes on. So a slow request can win over a fast
   * one, and a query that makes requests can win over one that does not. This
   * is a known bug.
   */
  def race[R1 <: R, E1 >: E, A1 >: A](
    that: => ZQuery[R1, E1, A1]
  )(implicit trace: Trace): ZQuery[R1, E1, A1] = {

    def coordinate(
      exit: Exit[Nothing, Result[R1, E1, A1]],
      fiber: Fiber[Nothing, Result[R1, E1, A1]]
    ): ZIO[R1, Nothing, Result[R1, E1, A1]] =
      exit.foldExitZIO(
        cause => fiber.join.map(_.mapErrorCause(_ && cause)),
        {
          case Result.Blocked(blockedRequests, continue) =>
            continue match {
              case Continue.Effect(query) =>
                Result.blockedExit(blockedRequests, Continue.effect(race(query, fiber)))
              case Continue.Get(io) =>
                Result.blockedExit(blockedRequests, Continue.effect(race(ZQuery.fromZIONow(io), fiber)))
            }
          case Result.Fail(cause) => fiber.join.map(_.mapErrorCause(_ && cause))
          case done               => fiber.interrupt.as(done)
        }
      )

    def race(
      query: ZQuery[R1, E1, A1],
      fiber: Fiber[Nothing, Result[R1, E1, A1]]
    ): ZQuery[R1, E1, A1] =
      ZQuery(query.step.raceWith(fiber.join)(coordinate, coordinate))

    ZQuery(self.step.raceWith(that.step)(coordinate, coordinate))
  }

  /**
   * Keeps some of the errors, and terminates the query with the rest
   */
  def refineOrDie[E1](
    pf: PartialFunction[E, E1]
  )(implicit ev1: E IsSubtypeOfError Throwable, ev2: CanFail[E], trace: Trace): ZQuery[R, E1, A] =
    refineOrDieWith(pf)(ev1)

  /**
   * Keeps some of the errors, and terminates the query with the rest, using the
   * specified function to convert the `E` into a `Throwable`.
   */
  def refineOrDieWith[E1](pf: PartialFunction[E, E1])(
    f: E => Throwable
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, A] =
    self catchAll (err => (pf lift err).fold[ZQuery[R, E1, A]](ZQuery.die(f(err)))(ZQuery.failNow))

  /**
   * "Zooms in" on the value in the `Right` side of an `Either`, moving the
   * possibility that the value is a `Left` to the error channel.
   */
  def right[B, C](implicit
    ev: A IsSubtypeOfOutput Either[B, C],
    trace: Trace
  ): ZQuery[R, Either[B, E], C] =
    self.foldQuery(
      e => ZQuery.failNow(Right(e)),
      a => ev(a).fold(b => ZQuery.failNow(Left(b)), ZQuery.succeedNow)
    )

  private[this] def runToZIO(implicit trace: Trace): ZIO[R, E, A] = {
    def run(query: ZQuery[R, E, A]): ZIO[R, E, A] =
      query.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) => br.run *> run(c)
        case Result.Blocked(br, Continue.Get(io))   => br.run *> io
        case Result.Done(a)                         => Exit.succeed(a)
        case Result.Fail(e)                         => Exit.failCause(e)
      }

    run(self)
  }

  /**
   * Returns an effect that models executing this query.
   *
   * Each evaluation of the effect starts with a new, empty [[Cache]]: while
   * caching is enabled, equal requests in one run are sent once, and the next
   * run sends them again. To share results between runs, use `runCache` with
   * the same cache.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val program: ZIO[Any, Nothing, (String, String)] =
   *   (getUserName(1) <&> getUserName(1)).run
   * // program succeeds with (user-1,user-1) after one call to UserNames, with
   * // the single request GetUserName(1); running program again calls
   * // UserNames again
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/running-queries Running Queries]]
   */
  def run(implicit trace: Trace): ZIO[R, E, A] =
    runCache(Cache.unsafeMake())

  /**
   * Returns an effect that models executing this query with the specified
   * cache.
   *
   * Requests whose results are already in the cache are not sent to their data
   * sources, and the results of new requests are added to it. Passing the same
   * cache to several runs shares results between them.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val program: ZIO[Any, Nothing, (String, String)] =
   *   for {
   *     cache  <- Cache.empty
   *     first  <- getUserName(1).runCache(cache)
   *     second <- getUserName(1).runCache(cache)
   *   } yield (first, second)
   * // program succeeds with (user-1,user-1) after one call to UserNames: the
   * // second run finds GetUserName(1) in the cache
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/running-queries Running Queries]]
   */
  def runCache(cache: => Cache)(implicit trace: Trace): ZIO[R, E, A] = {
    import ZQuery.{currentCache, currentScope}

    def resetRef[V <: AnyRef](
      fid: FiberId.Runtime,
      oldRefs: FiberRefs,
      newRefs: FiberRefs
    )(
      fiberRef: FiberRef[V]
    ): FiberRefs = {
      val oldValue = oldRefs.getOrNull(fiberRef)
      if (oldValue ne null) newRefs.updatedAs(fid)(fiberRef, oldValue) else newRefs.delete(fiberRef)
    }

    asExitOrElse(null) match {
      case null =>
        ZIO.uninterruptibleMask { restore =>
          ZIO.withFiberRuntime[R, E, A] { (state, _) =>
            // NOTE: Running a ZQuery requires up to 3 FiberRefs, which can be expensive to use `locally` with for simple queries.
            // Therefore, we handle them all together to avoid the added penalty of running `locally` 3 times
            val fid     = state.id
            val scope   = QueryScope.make()
            val oldRefs = state.getFiberRefs(false)
            val newRefs = {
              val refs = oldRefs.updatedAs(fid)(currentCache, Some(cache)).updatedAs(fid)(currentScope, scope)
              if (refs.getOrNull(disabledCache) ne null)
                refs.delete(disabledCache)
              else refs
            }
            state.setFiberRefs(newRefs)
            restore(runToZIO).exitWith { exit =>
              var curRefs = state.getFiberRefs(false)
              if (curRefs eq newRefs) {
                // Cheap and common: FiberRefs were not modified during the execution so we just replace them with the old ones
                state.setFiberRefs(oldRefs)
              } else {
                // FiberRefs were modified so we need to manually revert each one
                curRefs = resetRef(fid, oldRefs, curRefs)(currentCache)
                curRefs = resetRef(fid, oldRefs, curRefs)(currentScope)
                curRefs = resetRef(fid, oldRefs, curRefs)(disabledCache)
                state.setFiberRefs(curRefs)
              }
              scope.closeAndExitWith(exit)
            }
          }
        }
      case exit => exit
    }
  }

  /**
   * Returns an effect that models executing this query with a new, empty cache,
   * returning the cache along with the query result. The cache holds the
   * results of the requests the query made, and can be passed to `runCache` to
   * reuse them.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val program: ZIO[Any, Nothing, String] =
   *   ZQuery.foreachPar(List(1, 2))(getUserName).runLog.flatMap { case (cache, _) =>
   *     getUserName(2).runCache(cache)
   *   }
   * // program succeeds with user-2 after one call to UserNames, with the batch
   * // GetUserName(1), GetUserName(2)
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/running-queries Running Queries]]
   */
  def runLog(implicit trace: Trace): ZIO[R, E, (Cache, A)] =
    for {
      cache <- Cache.empty
      a     <- runCache(cache)
    } yield (cache, a)

  /**
   * Expose the full cause of failure of this query
   */
  def sandbox(implicit trace: Trace): ZQuery[R, Cause[E], A] =
    catchAllCause(ZQuery.failNow)

  /**
   * Companion helper to `sandbox`. Applies `f` to the sandboxed query and
   * unsandboxes the result, which allows recovery, and partial recovery, from
   * errors and defects alike.
   */
  def sandboxWith[R1 <: R, E2, B](f: ZQuery[R1, Cause[E], A] => ZQuery[R1, Cause[E2], B])(implicit
    trace: Trace
  ): ZQuery[R1, E2, B] =
    ZQuery.unsandbox(f(self.sandbox))

  /**
   * Extracts a Some value into the value channel while moving the None into the
   * error channel for easier composition
   *
   * Inverse of [[ZQuery.unoption]]
   */
  def some[B](implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZQuery[R, Option[E], B] =
    self.foldQuery[R, Option[E], B](
      e => ZQuery.failNow(Some(e)),
      ev(_) match {
        case Some(b) => ZQuery.succeedNow(b)
        case _       => ZQuery.failNow(None)
      }
    )

  /**
   * Extracts the optional value or succeeds with the given 'default' value.
   */
  def someOrElse[B](default: => B)(implicit ev: A <:< Option[B], trace: Trace): ZQuery[R, E, B] =
    self.map(_.getOrElse(default))

  /**
   * Extracts the optional value or executes the given 'default' query.
   */
  def someOrElseZIO[B, R1 <: R, E1 >: E](
    default: ZQuery[R1, E1, B]
  )(implicit ev: A <:< Option[B], trace: Trace): ZQuery[R1, E1, B] =
    self.flatMap(ev(_) match {
      case Some(value) => ZQuery.succeedNow(value)
      case _           => default
    })

  /**
   * Extracts the optional value or fails with the given error `e`.
   */
  def someOrFail[B, E1 >: E](
    e: => E1
  )(implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZQuery[R, E1, B] =
    self.flatMap { a =>
      ev(a) match {
        case Some(b) => ZQuery.succeedNow(b)
        case _       => ZQuery.failNow(e)
      }
    }

  /**
   * Summarizes a query by computing some value before and after execution, and
   * then combining the values to produce a summary, together with the result of
   * execution.
   */
  def summarized[R1 <: R, E1 >: E, B, C](
    summary0: ZIO[R1, E1, B]
  )(f: (B, B) => C)(implicit trace: Trace): ZQuery[R1, E1, (C, A)] =
    for {
      start <- ZQuery.fromZIONow(summary0)
      value <- self
      end   <- ZQuery.fromZIONow(summary0)
    } yield (f(start, end), value)

  /**
   * Returns a new query that executes this one and times the execution.
   */
  def timed(implicit trace: Trace): ZQuery[R, E, (Duration, A)] =
    summarized(Clock.nanoTime)((start, end) => Duration.fromNanos(end - start))

  /**
   * Returns a query that times out this query, returning `None` if the timeout
   * elapses before the query has completed, and `Some` of its result otherwise.
   *
   * The timeout applies to the steps of the query, such as effects lifted with
   * `ZQuery.fromZIO`, not to the calls to data sources: a request that is
   * already sent runs to completion, and the query can then return `Some` of
   * its result after the timeout has elapsed. This is a known bug. A timeout
   * that elapses during a step stops the query before it sends its later
   * requests. A timeout that elapses during a request does not reliably do so:
   * once that request completes, the query can go on to send its next requests
   * and return `Some` of its result.
   */
  def timeout(duration: => Duration)(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    timeoutTo(None)(Some(_))(duration)

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event of
   * timeout, it will produce the specified error.
   */
  def timeoutFail[E1 >: E](e: => E1)(duration: => Duration)(implicit
    trace: Trace
  ): ZQuery[R, E1, A] =
    timeoutTo(ZQuery.failNow(e))(ZQuery.succeedNow)(duration).flatten

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event of
   * timeout, it will produce the specified failure.
   */
  def timeoutFailCause[E1 >: E](cause: => Cause[E1])(duration: => Duration)(implicit
    trace: Trace
  ): ZQuery[R, E1, A] =
    timeoutTo(ZQuery.failCause(cause))(ZQuery.succeedNow)(duration).flatten

  /**
   * Returns a query that will timeout this query, returning either the default
   * value if the timeout elapses before the query has completed or the result
   * of applying the function `f` to the successful result of the query. The
   * timeout does not bound calls to data sources, as described for `timeout`.
   */
  def timeoutTo[B](b: => B): ZQuery.TimeoutTo[R, E, A, B] =
    new ZQuery.TimeoutTo(self, () => b)

  /**
   * Disables caching for this query.
   */
  def uncached(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.unwrap(ZQuery.currentCache.get.map {
      case None => self
      case s    =>
        val acq = ZQuery.disabledCache.set(s) *> ZQuery.currentCache.set(None)
        val rel = ZQuery.disabledCache.set(None) *> ZQuery.currentCache.set(s)
        ZQuery.acquireReleaseWith(acq)(_ => rel)(_ => self)
    })

  /**
   * Converts a `ZQuery[R, Either[E, B], A]` into a `ZQuery[R, E, Either[A,
   * B]]`. The inverse of `left`.
   */
  def unleft[E1, B](implicit
    ev: E IsSubtypeOfError Either[E1, B],
    trace: Trace
  ): ZQuery[R, E1, Either[A, B]] =
    self.foldQuery(
      e => ev(e).fold(e1 => ZQuery.failNow(e1), b => ZQuery.succeedNow(Right(b))),
      a => ZQuery.succeedNow(Left(a))
    )

  /**
   * Converts an option on errors into an option on values.
   */
  def unoption[E1](implicit ev: E IsSubtypeOfError Option[E1], trace: Trace): ZQuery[R, E1, Option[A]] =
    self.foldQuery(
      e => ev(e).fold[ZQuery[R, E1, Option[A]]](ZQuery.succeedNow(None))(ZQuery.failNow),
      a => ZQuery.succeedNow(Some(a))
    )

  /**
   * Takes some fiber failures and converts them into errors.
   */
  def unrefine[E1 >: E](pf: PartialFunction[Throwable, E1])(implicit trace: Trace): ZQuery[R, E1, A] =
    unrefineWith(pf)(ZIO.identityFn)

  /**
   * Takes some fiber failures and converts them into errors.
   */
  def unrefineTo[E1 >: E: ClassTag](implicit trace: Trace): ZQuery[R, E1, A] =
    unrefine { case e: E1 => e }

  /**
   * Takes some fiber failures and converts them into errors, using the
   * specified function to convert the `E` into an `E1`.
   */
  def unrefineWith[E1](
    pf: PartialFunction[Throwable, E1]
  )(f: E => E1)(implicit trace: Trace): ZQuery[R, E1, A] =
    catchAllCause { cause =>
      cause.find {
        case Cause.Die(t, _) if pf.isDefinedAt(t) => pf(t)
      }.fold(ZQuery.failCause(cause.map(f)))(ZQuery.failNow)
    }

  /**
   * Converts a `ZQuery[R, Either[B, E], A]` into a `ZQuery[R, E, Either[B,
   * A]]`. The inverse of `right`.
   */
  def unright[E1, B](implicit
    ev: E IsSubtypeOfError Either[B, E1],
    trace: Trace
  ): ZQuery[R, E1, Either[B, A]] =
    self.foldQuery(
      e => ev(e).fold(b => ZQuery.succeedNow(Left(b)), e1 => ZQuery.failNow(e1)),
      a => ZQuery.succeedNow(Right(a))
    )

  /**
   * Sets the parallelism for this query to the specified maximum number of
   * fibers.
   */
  def withParallelism(n: => Int)(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZIO.Parallelism.getAndSet(Some(n)))(ZIO.Parallelism.set)(_ => self)

  /**
   * Removes the limit on the number of fibers this query uses for parallel
   * work.
   */
  def withParallelismUnbounded(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZIO.Parallelism.getAndSet(None))(ZIO.Parallelism.set)(_ => self)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results into a tuple.
   */
  def zip[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWith(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and combining their results into a
   * tuple.
   */
  def zipBatched[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithBatched(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of this
   * query.
   */
  def zipBatchedLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, A] =
    zipWithBatched(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of the
   * specified query.
   */
  def zipBatchedRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    zipWithBatched(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of this query.
   */
  def zipLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipWith(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results into a tuple.
   */
  def zipPar[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithPar(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of this query.
   */
  def zipParLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, A] =
    zipWithPar(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of the specified query.
   */
  def zipParRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    zipWithPar(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of the specified query.
   */
  def zipRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    zipWith(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results with the specified function.
   * Requests composed with `zipWith` or combinators derived from it are
   * pipelined when the requests of both queries go to the same single data
   * source: it receives them in one call to `runAll`, as batches performed one
   * after another, and a data source built with one of the
   * `DataSource.fromFunction` constructors then calls its function once for
   * each batch. A `flatMap` in this query after its requests prevents this, and
   * so do `catchAll`, `foldQuery`, `optional`, `sandbox`, `timeout` and other
   * combinators that continue with a query; the requests of the specified query
   * are then sent in a later call.
   */
  def zipWith[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) =>
          Result.blockedExit(br, Continue.effect(c.zipWith(that)(f)))
        case Result.Blocked(br1, c1) =>
          that.step.map {
            case Result.Blocked(br2, c2) =>
              val br = if (br1.isEmpty) br2 else if (br2.isEmpty) br1 else br1 ++ br2
              Result.blocked(br, c1.zipWith(c2)(f))
            case Result.Done(b) => Result.blocked(br1, c1.map(a => f(a, b)))
            case e              => e.widen
          }
        case Result.Done(a) =>
          that.step.map {
            case Result.Blocked(br, c) => Result.blocked(br, c.map(b => f(a, b)))
            case Result.Done(b)        => Result.done(f(a, b))
            case e                     => e.widen
          }
        case e => Exit.succeed(e.widen)
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources.
   */
  def zipWithBatched[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.zipWith(that.step) {
        case (Result.Blocked(br1, c1), Result.Blocked(br2, c2)) =>
          val br = if (br1.isEmpty) br2 else if (br2.isEmpty) br1 else br1 && br2
          Result.blocked(br, c1.zipWithBatched(c2)(f))
        case (Result.Blocked(br, c), Result.Done(b)) => Result.blocked(br, c.map(a => f(a, b)))
        case (Result.Done(a), Result.Blocked(br, c)) => Result.blocked(br, c.map(b => f(a, b)))
        case (Result.Done(a), Result.Done(b))        => Result.done(f(a, b))
        case (Result.Fail(e1), Result.Fail(e2))      => Result.fail(Cause.Both(e1, e2))
        case (e: Result.Fail[E1], _)                 => e
        case (_, e: Result.Fail[E1])                 => e
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results with the specified function.
   * Requests composed with `zipWithPar` or combinators derived from it will
   * automatically be batched.
   */
  def zipWithPar[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.zipWithPar(that.step) {
        case (Result.Blocked(br1, c1), Result.Blocked(br2, c2)) =>
          val br = if (br1.isEmpty) br2 else if (br2.isEmpty) br1 else br1 && br2
          Result.blocked(br, c1.zipWithPar(c2)(f))
        case (Result.Blocked(br, c), Result.Done(b)) => Result.blocked(br, c.map(a => f(a, b)))
        case (Result.Done(a), Result.Blocked(br, c)) => Result.blocked(br, c.map(b => f(a, b)))
        case (Result.Done(a), Result.Done(b))        => Result.done(f(a, b))
        case (Result.Fail(e1), Result.Fail(e2))      => Result.fail(Cause.Both(e1, e2))
        case (e: Result.Fail[E1], _)                 => e
        case (_, e: Result.Fail[E1])                 => e
      }
    }

}

/**
 * Constructors for `ZQuery` values: from requests to data sources
 * (`fromRequest` and its variants), from values and effects (`succeed`,
 * `fromZIO`, ...), and over collections of queries (`foreach`,
 * `foreachBatched`, `foreachPar` and their `collectAll` forms).
 *
 * @see
 *   [[https://zio.dev/zio-query/creating-queries Creating Queries]]
 */
object ZQuery {

  /**
   * Returns a query that moves the `Left` of the `Either` that `v` succeeds
   * with into the error channel. The inverse of `ZQuery#either`.
   */
  def absolve[R, E, A](v: => ZQuery[R, E, Either[E, A]])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.suspend(v).flatMap {
      case Right(v) => ZQuery.succeedNow(v)
      case Left(e)  => ZQuery.failNow(e)
    }

  /**
   * Acquires the specified resource before the query begins execution and
   * releases it after the query completes execution, whether by success,
   * failure, or interruption. The release function also receives the `Exit` of
   * the query.
   *
   * The acquire and release steps are `ZIO` effects, and acquiring is
   * uninterruptible. If the query is interrupted, the release runs when the
   * whole run ends with a failure or an interruption. When `timeout` or `race`
   * interrupts the query and the run still succeeds, the release does not run:
   * this is a known bug.
   */
  def acquireReleaseExitWith[R, E, A](acquire: => ZIO[R, E, A]): ZQuery.AcquireExit[R, E, A] =
    new ZQuery.AcquireExit(() => acquire)

  /**
   * Acquires the specified resource before the query begins execution and
   * releases it after the query completes execution, whether by success,
   * failure, or interruption.
   *
   * The acquire and release steps are `ZIO` effects, and acquiring is
   * uninterruptible. If the query is interrupted, the release runs when the
   * whole run ends with a failure or an interruption. When `timeout` or `race`
   * interrupts the query and the run still succeeds, the release does not run:
   * this is a known bug.
   */
  def acquireReleaseWith[R, E, A](acquire: => ZIO[R, E, A]): ZQuery.Acquire[R, E, A] =
    new ZQuery.Acquire(() => acquire)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]],
    trace: Trace
  ): ZQuery[R, E, Collection[A]] =
    foreach(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreach(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Array[A]] =
    foreach(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: Option[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    foreach(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreach(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]],
    trace: Trace
  ): ZQuery[R, E, Collection[A]] =
    foreachBatched(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreachBatched(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[A]] =
    foreachBatched(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreachBatched(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]],
    trace: Trace
  ): ZQuery[R, E, Collection[A]] =
    foreachPar(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreachPar(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[A]] =
    foreachPar(as)(ZIO.identityFn)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreachPar(as)(ZIO.identityFn)

  /**
   * Constructs a query that dies with the specified error.
   */
  def die(t: => Throwable)(implicit trace: Trace): ZQuery[Any, Nothing, Nothing] =
    ZQuery(ZIO.die(t))

  /**
   * Accesses the whole environment of the query.
   */
  def environment[R](implicit trace: Trace): ZQuery[R, Nothing, ZEnvironment[R]] =
    ZQuery(ZIO.environmentWith[R](Result.done))

  /**
   * Accesses the environment of the query and applies the specified function to
   * it.
   *
   * {{{
   * final case class Config(portNumber: Int)
   *
   * val portNumber: ZQuery[Config, Nothing, Int] =
   *   ZQuery.environmentWith[Config](_.get.portNumber)
   * }}}
   */
  def environmentWith[R]: EnvironmentWithPartiallyApplied[R] =
    new EnvironmentWithPartiallyApplied[R]

  /**
   * Accesses the environment of the query and uses it to build the query to run
   * next.
   */
  def environmentWithQuery[R]: EnvironmentWithQueryPartiallyApplied[R] =
    new EnvironmentWithQueryPartiallyApplied[R]

  /**
   * Accesses the environment of the query and uses it to build a `ZIO` effect
   * to run.
   */
  def environmentWithZIO[R]: EnvironmentWithZIOPartiallyApplied[R] =
    new EnvironmentWithZIOPartiallyApplied[R]

  /**
   * Lazily constructs a query that fails with the specified error.
   */
  def fail[E](error: => E)(implicit trace: Trace): ZQuery[Any, E, Nothing] =
    failCause(Cause.fail(error))

  /**
   * Eagerly constructs a query that fails with the specified error.
   */
  def failNow[E](error: E): ZQuery[Any, E, Nothing] =
    failCauseNow(Cause.fail(error))

  /**
   * Lazily constructs a query that fails with the specified cause.
   */
  def failCause[E](cause: => Cause[E])(implicit trace: Trace): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(cause)))

  /**
   * Eagerly constructs a query that fails with the specified cause.
   */
  def failCauseNow[E](cause: Cause[E]): ZQuery[Any, E, Nothing] =
    ZQuery(Result.failExit(cause))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed sequentially and will be pipelined.
   */
  def foreach[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] = {
    implicit def ct1: ClassTag[A] = anyRefClassTag
    implicit def ct2: ClassTag[B] = anyRefClassTag
    foreachSequentialOuter[R, E, A, B, Collection](as.toArray, mode = Mode.Sequential, bf.fromSpecific(as)(_))(f)
  }

  /**
   * Applies the function `f` to each element of the `Set[A]` and returns the
   * results in a new `Set[B]`.
   *
   * Requests will be executed sequentially and will be pipelined. For a version
   * that batches requests, see `foreachPar` or `foreachBatched`.
   */
  def foreach[R, E, A, B](in: Set[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Set[B]] =
    foreach[R, E, A, B, Iterable](in)(f).map(_.toSet)

  /**
   * Applies the function `f` to each element of the `Array[A]` and returns the
   * results in a new `Array[B]`.
   *
   * Requests will be executed sequentially and will be pipelined. For a version
   * that batches requests, see `foreachPar` or `foreachBatched`.
   */
  def foreach[R, E, A, B: ClassTag](in: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachSequentialOuter(in, mode = Mode.Sequential, ZIO.identityFn[Array[B]])(f)

  /**
   * Applies the function `f` to each element of the `Map[Key, Value]` and
   * returns the results in a new `Map[Key2, Value2]`.
   *
   * Requests will be executed sequentially and will be pipelined. For a version
   * that batches requests, see `foreachPar` or `foreachBatched`.
   */
  def foreach[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)])(implicit trace: Trace): ZQuery[R, E, Map[Key2, Value2]] =
    foreach[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Applies the function `f` if the argument is non-empty and returns the
   * results in a new `Option[B]`.
   */
  def foreach[R, E, A, B](in: Option[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Option[B]] =
    in.fold[ZQuery[R, E, Option[B]]](none)(f(_).map(Some(_)))

  /**
   * Applies the function `f` to each element of the `NonEmptyChunk[A]` and
   * returns the results in a new `NonEmptyChunk[B]`.
   *
   * Requests will be executed sequentially and will be pipelined. For a version
   * that batches requests, see `foreachPar` or `foreachBatched`.
   */
  def foreach[R, E, A, B](in: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreach[R, E, A, B, Chunk](in)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, batching requests to
   * data sources and collecting the results into a query returning a collection
   * of their results.
   *
   * Unlike `foreachPar`, the queries run one after another, so effects lifted
   * with `ZQuery.fromZIO` run in order, but their requests are still batched.
   * Use it when the order of those effects matters or when the queries should
   * not run on parallel fibers; calls to different data sources in the same
   * round still run in parallel, as for any query.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val names: UQuery[List[String]] =
   *   ZQuery.foreachBatched(List(1, 2, 3)) { id =>
   *     ZQuery.fromZIO(Console.printLine(s"loading \$id").orDie) *> getUserName(id)
   *   }
   * // names.run prints "loading 1", "loading 2" and "loading 3" in order, then
   * // calls UserNames once with GetUserName(1), GetUserName(2), GetUserName(3),
   * // and succeeds with List(user-1, user-2, user-3)
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/how-batching-works How Batching Works]]
   */
  def foreachBatched[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] = {
    implicit def ct1: ClassTag[A] = anyRefClassTag
    implicit def ct2: ClassTag[B] = anyRefClassTag
    foreachSequentialOuter[R, E, A, B, Collection](as.toArray, mode = Mode.Batched, bf.fromSpecific(as)(_))(f)
  }

  /**
   * Performs a query for each element in a Set, batching requests to data
   * sources and collecting the results into a query returning a `Set` of their
   * results.
   */
  def foreachBatched[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Set[B]] =
    foreachBatched[R, E, A, B, Iterable](as)(fn).map(_.toSet)

  /**
   * Performs a query for each element in an Array, batching requests to data
   * sources and collecting the results into a query returning a collection of
   * their results.
   *
   * For a version that pipelines requests instead, see `foreach`.
   */
  def foreachBatched[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachSequentialOuter(as, mode = Mode.Batched, ZIO.identityFn[Array[B]])(f)

  /**
   * Performs a query for each element in a Map, batching requests to data
   * sources and collecting the results into a query returning a collection of
   * their results.
   *
   * For a version that pipelines requests instead, see `foreach`.
   */
  def foreachBatched[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)])(implicit trace: Trace): ZQuery[R, E, Map[Key2, Value2]] =
    foreachBatched[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Performs a query for each element in a NonEmptyChunk, batching requests to
   * data sources and collecting the results into a query returning a collection
   * of their results.
   *
   * For a version that pipelines requests instead, see `foreach`.
   */
  def foreachBatched[R, E, A, B](as: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachBatched[R, E, A, B, Chunk](as)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   *
   * This is the main way to avoid the N + 1 problem: the requests that the
   * queries make in the same step reach each data source in one batch, and
   * equal requests are sent once while caching is enabled. Other work, such as
   * effects lifted with `ZQuery.fromZIO`, runs on parallel fibers; see
   * `withParallelism` to limit them.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val names: UQuery[List[String]] =
   *   ZQuery.foreachPar(List(1, 2, 2, 3))(getUserName)
   * // names.run calls UserNames once with GetUserName(1), GetUserName(2),
   * // GetUserName(3), and succeeds with List(user-1, user-2, user-2, user-3)
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/how-batching-works How Batching Works]]
   */
  def foreachPar[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] =
    as.sizeCompare(1) match {
      case -1 => ZQuery.succeedNow(bf.fromSpecific(as)(Nil))
      case 0  => f(as.head).map(v => (bf.newBuilder(as) += v).result())
      case _  =>
        implicit def ct: ClassTag[A] = anyRefClassTag
        ZQuery {
          ZIO
            .foreachPar(as.toArray)(f(_).step)
            .map(collectResults[R, E, A, B, Collection](_, mode = Mode.Parallel, bf.fromSpecific(as)(_)))
        }
    }

  /**
   * Performs a query for each element in a Set, collecting the results into a
   * query returning a collection of their results. Requests will be executed in
   * parallel and will be batched.
   */
  def foreachPar[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Set[B]] =
    foreachPar[R, E, A, B, Iterable](as)(fn).map(_.toSet)

  /**
   * Performs a query for each element in an Array, collecting the results into
   * a query returning a collection of their results. Requests will be executed
   * in parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachPar[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachPar[R, E, A, B, Iterable](as)(f).map(_.toArray)

  /**
   * Performs a query for each element in a Map, collecting the results into a
   * query returning a collection of their results. Requests will be executed in
   * parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachPar[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)])(implicit trace: Trace): ZQuery[R, E, Map[Key2, Value2]] =
    foreachPar[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Performs a query for each element in a NonEmptyChunk, collecting the
   * results into a query returning a collection of their results. Requests will
   * be executed in parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachPar[R, E, A, B](as: NonEmptyChunk[A])(fn: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachPar[R, E, A, B, Chunk](as)(fn).map(NonEmptyChunk.nonEmpty)

  /**
   * Constructs a query from an either
   */
  def fromEither[E, A](either: => Either[E, A])(implicit trace: Trace): ZQuery[Any, E, A] =
    ZQuery.succeed(either).flatMap(_.fold[ZQuery[Any, E, A]](ZQuery.failNow, ZQuery.succeedNow))

  /**
   * Constructs a query from an option
   */
  def fromOption[A](option: => Option[A])(implicit trace: Trace): ZQuery[Any, Option[Nothing], A] =
    ZQuery.succeed(option).flatMap(_.fold[ZQuery[Any, Option[Nothing], A]](ZQuery.failNow(None))(ZQuery.succeedNow))

  /**
   * Constructs a query from a request and a data source. Queries will die with
   * a `QueryFailure` when run if the data source does not provide results for
   * all requests received. Queries must be constructed with `fromRequest` or
   * one of its variants for optimizations to be applied.
   *
   * The request is not sent when the query is built. It is sent when the query
   * runs, together with the other requests to the same data source that the
   * query makes at the same step. While caching is enabled, which is the
   * default, a request equal to one already made in the same run is answered
   * from the cache. The request type must extend `Request[E, B]`; define it as
   * a case class so that equal requests compare equal.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched("UserNames") { (requests: Chunk[GetUserName]) =>
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * def getUserName(id: Int): UQuery[String] =
   *   ZQuery.fromRequest(GetUserName(id))(userNames)
   *
   * val both: UQuery[(String, String)] = getUserName(1) <&> getUserName(2)
   * // both.run calls UserNames once with GetUserName(1), GetUserName(2), and
   * // succeeds with (user-1,user-2)
   * }}}
   *
   * @see
   *   `fromRequests` for variants that allow for multiple requests to be
   *   submitted at once
   * @see
   *   [[https://zio.dev/zio-query/creating-queries Creating Queries]]
   */
  def fromRequest[R, E, A, B](
    request0: => A
  )(dataSource0: => DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, B] =
    ZQuery {
      ZQuery.currentCache.getWith {
        case Some(cache) => cachedResult(cache, dataSource0, request0).toZIO
        case _           => uncachedResult(dataSource0, request0)
      }
    }

  /**
   * Constructs a query from a request and a data source but does not apply
   * caching to the query.
   */
  def fromRequestUncached[R, E, A, B](
    request: => A
  )(dataSource: => DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, B] =
    new ZQuery(uncachedResult(dataSource, request)).uncached

  /**
   * Constructs a query from a Chunk of requests and a data source, sending the
   * requests to the data source in one batch. Queries will die with a
   * `QueryFailure` when run if the data source does not provide results for all
   * requests received.
   *
   * @see
   *   [[fromRequest]] for submitting a single request to a datasource
   * @see
   *   `fromRequestsWith` for a variant that allows transforming the input to a
   *   request
   */
  def fromRequests[R, E, A, B](
    requests: Chunk[A]
  )(dataSource: DataSource[R, A])(implicit trace: Trace, ev: A <:< Request[E, B]): ZQuery[R, E, Chunk[B]] =
    fromRequestsWith(requests, ZIO.identityFn[A])(dataSource)

  /**
   * Constructs a query from a List of requests and a data source, sending the
   * requests to the data source in one batch. Queries will die with a
   * `QueryFailure` when run if the data source does not provide results for all
   * requests received.
   *
   * @see
   *   [[fromRequest]] for submitting a single request to a datasource
   * @see
   *   `fromRequestsWith` for a variant that allows transforming the input to a
   *   request
   */
  def fromRequests[R, E, A, B](
    requests: List[A]
  )(dataSource: DataSource[R, A])(implicit trace: Trace, ev: A <:< Request[E, B]): ZQuery[R, E, List[B]] =
    fromRequestsWith(requests, ZIO.identityFn[A])(dataSource)

  /**
   * Constructs a query from a Chunk of values, a function transforming them
   * into requests and a data source, sending the requests in one batch. Queries
   * will die with a `QueryFailure` when run if the data source does not provide
   * results for all requests received.
   */
  def fromRequestsWith[R, E, In, A, B](
    as: Chunk[In],
    f: In => A
  )(dataSource: DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, Chunk[B]] =
    ZQuery {
      ZQuery.currentCache.getWith {
        case Some(cache) => CachedResult.foreachAsArr(as)(r => cachedResult(cache, dataSource, f(r)))
        case _           => ZIO.foreach(as)(r => uncachedResult(dataSource, f(r))).map(_.toArray)
      }.map(v => collectResults(v, mode = Mode.Batched, Chunk.fromArray))
    }

  /**
   * Constructs a query from a List of values, a function transforming them into
   * requests and a data source, sending the requests in one batch. Queries will
   * die with a `QueryFailure` when run if the data source does not provide
   * results for all requests received.
   */
  def fromRequestsWith[R, E, In, A, B](
    as: List[In],
    f: In => A
  )(dataSource: DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, List[B]] =
    ZQuery {
      ZQuery.currentCache.getWith {
        case Some(cache) => CachedResult.foreachAsArr(as)(r => cachedResult(cache, dataSource, f(r)))
        case _           => ZIO.foreach(as)(r => uncachedResult(dataSource, f(r))).map(_.toArray)
      }.map(v => collectResults(v, mode = Mode.Batched, _.toList))
    }

  private def cachedResult[R, E, A, B](
    cache: Cache,
    dataSource: DataSource[R, A],
    request: A
  )(implicit ev: A <:< Request[E, B], trace: Trace): CachedResult[R, E, B] = {

    def foldPromise(either: Either[Promise[E, B], Promise[E, B]]): CachedResult[R, E, B] =
      either match {
        case Left(promise) =>
          CachedResult.Pure(
            Result.blocked(
              BlockedRequests.single(dataSource, BlockedRequest(request, promise)),
              Continue(promise)
            )
          )
        case Right(promise) =>
          promise.unsafe.poll(Unsafe) match {
            case Some(io: Exit[E, B]) => CachedResult.Pure(Result.fromExit(io))
            case Some(io)             => CachedResult.Effectful(io.exit.map(Result.fromExit))
            case _                    => CachedResult.Pure(Result.blocked(BlockedRequests.empty, Continue(promise)))
          }
      }

    cache match {
      case cache: Cache.Default => foldPromise(cache.lookupUnsafe(request)(Unsafe))
      case cache                => CachedResult.Effectful(cache.lookup(request).flatMap(foldPromise(_).toZIO))
    }
  }

  private def uncachedResult[R, E, A, B](dataSource: DataSource[R, A], request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Result[R, E, B]] = {
    val promise = Promise.unsafe.make[E, B](FiberId.None)(Unsafe)
    Result.blockedExit(
      BlockedRequests.single(dataSource, BlockedRequest(request, promise)),
      Continue(promise)
    )
  }

  /**
   * Constructs a query from an effect.
   */
  def fromZIO[R, E, A](effect: => ZIO[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery(ZIO.suspendSucceed(effect).foldCause(Result.fail, Result.done))

  /**
   * Constructs a query from an effect. Unlike [[fromZIO]], this method does not
   * suspend the creation of the effect which can lead to improved performance
   * in some cases, but it should only be used when the creation of the effect
   * is side effect-free.
   *
   * Note that this method is meant mostly for internal use, but it's made
   * public so that library authors can make use of this optimization. Most
   * users should use [[fromZIO]] instead.
   */
  def fromZIONow[R, E, A](effect: ZIO[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery(effect.foldCauseZIO(Result.failExit, Result.doneExit))

  /**
   * Constructs a query that never completes.
   */
  def never(implicit trace: Trace): ZQuery[Any, Nothing, Nothing] =
    ZQuery.fromZIONow(ZIO.never)

  /**
   * A query that succeeds with `None`.
   */
  val none: ZQuery[Any, Nothing, Option[Nothing]] =
    succeed(None)(Trace.empty)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful results.
   * Requests will be executed sequentially and will be pipelined.
   */
  def partitionQuery[R, E, A, B](
    as: Iterable[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    ZQuery.foreach(as)(f(_).either).map(partitionMap(_)(ZIO.identityFn))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful results.
   * Requests will be executed in parallel and will be batched.
   */
  def partitionQueryPar[R, E, A, B](
    as: Iterable[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    ZQuery.foreachPar(as)(f(_).either).map(partitionMap(_)(ZIO.identityFn))

  /**
   * Accesses the service of type `R` in the environment of the query.
   */
  def service[R: Tag](implicit trace: Trace): ZQuery[R, Nothing, R] =
    ZQuery(ZIO.serviceWith[R](Result.done))

  /**
   * Accesses the service of type `R` in the environment of the query and
   * applies the specified function to it.
   *
   * {{{
   * final case class Config(portNumber: Int)
   *
   * val portNumber: ZQuery[Config, Nothing, Int] =
   *   ZQuery.serviceWith[Config](_.portNumber)
   * }}}
   */
  def serviceWith[R]: ServiceWithPartiallyApplied[R] =
    new ServiceWithPartiallyApplied[R]

  /**
   * Accesses the service of type `R` in the environment of the query and uses
   * it to build the query to run next.
   */
  def serviceWithQuery[R]: ServiceWithQueryPartiallyApplied[R] =
    new ServiceWithQueryPartiallyApplied[R]

  /**
   * Accesses the service of type `R` in the environment of the query and uses
   * it to build a `ZIO` effect to run.
   */
  def serviceWithZIO[R]: ServiceWithZIOPartiallyApplied[R] =
    new ServiceWithZIOPartiallyApplied[R]

  /**
   * Constructs a query that succeeds with the optional value.
   */
  def some[A](a: => A)(implicit trace: Trace): ZQuery[Any, Nothing, Option[A]] =
    succeed(Some(a))

  /**
   * Lazily constructs a query that succeeds with the specified value.
   *
   * '''NOTE''': If the `value` is side-effect free, prefer using [[succeedNow]]
   * instead
   */
  def succeed[A](value: => A)(implicit trace: Trace): ZQuery[Any, Nothing, A] =
    ZQuery(ZIO.succeed(Result.done(value)))

  /**
   * Eagerly constructs a query that succeeds with the specified value.
   */
  def succeedNow[A](value: A): ZQuery[Any, Nothing, A] =
    ZQuery(Result.doneExit(value))

  /**
   * Returns a lazily constructed query.
   */
  def suspend[R, E, A](query: => ZQuery[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
    new ZQuery(ZIO.suspendSucceed(query.step))

  /**
   * The query that succeeds with the unit value.
   */
  val unit: ZQuery[Any, Nothing, Unit] =
    new ZQuery(ZIO.succeed(Result.unit)(Trace.empty))

  /**
   * These methods can improve UX and performance in some cases, but when used
   * improperly they can lead to unexpected behaviour in the application code.
   *
   * Make sure you really understand them before using them!
   */
  object unsafe {

    /**
     * Returns a memoized version of `query` without suspending its creation in
     * an effect. See `ZQuery#memoize`, including its caveat for queries that
     * make requests.
     */
    def memoize[R, E, A](query: ZQuery[R, E, A])(implicit unsafe: Unsafe, trace: Trace): ZQuery[R, E, A] = {
      val ref = Ref.Synchronized.unsafe.make[Option[Result[R, E, A]]](None)
      new ZQuery[R, E, A](ref.modifyZIO {
        case s @ Some(result) => Exit.succeed((result, s))
        case _                => query.step.map(result => (result, Some(result)))
      })
    }

  }

  /**
   * The inverse operation of `ZQuery#sandbox`: flattens the `Cause[Cause[E]]`
   * that the query fails with back into a `Cause[E]`, so that defects become
   * defects again.
   */
  def unsandbox[R, E, A](v: => ZQuery[R, Cause[E], A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.suspend(v).mapErrorCause(_.flatten)

  /**
   * Unwraps a query that is produced by an effect.
   */
  def unwrap[R, E, A](zio: => ZIO[R, E, ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.fromZIO(zio).flatten

  /**
   * Partially applied form of `ZQuery.environmentWith`, which fixes the
   * environment type `R`.
   */
  final class EnvironmentWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Applies `f` to the environment of the query.
     */
    def apply[A](f: ZEnvironment[R] => A)(implicit trace: Trace): ZQuery[R, Nothing, A] =
      ZQuery(ZIO.environmentWith[R](e => Result.done(f(e))))
  }

  /**
   * Partially applied form of `ZQuery.environmentWithQuery`, which fixes the
   * environment type `R`.
   */
  final class EnvironmentWithQueryPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Builds the query to run next from the environment of the query.
     */
    def apply[E, A](f: ZEnvironment[R] => ZQuery[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
      environment[R].flatMap(f)
  }

  /**
   * Partially applied form of `ZQuery.environmentWithZIO`, which fixes the
   * environment type `R`.
   */
  final class EnvironmentWithZIOPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Builds a `ZIO` effect to run from the environment of the query.
     */
    def apply[E, A](f: ZEnvironment[R] => ZIO[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
      ZQuery(
        ZIO.environmentWithZIO[R](
          f(_).foldCauseZIO(
            c => Result.failExit(c),
            v => Result.doneExit(v)
          )
        )
      )
  }

  /**
   * Partially applied form of `ZQuery#provideSomeLayer`, which fixes the
   * remaining environment type `R0`.
   */
  final class ProvideSomeLayer[R0, -R, +E, +A](private val self: ZQuery[R, E, A]) extends AnyVal {

    /**
     * Provides the part `R1` of the environment with the specified layer.
     */
    def apply[E1 >: E, R1](
      layer: => Described[ZLayer[R0, E1, R1]]
    )(implicit ev: R0 with R1 <:< R, tag: Tag[R1], trace: Trace): ZQuery[R0, E1, A] =
      self
        .asInstanceOf[ZQuery[R0 with R1, E, A]]
        .provideLayer(Described(ZLayer.environment[R0] ++ layer.value, layer.description))
  }

  /**
   * Partially applied form of `ZQuery#timeoutTo`, which holds the query and the
   * default value.
   */
  final class TimeoutTo[-R, +E, +A, +B](self: ZQuery[R, E, A], b: () => B) {

    /**
     * Applies `f` to the result of the query, or returns the default value if
     * `duration` elapses first. Like `ZQuery#timeout`, it does not bound calls
     * to data sources.
     */
    def apply[B1 >: B](
      f: A => B1
    )(duration: => Duration)(implicit trace: Trace): ZQuery[R, E, B1] = {

      def race(
        query: ZQuery[R, E, B1],
        fiber: Fiber[Nothing, B1]
      ): ZQuery[R, E, B1] =
        ZQuery {
          query.step.raceWith[R, Nothing, Nothing, B1, Result[R, E, B1]](fiber.join)(
            (leftExit, rightFiber) =>
              leftExit.foldExitZIO(
                cause => rightFiber.interrupt *> Result.failExit(cause),
                {
                  case Result.Blocked(blockedRequests, Continue.Effect(query)) =>
                    Result.blockedExit(blockedRequests, Continue.effect(race(query, fiber)))
                  case Result.Blocked(blockedRequests, Continue.Get(io)) =>
                    Result.blockedExit(blockedRequests, Continue.effect(race(ZQuery.fromZIONow(io), fiber)))
                  case Result.Done(value) =>
                    rightFiber.interrupt *> Result.doneExit(value)
                  case Result.Fail(cause) =>
                    rightFiber.interrupt *> Result.failExit(cause)
                }
              ),
            (rightExit, leftFiber) => leftFiber.interrupt *> Exit.succeed(Result.fromExit(rightExit))
          )
        }

      ZQuery.fromZIONow(ZIO.sleep(duration).interruptible.as(b()).fork).flatMap(fiber => race(self.map(f), fiber))
    }
  }

  /**
   * Partially applied form of `ZQuery.serviceWith`, which fixes the service
   * type `R`.
   */
  final class ServiceWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Applies `f` to the service of type `R`.
     */
    def apply[A](
      f: R => A
    )(implicit tag: Tag[R], trace: Trace): ZQuery[R, Nothing, A] =
      ZQuery(ZIO.serviceWith[R](s => Result.done(f(s))))
  }

  /**
   * Partially applied form of `ZQuery.serviceWithQuery`, which fixes the
   * service type.
   */
  final class ServiceWithQueryPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Builds the query to run next from the service.
     */
    def apply[R <: Service, E, A](
      f: Service => ZQuery[R, E, A]
    )(implicit tag: Tag[Service], trace: Trace): ZQuery[R with Service, E, A] =
      service[Service].flatMap(f)
  }

  /**
   * Partially applied form of `ZQuery.serviceWithZIO`, which fixes the service
   * type.
   */
  final class ServiceWithZIOPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Builds a `ZIO` effect to run from the service.
     */
    def apply[R <: Service, E, A](
      f: Service => ZIO[R, E, A]
    )(implicit tag: Tag[Service], trace: Trace): ZQuery[R with Service, E, A] =
      ZQuery(
        ZIO.serviceWithZIO[Service](
          f(_).foldCauseZIO(
            c => Result.failExit(c),
            v => Result.doneExit(v)
          )
        )
      )
  }

  /**
   * Constructs a query from an effect that returns a result.
   */
  private def apply[R, E, A](step: ZIO[R, Nothing, Result[R, E, A]]): ZQuery[R, E, A] =
    new ZQuery(step)

  /**
   * Partitions the elements of a collection using the specified function.
   */
  private def partitionMap[A, B, C](as: Iterable[A])(f: A => Either[B, C]): (Iterable[B], Iterable[C]) = {
    val bs = ChunkBuilder.make[B]()
    val cs = ChunkBuilder.make[C]()
    as.foreach { a =>
      f(a) match {
        case Left(b)  => bs addOne b
        case Right(c) => cs addOne c
      }
    }
    (bs.result(), cs.result())
  }

  /**
   * Not read by ZIO Query: kept for binary compatibility only. Use
   * `ZQuery#cached` and `ZQuery#uncached` to control caching.
   */
  @deprecated("No longer used, kept for binary compatibility only", "0.7.4")
  val cachingEnabled: FiberRef[Boolean] =
    FiberRef.unsafe.make(true)(Unsafe)

  /**
   * The cache that requests use in the current fiber. `runCache` sets it to its
   * cache for the duration of a run, and it is `None` while caching is disabled
   * with `ZQuery#uncached` or outside a run.
   */
  val currentCache: FiberRef[Option[Cache]] =
    FiberRef.unsafe.make(Option.empty[Cache])(Unsafe)

  private val disabledCache: FiberRef[Option[Cache]] =
    FiberRef.unsafe.make(Option.empty[Cache])(Unsafe)

  /**
   * The scope of the current run, which holds the finalizers that
   * `acquireReleaseWith` and `ensuring` register. `runCache` sets a new scope
   * for each run and closes it at the end of the run; the finalizers then run
   * if the run ended with a failure or an interruption.
   */
  val currentScope: FiberRef[QueryScope] =
    FiberRef.unsafe.make[QueryScope](QueryScope.NoOp)(Unsafe)

  /**
   * Partially applied form of `ZQuery.acquireReleaseWith`, which holds the
   * acquire effect.
   */
  final class Acquire[-R, +E, +A](private val acquire: () => ZIO[R, E, A]) extends AnyVal {

    /**
     * Sets the effect that releases the resource.
     */
    def apply[R1](release: A => URIO[R1, Any]): Release[R with R1, E, A] =
      new Release[R with R1, E, A](acquire, release)
  }

  /**
   * Partially applied form of `ZQuery.acquireReleaseWith`, which holds the
   * acquire and release effects.
   */
  final class Release[-R, +E, +A](acquire: () => ZIO[R, E, A], release: A => URIO[R, Any]) {

    /**
     * Sets the query that uses the resource and returns the resulting query.
     */
    def apply[R1 <: R, E1 >: E, B](use: A => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
      acquireReleaseExitWith(acquire())((a: A, _: Exit[E1, B]) => release(a))(use)
  }

  /**
   * Partially applied form of `ZQuery.acquireReleaseExitWith`, which holds the
   * acquire effect.
   */
  final class AcquireExit[-R, +E, +A](private val acquire: () => ZIO[R, E, A]) extends AnyVal {

    /**
     * Sets the effect that releases the resource, given the `Exit` of the
     * query.
     */
    def apply[R1 <: R, E1 >: E, B](
      release: (A, Exit[E1, B]) => URIO[R1, Any]
    ): ReleaseExit[R1, E, E1, A, B] =
      new ReleaseExit(acquire, release)
  }

  /**
   * Partially applied form of `ZQuery.acquireReleaseExitWith`, which holds the
   * acquire and release effects.
   */
  final class ReleaseExit[-R, +E, E1, +A, B](
    acquire: () => ZIO[R, E, A],
    release: (A, Exit[E1, B]) => URIO[R, Any]
  ) {

    /**
     * Sets the query that uses the resource and returns the resulting query.
     */
    def apply[R1 <: R, E2 >: E <: E1, B1 <: B](use: A => ZQuery[R1, E2, B1])(implicit
      trace: Trace
    ): ZQuery[R1, E2, B1] =
      ZQuery.unwrap {
        ZQuery.currentScope.getWith { scope =>
          ZIO.environmentWithZIO[R] { environment =>
            val ref = new AtomicBoolean(true)
            ZIO.uninterruptible {
              ZIO.suspendSucceed(acquire()).tap { a =>
                scope.addFinalizerExit {
                  case Exit.Failure(cause) =>
                    release(a, Exit.failCause(cause.stripFailures))
                      .provideEnvironment(environment)
                      .when(ref.getAndSet(false))
                  case Exit.Success(_) =>
                    Exit.unit
                }
              }
            }.map { a =>
              use(a)
                .foldCauseZIO(
                  cause =>
                    ZIO
                      .suspendSucceed(release(a, Exit.failCause(cause)))
                      .when(ref.getAndSet(false))
                      .mapErrorCause(cause ++ _) *>
                      ZIO.refailCause(cause),
                  b =>
                    ZIO
                      .suspendSucceed(release(a, Exit.succeed(b)))
                      .when(ref.getAndSet(false))
                      .as(b)
                )
            }
          }
        }
      }
  }

  @inline private def anyRefClassTag[A]: ClassTag[A] =
    ClassTag.AnyRef.asInstanceOf[ClassTag[A]]

  private val FailCauseNone: Cause[Nothing] => ZQuery[Any, Nothing, Nothing] =
    cause => ZQuery(Exit.failCause(cause))

  /**
   * `foreach` base implementation for sequential and batched modes
   */
  private def foreachSequentialOuter[R, E, A, B, F[_]](
    as: Array[A],
    mode: Mode, // Sequential or Batched
    mapOut: Array[B] => F[B]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit trace: Trace, ct: ClassTag[B]): ZQuery[R, E, F[B]] = {
    assert(BuildUtils.optimizationsEnabled || mode != Mode.Parallel)

    as.length match {
      case 0 => ZQuery.succeedNow(mapOut(Array.empty[B]))
      case 1 => f(as.head).map(v => mapOut(Array(v)))
      case n =>
        ZQuery {
          ZIO.suspendSucceed {
            var i   = 0
            val arr = Array.ofDim[Result[R, E, B]](n)

            ZIO
              .whileLoop(i < n)(f(as(i)).step) { v => arr(i) = v; i += 1 }
              .as(collectResults(arr, mode, mapOut))
          }
        }
    }
  }

  @inline private def collectArrayZIO[R, E, A: ClassTag](
    in: Array[ZIO[R, E, A]]
  )(implicit trace: Trace): ZIO[R, E, Array[A]] =
    (in.length: @switch) match {
      case 0 => Exit.succeed(Array.empty)
      case 1 => in(0).flatMap(v => Exit.succeed(Array(v)))
      case n =>
        val out = Array.ofDim[A](n)
        var i   = 0
        ZIO.whileLoop(i < n)(in(i)) { v => out(i) = v; i += 1 }.as(out)
    }

  private def collectResults[R, E, A, B, F[_]](
    results: Array[Result[R, E, B]],
    mode: Mode,
    mapOut: Array[B] => F[B]
  )(implicit trace: Trace): Result[R, E, F[B]] = {
    implicit def ct: ClassTag[B] = anyRefClassTag

    @inline def addToArray(array: Array[B])(idxs: Array[Int], values: Array[B]): Unit = {
      var i = 0
      while (i < idxs.length) {
        val idx   = idxs(i)
        val value = values(i)
        array(idx) = value
        i += 1
      }
    }

    val size                             = results.length
    var i, doneSize, effectSize, getSize = 0

    // 2-pass to pre-size arrays. Benchmarks show that this is faster than a single-pass using unsized builders
    while (i < size) {
      val n = results(i)
      if (n.isInstanceOf[Result.Blocked[?, ?, ?]]) {
        val continue = n.asInstanceOf[Result.Blocked[?, ?, ?]].continue
        if (continue.isInstanceOf[Continue.Effect[?, ?, ?]])
          effectSize += 1
        else
          getSize += 1
      } else if (n.isInstanceOf[Result.Done[?]]) {
        doneSize += 1
      }
      i += 1
    }

    var blockedRequests: BlockedRequests[R] = BlockedRequests.empty

    val dones         = Array.ofDim[B](doneSize)
    val doneIndices   = Array.ofDim[Int](doneSize)
    val effects       = Array.ofDim[ZQuery[R, E, B]](effectSize)
    val effectIndices = Array.ofDim[Int](effectSize)
    val gets          = Array.ofDim[ZIO[R, E, B]](getSize)
    val getIndices    = Array.ofDim[Int](getSize)

    val failBuilder = new ArrayBuilder.ofRef[Cause[E]]

    var eIdx, gIdx, dIdx, index = 0
    val brEmpty                 = BlockedRequests.Empty

    while (index < size) {
      results(index) match {
        case Result.Blocked(br, continue) =>
          if (br ne brEmpty) {
            blockedRequests = if (mode == 0) blockedRequests ++ br else blockedRequests && br
          }
          continue match {
            case Continue.Effect(query) =>
              effects(eIdx) = query
              effectIndices(eIdx) = index
              eIdx += 1
            case Continue.Get(io) =>
              gets(gIdx) = io
              getIndices(gIdx) = index
              gIdx += 1
          }
          index += 1
        case Result.Done(b) =>
          dones(dIdx) = b
          doneIndices(dIdx) = index
          dIdx += 1
          index += 1
        case Result.Fail(e) =>
          failBuilder.addOne(e)
          index += 1
      }
    }

    val fails = failBuilder.result()

    if (gets.isEmpty && effects.isEmpty && fails.isEmpty)
      Result.done(mapOut(dones))
    else if (fails.isEmpty) {
      val continue =
        if (effects.isEmpty) {
          val io = collectArrayZIO(gets).flatMap { gets =>
            val array = Array.ofDim[B](size)
            addToArray(array)(getIndices, gets)
            addToArray(array)(doneIndices, dones)
            Exit.succeed(mapOut(array))
          }
          Continue.get(io)
        } else {
          val collect = (mode: @switch) match {
            case Mode.Sequential => ZQuery.collectAll[R, E, B](effects)
            case Mode.Parallel   => ZQuery.collectAllPar[R, E, B](effects)
            case Mode.Batched    => ZQuery.collectAllBatched[R, E, B](effects)
          }
          val query = collect.mapZIO { effects =>
            collectArrayZIO(gets).flatMap { gets =>
              val array = Array.ofDim[B](size)
              addToArray(array)(effectIndices, effects)
              addToArray(array)(getIndices, gets)
              addToArray(array)(doneIndices, dones)
              Exit.succeed(mapOut(array))
            }
          }
          Continue.effect(query)
        }
      Result.blocked(blockedRequests, continue)
    } else
      Result.fail(fails.foldLeft[Cause[E]](Cause.empty)(_ && _))
  }

  /**
   * The `CachedResult` represents a cache entry that can either be extracted
   * purely, or that it requires an effect to be run to obtain the result.
   */
  private sealed abstract class CachedResult[R, E, A] { self =>
    def toZIO: UIO[Result[R, E, A]]
  }

  private object CachedResult {

    final case class Pure[R, E, B](result: Result[R, E, B]) extends CachedResult[R, E, B] {
      def toZIO: UIO[Result[R, E, B]] = Exit.succeed(result)
    }

    final case class Effectful[R, E, B](toZIO: UIO[Result[R, E, B]]) extends CachedResult[R, E, B]

    @deprecated("Kept for bin-compat, use `foreachAsArr` instead", "0.7.5")
    def foreach[R, E, A, B, Collection[+x] <: Iterable[x]](
      as: Collection[A]
    )(
      f: A => CachedResult[R, E, B]
    )(implicit
      trace: Trace,
      bf: BuildFrom[Collection[A], Result[R, E, B], Collection[Result[R, E, B]]]
    ): UIO[Collection[Result[R, E, B]]] =
      foreachAsArr(as)(f).map(bf.fromSpecific(as)(_))

    def foreachAsArr[R, E, A, B](
      as: Iterable[A]
    )(
      f: A => CachedResult[R, E, B]
    )(implicit
      trace: Trace
    ): UIO[Array[Result[R, E, B]]] = {
      val puresB  = Array.newBuilder[Result[R, E, B]]
      val pureIdx = new ArrayBuilder.ofInt

      val effectfulB   = new ArrayBuilder.ofRef[UIO[Result[R, E, B]]]
      val effectfulIdx = new ArrayBuilder.ofInt

      val iter = as.iterator
      var i    = 0
      while (iter.hasNext) {
        val next = f(iter.next())
        next match {
          case Pure(result) =>
            puresB.addOne(result)
            pureIdx.addOne(i)
          case Effectful(io) =>
            effectfulB.addOne(io)
            effectfulIdx.addOne(i)
        }
        i += 1
      }

      val pures     = puresB.result()
      val effectful = effectfulB.result()

      if (effectful.isEmpty) Exit.succeed(pures)
      else if (pures.isEmpty) collectArrayZIO(effectful)
      else {
        val pIdxs = pureIdx.result()
        val eIdxs = effectfulIdx.result()

        collectArrayZIO(effectful).map { effectful =>
          val arr = Array.ofDim[Result[R, E, B]](pIdxs.length + eIdxs.length)

          def addToArray(idxs: Array[Int], values: Array[Result[R, E, B]]): Unit = {
            var i = 0
            while (i < idxs.length) {
              val idx   = idxs(i)
              val value = values(i)
              arr(idx) = value
              i += 1
            }
          }

          addToArray(pIdxs, pures)
          addToArray(eIdxs, effectful)
          arr
        }
      }
    }
  }

  // Patches for Scala 2.12
  private implicit class ArrBuilderOps[A](private val builder: ArrayBuilder[A]) extends AnyVal {
    def addOne(value: A): Unit = builder += value
  }

  private implicit class IterableOps[A](private val it: Iterable[A]) extends AnyVal {
    def knownSize: Int = it.size
  }

  private type Mode = Int
  private object Mode {
    final val Sequential = 0
    final val Parallel   = 1
    final val Batched    = 2
  }
}
