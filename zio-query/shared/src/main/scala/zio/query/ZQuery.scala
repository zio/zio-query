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
import zio.query.internal._
import zio.stacktracer.TracingImplicits.disableAutoTrace

import scala.annotation.tailrec
import scala.collection.mutable.Builder
import scala.reflect.ClassTag

/**
 * A `ZQuery[R, E, A]` is a purely functional description of an effectual query
 * that may contain requests from one or more data sources, requires an
 * environment `R`, and may fail with an `E` or succeed with an `A`.
 *
 * Requests that can be performed in parallel, as expressed by `zipWithPar` and
 * combinators derived from it, will automatically be batched. Requests that
 * must be performed sequentially, as expressed by `zipWith` and combinators
 * derived from it, will automatically be pipelined. This allows for aggressive
 * data source specific optimizations. Requests can also be deduplicated and
 * cached.
 *
 * This allows for writing queries in a high level, compositional style, with
 * confidence that they will automatically be optimized. For example, consider
 * the following query from a user service.
 *
 * {{{
 * val getAllUserIds: ZQuery[Any, Nothing, List[Int]]         = ???
 * def getUserNameById(id: Int): ZQuery[Any, Nothing, String] = ???
 *
 * for {
 *   userIds   <- getAllUserIds
 *   userNames <- ZQuery.foreachPar(userIds)(getUserNameById)
 * } yield userNames
 * }}}
 *
 * This would normally require N + 1 queries, one for `getAllUserIds` and one
 * for each call to `getUserNameById`. In contrast, `ZQuery` will automatically
 * optimize this to two queries, one for `userIds` and one for `userNames`,
 * assuming an implementation of the user service that supports batching.
 *
 * Based on "There is no Fork: an Abstraction for Efficient, Concurrent, and
 * Concise Data Access" by Simon Marlow, Louis Brandy, Jonathan Coens, and Jon
 * Purdy. [[http://simonmar.github.io/bib/papers/haxl-icfp14.pdf]]
 */
final class ZQuery[-R, +E, +A] private (private val step: ZIO[R, Nothing, Result[R, E, A]]) { self =>

  /**
   * Syntax for adding aspects.
   */
  final def @@[LowerR <: UpperR, UpperR <: R, LowerE >: E, UpperE >: LowerE, LowerA >: A, UpperA >: LowerA](
    aspect: => QueryAspect[LowerR, UpperR, LowerE, UpperE, LowerA, UpperA]
  )(implicit trace: Trace): ZQuery[UpperR, LowerE, LowerA] =
    ZQuery.suspend(aspect(self))

  /**
   * A symbolic alias for `zipParRight`.
   */
  final def &>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    zipParRight(that)

  /**
   * A symbolic alias for `zipRight`.
   */
  final def *>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    zipRight(that)

  /**
   * A symbolic alias for `zipParLeft`.
   */
  final def <&[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipParLeft(that)

  /**
   * A symbolic alias for `zipPar`.
   */
  final def <&>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipPar(that)

  /**
   * A symbolic alias for `zipLeft`.
   */
  final def <*[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipLeft(that)

  /**
   * A symbolic alias for `zip`.
   */
  final def <*>[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zip(that)

  /**
   * Returns a query which submerges the error case of `Either` into the error
   * channel of the query
   *
   * The inverse of [[ZQuery.either]]
   */
  def absolve[E1 >: E, B](implicit ev: A IsSubtypeOfOutput Either[E1, B], trace: Trace): ZQuery[R, E1, B] =
    ZQuery.absolve(self.map(ev))

  /**
   * Maps the success value of this query to the specified constant value.
   */
  final def as[B](b: => B)(implicit trace: Trace): ZQuery[R, E, B] =
    map(_ => b)

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
   */
  def cached(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZQuery.cachingEnabled.getAndSet(true))(ZQuery.cachingEnabled.set)(_ => self)

  /**
   * Recovers from all errors.
   */
  def catchAll[R1 <: R, E2, A1 >: A](
    h: E => ZQuery[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R1, E2, A1] =
    self.foldQuery[R1, E2, A1](h, ZQuery.succeed(_))

  /**
   * Recovers from all errors with provided Cause.
   */
  def catchAllCause[R1 <: R, E2, A1 >: A](h: Cause[E] => ZQuery[R1, E2, A1])(implicit
    trace: Trace
  ): ZQuery[R1, E2, A1] =
    self.foldCauseQuery[R1, E2, A1](h, ZQuery.succeed(_))

  /**
   * Returns a query whose failure and success have been lifted into an
   * `Either`. The resulting query cannot fail, because the failure case has
   * been exposed as part of the `Either` success case.
   */
  final def either(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, Either[E, A]] =
    fold(Left(_), Right(_))

  /**
   * Ensures that if this query starts executing, the specified query will be
   * executed immediately after this query completes execution, whether by
   * success or failure.
   */
  final def ensuring[R1 <: R](finalizer: => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZQuery[R1, E, A] =
    ZQuery.acquireReleaseWith(ZIO.unit)(_ => finalizer)(_ => self)

  /**
   * Returns a query that models execution of this query, followed by passing
   * its result to the specified function that returns a query. Requests
   * composed with `flatMap` or combinators derived from it will be executed
   * sequentially and will not be pipelined, though deduplication and caching of
   * requests may still be applied.
   */
  final def flatMap[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => ZIO.succeed(Result.blocked(br, c.mapQuery(f)))
        case Result.Done(a)        => f(a).step
        case Result.Fail(e)        => ZIO.succeed(Result.fail(e))
      }
    }

  /**
   * Returns a query that performs the outer query first, followed by the inner
   * query, yielding the value of the inner query.
   *
   * This method can be used to "flatten" nested queries.
   */
  final def flatten[R1 <: R, E1 >: E, B](implicit
    ev: A IsSubtypeOfOutput ZQuery[R1, E1, B],
    trace: Trace
  ): ZQuery[R1, E1, B] =
    flatMap(ev)

  /**
   * Folds over the failed or successful result of this query to yield a query
   * that does not fail, but succeeds with the value returned by the left or
   * right function passed to `fold`.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZQuery[R, Nothing, B] =
    foldQuery(e => ZQuery.succeed(failure(e)), a => ZQuery.succeed(success(a)))

  /**
   * A more powerful version of `foldQuery` that allows recovering from any type
   * of failure except interruptions.
   */
  final def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.foldCauseZIO(
        failure(_).step,
        {
          case Result.Blocked(br, c) => ZIO.succeed(Result.blocked(br, c.foldCauseQuery(failure, success)))
          case Result.Done(a)        => success(a).step
          case Result.Fail(e)        => failure(e).step
        }
      )
    }

  /**
   * Recovers from errors by accepting one query to execute for the case of an
   * error, and one query to execute for the case of success.
   */
  final def foldQuery[R1 <: R, E1, B](failure: E => ZQuery[R1, E1, B], success: A => ZQuery[R1, E1, B])(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZQuery[R1, E1, B] =
    foldCauseQuery(_.failureOrCause.fold(failure, ZQuery.failCause(_)), success)

  /**
   * "Zooms in" on the value in the `Left` side of an `Either`, moving the
   * possibility that the value is a `Right` to the error channel.
   */
  final def left[B, C](implicit
    ev: A IsSubtypeOfOutput Either[B, C],
    trace: Trace
  ): ZQuery[R, Either[E, C], B] =
    self.foldQuery(
      e => ZQuery.fail(Left(e)),
      a => ev(a).fold(b => ZQuery.succeed(b), c => ZQuery.fail(Right(c)))
    )

  /**
   * Maps the specified function over the successful result of this query.
   */
  final def map[B](f: A => B)(implicit trace: Trace): ZQuery[R, E, B] =
    ZQuery(step.map(_.map(f)))

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`.
   */
  final def mapBoth[E1, B](f: E => E1, g: A => B)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, B] =
    foldQuery(e => ZQuery.fail(f(e)), a => ZQuery.succeed(g(a)))

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: => DataSourceAspect[R1])(implicit trace: Trace): ZQuery[R1, E, A] =
    ZQuery(step.map(_.mapDataSources(f)))

  /**
   * Maps the specified function over the failed result of this query.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, A] =
    mapBoth(f, identity)

  /**
   * Returns a query with its full cause of failure mapped using the specified
   * function. This can be used to transform errors while preserving the
   * original structure of `Cause`.
   */
  def mapErrorCause[E2](h: Cause[E] => Cause[E2])(implicit trace: Trace): ZQuery[R, E2, A] =
    self.foldCauseQuery(c => ZQuery.failCause(h(c)), ZQuery.succeed(_))

  /**
   * Maps the specified effectual function over the result of this query.
   */
  final def mapZIO[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    flatMap(a => ZQuery.fromZIO(f(a)))

  /**
   * Converts this query to one that returns `Some` if data sources return
   * results for all requests received and `None` otherwise.
   */
  final def optional(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    foldCauseQuery(
      _.stripSomeDefects { case _: QueryFailure => () }.fold[ZQuery[R, E, Option[A]]](ZQuery.none)(ZQuery.failCause(_)),
      ZQuery.some(_)
    )

  /**
   * Converts this query to one that dies if a query failure occurs.
   */
  final def orDie(implicit
    ev1: E IsSubtypeOfError Throwable,
    ev2: CanFail[E],
    trace: Trace
  ): ZQuery[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Converts this query to one that dies if a query failure occurs, using the
   * specified function to map the error to a `Throwable`.
   */
  final def orDieWith(f: E => Throwable)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, A] =
    foldQuery(e => ZQuery.die(f(e)), a => ZQuery.succeed(a))

  /**
   * Provides a layer to this query, which translates it to another level.
   */
  final def provideLayer[E1 >: E, R0](
    layer: => Described[ZLayer[R0, E1, R]]
  )(implicit trace: Trace): ZQuery[R0, E1, A] =
    ZQuery {
      ZIO.scoped[R0] {
        layer.value.build.exit.flatMap {
          case Exit.Failure(e) => ZIO.succeed(Result.fail(e))
          case Exit.Success(r) => self.provideEnvironment(Described(r, layer.description)).step
        }
      }
    }

  /**
   * Provides this query with its required environment.
   */
  final def provideEnvironment(
    r: => Described[ZEnvironment[R]]
  )(implicit trace: Trace): ZQuery[Any, E, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Splits the environment into two parts, providing one part using the
   * specified layer and leaving the remainder `R0`.
   */
  final def provideSomeLayer[R0]: ZQuery.ProvideSomeLayer[R0, R, E, A] =
    new ZQuery.ProvideSomeLayer(self)

  /**
   * Provides this query with part of its required environment.
   */
  final def provideSomeEnvironment[R0](
    f: => Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): ZQuery[R0, E, A] =
    ZQuery(step.map(_.provideSomeEnvironment(f)).provideSomeEnvironment((r => (f.value(r)))))

  /**
   * Races this query with the specified query, returning the result of the
   * first to complete successfully and safely interrupting the other.
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
                ZIO.succeed(Result.blocked(blockedRequests, Continue.effect(race(query, fiber))))
              case Continue.Get(io) =>
                ZIO.succeed(Result.blocked(blockedRequests, Continue.effect(race(ZQuery.fromZIO(io), fiber))))
            }
          case Result.Done(value) => fiber.interrupt *> ZIO.succeed(Result.done(value))
          case Result.Fail(cause) => fiber.join.map(_.mapErrorCause(_ && cause))
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
    self catchAll (err => (pf lift err).fold[ZQuery[R, E1, A]](ZQuery.die(f(err)))(ZQuery.fail(_)))

  /**
   * "Zooms in" on the value in the `Right` side of an `Either`, moving the
   * possibility that the value is a `Left` to the error channel.
   */
  final def right[B, C](implicit
    ev: A IsSubtypeOfOutput Either[B, C],
    trace: Trace
  ): ZQuery[R, Either[B, E], C] =
    self.foldQuery(
      e => ZQuery.fail(Right(e)),
      a => ev(a).fold(b => ZQuery.fail(Left(b)), c => ZQuery.succeed(c))
    )

  /**
   * Returns an effect that models executing this query.
   */
  final def run(implicit trace: Trace): ZIO[R, E, A] =
    runLog.map(_._2)

  /**
   * Returns an effect that models executing this query with the specified
   * cache.
   */
  final def runCache(cache: => Cache)(implicit trace: Trace): ZIO[R, E, A] = {

    def run(query: ZQuery[R, E, A]): ZIO[R, E, A] =
      query.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) => br.run *> run(c)
        case Result.Blocked(br, Continue.Get(io))   => br.run *> io
        case Result.Done(a)                         => ZIO.succeed(a)
        case Result.Fail(e)                         => ZIO.failCause(e)
      }

    ZIO.acquireReleaseExitWith {
      Scope.make
    } { (scope: Scope.Closeable, exit: Exit[E, A]) =>
      scope.close(exit)
    } { scope =>
      ZQuery.currentScope.locally(scope)(ZQuery.currentCache.locally(cache)(run(self)))
    }
  }

  /**
   * Returns an effect that models executing this query, returning the query
   * result along with the cache.
   */
  final def runLog(implicit trace: Trace): ZIO[R, E, (Cache, A)] =
    for {
      cache <- Cache.empty
      a     <- runCache(cache)
    } yield (cache, a)

  /**
   * Expose the full cause of failure of this query
   */
  def sandbox(implicit trace: Trace): ZQuery[R, Cause[E], A] =
    foldCauseQuery(ZQuery.fail(_), ZQuery.succeed(_))

  /**
   * Companion helper to `sandbox`. Allows recovery, and partial recovery, from
   * errors and defects alike, as in:
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
      e => ZQuery.fail(Some(e)),
      ev(_) match {
        case Some(b) => ZQuery.succeed(b)
        case None    => ZQuery.fail(None)
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
  final def someOrElseZIO[B, R1 <: R, E1 >: E](
    default: ZQuery[R1, E1, B]
  )(implicit ev: A <:< Option[B], trace: Trace): ZQuery[R1, E1, B] =
    self.flatMap(ev(_) match {
      case Some(value) => ZQuery.succeed(value)
      case None        => default
    })

  /**
   * Extracts the optional value or fails with the given error `e`.
   */
  final def someOrFail[B, E1 >: E](
    e: => E1
  )(implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZQuery[R, E1, B] =
    self.flatMap { a =>
      ev(a) match {
        case Some(b) => ZQuery.succeed(b)
        case None    => ZQuery.fail(e)
      }
    }

  /**
   * Summarizes a query by computing some value before and after execution, and
   * then combining the values to produce a summary, together with the result of
   * execution.
   */
  final def summarized[R1 <: R, E1 >: E, B, C](
    summary0: ZIO[R1, E1, B]
  )(f: (B, B) => C)(implicit trace: Trace): ZQuery[R1, E1, (C, A)] =
    ZQuery.suspend {
      val summary = summary0
      for {
        start <- ZQuery.fromZIO(summary)
        value <- self
        end   <- ZQuery.fromZIO(summary)
      } yield (f(start, end), value)
    }

  /**
   * Returns a new query that executes this one and times the execution.
   */
  final def timed(implicit trace: Trace): ZQuery[R, E, (Duration, A)] =
    summarized(Clock.nanoTime)((start, end) => Duration.fromNanos(end - start))

  /**
   * Returns an effect that will timeout this query, returning `None` if the
   * timeout elapses before the query was completed.
   */
  final def timeout(duration: => Duration)(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    timeoutTo(None)(Some(_))(duration)

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event of
   * timeout, it will produce the specified error.
   */
  final def timeoutFail[E1 >: E](e: => E1)(duration: => Duration)(implicit
    trace: Trace
  ): ZQuery[R, E1, A] =
    timeoutTo(ZQuery.fail(e))(ZQuery.succeed(_))(duration).flatten

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event of
   * timeout, it will produce the specified failure.
   */
  final def timeoutFailCause[E1 >: E](cause: => Cause[E1])(duration: => Duration)(implicit
    trace: Trace
  ): ZQuery[R, E1, A] =
    timeoutTo(ZQuery.failCause(cause))(ZQuery.succeed(_))(duration).flatten

  /**
   * Returns a query that will timeout this query, returning either the default
   * value if the timeout elapses before the query has completed or the result
   * of applying the function `f` to the successful result of the query.
   */
  final def timeoutTo[B](b: => B): ZQuery.TimeoutTo[R, E, A, B] =
    new ZQuery.TimeoutTo(self, () => b)

  /**
   * Disables caching for this query.
   */
  def uncached(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZQuery.cachingEnabled.getAndSet(false))(ZQuery.cachingEnabled.set)(_ => self)

  /**
   * Converts a `ZQuery[R, Either[E, B], A]` into a `ZQuery[R, E, Either[A,
   * B]]`. The inverse of `left`.
   */
  final def unleft[E1, B](implicit
    ev: E IsSubtypeOfError Either[E1, B],
    trace: Trace
  ): ZQuery[R, E1, Either[A, B]] =
    self.foldQuery(
      e => ev(e).fold(e1 => ZQuery.fail(e1), b => ZQuery.succeed(Right(b))),
      a => ZQuery.succeed(Left(a))
    )

  /**
   * Converts an option on errors into an option on values.
   */
  final def unoption[E1](implicit ev: E IsSubtypeOfError Option[E1], trace: Trace): ZQuery[R, E1, Option[A]] =
    self.foldQuery(
      e => ev(e).fold[ZQuery[R, E1, Option[A]]](ZQuery.succeed(Option.empty[A]))(ZQuery.fail(_)),
      a => ZQuery.succeed(Some(a))
    )

  /**
   * Takes some fiber failures and converts them into errors.
   */
  final def unrefine[E1 >: E](pf: PartialFunction[Throwable, E1])(implicit trace: Trace): ZQuery[R, E1, A] =
    unrefineWith(pf)(identity)

  /**
   * Takes some fiber failures and converts them into errors.
   */
  final def unrefineTo[E1 >: E: ClassTag](implicit trace: Trace): ZQuery[R, E1, A] =
    unrefine { case e: E1 => e }

  /**
   * Takes some fiber failures and converts them into errors, using the
   * specified function to convert the `E` into an `E1`.
   */
  final def unrefineWith[E1](
    pf: PartialFunction[Throwable, E1]
  )(f: E => E1)(implicit trace: Trace): ZQuery[R, E1, A] =
    catchAllCause { cause =>
      cause.find {
        case Cause.Die(t, _) if pf.isDefinedAt(t) => pf(t)
      }.fold(ZQuery.failCause(cause.map(f)))(ZQuery.fail(_))
    }

  /**
   * Converts a `ZQuery[R, Either[B, E], A]` into a `ZQuery[R, E, Either[B,
   * A]]`. The inverse of `right`.
   */
  final def unright[E1, B](implicit
    ev: E IsSubtypeOfError Either[B, E1],
    trace: Trace
  ): ZQuery[R, E1, Either[B, A]] =
    self.foldQuery(
      e => ev(e).fold(b => ZQuery.succeed(Left(b)), e1 => ZQuery.fail(e1)),
      a => ZQuery.succeed(Right(a))
    )

  /**
   * Sets the parallelism for this query to the specified maximum number of
   * fibers.
   */
  def withParallelism(n: => Int)(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZIO.Parallelism.getAndSet(Some(n)))(ZIO.Parallelism.set)(_ => self)

  /**
   * Sets the parallelism for this query to the specified maximum number of
   * fibers.
   */
  def withParallelismUnbounded(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.acquireReleaseWith(ZIO.Parallelism.getAndSet(None))(ZIO.Parallelism.set)(_ => self)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results into a tuple.
   */
  final def zip[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWith(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and combining their results into a
   * tuple.
   */
  final def zipBatched[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithBatched(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of this
   * query.
   */
  final def zipBatchedLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, A] =
    zipWithBatched(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of the
   * specified query.
   */
  final def zipBatchedRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    zipWithBatched(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of this query.
   */
  final def zipLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, A] =
    zipWith(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results into a tuple.
   */
  final def zipPar[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B],
    trace: Trace
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithPar(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of this query.
   */
  final def zipParLeft[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, A] =
    zipWithPar(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of the specified query.
   */
  final def zipParRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    zipWithPar(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of the specified query.
   */
  final def zipRight[R1 <: R, E1 >: E, B](that: => ZQuery[R1, E1, B])(implicit
    trace: Trace
  ): ZQuery[R1, E1, B] =
    ZQuery {
      self.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) =>
          ZIO.succeed(Result.blocked(br, Continue.effect(c.zipRight(that))))
        case Result.Blocked(br1, c1) =>
          that.step.map {
            case Result.Blocked(br2, c2) => Result.blocked(br1 ++ br2, c1.zipRight(c2))
            case Result.Done(b)          => Result.blocked(br1, c1.map(_ => b))
            case Result.Fail(e)          => Result.fail(e)
          }
        case Result.Done(_) => that.step
        case Result.Fail(e) => ZIO.succeed(Result.fail(e))
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results with the specified function.
   * Requests composed with `zipWith` or combinators derived from it will
   * automatically be pipelined.
   */
  final def zipWith[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) =>
          ZIO.succeed(Result.blocked(br, Continue.effect(c.zipWith(that)(f))))
        case Result.Blocked(br1, c1) =>
          that.step.map {
            case Result.Blocked(br2, c2) => Result.blocked(br1 ++ br2, c1.zipWith(c2)(f))
            case Result.Done(b)          => Result.blocked(br1, c1.map(a => f(a, b)))
            case Result.Fail(e)          => Result.fail(e)
          }
        case Result.Done(a) =>
          that.step.map {
            case Result.Blocked(br, c) => Result.blocked(br, c.map(b => f(a, b)))
            case Result.Done(b)        => Result.done(f(a, b))
            case Result.Fail(e)        => Result.fail(e)
          }
        case Result.Fail(e) => ZIO.succeed(Result.fail(e))
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources.
   */
  final def zipWithBatched[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.zipWith(that.step) {
        case (Result.Blocked(br1, c1), Result.Blocked(br2, c2)) => Result.blocked(br1 && br2, c1.zipWithBatched(c2)(f))
        case (Result.Blocked(br, c), Result.Done(b))            => Result.blocked(br, c.map(a => f(a, b)))
        case (Result.Done(a), Result.Blocked(br, c))            => Result.blocked(br, c.map(b => f(a, b)))
        case (Result.Done(a), Result.Done(b))                   => Result.done(f(a, b))
        case (Result.Fail(e1), Result.Fail(e2))                 => Result.fail(Cause.Both(e1, e2))
        case (Result.Fail(e), _)                                => Result.fail(e)
        case (_, Result.Fail(e))                                => Result.fail(e)
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results with the specified function.
   * Requests composed with `zipWithPar` or combinators derived from it will
   * automatically be batched.
   */
  final def zipWithPar[R1 <: R, E1 >: E, B, C](
    that: => ZQuery[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.zipWithPar(that.step) {
        case (Result.Blocked(br1, c1), Result.Blocked(br2, c2)) => Result.blocked(br1 && br2, c1.zipWithPar(c2)(f))
        case (Result.Blocked(br, c), Result.Done(b))            => Result.blocked(br, c.map(a => f(a, b)))
        case (Result.Done(a), Result.Blocked(br, c))            => Result.blocked(br, c.map(b => f(a, b)))
        case (Result.Done(a), Result.Done(b))                   => Result.done(f(a, b))
        case (Result.Fail(e1), Result.Fail(e2))                 => Result.fail(Cause.Both(e1, e2))
        case (Result.Fail(e), _)                                => Result.fail(e)
        case (_, Result.Fail(e))                                => Result.fail(e)
      }
    }
}

object ZQuery {

  final def absolve[R, E, A](v: => ZQuery[R, E, Either[E, A]])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.suspend(v).flatMap(fromEither(_))

  /**
   * Acquires the specified resource before the query begins execution and
   * releases it after the query completes execution, whether by success,
   * failure, or interruption.
   */
  def acquireReleaseExitWith[R, E, A](acquire: => ZIO[R, E, A]): ZQuery.AcquireExit[R, E, A] =
    new ZQuery.AcquireExit(() => acquire)

  /**
   * Acquires the specified resource before the query begins execution and
   * releases it after the query completes execution, whether by success,
   * failure, or interruption.
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
    foreach(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreach(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Array[A]] =
    foreach(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: Option[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    foreach(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreach(as)(identity)

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
    foreachBatched(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreachBatched(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[A]] =
    foreachBatched(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreachBatched(as)(identity)

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
    foreachPar(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A](as: Set[ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, Set[A]] =
    foreachPar(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A: ClassTag](as: Array[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[A]] =
    foreachPar(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A](as: NonEmptyChunk[ZQuery[R, E, A]])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[A]] =
    foreachPar(as)(identity)

  /**
   * Constructs a query that dies with the specified error.
   */
  def die(t: => Throwable)(implicit trace: Trace): ZQuery[Any, Nothing, Nothing] =
    ZQuery(ZIO.die(t))

  /**
   * Accesses the whole environment of the query.
   */
  def environment[R](implicit trace: Trace): ZQuery[R, Nothing, ZEnvironment[R]] =
    ZQuery.fromZIO(ZIO.environment)

  /**
   * Accesses the environment of the effect.
   * {{{
   * val portNumber = effect.access(_.config.portNumber)
   * }}}
   */
  final def environmentWith[R]: EnvironmentWithPartiallyApplied[R] =
    new EnvironmentWithPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  final def environmentWithQuery[R]: EnvironmentWithQueryPartiallyApplied[R] =
    new EnvironmentWithQueryPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  final def environmentWithZIO[R]: EnvironmentWithZIOPartiallyApplied[R] =
    new EnvironmentWithZIOPartiallyApplied[R]

  /**
   * Constructs a query that fails with the specified error.
   */
  def fail[E](error: => E)(implicit trace: Trace): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(Cause.fail(error))))

  /**
   * Constructs a query that fails with the specified cause.
   */
  def failCause[E](cause: => Cause[E])(implicit trace: Trace): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(cause)))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed sequentially and will be pipelined.
   */
  def foreach[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result())
    else {
      val iterator                                         = as.iterator
      var builder: ZQuery[R, E, Builder[B, Collection[B]]] = null
      while (iterator.hasNext) {
        val a = iterator.next()
        if (builder eq null) builder = f(a).map(bf.newBuilder(as) += _)
        else builder = builder.zipWith(f(a))(_ += _)
      }
      builder.map(_.result())
    }

  /**
   * Applies the function `f` to each element of the `Set[A]` and returns the
   * results in a new `Set[B]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B](in: Set[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Set[B]] =
    foreach[R, E, A, B, Iterable](in)(f).map(_.toSet)

  /**
   * Applies the function `f` to each element of the `Array[A]` and returns the
   * results in a new `Array[B]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B: ClassTag](in: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreach[R, E, A, B, Iterable](in)(f).map(_.toArray)

  /**
   * Applies the function `f` to each element of the `Map[Key, Value]` and
   * returns the results in a new `Map[Key2, Value2]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreach_` for a more efficient implementation.
   */
  def foreach[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)])(implicit trace: Trace): ZQuery[R, E, Map[Key2, Value2]] =
    foreach[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Applies the function `f` if the argument is non-empty and returns the
   * results in a new `Option[B]`.
   */
  final def foreach[R, E, A, B](in: Option[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Option[B]] =
    in.fold[ZQuery[R, E, Option[B]]](none)(f(_).map(Some(_)))

  /**
   * Applies the function `f` to each element of the `NonEmptyChunk[A]` and
   * returns the results in a new `NonEmptyChunk[B]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B](in: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreach[R, E, A, B, Chunk](in)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, batching requests to
   * data sources and collecting the results into a query returning a collection
   * of their results.
   */
  def foreachBatched[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result())
    else
      ZQuery(
        ZIO.suspendSucceed {
          var blockedRequests: BlockedRequests[R]          = BlockedRequests.empty
          val doneBuilder: Builder[B, Collection[B]]       = bf.newBuilder(as)
          val doneIndicesBuilder: ChunkBuilder[Int]        = new ChunkBuilder.Int
          val effectBuilder: ChunkBuilder[ZQuery[R, E, B]] = ChunkBuilder.make[ZQuery[R, E, B]]()
          val effectIndicesBuilder: ChunkBuilder[Int]      = new ChunkBuilder.Int
          val failBuilder: ChunkBuilder[Cause[E]]          = ChunkBuilder.make[Cause[E]]()
          val getBuilder: ChunkBuilder[IO[E, B]]           = ChunkBuilder.make[IO[E, B]]()
          val getIndicesBuilder: ChunkBuilder[Int]         = new ChunkBuilder.Int
          var index: Int                                   = 0
          val iterator: Iterator[A]                        = as.iterator

          ZIO.whileLoop {
            iterator.hasNext
          } {
            f(iterator.next()).step
          } {
            case Result.Blocked(blockedRequest, Continue.Effect(query)) =>
              blockedRequests = blockedRequests && blockedRequest
              effectBuilder += query
              effectIndicesBuilder += index
              index += 1
            case Result.Blocked(blockedRequest, Continue.Get(io)) =>
              blockedRequests = blockedRequests && blockedRequest
              getBuilder += io
              getIndicesBuilder += index
              index += 1
            case Result.Done(b) =>
              doneBuilder += b
              doneIndicesBuilder += index
              index += 1
            case Result.Fail(e) =>
              failBuilder += e
              index += 1
          }.as {
            val dones         = doneBuilder.result()
            val doneIndices   = doneIndicesBuilder.result()
            val effects       = effectBuilder.result()
            val effectIndices = effectIndicesBuilder.result()
            val fails         = failBuilder.result()
            val gets          = getBuilder.result()
            val getIndices    = getIndicesBuilder.result()
            if (gets.isEmpty && effects.isEmpty && fails.isEmpty)
              Result.done(bf.fromSpecific(as)(dones))
            else if (fails.isEmpty) {
              val continue = if (effects.isEmpty) {
                val io = ZIO.collectAll(gets).map { gets =>
                  val array              = Array.ofDim[AnyRef](index)
                  val getsIterator       = gets.iterator
                  val getIndicesIterator = getIndices.iterator
                  while (getsIterator.hasNext) {
                    val get   = getsIterator.next()
                    val index = getIndicesIterator.next()
                    array(index) = get.asInstanceOf[AnyRef]
                  }
                  val donesIterator       = dones.iterator
                  val doneIndicesIterator = doneIndices.iterator
                  while (donesIterator.hasNext) {
                    val done  = donesIterator.next()
                    val index = doneIndicesIterator.next()
                    array(index) = done.asInstanceOf[AnyRef]
                  }
                  bf.fromSpecific(as)(array.asInstanceOf[Array[B]])
                }
                Continue.get(io)
              } else {
                val query = ZQuery.collectAllBatched(effects).flatMap { effects =>
                  ZQuery.fromZIO(ZIO.collectAll(gets).map { gets =>
                    val array                 = Array.ofDim[AnyRef](index)
                    val effectsIterator       = effects.iterator
                    val effectIndicesIterator = effectIndices.iterator
                    while (effectsIterator.hasNext) {
                      val effect = effectsIterator.next()
                      val index  = effectIndicesIterator.next()
                      array(index) = effect.asInstanceOf[AnyRef]
                    }
                    val getsIterator       = gets.iterator
                    val getIndicesIterator = getIndices.iterator
                    while (getsIterator.hasNext) {
                      val get   = getsIterator.next()
                      val index = getIndicesIterator.next()
                      array(index) = get.asInstanceOf[AnyRef]
                    }
                    val donesIterator       = dones.iterator
                    val doneIndicesIterator = doneIndices.iterator
                    while (donesIterator.hasNext) {
                      val done  = donesIterator.next()
                      val index = doneIndicesIterator.next()
                      array(index) = done.asInstanceOf[AnyRef]
                    }
                    bf.fromSpecific(as)(array.asInstanceOf[Array[B]])
                  })
                }
                Continue.effect(query)
              }
              Result.blocked(blockedRequests, continue)
            } else
              Result.fail(fails.foldLeft[Cause[E]](Cause.empty)(_ && _))
          }
        }
      )

  final def foreachBatched[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Set[B]] =
    foreachBatched[R, E, A, B, Iterable](as)(fn).map(_.toSet)

  /**
   * Performs a query for each element in an Array, batching requests to data
   * sources and collecting the results into a query returning a collection of
   * their results.
   *
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachBatched[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachBatched[R, E, A, B, Iterable](as)(f).map(_.toArray)

  /**
   * Performs a query for each element in a Map, batching requests to data
   * sources and collecting the results into a query returning a collection of
   * their results.
   *
   * For a sequential version of this method, see `foreach`.
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
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachBatched[R, E, A, B](as: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachBatched[R, E, A, B, Chunk](as)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   */
  def foreachPar[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(
    f: A => ZQuery[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] =
    ZQuery.suspend {
      val size = as.size
      if (size == 0)
        ZQuery.succeed(bf.newBuilder(as).result())
      else if (size == 1)
        f(as.head).map(bf.newBuilder(as) += _).map(_.result())
      else
        ZQuery(
          ZIO
            .foreachPar[R, Nothing, A, Result[R, E, B], Iterable](as)(f(_).step)
            .map(Result.collectAllPar(_).map(bf.fromSpecific(as)))
        )
    }

  /**
   * Performs a query for each element in a Set, collecting the results into a
   * query returning a collection of their results. Requests will be executed in
   * parallel and will be batched.
   */
  final def foreachPar[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B])(implicit
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
  final def foreachPar[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B])(implicit
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
  final def foreachPar[R, E, A, B](as: NonEmptyChunk[A])(fn: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachPar[R, E, A, B, Chunk](as)(fn).map(NonEmptyChunk.nonEmpty)

  /**
   * Constructs a query from an either
   */
  def fromEither[E, A](either: => Either[E, A])(implicit trace: Trace): ZQuery[Any, E, A] =
    ZQuery.succeed(either).flatMap(_.fold[ZQuery[Any, E, A]](ZQuery.fail(_), ZQuery.succeed(_)))

  /**
   * Constructs a query from an option
   */
  def fromOption[A](option: => Option[A])(implicit trace: Trace): ZQuery[Any, Option[Nothing], A] =
    ZQuery.succeed(option).flatMap(_.fold[ZQuery[Any, Option[Nothing], A]](ZQuery.fail(None))(ZQuery.succeed(_)))

  /**
   * Constructs a query from a request and a data source. Queries will die with
   * a `QueryFailure` when run if the data source does not provide results for
   * all requests received. Queries must be constructed with `fromRequest` or
   * one of its variants for optimizations to be applied.
   */
  def fromRequest[R, E, A, B](
    request0: => A
  )(dataSource0: => DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, B] =
    ZQuery {
      ZIO.suspendSucceed {
        val request    = request0
        val dataSource = dataSource0
        ZQuery.cachingEnabled.get.flatMap { cachingEnabled =>
          if (cachingEnabled) {
            ZQuery.currentCache.get.flatMap { cache =>
              cache.lookup(request).flatMap {
                case Left(promise) =>
                  ZIO.succeed(
                    Result.blocked(
                      BlockedRequests.single(dataSource, BlockedRequest(request, promise)),
                      Continue(request, dataSource, promise)
                    )
                  )
                case Right(promise) =>
                  promise.poll.flatMap {
                    case None =>
                      ZIO.succeed(Result.blocked(BlockedRequests.empty, Continue(request, dataSource, promise)))
                    case Some(io) =>
                      io.exit.map(Result.fromExit)
                  }
              }
            }
          } else {
            Promise.make[E, B].map { promise =>
              Result.blocked(
                BlockedRequests.single(dataSource, BlockedRequest(request, promise)),
                Continue(request, dataSource, promise)
              )
            }
          }
        }
      }
    }

  /**
   * Constructs a query from a request and a data source but does not apply
   * caching to the query.
   */
  def fromRequestUncached[R, E, A, B](
    request: => A
  )(dataSource: => DataSource[R, A])(implicit ev: A <:< Request[E, B], trace: Trace): ZQuery[R, E, B] =
    fromRequest(request)(dataSource).uncached

  /**
   * Constructs a query from an effect.
   */
  def fromZIO[R, E, A](effect: => ZIO[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery(ZIO.suspendSucceed(effect).foldCause(Result.fail, Result.done))

  /**
   * Constructs a query that never completes.
   */
  def never(implicit trace: Trace): ZQuery[Any, Nothing, Nothing] =
    ZQuery.fromZIO(ZIO.never)

  /**
   * Constructs a query that succeds with the empty value.
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
    ZQuery.foreach(as)(f(_).either).map(partitionMap(_)(identity))

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
    ZQuery.foreachPar(as)(f(_).either).map(partitionMap(_)(identity))

  /**
   * Accesses the whole environment of the query.
   */
  def service[R: Tag](implicit trace: Trace): ZQuery[R, Nothing, R] =
    ZQuery.fromZIO(ZIO.service)

  /**
   * Accesses the environment of the effect.
   * {{{
   * val portNumber = effect.access(_.config.portNumber)
   * }}}
   */
  final def serviceWith[R]: ServiceWithPartiallyApplied[R] =
    new ServiceWithPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  final def serviceWithQuery[R]: ServiceWithQueryPartiallyApplied[R] =
    new ServiceWithQueryPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  final def serviceWithZIO[R]: ServiceWithZIOPartiallyApplied[R] =
    new ServiceWithZIOPartiallyApplied[R]

  /**
   * Constructs a query that succeeds with the optional value.
   */
  def some[A](a: => A)(implicit trace: Trace): ZQuery[Any, Nothing, Option[A]] =
    succeed(Some(a))

  /**
   * Constructs a query that succeeds with the specified value.
   */
  def succeed[A](value: => A)(implicit trace: Trace): ZQuery[Any, Nothing, A] =
    ZQuery(ZIO.succeed(Result.done(value)))

  /**
   * Returns a lazily constructed query.
   */
  def suspend[R, E, A](query: => ZQuery[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.unit.flatMap(_ => query)

  /**
   * The query that succeeds with the unit value.
   */
  val unit: ZQuery[Any, Nothing, Unit] =
    ZQuery.succeed(())(Trace.empty)

  /**
   * The inverse operation [[ZQuery.sandbox]]
   *
   * Terminates with exceptions on the `Left` side of the `Either` error, if it
   * exists. Otherwise extracts the contained `IO[E, A]`
   */
  def unsandbox[R, E, A](v: => ZQuery[R, Cause[E], A])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.suspend(v).mapErrorCause(_.flatten)

  /**
   * Unwraps a query that is produced by an effect.
   */
  def unwrap[R, E, A](zio: => ZIO[R, E, ZQuery[R, E, A]])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.fromZIO(zio).flatten

  final class EnvironmentWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](f: ZEnvironment[R] => A)(implicit trace: Trace): ZQuery[R, Nothing, A] =
      environment[R].map(f)
  }

  final class EnvironmentWithQueryPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[E, A](f: ZEnvironment[R] => ZQuery[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
      environment[R].flatMap(f)
  }

  final class EnvironmentWithZIOPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[E, A](f: ZEnvironment[R] => ZIO[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
      environment[R].mapZIO(f)
  }

  final class ProvideSomeLayer[R0, -R, +E, +A](private val self: ZQuery[R, E, A]) extends AnyVal {
    def apply[E1 >: E, R1](
      layer: => Described[ZLayer[R0, E1, R1]]
    )(implicit ev: R0 with R1 <:< R, tag: Tag[R1], trace: Trace): ZQuery[R0, E1, A] =
      self
        .asInstanceOf[ZQuery[R0 with R1, E, A]]
        .provideLayer(Described(ZLayer.environment[R0] ++ layer.value, layer.description))
  }

  final class TimeoutTo[-R, +E, +A, +B](self: ZQuery[R, E, A], b: () => B) {
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
                cause => rightFiber.interrupt *> ZIO.succeed(Result.fail(cause)),
                result =>
                  result match {
                    case Result.Blocked(blockedRequests, continue) =>
                      continue match {
                        case Continue.Effect(query) =>
                          ZIO.succeed(Result.blocked(blockedRequests, Continue.effect(race(query, fiber))))
                        case Continue.Get(io) =>
                          ZIO.succeed(
                            Result.blocked(blockedRequests, Continue.effect(race(ZQuery.fromZIO(io), fiber)))
                          )
                      }
                    case Result.Done(value) => rightFiber.interrupt *> ZIO.succeed(Result.done(value))
                    case Result.Fail(cause) => rightFiber.interrupt *> ZIO.succeed(Result.fail(cause))
                  }
              ),
            (rightExit, leftFiber) => leftFiber.interrupt *> ZIO.succeed(Result.fromExit(rightExit))
          )
        }

      ZQuery.fromZIO(ZIO.sleep(duration).interruptible.as(b()).fork).flatMap(fiber => race(self.map(f), fiber))
    }
  }

  final class ServiceWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](
      f: R => A
    )(implicit tag: Tag[R], trace: Trace): ZQuery[R, Nothing, A] =
      service[R].map(f)
  }

  final class ServiceWithQueryPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {
    def apply[R <: Service, E, A](
      f: Service => ZQuery[R, E, A]
    )(implicit tag: Tag[Service], trace: Trace): ZQuery[R with Service, E, A] =
      service[Service].flatMap(f)
  }

  final class ServiceWithZIOPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {
    def apply[R <: Service, E, A](
      f: Service => ZIO[R, E, A]
    )(implicit tag: Tag[Service], trace: Trace): ZQuery[R with Service, E, A] =
      service[Service].mapZIO(f)
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
        case Left(b)  => bs += b
        case Right(c) => cs += c
      }
    }
    (bs.result(), cs.result())
  }

  val cachingEnabled: FiberRef[Boolean] =
    FiberRef.unsafe.make(true)(Unsafe.unsafe)

  val currentCache: FiberRef[Cache] =
    FiberRef.unsafe.make(Cache.unsafeMake())(Unsafe.unsafe)

  val currentScope: FiberRef[Scope] =
    FiberRef.unsafe.make[Scope](Scope.global)(Unsafe.unsafe)

  final class Acquire[-R, +E, +A](private val acquire: () => ZIO[R, E, A]) extends AnyVal {
    def apply[R1](release: A => URIO[R1, Any]): Release[R with R1, E, A] =
      new Release[R with R1, E, A](acquire, release)
  }
  final class Release[-R, +E, +A](acquire: () => ZIO[R, E, A], release: A => URIO[R, Any]) {
    def apply[R1 <: R, E1 >: E, B](use: A => ZQuery[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
      acquireReleaseExitWith(acquire())((a: A, _: Exit[E1, B]) => release(a))(use)
  }

  final class AcquireExit[-R, +E, +A](private val acquire: () => ZIO[R, E, A]) extends AnyVal {
    def apply[R1 <: R, E1 >: E, B](
      release: (A, Exit[E1, B]) => URIO[R1, Any]
    ): ReleaseExit[R1, E, E1, A, B] =
      new ReleaseExit(acquire, release)
  }
  final class ReleaseExit[-R, +E, E1, +A, B](
    acquire: () => ZIO[R, E, A],
    release: (A, Exit[E1, B]) => URIO[R, Any]
  ) {
    def apply[R1 <: R, E2 >: E <: E1, B1 <: B](use: A => ZQuery[R1, E2, B1])(implicit
      trace: Trace
    ): ZQuery[R1, E2, B1] =
      ZQuery.unwrap {
        ZQuery.currentScope.getWith { scope =>
          ZIO.environmentWithZIO[R] { environment =>
            Ref.make(true).flatMap { ref =>
              ZIO.uninterruptible {
                ZIO.suspendSucceed(acquire()).tap { a =>
                  scope.addFinalizerExit {
                    case Exit.Failure(cause) =>
                      release(a, Exit.failCause(cause.stripFailures))
                        .provideEnvironment(environment)
                        .whenZIO(ref.getAndSet(false))
                    case Exit.Success(_) =>
                      ZIO.unit
                  }
                }
              }.map { a =>
                ZQuery
                  .suspend(use(a))
                  .foldCauseQuery(
                    cause =>
                      ZQuery.fromZIO {
                        ZIO
                          .suspendSucceed(release(a, Exit.failCause(cause)))
                          .whenZIO(ref.getAndSet(false))
                          .mapErrorCause(cause ++ _) *>
                          ZIO.refailCause(cause)
                      },
                    b =>
                      ZQuery.fromZIO {
                        ZIO
                          .suspendSucceed(release(a, Exit.succeed(b)))
                          .whenZIO(ref.getAndSet(false)) *>
                          ZIO.succeed(b)
                      }
                  )
              }
            }
          }
        }
      }
  }
}
