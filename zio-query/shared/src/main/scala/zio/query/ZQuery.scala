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
import zio.stacktracer.TracingImplicits.disableAutoTrace

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
   * Extracts the value of this ZQuery as an exit if the result of the query is
   * known, and it does not contain any side effects.
   *
   * This applies to queries that have been constructed via
   * [[ZQuery.succeedNow]] and other xNow constructors. The only exception is
   * [[ZQuery.fromZIONow]], which is assumed to contain side effects unless the
   * provided effect was an [[zio.Exit]]
   */
  def asExitMaybe: Option[Exit[E, A]] =
    Option(asExitOrElse(null))

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
      case s =>
        val acq = ZQuery.disabledCache.set(None) *> ZQuery.currentCache.set(s)
        val rel = ZQuery.disabledCache.set(s) *> ZQuery.currentCache.set(None)
        ZQuery.acquireReleaseWith(acq)(_ => rel)(_ => self)
    })

  /**
   * Recovers from all errors.
   */
  def catchAll[R1 <: R, E2, A1 >: A](
    h: E => ZQuery[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R1, E2, A1] =
    self.foldQuery[R1, E2, A1](h, ZQuery.succeedNow)

  /**
   * Recovers from all errors with provided Cause.
   */
  def catchAllCause[R1 <: R, E2, A1 >: A](h: Cause[E] => ZQuery[R1, E2, A1])(implicit
    trace: Trace
  ): ZQuery[R1, E2, A1] =
    self.foldCauseQuery[R1, E2, A1](h, ZQuery.succeedNow)

  /**
   * Recovers from all errors and maps the provided Cause to a ZIO effect
   */
  def catchAllCauseZIO[R1 <: R, E2, A1 >: A](h: Cause[E] => ZIO[R1, E2, A1])(implicit
    trace: Trace
  ): ZQuery[R1, E2, A1] =
    foldCauseZIO(h, Exit.succeed)

  /**
   * Catches all errors and maps them to a ZIO effect
   */
  def catchAllZIO[R1 <: R, E2, A1 >: A](
    h: E => ZIO[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZQuery[R1, E2, A1] =
    catchAllCauseZIO(_.failureOrCause.fold(h, Exit.failCause))

  /**
   * Returns a query whose failure and success have been lifted into an
   * `Either`. The resulting query cannot fail, because the failure case has
   * been exposed as part of the `Either` success case.
   */
  def either(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, Either[E, A]] =
    fold(Left(_), Right(_))

  /**
   * Ensures that if this query starts executing, the specified query will be
   * executed immediately after this query completes execution, whether by
   * success or failure.
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
        case Result.Fail(e)        => Result.failExit(e)
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
   * that does not fail, but succeeds with the value returned by the left or
   * right function passed to `fold`.
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
    foldCauseQuery(_.failureOrCause.fold(failure, ZQuery.failCause(_)), success)

  /**
   * Recovers from errors by accepting one ZIO effect to execute for the case of
   * an error, and one ZIO effect to execute for the case of success.
   */
  def foldZIO[R1 <: R, E1, B](
    failure: E => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): ZQuery[R1, E1, B] =
    foldCauseZIO(_.failureOrCause.fold(failure, Exit.failCause), success)

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
   * Returns an effect that, that if evaluated, will return a lazily computed
   * version of this query
   *
   * This differs from query caching, as caching will only cache the output of a
   * [[DataSource]]. `memoize` will ensure that the query (including
   * non-DataSource backed queries) is computed at-most-once.
   *
   * This can beneficial for cases that a query is composed of multiple queries
   * and it's reused multiple times, e.g.,
   *
   * {{{
   *    case class Foo(x: UQuery[Int], y: UQuery[Int])
   *    val query: UQuery[Int] = ???
   *
   *    // Query will be run exactly once; might not be necessary if `x` or `y` are not used afterwards
   *    query.map(i => Foo(ZQuery.succeed(i +1), ZQuery.succeed(i + 2)))
   *
   *    // Query will be recomputed each time x or y are used
   *    Foo(query.map(_ + 1), query.map(_ + 2))
   *
   *    // Query will be computed / run at-most-once
   *    query.memoize.map(q => Foo(q.map(_ + 1), q.map(_ + 2)))
   *
   * }}}
   */
  def memoize(implicit trace: Trace): UIO[ZQuery[R, E, A]] =
    ZIO.succeed(ZQuery.unsafe.memoize(self)(Unsafe.unsafe, trace))

  /**
   * Maps the specified function over the successful result of this query.
   */
  def map[B](f: A => B)(implicit trace: Trace): ZQuery[R, E, B] =
    ZQuery(step.map(_.map(f)))

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`.
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
   */
  def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, E1, A] =
    mapBoth(f, identity)

  /**
   * Returns a query with its full cause of failure mapped using the specified
   * function. This can be used to transform errors while preserving the
   * original structure of `Cause`.
   */
  def mapErrorCause[E2](h: Cause[E] => Cause[E2])(implicit trace: Trace): ZQuery[R, E2, A] =
    mapBothCause(h, ZIO.identityFn)

  /**
   * Maps the specified effectual function over the result of this query.
   */
  def mapZIO[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B])(implicit trace: Trace): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => Result.blockedExit(br, c.mapZIO(f))
        case Result.Done(a)        => f(a).foldCauseZIO(Result.failExit, Result.doneExit)
        case f: Result.Fail[E1]    => Exit.succeed(f)
      }
    }

  /**
   * Converts this query to one that returns `Some` if data sources return
   * results for all requests received and `None` otherwise.
   */
  def optional(implicit trace: Trace): ZQuery[R, E, Option[A]] =
    foldCauseQuery(
      _.stripSomeDefects { case _: QueryFailure => () }.fold[ZQuery[R, E, Option[A]]](ZQuery.none)(ZQuery.failCause(_)),
      ZQuery.some(_)
    )

  /**
   * Converts this query to one that dies if a query failure occurs.
   */
  def orDie(implicit
    ev1: E IsSubtypeOfError Throwable,
    ev2: CanFail[E],
    trace: Trace
  ): ZQuery[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Converts this query to one that dies if a query failure occurs, using the
   * specified function to map the error to a `Throwable`.
   */
  def orDieWith(f: E => Throwable)(implicit ev: CanFail[E], trace: Trace): ZQuery[R, Nothing, A] =
    foldQuery(e => ZQuery.die(f(e)), a => ZQuery.succeedNow(a))

  /**
   * Provides a layer to this query, which translates it to another level.
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
   * specified layer and leaving the remainder `R0`.
   */
  def provideSomeLayer[R0]: ZQuery.ProvideSomeLayer[R0, R, E, A] =
    new ZQuery.ProvideSomeLayer(self)

  /**
   * Provides this query with part of its required environment.
   */
  def provideSomeEnvironment[R0](
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
                Result.blockedExit(blockedRequests, Continue.effect(race(query, fiber)))
              case Continue.Get(io) =>
                Result.blockedExit(blockedRequests, Continue.effect(race(ZQuery.fromZIONow(io), fiber)))
            }
          case Result.Done(value) => fiber.interrupt *> Result.doneExit(value)
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
   */
  def run(implicit trace: Trace): ZIO[R, E, A] =
    runCache(Cache.unsafeMake())

  /**
   * Returns an effect that models executing this query with the specified
   * cache.
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
              val curRefs = state.getFiberRefs(false)
              if (curRefs eq newRefs) {
                // Cheap and common: FiberRefs were not modified during the execution so we just replace them with the old ones
                state.setFiberRefs(oldRefs)
              } else {
                // FiberRefs were mdified so we need to manually revert each one
                var revertedRefs = oldRefs
                revertedRefs = resetRef(fid, oldRefs, revertedRefs)(currentCache)
                revertedRefs = resetRef(fid, oldRefs, revertedRefs)(currentScope)
                revertedRefs = resetRef(fid, oldRefs, revertedRefs)(disabledCache)
                state.setFiberRefs(revertedRefs)
              }
              scope.closeAndExitWith(exit)
            }
          }
        }
      case exit => exit
    }
  }

  /**
   * Returns an effect that models executing this query, returning the query
   * result along with the cache.
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
    foldCauseQuery(ZQuery.failNow, ZQuery.succeedNow)

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
      e => ZQuery.failNow(Some(e)),
      ev(_) match {
        case Some(b) => ZQuery.succeedNow(b)
        case None    => ZQuery.failNow(None)
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
      case None        => default
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
        case None    => ZQuery.failNow(e)
      }
    }

  /**
   * Summarizes a query by computing some value before and after execution, and
   * then combining the values to produce a summary, together with the result of
   * execution.
   */
  def summarized[R1 <: R, E1 >: E, B, C](
    summary0: ZIO[R1, E1, B]
  )(f: (B, B) => C)(implicit trace: Trace): ZQuery[R1, E1, (C, A)] = {
    val summary = summary0
    for {
      start <- ZQuery.fromZIONow(summary)
      value <- self
      end   <- ZQuery.fromZIONow(summary)
    } yield (f(start, end), value)
  }

  /**
   * Returns a new query that executes this one and times the execution.
   */
  def timed(implicit trace: Trace): ZQuery[R, E, (Duration, A)] =
    summarized(Clock.nanoTime)((start, end) => Duration.fromNanos(end - start))

  /**
   * Returns an effect that will timeout this query, returning `None` if the
   * timeout elapses before the query was completed.
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
   * of applying the function `f` to the successful result of the query.
   */
  def timeoutTo[B](b: => B): ZQuery.TimeoutTo[R, E, A, B] =
    new ZQuery.TimeoutTo(self, () => b)

  /**
   * Disables caching for this query.
   */
  def uncached(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.unwrap(ZQuery.currentCache.get.map {
      case None => self
      case s =>
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
    unrefineWith(pf)(identity)

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
   * Sets the parallelism for this query to the specified maximum number of
   * fibers.
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
   * Requests composed with `zipWith` or combinators derived from it will
   * automatically be pipelined.
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
            case Result.Fail(e) => Result.fail(e)
          }
        case Result.Done(a) =>
          that.step.map {
            case Result.Blocked(br, c) => Result.blocked(br, c.map(b => f(a, b)))
            case Result.Done(b)        => Result.done(f(a, b))
            case Result.Fail(e)        => Result.fail(e)
          }
        case e: Result.Fail[E1] => Exit.succeed(e)
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
        case (Result.Fail(e), _)                     => Result.fail(e)
        case (_, Result.Fail(e))                     => Result.fail(e)
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
        case (Result.Fail(e), _)                     => Result.fail(e)
        case (_, Result.Fail(e))                     => Result.fail(e)
      }
    }

}

object ZQuery {

  def absolve[R, E, A](v: => ZQuery[R, E, Either[E, A]])(implicit trace: Trace): ZQuery[R, E, A] =
    ZQuery.suspend(v).flatMap {
      case Right(v) => ZQuery.succeedNow(v)
      case Left(e)  => ZQuery.failNow(e)
    }

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
    ZQuery(ZIO.environmentWith[R](Result.done))

  /**
   * Accesses the environment of the effect.
   * {{{
   * val portNumber = effect.access(_.config.portNumber)
   * }}}
   */
  def environmentWith[R]: EnvironmentWithPartiallyApplied[R] =
    new EnvironmentWithPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  def environmentWithQuery[R]: EnvironmentWithQueryPartiallyApplied[R] =
    new EnvironmentWithQueryPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
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
    implicit val ct1: ClassTag[A] = anyRefClassTag
    implicit val c2: ClassTag[B]  = anyRefClassTag
    foreachSequentialOuter[R, E, A, B, Collection](as.toArray, mode = Mode.Sequential, bf.fromSpecific(as)(_))(f)
  }

  /**
   * Applies the function `f` to each element of the `Set[A]` and returns the
   * results in a new `Set[B]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreach_` for a more efficient implementation.
   */
  def foreach[R, E, A, B](in: Set[A])(f: A => ZQuery[R, E, B])(implicit
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
  def foreach[R, E, A, B: ClassTag](in: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachSequentialOuter(in, mode = Mode.Sequential, ZIO.identityFn[Array[B]])(f)

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
  def foreach[R, E, A, B](in: Option[A])(f: A => ZQuery[R, E, B])(implicit
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
  def foreach[R, E, A, B](in: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
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
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZQuery[R, E, Collection[B]] = {
    implicit val ct1: ClassTag[A] = anyRefClassTag
    implicit val ct2: ClassTag[B] = anyRefClassTag
    foreachSequentialOuter[R, E, A, B, Collection](as.toArray, mode = Mode.Batched, bf.fromSpecific(as)(_))(f)
  }

  def foreachBatched[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B])(implicit
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
  def foreachBatched[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B])(implicit
    trace: Trace
  ): ZQuery[R, E, Array[B]] =
    foreachSequentialOuter(as, mode = Mode.Batched, ZIO.identityFn[Array[B]])(f)

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
  def foreachBatched[R, E, A, B](as: NonEmptyChunk[A])(f: A => ZQuery[R, E, B])(implicit
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
    as.sizeCompare(1) match {
      case -1 => ZQuery.succeedNow(bf.fromSpecific(as)(Nil))
      case 0  => f(as.head).map(v => (bf.newBuilder(as) += v).result())
      case _ =>
        implicit val ct: ClassTag[A] = anyRefClassTag
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
   * @see
   *   `fromRequests` for variants that allow for multiple requests to be
   *   submitted at once
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
   * Constructs a query from a Chunk of requests and a data source. Queries will
   * die with a QueryFailure when run the data source does not provide results
   * for all requests received.
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
   * Constructs a query from a List of requests and a data source. Queries will
   * die with a QueryFailure when run the data source does not provide results
   * for all requests received.
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
   * into requests and a data source. Queries will die with a QueryFailure when
   * run the data source does not provide results for all requests received.
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
   * requests and a data source. Queries will die with a QueryFailure when run
   * the data source does not provide results for all requests received.
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
          promise.unsafe.poll(Unsafe.unsafe) match {
            case None                 => CachedResult.Pure(Result.blocked(BlockedRequests.empty, Continue(promise)))
            case Some(io: Exit[E, B]) => CachedResult.Pure(Result.fromExit(io))
            case Some(io)             => CachedResult.Effectful(io.exit.map(Result.fromExit))
          }
      }

    cache match {
      case cache: Cache.Default => foldPromise(cache.lookupUnsafe(request)(Unsafe.unsafe))
      case cache                => CachedResult.Effectful(cache.lookup(request).flatMap(foldPromise(_).toZIO))
    }
  }

  private def uncachedResult[R, E, A, B](dataSource: DataSource[R, A], request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Result[R, E, B]] = {
    val promise = Promise.unsafe.make[E, B](FiberId.None)(Unsafe.unsafe)
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
   * Note that this is method is meant mostly for internal use, but it's made
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
    ZQuery(ZIO.serviceWith[R](Result.done))

  /**
   * Accesses the environment of the effect.
   * {{{
   * val portNumber = effect.access(_.config.portNumber)
   * }}}
   */
  def serviceWith[R]: ServiceWithPartiallyApplied[R] =
    new ServiceWithPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  def serviceWithQuery[R]: ServiceWithQueryPartiallyApplied[R] =
    new ServiceWithQueryPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
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
    ZQuery.unit.flatMap(_ => query)

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

    def memoize[R, E, A](query: ZQuery[R, E, A])(implicit unsafe: Unsafe, trace: Trace): ZQuery[R, E, A] = {
      val ref = Ref.Synchronized.unsafe.make[Option[Result[R, E, A]]](None)
      new ZQuery[R, E, A](ref.modifyZIO {
        case s @ Some(result) => Exit.succeed((result, s))
        case _                => query.step.map(result => (result, Some(result)))
      })
    }

  }

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
      ZQuery(ZIO.environmentWith[R](e => Result.done(f(e))))
  }

  final class EnvironmentWithQueryPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[E, A](f: ZEnvironment[R] => ZQuery[R, E, A])(implicit trace: Trace): ZQuery[R, E, A] =
      environment[R].flatMap(f)
  }

  final class EnvironmentWithZIOPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
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

  final class ServiceWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](
      f: R => A
    )(implicit tag: Tag[R], trace: Trace): ZQuery[R, Nothing, A] =
      ZQuery(ZIO.serviceWith[R](s => Result.done(f(s))))
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

  @deprecated("No longer used, kept for binary compatibility only", "0.7.4")
  val cachingEnabled: FiberRef[Boolean] =
    FiberRef.unsafe.make(true)(Unsafe.unsafe)

  val currentCache: FiberRef[Option[Cache]] =
    FiberRef.unsafe.make(Option.empty[Cache])(Unsafe.unsafe)

  private val disabledCache: FiberRef[Option[Cache]] =
    FiberRef.unsafe.make(Option.empty[Cache])(Unsafe.unsafe)

  val currentScope: FiberRef[QueryScope] =
    FiberRef.unsafe.make[QueryScope](QueryScope.NoOp)(Unsafe.unsafe)

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
            val ref = new AtomicBoolean(true)
            ZIO.uninterruptible {
              ZIO.suspendSucceed(acquire()).tap { a =>
                scope.addFinalizerExit {
                  case Exit.Failure(cause) =>
                    release(a, Exit.failCause(cause.stripFailures))
                      .provideEnvironment(environment)
                      .when(ref.getAndSet(false))
                  case Exit.Success(_) =>
                    ZIO.unit
                }
              }
            }.map { a =>
              ZQuery
                .suspend(use(a))
                .foldCauseQuery(
                  cause =>
                    ZQuery.fromZIONow {
                      ZIO
                        .suspendSucceed(release(a, Exit.failCause(cause)))
                        .when(ref.getAndSet(false))
                        .mapErrorCause(cause ++ _) *>
                        ZIO.refailCause(cause)
                    },
                  b =>
                    ZQuery.fromZIONow {
                      ZIO
                        .suspendSucceed(release(a, Exit.succeed(b)))
                        .when(ref.getAndSet(false))
                        .as(b)
                    }
                )
            }
          }
        }
      }
  }

  @inline private def anyRefClassTag[A]: ClassTag[A] =
    ClassTag.AnyRef.asInstanceOf[ClassTag[A]]

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
    implicit val classTag: ClassTag[B] = anyRefClassTag

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
