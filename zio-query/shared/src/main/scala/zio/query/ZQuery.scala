package zio.query

import zio._
import zio.query.internal._

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
final class ZQuery[-R, +E, +A] private (private val step: ZIO[(R, QueryContext), Nothing, Result[R, E, A]]) { self =>

  /**
   * Syntax for adding aspects.
   */
  final def @@[R1 <: R](aspect: DataSourceAspect[R1]): ZQuery[R1, E, A] =
    mapDataSources(aspect)

  /**
   * A symbolic alias for `zipParRight`.
   */
  final def &>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    zipParRight(that)

  /**
   * A symbolic alias for `zipRight`.
   */
  final def *>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    zipRight(that)

  /**
   * A symbolic alias for `zipParLeft`.
   */
  final def <&[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipParLeft(that)

  /**
   * A symbolic alias for `zipPar`.
   */
  final def <&>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B]
  ): ZQuery[R1, E1, zippable.Out] =
    zipPar(that)

  /**
   * A symbolic alias for `zipLeft`.
   */
  final def <*[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipLeft(that)

  /**
   * A symbolic alias for `zip`.
   */
  final def <*>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B]
  ): ZQuery[R1, E1, zippable.Out] =
    zip(that)

  /**
   * A symbolic alias for `flatMap`.
   */
  @deprecated("use flatMap", "0.3.0")
  final def >>=[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    flatMap(f)

  /**
   * Returns a query which submerges the error case of `Either` into the error channel of the query
   *
   * The inverse of [[ZQuery.either]]
   */
  def absolve[E1 >: E, B](implicit ev: A IsSubtypeOfOutput Either[E1, B]): ZQuery[R, E1, B] =
    ZQuery.absolve(self.map(ev))

  /**
   * Maps the success value of this query to the specified constant value.
   */
  final def as[B](b: => B): ZQuery[R, E, B] =
    map(_ => b)

  /**
   * Lifts the error channel into a `Some` value for composition with other optional queries
   *
   * @see [[ZQuery.some]]
   */
  def asSomeError: ZQuery[R, Option[E], A] =
    mapError(Some(_))

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`.
   */
  @deprecated("use mapBoth", "0.3.0")
  final def bimap[E1, B](f: E => E1, g: A => B)(implicit ev: CanFail[E]): ZQuery[R, E1, B] =
    mapBoth(f, g)

  /**
   * Enables caching for this query. Note that caching is enabled by default
   * so this will only be effective to enable caching in part of a larger
   * query in which caching has been disabled.
   */
  def cached: ZQuery[R, E, A] =
    for {
      queryContext   <- ZQuery.queryContext
      cachingEnabled <- ZQuery.fromZIO(queryContext.cachingEnabled.getAndSet(true))
      a              <- self.ensuring(ZQuery.fromZIO(queryContext.cachingEnabled.set(cachingEnabled)))
    } yield a

  /**
   * Recovers from all errors.
   */
  def catchAll[R1 <: R, E2, A1 >: A](h: E => ZQuery[R1, E2, A1])(implicit ev: CanFail[E]): ZQuery[R1, E2, A1] =
    self.foldQuery[R1, E2, A1](h, ZQuery.succeed(_))

  /**
   * Recovers from all errors with provided Cause.
   *
   * @see [[ZQuery.sandbox]] - other functions that can recover from defects
   */
  def catchAllCause[R1 <: R, E2, A1 >: A](h: Cause[E] => ZQuery[R1, E2, A1]): ZQuery[R1, E2, A1] =
    self.foldCauseQuery[R1, E2, A1](h, ZQuery.succeed(_))

  /**
   * Moves a `None` value in the error channel into the value channel while converting the existing value into a `Some`
   *
   * Inverse of [[ZQuery.some]]
   */
  def collectSome[E1](implicit ev: E IsSubtypeOfError Option[E1]): ZQuery[R, E1, Option[A]] =
    self.foldQuery(
      _.fold[ZQuery[R, E1, Option[A]]](ZQuery.none)(ZQuery.fail(_)),
      a => ZQuery.some(a)
    )

  /**
   * Returns a query whose failure and success have been lifted into an
   * `Either`. The resulting query cannot fail, because the failure case has
   * been exposed as part of the `Either` success case.
   */
  final def either(implicit ev: CanFail[E]): ZQuery[R, Nothing, Either[E, A]] =
    fold(Left(_), Right(_))

  /**
   * Ensures that if this query starts executing, the specified query will be
   * executed immediately after this query completes execution, whether by
   * success or failure.
   */
  final def ensuring[R1 <: R](finalizer: ZQuery[R1, Nothing, Any]): ZQuery[R1, E, A] =
    self.foldCauseQuery(
      cause1 =>
        finalizer.foldCauseQuery(
          cause2 => ZQuery.failCause(cause1 ++ cause2),
          _ => ZQuery.failCause(cause1)
        ),
      value =>
        finalizer.foldCauseQuery(
          cause => ZQuery.failCause(cause),
          _ => ZQuery.succeedNow(value)
        )
    )

  /**
   * Returns a query that models execution of this query, followed by passing
   * its result to the specified function that returns a query. Requests
   * composed with `flatMap` or combinators derived from it will be executed
   * sequentially and will not be pipelined, though deduplication and caching of
   * requests may still be applied.
   */
  final def flatMap[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    ZQuery {
      step.flatMap {
        case Result.Blocked(br, c) => ZIO.succeedNow(Result.blocked(br, c.mapQuery(f)))
        case Result.Done(a)        => f(a).step
        case Result.Fail(e)        => ZIO.succeedNow(Result.fail(e))
      }
    }

  /**
   * Returns a query that performs the outer query first, followed by the inner
   * query, yielding the value of the inner query.
   *
   * This method can be used to "flatten" nested queries.
   */
  final def flatten[R1 <: R, E1 >: E, B](implicit ev: A IsSubtypeOfOutput ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    flatMap(ev)

  /**
   * Folds over the failed or successful result of this query to yield a query
   * that does not fail, but succeeds with the value returned by the left or
   * right function passed to `fold`.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit ev: CanFail[E]): ZQuery[R, Nothing, B] =
    foldQuery(e => ZQuery.succeed(failure(e)), a => ZQuery.succeed(success(a)))

  /**
   * A more powerful version of `foldM` that allows recovering from any type
   * of failure except interruptions.
   */
  @deprecated("use foldCauseQuery", "0.3.0")
  final def foldCauseM[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  ): ZQuery[R1, E1, B] =
    foldCauseQuery(failure, success)

  /**
   * A more powerful version of `foldQuery` that allows recovering from any
   * type of failure except interruptions.
   */
  final def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  ): ZQuery[R1, E1, B] =
    ZQuery {
      step.foldCauseZIO(
        failure(_).step,
        {
          case Result.Blocked(br, c) => ZIO.succeedNow(Result.blocked(br, c.foldCauseQuery(failure, success)))
          case Result.Done(a)        => success(a).step
          case Result.Fail(e)        => failure(e).step
        }
      )
    }

  /**
   * Recovers from errors by accepting one query to execute for the case of an
   * error, and one query to execute for the case of success.
   */
  @deprecated("use foldQuery", "0.3.0")
  final def foldM[R1 <: R, E1, B](failure: E => ZQuery[R1, E1, B], success: A => ZQuery[R1, E1, B])(implicit
    ev: CanFail[E]
  ): ZQuery[R1, E1, B] =
    foldQuery(failure, success)

  /**
   * Recovers from errors by accepting one query to execute for the case of an
   * error, and one query to execute for the case of success.
   */
  final def foldQuery[R1 <: R, E1, B](failure: E => ZQuery[R1, E1, B], success: A => ZQuery[R1, E1, B])(implicit
    ev: CanFail[E]
  ): ZQuery[R1, E1, B] =
    foldCauseQuery(_.failureOrCause.fold(failure, ZQuery.failCause(_)), success)

  /**
   * "Zooms in" on the value in the `Left` side of an `Either`, moving the
   * possibility that the value is a `Right` to the error channel.
   */
  final def left[B, C](implicit ev: A IsSubtypeOfOutput Either[B, C]): ZQuery[R, Either[E, C], B] =
    self.foldQuery(
      e => ZQuery.fail(Left(e)),
      a => ev(a).fold(b => ZQuery.succeedNow(b), c => ZQuery.fail(Right(c)))
    )

  /**
   * Maps the specified function over the successful result of this query.
   */
  final def map[B](f: A => B): ZQuery[R, E, B] =
    ZQuery(step.map(_.map(f)))

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`.
   */
  final def mapBoth[E1, B](f: E => E1, g: A => B)(implicit ev: CanFail[E]): ZQuery[R, E1, B] =
    foldQuery(e => ZQuery.fail(f(e)), a => ZQuery.succeed(g(a)))

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1]): ZQuery[R1, E, A] =
    ZQuery(step.map(_.mapDataSources(f)))

  /**
   * Maps the specified function over the failed result of this query.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E]): ZQuery[R, E1, A] =
    mapBoth(f, identity)

  /**
   * Returns a query with its full cause of failure mapped using the
   * specified function. This can be used to transform errors while
   * preserving the original structure of `Cause`.
   *
   * @see [[sandbox]], [[catchAllCause]] - other functions for dealing with defects
   */
  def mapErrorCause[E2](h: Cause[E] => Cause[E2]): ZQuery[R, E2, A] =
    self.foldCauseQuery(c => ZQuery.failCause(h(c)), ZQuery.succeedNow)

  /**
   * Maps the specified effectual function over the result of this query.
   */
  @deprecated("use mapQuery", "0.3.0")
  final def mapM[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B]): ZQuery[R1, E1, B] =
    mapQuery(f)

  /**
   * Maps the specified effectual function over the result of this query.
   */
  final def mapQuery[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B]): ZQuery[R1, E1, B] =
    flatMap(a => ZQuery.fromZIO(f(a)))

  /**
   * Converts this query to one that returns `Some` if data sources return
   * results for all requests received and `None` otherwise.
   */
  final def optional: ZQuery[R, E, Option[A]] =
    foldCauseQuery(
      _.stripSomeDefects { case _: QueryFailure => () }.fold[ZQuery[R, E, Option[A]]](ZQuery.none)(ZQuery.failCause(_)),
      ZQuery.some(_)
    )

  /**
   * Converts this query to one that dies if a query failure occurs.
   */
  final def orDie(implicit ev1: E IsSubtypeOfError Throwable, ev2: CanFail[E]): ZQuery[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Converts this query to one that dies if a query failure occurs, using the
   * specified function to map the error to a `Throwable`.
   */
  final def orDieWith(f: E => Throwable)(implicit ev: CanFail[E]): ZQuery[R, Nothing, A] =
    foldQuery(e => ZQuery.die(f(e)), a => ZQuery.succeed(a))

  /**
   * Provides this query with its required environment.
   */
  final def provide(r: Described[R])(implicit ev: NeedsEnv[R]): ZQuery[Any, E, A] =
    provideSome(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Provides the part of the environment that is not part of the `ZEnv`,
   * leaving a query that only depends on the `ZEnv`.
   */
  final def provideCustomLayer[E1 >: E, R1 <: Has[_]](
    layer: Described[ZLayer[ZEnv, E1, R1]]
  )(implicit ev: ZEnv with R1 <:< R, tag: Tag[R1]): ZQuery[ZEnv, E1, A] =
    provideSomeLayer(layer)

  /**
   * Provides a layer to this query, which translates it to another level.
   */
  final def provideLayer[E1 >: E, R0, R1 <: Has[_]](
    layer: Described[ZLayer[R0, E1, R1]]
  )(implicit ev1: R1 <:< R, ev2: NeedsEnv[R]): ZQuery[R0, E1, A] =
    ZQuery {
      layer.value.build.provideSome[(R0, QueryContext)](_._1).exit.use {
        case Exit.Failure(e) => ZIO.succeedNow(Result.fail(e))
        case Exit.Success(r) => self.provide(Described(r, layer.description)).step
      }
    }

  /**
   * Provides this query with part of its required environment.
   */
  final def provideSome[R0](f: Described[R0 => R])(implicit ev: NeedsEnv[R]): ZQuery[R0, E, A] =
    ZQuery(step.map(_.provideSome(f)).provideSome(r => (f.value(r._1), r._2)))

  /**
   * Splits the environment into two parts, providing one part using the
   * specified layer and leaving the remainder `R0`.
   */
  final def provideSomeLayer[R0 <: Has[_]]: ZQuery.ProvideSomeLayer[R0, R, E, A] =
    new ZQuery.ProvideSomeLayer(self)

  /**
   * Races this query with the specified query, returning the result of the
   * first to complete successfully and safely interrupting the other.
   */
  def race[R1 <: R, E1 >: E, A1 >: A](that: ZQuery[R1, E1, A1]): ZQuery[R1, E1, A1] = {

    def coordinate(
      exit: Exit[Nothing, Result[R1, E1, A1]],
      fiber: Fiber[Nothing, Result[R1, E1, A1]]
    ): ZIO[(R1, QueryContext), Nothing, Result[R1, E1, A1]] =
      exit.foldZIO(
        cause => fiber.join.map(_.mapErrorCause(_ && cause)),
        {
          case Result.Blocked(blockedRequests, continue) =>
            continue match {
              case Continue.Effect(query) =>
                ZIO.succeedNow(Result.blocked(blockedRequests, Continue.effect(race(query, fiber))))
              case Continue.Get(io) =>
                ZIO.succeedNow(Result.blocked(blockedRequests, Continue.effect(race(ZQuery.fromZIO(io), fiber))))
            }
          case Result.Done(value) => fiber.interrupt *> ZIO.succeedNow(Result.done(value))
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
  )(implicit ev1: E IsSubtypeOfError Throwable, ev2: CanFail[E]): ZQuery[R, E1, A] =
    refineOrDieWith(pf)(ev1)

  /**
   * Keeps some of the errors, and terminates the query with the rest, using
   * the specified function to convert the `E` into a `Throwable`.
   */
  def refineOrDieWith[E1](pf: PartialFunction[E, E1])(f: E => Throwable)(implicit ev: CanFail[E]): ZQuery[R, E1, A] =
    self catchAll (err => (pf lift err).fold[ZQuery[R, E1, A]](ZQuery.die(f(err)))(ZQuery.fail(_)))

  /**
   * "Zooms in" on the value in the `Right` side of an `Either`, moving the
   * possibility that the value is a `Left` to the error channel.
   */
  final def right[B, C](implicit ev: A IsSubtypeOfOutput Either[B, C]): ZQuery[R, Either[B, E], C] =
    self.foldQuery(
      e => ZQuery.fail(Right(e)),
      a => ev(a).fold(b => ZQuery.fail(Left(b)), c => ZQuery.succeedNow(c))
    )

  /**
   * Returns an effect that models executing this query.
   */
  final val run: ZIO[R, E, A] =
    runLog.map(_._2)

  /**
   * Returns an effect that models executing this query with the specified
   * cache.
   */
  final def runCache(cache: Cache): ZIO[R, E, A] =
    for {
      ref <- FiberRef.make(true)
      a   <- runContext(QueryContext(cache, ref))
    } yield a

  /**
   * Returns an effect that models executing this query, returning the query
   * result along with the cache.
   */
  final def runLog: ZIO[R, E, (Cache, A)] =
    for {
      cache <- Cache.empty
      a     <- runCache(cache)
    } yield (cache, a)

  /**
   * Expose the full cause of failure of this query
   */
  def sandbox: ZQuery[R, Cause[E], A] =
    foldCauseQuery(ZQuery.fail(_), ZQuery.succeed(_))

  /**
   * Companion helper to `sandbox`. Allows recovery, and partial recovery, from
   * errors and defects alike, as in:
   */
  def sandboxWith[R1 <: R, E2, B](f: ZQuery[R1, Cause[E], A] => ZQuery[R1, Cause[E2], B]): ZQuery[R1, E2, B] =
    ZQuery.unsandbox(f(self.sandbox))

  /**
   * Extracts a Some value into the value channel while moving the None into the error channel for easier composition
   *
   * Inverse of [[ZQuery.collectSome]]
   */
  def some[B](implicit ev: A IsSubtypeOfOutput Option[B]): ZQuery[R, Option[E], B] =
    self.foldQuery[R, Option[E], B](
      e => ZQuery.fail(Some(e)),
      ev(_) match {
        case Some(b) => ZQuery.succeed(b)
        case None    => ZQuery.fail(None)
      }
    )

  /**
   * Extracts the optional value or fails with the given error `e`.
   */
  final def someOrFail[B, E1 >: E](e: => E1)(implicit ev: A IsSubtypeOfOutput Option[B]): ZQuery[R, E1, B] =
    self.flatMap { a =>
      ev(a) match {
        case Some(b) => ZQuery.succeed(b)
        case None    => ZQuery.fail(e)
      }
    }

  /**
   * Summarizes a query by computing some value before and after execution,
   * and then combining the values to produce a summary, together with the
   * result of execution.
   */
  final def summarized[R1 <: R, E1 >: E, B, C](summary: ZIO[R1, E1, B])(f: (B, B) => C): ZQuery[R1, E1, (C, A)] =
    for {
      start <- ZQuery.fromZIO(summary)
      value <- self
      end   <- ZQuery.fromZIO(summary)
    } yield (f(start, end), value)

  /**
   * Returns a new query that executes this one and times the execution.
   */
  final def timed: ZQuery[R with Has[Clock], E, (Duration, A)] =
    summarized(Clock.nanoTime)((start, end) => Duration.fromNanos(end - start))

  /**
   * Returns an effect that will timeout this query, returning `None` if the
   * timeout elapses before the query was completed.
   */
  final def timeout(duration: Duration): ZQuery[R with Has[Clock], E, Option[A]] =
    timeoutTo(None)(Some(_))(duration)

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event
   * of timeout, it will produce the specified error.
   */
  final def timeoutFail[E1 >: E](e: => E1)(duration: Duration): ZQuery[R with Has[Clock], E1, A] =
    timeoutTo(ZQuery.fail(e))(ZQuery.succeedNow)(duration).flatten

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event
   * of timeout, it will produce the specified failure.
   */
  final def timeoutFailCause[E1 >: E](cause: Cause[E1])(duration: Duration): ZQuery[R with Has[Clock], E1, A] =
    timeoutTo(ZQuery.failCause(cause))(ZQuery.succeedNow)(duration).flatten

  /**
   * The same as [[timeout]], but instead of producing a `None` in the event
   * of timeout, it will produce the specified failure.
   */
  @deprecated("use timeoutFailCause", "0.3.0")
  final def timeoutHalt[E1 >: E](cause: Cause[E1])(duration: Duration): ZQuery[R with Has[Clock], E1, A] =
    timeoutFailCause(cause)(duration)

  /**
   * Returns a query that will timeout this query, returning either the default
   * value if the timeout elapses before the query has completed or the result
   * of applying the function `f` to the successful result of the query.
   */
  final def timeoutTo[B](b: B): ZQuery.TimeoutTo[R, E, A, B] =
    new ZQuery.TimeoutTo(self, b)

  /**
   * Disables caching for this query.
   */
  def uncached: ZQuery[R, E, A] =
    for {
      queryContext   <- ZQuery.queryContext
      cachingEnabled <- ZQuery.fromZIO(queryContext.cachingEnabled.getAndSet(false))
      a              <- self.ensuring(ZQuery.fromZIO(queryContext.cachingEnabled.set(cachingEnabled)))
    } yield a

  /**
   * Converts a `ZQuery[R, Either[E, B], A]` into a
   * `ZQuery[R, E, Either[A, B]]`. The inverse of `left`.
   */
  final def unleft[E1, B](implicit ev: E IsSubtypeOfError Either[E1, B]): ZQuery[R, E1, Either[A, B]] =
    self.foldQuery(
      e => ev(e).fold(e1 => ZQuery.fail(e1), b => ZQuery.succeedNow(Right(b))),
      a => ZQuery.succeedNow(Left(a))
    )

  /**
   * Converts this query to one that returns `Some` if data sources return
   * results for all requests received and `None` otherwise.
   */
  final def unoption[E1](implicit ev: E IsSubtypeOfError Option[E1]): ZQuery[R, E1, Option[A]] =
    self.foldQuery(
      e => ev(e).fold[ZQuery[R, E1, Option[A]]](ZQuery.succeedNow(Option.empty[A]))(ZQuery.fail(_)),
      a => ZQuery.succeedNow(Some(a))
    )

  /**
   * Takes some fiber failures and converts them into errors.
   */
  final def unrefine[E1 >: E](pf: PartialFunction[Throwable, E1]): ZQuery[R, E1, A] =
    unrefineWith(pf)(identity)

  /**
   * Takes some fiber failures and converts them into errors.
   */
  final def unrefineTo[E1 >: E: ClassTag]: ZQuery[R, E1, A] =
    unrefine { case e: E1 => e }

  /**
   * Takes some fiber failures and converts them into errors, using the
   * specified function to convert the `E` into an `E1`.
   */
  final def unrefineWith[E1](pf: PartialFunction[Throwable, E1])(f: E => E1): ZQuery[R, E1, A] =
    catchAllCause { cause =>
      cause.find {
        case Cause.Die(t) if pf.isDefinedAt(t) => pf(t)
      }.fold(ZQuery.failCause(cause.map(f)))(ZQuery.fail(_))
    }

  /**
   * Converts a `ZQuery[R, Either[B, E], A]` into a
   * `ZQuery[R, E, Either[B, A]]`. The inverse of `right`.
   */
  final def unright[E1, B](implicit ev: E IsSubtypeOfError Either[B, E1]): ZQuery[R, E1, Either[B, A]] =
    self.foldQuery(
      e => ev(e).fold(b => ZQuery.succeedNow(Left(b)), e1 => ZQuery.fail(e1)),
      a => ZQuery.succeedNow(Right(a))
    )

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results into a tuple.
   */
  final def zip[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B]
  ): ZQuery[R1, E1, zippable.Out] =
    zipWith(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and combining their results into
   * a tuple.
   */
  final def zipBatched[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B]
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithBatched(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of this
   * query.
   */
  final def zipBatchedLeft[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipWithBatched(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources and returning the result of the
   * specified query.
   */
  final def zipBatchedRight[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    zipWithBatched(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of this query.
   */
  final def zipLeft[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipWith(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results into a tuple.
   */
  final def zipPar[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B])(implicit
    zippable: Zippable[A, B]
  ): ZQuery[R1, E1, zippable.Out] =
    zipWithPar(that)(zippable.zip(_, _))

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of this query.
   */
  final def zipParLeft[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipWithPar(that)((a, _) => a)

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, returning the result of the specified query.
   */
  final def zipParRight[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    zipWithPar(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, returning the result of the specified query.
   */
  final def zipRight[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    zipWith(that)((_, b) => b)

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results with the specified function.
   * Requests composed with `zipWith` or combinators derived from it will
   * automatically be pipelined.
   */
  final def zipWith[R1 <: R, E1 >: E, B, C](that: ZQuery[R1, E1, B])(f: (A, B) => C): ZQuery[R1, E1, C] =
    ZQuery {
      self.step.flatMap {
        case Result.Blocked(br, Continue.Effect(c)) =>
          ZIO.succeedNow(Result.blocked(br, Continue.effect(c.zipWith(that)(f))))
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
        case Result.Fail(e) => ZIO.succeedNow(Result.fail(e))
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query, batching requests to data sources.
   */
  final def zipWithBatched[R1 <: R, E1 >: E, B, C](that: ZQuery[R1, E1, B])(f: (A, B) => C): ZQuery[R1, E1, C] =
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
  final def zipWithPar[R1 <: R, E1 >: E, B, C](that: ZQuery[R1, E1, B])(f: (A, B) => C): ZQuery[R1, E1, C] =
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

  /**
   * Returns an effect that models executing this query with the specified
   * context.
   */
  private[query] final def runContext(queryContext: QueryContext): ZIO[R, E, A] =
    step.provideSome[R]((_, queryContext)).flatMap {
      case Result.Blocked(br, c) => br.run(queryContext.cache) *> c.runContext(queryContext)
      case Result.Done(a)        => ZIO.succeedNow(a)
      case Result.Fail(e)        => ZIO.failCause(e)
    }
}

object ZQuery {

  final def absolve[R, E, A](v: ZQuery[R, E, Either[E, A]]): ZQuery[R, E, A] =
    v.flatMap(fromEither(_))

  /**
   * Accesses the environment of the effect.
   * {{{
   * val portNumber = effect.access(_.config.portNumber)
   * }}}
   */
  final def access[R]: AccessPartiallyApplied[R] =
    new AccessPartiallyApplied[R]

  /**
   * Effectfully accesses the environment of the effect.
   */
  @deprecated("use accessQuery", "0.3.0")
  final def accessM[R]: AccessQueryPartiallyApplied[R] =
    accessQuery

  /**
   * Effectfully accesses the environment of the effect.
   */
  final def accessQuery[R]: AccessQueryPartiallyApplied[R] =
    new AccessQueryPartiallyApplied[R]

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed sequentially and will be
   * pipelined.
   */
  def collectAll[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]]): ZQuery[R, E, Collection[A]] =
    foreach(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]]): ZQuery[R, E, Collection[A]] =
    foreachBatched(as)(identity)

  /**
   * Collects a collection of queries into a query returning a collection of
   * their results. Requests will be executed in parallel and will be batched.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZQuery[R, E, A]]
  )(implicit bf: BuildFrom[Collection[ZQuery[R, E, A]], A, Collection[A]]): ZQuery[R, E, Collection[A]] =
    foreachPar(as)(identity)

  /**
   * Constructs a query that dies with the specified error.
   */
  def die(t: => Throwable): ZQuery[Any, Nothing, Nothing] =
    ZQuery(ZIO.die(t))

  /**
   * Accesses the whole environment of the query.
   */
  def environment[R]: ZQuery[R, Nothing, R] =
    ZQuery.fromZIO(ZIO.environment)

  /**
   * Constructs a query that fails with the specified error.
   */
  def fail[E](error: => E): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(Cause.fail(error))))

  /**
   * Constructs a query that fails with the specified cause.
   */
  def failCause[E](cause: => Cause[E]): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(cause)))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed sequentially and will be pipelined.
   */
  def foreach[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(f: A => ZQuery[R, E, B])(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): ZQuery[R, E, Collection[B]] =
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
   * Applies the function `f` to each element of the `Set[A]` and
   * returns the results in a new `Set[B]`.
   *
   * For a parallel version of this method, see `foreachPar`.
   * If you do not need the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B](in: Set[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, Set[B]] =
    foreach[R, E, A, B, Iterable](in)(f).map(_.toSet)

  /**
   * Applies the function `f` to each element of the `Array[A]` and
   * returns the results in a new `Array[B]`.
   *
   * For a parallel version of this method, see `foreachPar`.
   * If you do not need the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B: ClassTag](in: Array[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, Array[B]] =
    foreach[R, E, A, B, Iterable](in)(f).map(_.toArray)

  /**
   * Applies the function `f` to each element of the `Map[Key, Value]` and
   * returns the results in a new `Map[Key2, Value2]`.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not
   * need the results, see `foreach_` for a more efficient implementation.
   */
  def foreach[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)]): ZQuery[R, E, Map[Key2, Value2]] =
    foreach[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Applies the function `f` if the argument is non-empty and
   * returns the results in a new `Option[B]`.
   */
  final def foreach[R, E, A, B](in: Option[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, Option[B]] =
    in.fold[ZQuery[R, E, Option[B]]](none)(f(_).map(Some(_)))

  /**
   * Applies the function `f` to each element of the `NonEmptyChunk[A]` and
   * returns the results in a new `NonEmptyChunk[B]`.
   *
   * For a parallel version of this method, see `foreachPar`.
   * If you do not need the results, see `foreach_` for a more efficient implementation.
   */
  final def foreach[R, E, A, B](in: NonEmptyChunk[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, NonEmptyChunk[B]] =
    foreach[R, E, A, B, Chunk](in)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, batching requests to
   * data sources and collecting the results into a query returning a
   * collection of their results.
   */
  def foreachBatched[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(f: A => ZQuery[R, E, B])(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result())
    else {
      val iterator                                         = as.iterator
      var builder: ZQuery[R, E, Builder[B, Collection[B]]] = null
      while (iterator.hasNext) {
        val a = iterator.next()
        if (builder eq null) builder = f(a).map(bf.newBuilder(as) += _)
        else builder = builder.zipWithBatched(f(a))(_ += _)
      }
      builder.map(_.result())
    }

  final def foreachBatched[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B]): ZQuery[R, E, Set[B]] =
    foreachBatched[R, E, A, B, Iterable](as)(fn).map(_.toSet)

  /**
   * Performs a query for each element in an Array, batching requests to
   * data sources and collecting the results into a query returning a
   * collection of their results.
   *
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachBatched[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, Array[B]] =
    foreachBatched[R, E, A, B, Iterable](as)(f).map(_.toArray)

  /**
   * Performs a query for each element in a Map, batching requests to
   * data sources and collecting the results into a query returning a
   * collection of their results.
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachBatched[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)]): ZQuery[R, E, Map[Key2, Value2]] =
    foreachBatched[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Performs a query for each element in a NonEmptyChunk, batching requests to
   * data sources and collecting the results into a query returning a
   * collection of their results.
   *
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachBatched[R, E, A, B](as: NonEmptyChunk[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachBatched[R, E, A, B, Chunk](as)(f).map(NonEmptyChunk.nonEmpty)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   */
  def foreachPar[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(f: A => ZQuery[R, E, B])(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result())
    else {
      val iterator                                         = as.iterator
      var builder: ZQuery[R, E, Builder[B, Collection[B]]] = null
      while (iterator.hasNext) {
        val a = iterator.next()
        if (builder eq null) builder = f(a).map(bf.newBuilder(as) += _)
        else builder = builder.zipWithPar(f(a))(_ += _)
      }
      builder.map(_.result())
    }

  /**
   * Performs a query for each element in a Set, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   */
  final def foreachPar[R, E, A, B](as: Set[A])(fn: A => ZQuery[R, E, B]): ZQuery[R, E, Set[B]] =
    foreachPar[R, E, A, B, Iterable](as)(fn).map(_.toSet)

  /**
   * Performs a query for each element in an Array, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachPar[R, E, A, B: ClassTag](as: Array[A])(f: A => ZQuery[R, E, B]): ZQuery[R, E, Array[B]] =
    foreachPar[R, E, A, B, Iterable](as)(f).map(_.toArray)

  /**
   * Performs a query for each element in a Map, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachPar[R, E, Key, Key2, Value, Value2](
    map: Map[Key, Value]
  )(f: (Key, Value) => ZQuery[R, E, (Key2, Value2)]): ZQuery[R, E, Map[Key2, Value2]] =
    foreachPar[R, E, (Key, Value), (Key2, Value2), Iterable](map)(f.tupled).map(_.toMap)

  /**
   * Performs a query for each element in a NonEmptyChunk, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   *
   * For a sequential version of this method, see `foreach`.
   */
  final def foreachPar[R, E, A, B](as: NonEmptyChunk[A])(fn: A => ZQuery[R, E, B]): ZQuery[R, E, NonEmptyChunk[B]] =
    foreachPar[R, E, A, B, Chunk](as)(fn).map(NonEmptyChunk.nonEmpty)

  /**
   * Constructs a query from an effect.
   */
  @deprecated("use fromZIO", "0.3.0")
  def fromEffect[R, E, A](effect: ZIO[R, E, A]): ZQuery[R, E, A] =
    fromZIO(effect)

  /**
   * Constructs a query from an either
   */
  def fromEither[E, A](either: => Either[E, A]): ZQuery[Any, E, A] =
    ZQuery.succeed(either).flatMap(_.fold[ZQuery[Any, E, A]](ZQuery.fail(_), ZQuery.succeedNow))

  /**
   * Constructs a query from an option
   */
  def fromOption[A](option: Option[A]): ZQuery[Any, Option[Nothing], A] =
    ZQuery.succeed(option).flatMap(_.fold[ZQuery[Any, Option[Nothing], A]](ZQuery.fail(None))(ZQuery.succeedNow))

  /**
   * Constructs a query from a request and a data source. Queries will die with
   * a `QueryFailure` when run if the data source does not provide results for
   * all requests received. Queries must be constructed with `fromRequest` or
   * one of its variants for optimizations to be applied.
   */
  def fromRequest[R, E, A, B](
    request: A
  )(dataSource: DataSource[R, A])(implicit ev: A <:< Request[E, B]): ZQuery[R, E, B] =
    ZQuery {
      ZIO.environment[(R, QueryContext)].flatMap { case (_, queryContext) =>
        queryContext.cachingEnabled.get.flatMap { cachingEnabled =>
          if (cachingEnabled) {
            queryContext.cache.lookup(request).flatMap {
              case Left(ref) =>
                UIO.succeedNow(
                  Result.blocked(
                    BlockedRequests.single(dataSource, BlockedRequest(request, ref)),
                    Continue(request, dataSource, ref)
                  )
                )
              case Right(ref) =>
                ref.get.map {
                  case None    => Result.blocked(BlockedRequests.empty, Continue(request, dataSource, ref))
                  case Some(b) => Result.fromEither(b)
                }
            }
          } else {
            Ref.make(Option.empty[Either[E, B]]).map { ref =>
              Result.blocked(
                BlockedRequests.single(dataSource, BlockedRequest(request, ref)),
                Continue(request, dataSource, ref)
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
    request: A
  )(dataSource: DataSource[R, A])(implicit ev: A <:< Request[E, B]): ZQuery[R, E, B] =
    fromRequest(request)(dataSource).uncached

  /**
   * Constructs a query from an effect.
   */
  def fromZIO[R, E, A](effect: ZIO[R, E, A]): ZQuery[R, E, A] =
    ZQuery(effect.foldCause(Result.fail, Result.done).provideSome(_._1))

  /**
   * Constructs a query that fails with the specified cause.
   */
  @deprecated("use failCause", "0.3.0")
  def halt[E](cause: => Cause[E]): ZQuery[Any, E, Nothing] =
    ZQuery(ZIO.succeed(Result.fail(cause)))

  /**
   * Constructs a query that never completes.
   */
  val never: ZQuery[Any, Nothing, Nothing] =
    ZQuery.fromZIO(ZIO.never)

  /**
   * Constructs a query that succeds with the empty value.
   */
  val none: ZQuery[Any, Nothing, Option[Nothing]] =
    succeedNow(None)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed sequentially and will be pipelined.
   */
  @deprecated("use partitionQuery", "0.3.0")
  def partitionM[R, E, A, B](
    as: Iterable[A]
  )(f: A => ZQuery[R, E, B])(implicit ev: CanFail[E]): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    partitionQuery(as)(f)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed in parallel and will be batched.
   */
  @deprecated("use partitionQueryPar", "0.3.0")
  def partitionMPar[R, E, A, B](
    as: Iterable[A]
  )(f: A => ZQuery[R, E, B])(implicit ev: CanFail[E]): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    partitionQueryPar(as)(f)

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed sequentially and will be pipelined.
   */
  def partitionQuery[R, E, A, B](
    as: Iterable[A]
  )(f: A => ZQuery[R, E, B])(implicit ev: CanFail[E]): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    ZQuery.foreach(as)(f(_).either).map(partitionMap(_)(identity))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed in parallel and will be batched.
   */
  def partitionQueryPar[R, E, A, B](
    as: Iterable[A]
  )(f: A => ZQuery[R, E, B])(implicit ev: CanFail[E]): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    ZQuery.foreachPar(as)(f(_).either).map(partitionMap(_)(identity))

  /**
   * Constructs a query that succeeds with the optional value.
   */
  def some[A](a: => A): ZQuery[Any, Nothing, Option[A]] =
    succeed(Some(a))

  /**
   *  Constructs a query that succeeds with the specified value.
   */
  def succeed[A](value: => A): ZQuery[Any, Nothing, A] =
    ZQuery(ZIO.succeed(Result.done(value)))

  /**
   * The inverse operation [[ZQuery.sandbox]]
   *
   * Terminates with exceptions on the `Left` side of the `Either` error, if it
   * exists. Otherwise extracts the contained `IO[E, A]`
   */
  def unsandbox[R, E, A](v: ZQuery[R, Cause[E], A]): ZQuery[R, E, A] =
    v.mapErrorCause(_.flatten)

  /**
   * Unwraps a query that is produced by an effect.
   */
  def unwrap[R, E, A](zio: ZIO[R, E, ZQuery[R, E, A]]): ZQuery[R, E, A] =
    ZQuery.fromZIO(zio).flatten

  final class AccessPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](f: R => A): ZQuery[R, Nothing, A] =
      environment[R].map(f)
  }

  final class AccessQueryPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[E, A](f: R => ZQuery[R, E, A]): ZQuery[R, E, A] =
      environment[R].flatMap(f)
  }

  final class ProvideSomeLayer[R0 <: Has[_], -R, +E, +A](private val self: ZQuery[R, E, A]) extends AnyVal {
    def apply[E1 >: E, R1 <: Has[_]](
      layer: Described[ZLayer[R0, E1, R1]]
    )(implicit ev1: R0 with R1 <:< R, ev2: NeedsEnv[R], tag: Tag[R1]): ZQuery[R0, E1, A] =
      self.provideLayer[E1, R0, R0 with R1](Described(ZLayer.identity[R0] ++ layer.value, layer.description))
  }

  final class TimeoutTo[-R, +E, +A, +B](self: ZQuery[R, E, A], b: B) {
    def apply[B1 >: B](f: A => B1)(duration: Duration): ZQuery[R with Has[Clock], E, B1] =
      ZQuery.environment[Has[Clock]].flatMap { clock =>
        def race(
          query: ZQuery[R, E, B1],
          fiber: Fiber[Nothing, B1]
        ): ZQuery[R, E, B1] =
          ZQuery {
            query.step.raceWith[(R, QueryContext), Nothing, Nothing, B1, Result[R, E, B1]](fiber.join)(
              (leftExit, rightFiber) =>
                leftExit.foldZIO(
                  cause => rightFiber.interrupt *> ZIO.succeedNow(Result.fail(cause)),
                  result =>
                    result match {
                      case Result.Blocked(blockedRequests, continue) =>
                        continue match {
                          case Continue.Effect(query) =>
                            ZIO.succeedNow(Result.blocked(blockedRequests, Continue.effect(race(query, fiber))))
                          case Continue.Get(io) =>
                            ZIO.succeedNow(
                              Result.blocked(blockedRequests, Continue.effect(race(ZQuery.fromZIO(io), fiber)))
                            )
                        }
                      case Result.Done(value) => rightFiber.interrupt *> ZIO.succeedNow(Result.done(value))
                      case Result.Fail(cause) => rightFiber.interrupt *> ZIO.succeedNow(Result.fail(cause))
                    }
                ),
              (rightExit, leftFiber) => leftFiber.interrupt *> ZIO.succeedNow(Result.fromExit(rightExit))
            )
          }

        ZQuery.fromZIO(clock.get.sleep(duration).interruptible.as(b).fork).flatMap(fiber => race(self.map(f), fiber))
      }
  }

  /**
   * Constructs a query from an effect that returns a result.
   */
  private def apply[R, E, A](step: ZIO[(R, QueryContext), Nothing, Result[R, E, A]]): ZQuery[R, E, A] =
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

  /**
   * Returns a query that accesses the context.
   */
  private def queryContext: ZQuery[Any, Nothing, QueryContext] =
    ZQuery(ZIO.access[(Any, QueryContext)] { case (_, queryContext) => Result.done(queryContext) })

  /**
   * Constructs a query that succeeds with the specified value.
   */
  private def succeedNow[A](value: A): ZQuery[Any, Nothing, A] =
    ZQuery(ZIO.succeedNow(Result.done(value)))
}
