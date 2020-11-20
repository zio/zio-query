package zio.query

import zio._
import zio.clock._
import zio.duration._
import zio.query.internal._

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
final class ZQuery[-R, +E, +A] private (
  private val start: (Result[R, E, A] => UIO[Any]) => ZIO[(R, QueryContext), Nothing, Any]
) { self =>

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
  final def <&>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, (A, B)] =
    zipPar(that)

  /**
   * A symbolic alias for `zipLeft`.
   */
  final def <*[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, A] =
    zipLeft(that)

  /**
   * A symbolic alias for `zip`.
   */
  final def <*>[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, (A, B)] =
    zip(that)

  /**
   * A symbolic alias for `flatMap`.
   */
  final def >>=[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    flatMap(f)

  /**
   * Maps the success value of this query to the specified constant value.
   */
  final def as[B](b: => B): ZQuery[R, E, B] =
    map(_ => b)

  /**
   * Returns a query whose failure and success channels have been mapped by the
   * specified pair of functions, `f` and `g`.
   */
  final def bimap[E1, B](f: E => E1, g: A => B)(implicit ev: CanFail[E]): ZQuery[R, E1, B] =
    foldM(e => ZQuery.fail(f(e)), a => ZQuery.succeed(g(a)))

  /**
   * Returns a query whose failure and success have been lifted into an
   * `Either`. The resulting query cannot fail, because the failure case has
   * been exposed as part of the `Either` success case.
   */
  final def either(implicit ev: CanFail[E]): ZQuery[R, Nothing, Either[E, A]] =
    fold(Left(_), Right(_))

  /**
   * Returns a query that models execution of this query, followed by passing
   * its result to the specified function that returns a query. Requests
   * composed with `flatMap` or combinators derived from it will be executed
   * sequentially and will not be pipelined, though deduplication and caching of
   * requests may still be applied.
   */
  final def flatMap[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): ZQuery[R1, E1, B] =
    ZQuery { cb =>
      ZIO.accessM { env =>
        self.start {
          case Result.Blocked(br, c) => ZIO.effectSuspendTotal(cb(Result.blocked(br, c.mapM(f))))
          case Result.Done(a)        => ZIO.effectSuspendTotal(f(a).start(cb).provide(env))
          case Result.Fail(e)        => ZIO.effectSuspendTotal(cb(Result.fail(e)))
        }
      }
    }

  /**
   * Folds over the failed or successful result of this query to yield a query
   * that does not fail, but succeeds with the value returned by the left or
   * right function passed to `fold`.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit ev: CanFail[E]): ZQuery[R, Nothing, B] =
    foldM(e => ZQuery.succeed(failure(e)), a => ZQuery.succeed(success(a)))

  /**
   * A more powerful version of `foldM` that allows recovering from any type
   * of failure except interruptions.
   */
  final def foldCauseM[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  ): ZQuery[R1, E1, B] =
    ZQuery { cb =>
      ZIO.accessM { env =>
        self.start {
          case Result.Blocked(br, c) => ZIO.effectSuspendTotal(cb(Result.blocked(br, c.foldCauseM(failure, success))))
          case Result.Done(a)        => ZIO.effectSuspendTotal(success(a).start(cb).provide(env))
          case Result.Fail(e)        => ZIO.effectSuspendTotal(failure(e).start(cb).provide(env))
        }
      }
    }

  /**
   * Recovers from errors by accepting one query to execute for the case of an
   * error, and one query to execute for the case of success.
   */
  final def foldM[R1 <: R, E1, B](failure: E => ZQuery[R1, E1, B], success: A => ZQuery[R1, E1, B])(implicit
    ev: CanFail[E]
  ): ZQuery[R1, E1, B] =
    foldCauseM(_.failureOrCause.fold(failure, ZQuery.halt(_)), success)

  /**
   * Maps the specified function over the successful result of this query.
   */
  final def map[B](f: A => B): ZQuery[R, E, B] =
    ZQuery(cb => ZIO.effectSuspendTotal(self.start(result => ZIO.effectSuspendTotal(cb(result.map(f))))))

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1]): ZQuery[R1, E, A] =
    new ZQuery(cb => ZIO.effectSuspendTotal(self.start(result => ZIO.effectSuspendTotal(cb(result.mapDataSources(f))))))

  /**
   * Maps the specified function over the failed result of this query.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E]): ZQuery[R, E1, A] =
    bimap(f, identity)

  /**
   * Converts this query to one that returns `Some` if data sources return
   * results for all requests received and `None` otherwise.
   */
  final def optional: ZQuery[R, E, Option[A]] =
    foldCauseM(
      _.stripSomeDefects { case _: QueryFailure => () }.fold[ZQuery[R, E, Option[A]]](ZQuery.none)(ZQuery.halt(_)),
      ZQuery.some(_)
    )

  /**
   * Converts this query to one that dies if a query failure occurs.
   */
  final def orDie(implicit ev1: E <:< Throwable, ev2: CanFail[E]): ZQuery[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Converts this query to one that dies if a query failure occurs, using the
   * specified function to map the error to a `Throwable`.
   */
  final def orDieWith(f: E => Throwable)(implicit ev: CanFail[E]): ZQuery[R, Nothing, A] =
    foldM(e => ZQuery.die(f(e)), a => ZQuery.succeed(a))

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
    ZQuery { cb =>
      ZIO.transplant { graft =>
        graft {
          layer.value.build.provideSome[(R0, QueryContext)](_._1).run.use {
            case Exit.Failure(e) => ZIO.effectSuspendTotal(cb(Result.fail(e)))
            case Exit.Success(r) => ZIO.effectSuspendTotal(self.provide(Described(r, layer.description)).start(cb))
          }
        }.fork
      }
    }

  /**
   * Provides this query with part of its required environment.
   */
  final def provideSome[R0](f: Described[R0 => R])(implicit ev: NeedsEnv[R]): ZQuery[R0, E, A] =
    ZQuery { cb =>
      ZIO.effectSuspendTotal {
        self
          .start(result => ZIO.effectSuspendTotal(cb(result.provideSome(f))))
          .provideSome { case (r, queryContext) => (f.value(r), queryContext) }
      }
    }

  /**
   * Splits the environment into two parts, providing one part using the
   * specified layer and leaving the remainder `R0`.
   */
  final def provideSomeLayer[R0 <: Has[_]]: ZQuery.ProvideSomeLayer[R0, R, E, A] =
    new ZQuery.ProvideSomeLayer(self)

  /**
   * Returns an effect that models executing this query.
   */
  final val run: ZIO[R, E, A] =
    runLog.map(_._2)

  /**
   * Returns an effect that models starting this query, providing the query
   * result to the specified callback.
   */
  final def runAsync(cache: Cache)(cb: Exit[E, A] => UIO[Any]): URIO[R, Any] =
    ZIO.accessM { r =>
      self.start {
        case Result.Blocked(br, c) => br.run(cache).provide(r) *> c.runAsync(cache)(cb).provide(r)
        case Result.Done(a)        => cb(Exit.succeed(a))
        case Result.Fail(e)        => cb(Exit.halt(e))
      }.provideSome[R]((_, QueryContext(cache)))
    }

  /**
   * Returns an effect that models executing this query with the specified
   * cache.
   */
  final def runCache(cache: Cache): ZIO[R, E, A] =
    for {
      promise <- Promise.make[E, A]
      _       <- runAsync(cache)(promise.done)
      value   <- promise.await
    } yield value

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
   * Extracts the optional value or fails with the given error `e`.
   */
  final def someOrFail[B, E1 >: E](e: => E1)(implicit ev: A <:< Option[B]): ZQuery[R, E1, B] =
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
      start <- ZQuery.fromEffect(summary)
      value <- self
      end   <- ZQuery.fromEffect(summary)
    } yield (f(start, end), value)

  /**
   * Returns a new query that executes this one and times the execution.
   */
  final def timed: ZQuery[R with Clock, E, (Duration, A)] =
    summarized(clock.nanoTime)((start, end) => Duration.fromNanos(end - start))

  /**
   * Returns a query that models the execution of this query and the specified
   * query sequentially, combining their results into a tuple.
   */
  final def zip[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, (A, B)] =
    zipWith(that)((_, _))

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
  final def zipPar[R1 <: R, E1 >: E, B](that: ZQuery[R1, E1, B]): ZQuery[R1, E1, (A, B)] =
    zipWithPar(that)((_, _))

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
    ZQuery { cb =>
      ZIO.accessM { r =>
        self.start {
          case Result.Blocked(br, Continue.Effect(c)) =>
            ZIO.effectSuspendTotal(cb(Result.blocked(br, Continue.effect(c.zipWith(that)(f)))))
          case Result.Blocked(br1, c1) =>
            ZIO.effectSuspendTotal {
              that.start {
                case Result.Blocked(br2, c2) =>
                  ZIO.effectSuspendTotal(cb(Result.blocked(br1 ++ br2, c1.zipWith(c2)(f))))
                case Result.Done(b) =>
                  ZIO.effectSuspendTotal(cb(Result.blocked(br1, c1.map(a => f(a, b)))))
                case Result.Fail(e) =>
                  ZIO.effectSuspendTotal(cb(Result.fail(e)))
              }.provide(r)
            }
          case Result.Done(a) =>
            ZIO.effectSuspendTotal {
              that.start {
                case Result.Blocked(br, c) =>
                  ZIO.effectSuspendTotal(cb(Result.blocked(br, c.map(b => f(a, b)))))
                case Result.Done(b) =>
                  ZIO.effectSuspendTotal(cb(Result.done(f(a, b))))
                case Result.Fail(e) =>
                  ZIO.effectSuspendTotal(cb(Result.fail(e)))
              }.provide(r)
            }
          case Result.Fail(e) =>
            ZIO.effectSuspendTotal(cb(Result.fail(e)))
        }
      }
    }

  /**
   * Returns a query that models the execution of this query and the specified
   * query in parallel, combining their results with the specified function.
   * Requests composed with `zipWithPar` or combinators derived from it will
   * automatically be batched.
   */
  final def zipWithPar[R1 <: R, E1 >: E, B, C](that: ZQuery[R1, E1, B])(f: (A, B) => C): ZQuery[R1, E1, C] =
    ZQuery { cb =>
      Ref.make[Option[Either[Result[R, E, A], Result[R1, E1, B]]]](None).flatMap { ref =>
        val left: URIO[(R, QueryContext), Any] =
          self.start { l =>
            ref.modify {
              case Some(Right(r)) =>
                ZIO.effectSuspendTotal(cb(l.zipWithPar(r)(f))) -> Some(Right(r))
              case _ =>
                ZIO.unit -> Some(Left(l))
            }.flatten
          }
        val right: URIO[(R1, QueryContext), Any] =
          that.start { r =>
            ref.modify {
              case Some(Left(l)) =>
                ZIO.effectSuspendTotal(cb(l.zipWithPar(r)(f))) -> Some(Left(l))
              case _ =>
                ZIO.unit -> Some(Right(r))
            }.flatten
          }
        left *> right
      }
    }
}

object ZQuery {

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
  final def accessM[R]: AccessMPartiallyApplied[R] =
    new AccessMPartiallyApplied[R]

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
    ZQuery.halt(Cause.die(t))

  /**
   * Accesses the whole environment of the query.
   */
  def environment[R]: ZQuery[R, Nothing, R] =
    ZQuery(cb => ZIO.accessM(r => cb(Result.done(r._1))))

  /**
   * Constructs a query that fails with the specified error.
   */
  def fail[E](error: => E): ZQuery[Any, E, Nothing] =
    ZQuery.halt(Cause.fail(error))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed sequentially and will be pipelined.
   */
  def foreach[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(f: A => ZQuery[R, E, B])(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result)
    else {
      val iterator = as.iterator
      var builder  = f(iterator.next()).map(bf.newBuilder(as) += _)
      while (iterator.hasNext) {
        val a = iterator.next()
        builder = builder.zipWith(f(a))(_ += _)
      }
      builder.map(_.result())
    }

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a query returning a collection of their results. Requests will be
   * executed in parallel and will be batched.
   */
  def foreachPar[R, E, A, B, Collection[+Element] <: Iterable[Element]](
    as: Collection[A]
  )(f: A => ZQuery[R, E, B])(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): ZQuery[R, E, Collection[B]] =
    if (as.isEmpty) ZQuery.succeed(bf.newBuilder(as).result)
    else {
      val size = as.size
      ZQuery { cb =>
        ZIO.effectTotal(Array.ofDim[Result[R, E, B]](size)).zip(Ref.make(size)).flatMap { case (array, ref) =>
          ZIO.foreach(as.zipWithIndex) { case (a, i) =>
            f(a).start { result =>
              ZIO.effectTotal(array(i) = result) *>
                ref.modify {
                  case n if n == 1 =>
                    (
                      ZIO.effectSuspendTotal {
                        var builder = array(0).map(bf.newBuilder(as) += _)
                        var i       = 1
                        while (i < size) {
                          val result = array(i)
                          builder = builder.zipWithPar(result)(_ += _)
                          i += 1
                        }
                        cb(builder.map(_.result()))
                      },
                      n - 1
                    )
                  case n =>
                    (ZIO.unit, n - 1)
                }.flatten
            }
          }
        }
      }
    }

  /**
   * Constructs a query from an effect.
   */
  def fromEffect[R, E, A](effect: ZIO[R, E, A]): ZQuery[R, E, A] =
    ZQuery { cb =>
      ZIO.transplant { graft =>
        graft {
          effect.foldCauseM(e => cb(Result.fail(e)), a => cb(Result.done(a))).provideSome[(R, QueryContext)](_._1)
        }.fork
      }
    }

  /**
   * Constructs a query from a request and a data source. Queries will die with
   * a `QueryFailure` when run if the data source does not provide results for
   * all requests received. Queries must be constructed with `fromRequest` or
   * one of its variants for optimizations to be applied.
   */
  def fromRequest[R, E, A, B](
    request: A
  )(dataSource: DataSource[R, A])(implicit ev: A <:< Request[E, B]): ZQuery[R, E, B] =
    ZQuery { cb =>
      ZIO.accessM[(R, QueryContext)] { case (r, queryContext) =>
        queryContext.cache.lookup(request).flatMap {
          case Left(ref) =>
            cb {
              Result.blocked(
                BlockedRequests.single(dataSource, BlockedRequest(request, ref)),
                Continue(request, dataSource, ref)
              )
            }
          case Right(ref) =>
            ref.get.flatMap {
              case None    => cb(Result.blocked(BlockedRequests.empty, Continue(request, dataSource, ref)))
              case Some(b) => cb(Result.fromEither(b))
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
    ZQuery { cb =>
      Ref.make(Option.empty[Either[E, B]]).flatMap { ref =>
        cb {
          Result.blocked(
            BlockedRequests.single(dataSource, BlockedRequest(request, ref)),
            Continue(request, dataSource, ref)
          )
        }
      }
    }

  /**
   * Constructs a query that fails with the specified cause.
   */
  def halt[E](cause: => Cause[E]): ZQuery[Any, E, Nothing] =
    ZQuery { cb =>
      ZIO.effectSuspendTotalWith { (platform, _) =>
        cb {
          try Result.fail(cause)
          catch { case t if !platform.fatal(t) => Result.fail(Cause.die(t)) }
        }
      }
    }

  /**
   * Constructs a query that never completes.
   */
  val never: ZQuery[Any, Nothing, Nothing] =
    ZQuery(_ => ZIO.unit)

  /**
   * Constructs a query that succeds with the empty value.
   */
  val none: ZQuery[Any, Nothing, Option[Nothing]] =
    ZQuery(cb => cb(Result.done(None)))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed sequentially and will be pipelined.
   */
  def partitionM[R, E, A, B](
    as: Iterable[A]
  )(f: A => ZQuery[R, E, B])(implicit ev: CanFail[E]): ZQuery[R, Nothing, (Iterable[E], Iterable[B])] =
    ZQuery.foreach(as)(f(_).either).map(partitionMap(_)(identity))

  /**
   * Performs a query for each element in a collection, collecting the results
   * into a collection of failed results and a collection of successful
   * results. Requests will be executed in parallel and will be batched.
   */
  def partitionMPar[R, E, A, B](
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
    ZQuery { cb =>
      ZIO.effectSuspendTotalWith { (platform, _) =>
        cb {
          try Result.done(value)
          catch { case t if !platform.fatal(t) => Result.fail(Cause.die(t)) }
        }
      }
    }

  /**
   * Constructs a query from a callback that takes a result.
   */
  private def apply[R, E, A](
    start: (Result[R, E, A] => UIO[Any]) => ZIO[(R, QueryContext), Nothing, Any]
  ): ZQuery[R, E, A] =
    new ZQuery(start)

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

  final class AccessPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[A](f: R => A): ZQuery[R, Nothing, A] =
      environment[R].map(f)
  }

  final class AccessMPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {
    def apply[E, A](f: R => ZQuery[R, E, A]): ZQuery[R, E, A] =
      environment[R].flatMap(f)
  }

  final class ProvideSomeLayer[R0 <: Has[_], -R, +E, +A](private val self: ZQuery[R, E, A]) extends AnyVal {
    def apply[E1 >: E, R1 <: Has[_]](
      layer: Described[ZLayer[R0, E1, R1]]
    )(implicit ev1: R0 with R1 <:< R, ev2: NeedsEnv[R], tag: Tag[R1]): ZQuery[R0, E1, A] =
      self.provideLayer[E1, R0, R0 with R1](Described(ZLayer.identity[R0] ++ layer.value, layer.description))
  }
}
