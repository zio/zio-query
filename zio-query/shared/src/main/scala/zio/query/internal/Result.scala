package zio.query.internal

import zio._
import zio.query.internal.Result._
import zio.query.{ DataSourceAspect, Described }
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Result[R, E, A]` is the result of running one step of a `ZQuery`. A
 * result may either by done with a value `A`, blocked on a set of requests
 * to data sources that require an environment `R`, or failed with an `E`.
 */
private[query] sealed trait Result[-R, +E, +A] { self =>

  /**
   * Folds over the successful or failed result.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Result[R, Nothing, B] =
    self match {
      case Blocked(br, c) => blocked(br, c.fold(failure, success))
      case Done(a)        => done(success(a))
      case Fail(e)        => e.failureOrCause.fold(e => done(failure(e)), c => fail(c))
    }

  /**
   * Maps the specified function over the successful value of this result.
   */
  final def map[B](f: A => B)(implicit trace: Trace): Result[R, E, B] =
    self match {
      case Blocked(br, c) => blocked(br, c.map(f))
      case Done(a)        => done(f(a))
      case Fail(e)        => fail(e)
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Result[R1, E, A] =
    self match {
      case Blocked(br, c) => Result.blocked(br.mapDataSources(f), c.mapDataSources(f))
      case Done(a)        => Result.done(a)
      case Fail(e)        => Result.fail(e)
    }

  /**
   * Maps the specified function over the failed value of this result.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Result[R, E1, A] =
    self match {
      case Blocked(br, c) => blocked(br, c.mapError(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(e.map(f))
    }

  /**
   * Maps the specified function over the failure cause of this result.
   */
  def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Result[R, E1, A] =
    self match {
      case Blocked(br, c) => blocked(br, c.mapErrorCause(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(f(e))
    }

  /**
   * Provides this result with its required environment.
   */
  @deprecated("use provideEnvironment", "2.0.0")
  final def provide(r: Described[ZEnvironment[R]])(implicit trace: Trace): Result[Any, E, A] =
    provideEnvironment(r)

  /**
   * Provides this result with its required environment.
   */
  final def provideEnvironment(
    r: Described[ZEnvironment[R]]
  )(implicit trace: Trace): Result[Any, E, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Provides this result with part of its required environment.
   */
  @deprecated("use provideSomeEnvironment", "2.0.0")
  final def provideSome[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Result[R0, E, A] =
    provideSomeEnvironment(f)

  /**
   * Provides this result with part of its required environment.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Result[R0, E, A] =
    self match {
      case Blocked(br, c) => blocked(br.provideSomeEnvironment(f), c.provideSomeEnvironment(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(e)
    }
}

private[query] object Result {

  /**
   * Constructs a result that is blocked on the specified requests with the
   * specified continuation.
   */
  def blocked[R, E, A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A]): Result[R, E, A] =
    Blocked(blockedRequests, continue)

  /**
   * Collects a collection of results into a single result. Blocked requests
   * and their continuations will be executed in parallel.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](results: Collection[Result[R, E, A]])(implicit
    bf: BuildFrom[Collection[Result[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Result[R, E, Collection[A]] =
    results.zipWithIndex
      .foldLeft[(Chunk[((BlockedRequests[R], Continue[R, E, A]), Int)], Chunk[(A, Int)], Chunk[(Cause[E], Int)])](
        (Chunk.empty, Chunk.empty, Chunk.empty)
      ) { case ((blocked, done, fails), (result, index)) =>
        result match {
          case Blocked(br, c) => (blocked :+ (((br, c), index)), done, fails)
          case Done(a)        => (blocked, done :+ ((a, index)), fails)
          case Fail(e)        => (blocked, done, fails :+ ((e, index)))
        }
      } match {
      case (Chunk(), done, Chunk()) =>
        Result.done(bf.fromSpecific(results)(done.map(_._1)))
      case (blocked, done, Chunk()) =>
        val blockedRequests = blocked.map(_._1._1).foldLeft[BlockedRequests[R]](BlockedRequests.empty)(_ && _)
        val continue = Continue.collectAllPar(blocked.map(_._1._2)).map { as =>
          val array = Array.ofDim[AnyRef](results.size)
          as.zip(blocked.map(_._2)).foreach { case (a, i) =>
            array(i) = a.asInstanceOf[AnyRef]
          }
          done.foreach { case (a, i) =>
            array(i) = a.asInstanceOf[AnyRef]
          }
          bf.fromSpecific(results)(array.asInstanceOf[Array[A]])
        }
        Result.blocked(blockedRequests, continue)
      case (_, _, fail) =>
        Result.fail(fail.map(_._1).foldLeft[Cause[E]](Cause.empty)(_ && _))
    }

  /**
   * Constructs a result that is done with the specified value.
   */
  def done[A](value: A): Result[Any, Nothing, A] =
    Done(value)

  /**
   * Constructs a result that is failed with the specified `Cause`.
   */
  def fail[E](cause: Cause[E]): Result[Any, E, Nothing] =
    Fail(cause)

  /**
   * Lifts an `Either` into a result.
   */
  def fromEither[E, A](either: Either[E, A]): Result[Any, E, A] =
    either.fold(e => Result.fail(Cause.fail(e)), a => Result.done(a))

  /**
   * Lifts an `Exit` into a result.
   */
  def fromExit[E, A](exit: Exit[E, A]): Result[Any, E, A] =
    exit.foldExit(Result.fail, Result.done)

  final case class Blocked[-R, +E, +A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A])
      extends Result[R, E, A]

  final case class Done[+A](value: A) extends Result[Any, Nothing, A]

  final case class Fail[+E](cause: Cause[E]) extends Result[Any, E, Nothing]
}
