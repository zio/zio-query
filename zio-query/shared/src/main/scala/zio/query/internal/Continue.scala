package zio.query.internal

import zio.query.internal.Continue._
import zio.query.{ Cache, DataSource, Described, QueryFailure, Request, ZQuery }
import zio.{ CanFail, Cause, IO, NeedsEnv, Ref, ZIO }

/**
 * A `Continue[R, E, A]` models a continuation of a blocked request that
 * requires an environment `R` and may either fail with an `E` or succeed with
 * an `A`. A continuation may either be a `Get` that merely gets the result of
 * a blocked request (potentially transforming it with pure functions) or an
 * `Effect` that may perform arbitrary effects. This is used by the library
 * internally to determine whether it is safe to pipeline two requests that
 * must be executed sequentially.
 */
private[query] sealed trait Continue[-R, +E, +A] { self =>

  /**
   * Purely folds over the failure and success types of this continuation.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit ev: CanFail[E]): Continue[R, Nothing, B] =
    self match {
      case Get(query)    => get(query.fold(failure, success))
      case Effect(query) => effect(query.fold(failure, success))
    }

  /**
   * Effectually folds over the failure and success types of this continuation.
   */
  final def foldCauseM[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  ): Continue[R1, E1, B] =
    self match {
      case Get(io)       => effect(ZQuery.fromEffect(io).foldCauseM(failure, success))
      case Effect(query) => effect(query.foldCauseM(failure, success))
    }

  /**
   * Purely maps over the success type of this continuation.
   */
  def map[B](f: A => B): Continue[R, E, B] =
    self match {
      case Get(io)       => get(io.map(f))
      case Effect(query) => effect(query.map(f))
    }

  /**
   * Purely maps over the failure type of this continuation.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E]): Continue[R, E1, A] =
    self match {
      case Get(io)       => get(io.mapError(f))
      case Effect(query) => effect(query.mapError(f))
    }

  /**
   * Effectually maps over the success type of this continuation.
   */
  def mapM[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): Continue[R1, E1, B] =
    self match {
      case Get(io)       => effect(ZQuery.fromEffect(io).flatMap(f))
      case Effect(query) => effect(query.flatMap(f))
    }

  /**
   * Purely contramaps over the environment type of this continuation.
   */
  def provideSome[R0](f: Described[R0 => R])(implicit ev: NeedsEnv[R]): Continue[R0, E, A] =
    self match {
      case Get(io)       => get(io)
      case Effect(query) => effect(query.provideSome(f))
    }

  /**
   * Runs this continuation..
   */
  def runCache(cache: Cache): ZIO[R, E, A] =
    self match {
      case Get(io)       => io
      case Effect(query) => query.runCache(cache)
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in sequence.
   */
  def zipWith[R1 <: R, E1 >: E, B, C](that: Continue[R1, E1, B])(f: (A, B) => C): Continue[R1, E1, C] =
    (self, that) match {
      case (Get(l), Get(r))       => get(l.zipWith(r)(f))
      case (Get(l), Effect(r))    => effect(ZQuery.fromEffect(l).zipWith(r)(f))
      case (Effect(l), Get(r))    => effect(l.zipWith(ZQuery.fromEffect(r))(f))
      case (Effect(l), Effect(r)) => effect(l.zipWith(r)(f))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in parallel.
   */
  def zipWithPar[R1 <: R, E1 >: E, B, C](that: Continue[R1, E1, B])(f: (A, B) => C): Continue[R1, E1, C] =
    (self, that) match {
      case (Get(l), Get(r))       => get(l.zipWithPar(r)(f))
      case (Get(l), Effect(r))    => effect(ZQuery.fromEffect(l).zipWithPar(r)(f))
      case (Effect(l), Get(r))    => effect(l.zipWithPar(ZQuery.fromEffect(r))(f))
      case (Effect(l), Effect(r)) => effect(l.zipWithPar(r)(f))
    }

}

private[query] object Continue {

  /**
   * Constructs a continuation from a request, a data source, and a `Ref` that
   * will contain the result of the request when it is executed.
   */
  def apply[R, E, A, B](request: A, dataSource: DataSource[R, A], ref: Ref[Option[Either[E, B]]])(
    implicit ev: A <:< Request[E, B]
  ): Continue[R, E, B] =
    Continue.get {
      ref.get.flatMap {
        case None    => IO.die(QueryFailure(dataSource, request))
        case Some(b) => IO.fromEither(b)
      }
    }

  /**
   * Constructs a continuation that may perform arbitrary effects.
   */
  def effect[R, E, A](query: ZQuery[R, E, A]): Continue[R, E, A] =
    Effect(query)

  /**
   * Constructs a continuation that merely gets the result of a blocked request
   * (potentially transforming it with pure functions).
   */
  def get[E, A](io: IO[E, A]): Continue[Any, E, A] =
    Get(io)

  final case class Effect[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A]
  final case class Get[E, A](io: IO[E, A])                 extends Continue[Any, E, A]
}
