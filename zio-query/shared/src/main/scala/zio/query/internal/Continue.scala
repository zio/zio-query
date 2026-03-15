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

package zio.query.internal

import zio._
import zio.query._
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Continue[R, E, A]` models a continuation of a blocked request that
 * requires an environment `R` and may either fail with an `E` or succeed with
 * an `A`. A continuation may either be a `Get` that merely gets the result of a
 * blocked request (potentially transforming it with pure functions) or an
 * `Effect` that may perform arbitrary effects. This is used by the library
 * internally to determine whether it is safe to pipeline two requests that must
 * be executed sequentially.
 */
private[query] abstract class Continue[-R, +E, +A] { self =>

  /**
   * Recovers from all errors.
   */
  def catchAll[R1 <: R, E1, A1 >: A](
    failure: E => ZQuery[R1, E1, A1]
  )(implicit trace: Trace): Continue[R1, E1, A1]

  /**
   * Recovers from all errors with the provided Cause.
   */
  def catchAllCause[R1 <: R, E1, A1 >: A](
    failure: Cause[E] => ZQuery[R1, E1, A1]
  )(implicit trace: Trace): Continue[R1, E1, A1]

  /**
   * Purely folds over the failure and success types of this continuation.
   */
  def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Continue[R, Nothing, B]

  /**
   * Effectually folds over the failure and success types of this continuation.
   */
  def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  def foldCauseZIO[R1 <: R, E1, B](
    failure: Cause[E] => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  def foldQuery[R1 <: R, E1, B](
    failure: E => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  def foldZIO[R1 <: R, E1, B](
    failure: E => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  /**
   * Purely maps over the success type of this continuation.
   */
  def map[B](f: A => B)(implicit trace: Trace): Continue[R, E, B]

  def mapBothCause[E1, B](failure: Cause[E] => Cause[E1], success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Continue[R, E1, B]

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Continue[R1, E, A]

  /**
   * Purely maps over the failure type of this continuation.
   */
  def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Continue[R, E1, A]

  /**
   * Purely maps over the failure cause of this continuation.
   */
  def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Continue[R, E1, A]

  /**
   * Effectually maps over the success type of this continuation.
   */
  def mapQuery[R1 <: R, E1 >: E, B](
    f: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  /**
   * Effectually maps over the success type of this continuation.
   */
  def mapZIO[R1 <: R, E1 >: E, B](
    f: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B]

  /**
   * Purely contramaps over the environment type of this continuation.
   */
  def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Continue[R0, E, A]

  /**
   * Combines this continuation with that continuation using the specified
   * function, in sequence.
   */
  def zipWith[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C]

  /**
   * Combines this continuation with that continuation using the specified
   * function, in parallel.
   */
  def zipWithPar[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C]

  /**
   * Combines this continuation with that continuation using the specified
   * function, batching requests to data sources.
   */
  def zipWithBatched[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C]
}

private[query] object Continue {

  /**
   * Constructs a continuation from a request, a data source, and a `Promise`
   * that will contain the result of the request when it is executed.
   */
  def apply[E, A](promise: Promise[E, A])(implicit trace: Trace): Continue[Any, E, A] =
    Get(promise.await)

  /**
   * Constructs a continuation that may perform arbitrary effects.
   */
  def effect[R, E, A](query: ZQuery[R, E, A]): Continue[R, E, A] =
    Effect(query)

  /**
   * Constructs a continuation that merely gets the result of a blocked request
   * (potentially transforming it with pure functions).
   */
  def get[R, E, A](io: ZIO[R, E, A]): Continue[R, E, A] =
    Get(io)

  final case class Effect[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A] {
    override def catchAll[R1 <: R, E1, A1 >: A](
      failure: E => ZQuery[R1, E1, A1]
    )(implicit trace: Trace): Continue[R1, E1, A1] =
      Effect(query.catchAll(failure))

    override def catchAllCause[R1 <: R, E1, A1 >: A](
      failure: Cause[E] => ZQuery[R1, E1, A1]
    )(implicit trace: Trace): Continue[R1, E1, A1] =
      Effect(query.catchAllCause(failure))

    override def fold[B](failure: E => B, success: A => B)(implicit
      ev: CanFail[E],
      trace: Trace
    ): Continue[R, Nothing, B] =
      Effect(query.fold(failure, success))

    override def foldCauseQuery[R1 <: R, E1, B](
      failure: Cause[E] => ZQuery[R1, E1, B],
      success: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.foldCauseQuery(failure, success))

    override def foldCauseZIO[R1 <: R, E1, B](
      failure: Cause[E] => ZIO[R1, E1, B],
      success: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.foldCauseZIO(failure, success))

    override def foldQuery[R1 <: R, E1, B](
      failure: E => ZQuery[R1, E1, B],
      success: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.foldQuery(failure, success))

    override def foldZIO[R1 <: R, E1, B](
      failure: E => ZIO[R1, E1, B],
      success: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.foldZIO(failure, success))

    override def map[B](f: A => B)(implicit trace: Trace): Continue[R, E, B] =
      Effect(query.map(f))

    override def mapBothCause[E1, B](failure: Cause[E] => Cause[E1], success: A => B)(implicit
      ev: CanFail[E],
      trace: Trace
    ): Continue[R, E1, B] =
      Effect(query.mapBothCause(failure, success))

    override def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Continue[R1, E, A] =
      Effect(query.mapDataSources(f))

    override def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Continue[R, E1, A] =
      Effect(query.mapError(f))

    override def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Continue[R, E1, A] =
      Effect(query.mapErrorCause(f))

    override def mapQuery[R1 <: R, E1 >: E, B](
      f: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.flatMap(f))

    override def mapZIO[R1 <: R, E1 >: E, B](
      f: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(query.mapZIO(f))

    override def provideSomeEnvironment[R0](
      f: Described[ZEnvironment[R0] => ZEnvironment[R]]
    )(implicit trace: Trace): Continue[R0, E, A] =
      Effect(query.provideSomeEnvironment(f))

    override def zipWith[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(query.zipWith(r)(f))
        case Get(r)    => Effect(query.zipWith(ZQuery.fromZIONow(r))(f))
      }

    override def zipWithPar[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(query.zipWithPar(r)(f))
        case Get(r)    => Effect(query.zipWith(ZQuery.fromZIONow(r))(f))
      }

    override def zipWithBatched[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(query.zipWithBatched(r)(f))
        case Get(r)    => Effect(query.zipWith(ZQuery.fromZIONow(r))(f))
      }
  }

  final case class Get[R, E, A](io: ZIO[R, E, A]) extends Continue[R, E, A] {
    override def catchAll[R1 <: R, E1, A1 >: A](
      failure: E => ZQuery[R1, E1, A1]
    )(implicit trace: Trace): Continue[R1, E1, A1] =
      Effect(ZQuery.fromZIONow(io).catchAll(failure))

    override def catchAllCause[R1 <: R, E1, A1 >: A](
      failure: Cause[E] => ZQuery[R1, E1, A1]
    )(implicit trace: Trace): Continue[R1, E1, A1] =
      Effect(ZQuery.fromZIONow(io).catchAllCause(failure))

    override def fold[B](failure: E => B, success: A => B)(implicit
      ev: CanFail[E],
      trace: Trace
    ): Continue[R, Nothing, B] =
      Get(io.fold(failure, success))

    override def foldCauseQuery[R1 <: R, E1, B](
      failure: Cause[E] => ZQuery[R1, E1, B],
      success: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(ZQuery.fromZIONow(io).foldCauseQuery(failure, success))

    override def foldCauseZIO[R1 <: R, E1, B](
      failure: Cause[E] => ZIO[R1, E1, B],
      success: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Get(io.foldCauseZIO(failure, success))

    override def foldQuery[R1 <: R, E1, B](
      failure: E => ZQuery[R1, E1, B],
      success: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(ZQuery.fromZIONow(io).foldQuery(failure, success))

    override def foldZIO[R1 <: R, E1, B](
      failure: E => ZIO[R1, E1, B],
      success: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Get(io.foldZIO(failure, success))

    override def map[B](f: A => B)(implicit trace: Trace): Continue[R, E, B] =
      Get(io.map(f))

    override def mapBothCause[E1, B](failure: Cause[E] => Cause[E1], success: A => B)(implicit
      ev: CanFail[E],
      trace: Trace
    ): Continue[R, E1, B] =
      Get(io.foldCauseZIO(e => Exit.failCause(failure(e)), a => Exit.succeed(success(a))))

    override def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Continue[R1, E, A] =
      this

    override def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Continue[R, E1, A] =
      Get(io.mapError(f))

    override def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Continue[R, E1, A] =
      Get(io.mapErrorCause(f))

    override def mapQuery[R1 <: R, E1 >: E, B](
      f: A => ZQuery[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Effect(ZQuery.fromZIONow(io).flatMap(f))

    override def mapZIO[R1 <: R, E1 >: E, B](
      f: A => ZIO[R1, E1, B]
    )(implicit trace: Trace): Continue[R1, E1, B] =
      Get(io.flatMap(f))

    override def provideSomeEnvironment[R0](
      f: Described[ZEnvironment[R0] => ZEnvironment[R]]
    )(implicit trace: Trace): Continue[R0, E, A] =
      Get(io.provideSomeEnvironment(f.value))

    override def zipWith[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(ZQuery.fromZIONow(io).zipWith(r)(f))
        case Get(r)    => Get(io.zipWith(r)(f))
      }

    override def zipWithPar[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(ZQuery.fromZIONow(io).zipWith(r)(f))
        case Get(r)    => Get(io.zipWith(r)(f))
      }

    override def zipWithBatched[R1 <: R, E1 >: E, B, C](
      that: Continue[R1, E1, B]
    )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
      that match {
        case Effect(r) => Effect(ZQuery.fromZIONow(io).zipWith(r)(f))
        case Get(r)    => Get(io.zipWith(r)(f))
      }
  }
}
