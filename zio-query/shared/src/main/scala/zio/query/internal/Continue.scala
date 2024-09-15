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
import zio.query.internal.Continue._
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
private[query] sealed trait Continue[-R, +E, +A] { self =>

  /**
   * Purely folds over the failure and success types of this continuation.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Continue[R, Nothing, B] =
    self match {
      case Effect(query) => Effect(query.fold(failure, success))
      case Get(io)       => Get(io.foldZIO(e => Exit.succeed(failure(e)), a => Exit.succeed(success(a))))
    }

  /**
   * Effectually folds over the failure and success types of this continuation.
   */
  final def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => Effect(query.foldCauseQuery(failure, success))
      case Get(io)       => Effect(ZQuery.fromZIONow(io).foldCauseQuery(failure, success))
    }

  final def foldCauseZIO[R1 <: R, E1, B](
    failure: Cause[E] => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => Effect(query.foldCauseZIO(failure, success))
      case Get(io)       => Get(io.foldCauseZIO(failure, success))
    }

  final def foldZIO[R1 <: R, E1, B](
    failure: E => ZIO[R1, E1, B],
    success: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    foldCauseZIO(_.failureOrCause.fold(failure, Exit.failCause), success)

  /**
   * Purely maps over the success type of this continuation.
   */
  final def map[B](f: A => B)(implicit trace: Trace): Continue[R, E, B] =
    self match {
      case Effect(query) => Effect(query.map(f))
      case Get(io)       => Get(io.map(f))
    }

  final def mapBothCause[E1, B](failure: Cause[E] => Cause[E1], success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Continue[R, E1, B] =
    self match {
      case Effect(query) => Effect(query.mapBothCause(failure, success))
      case Get(io)       => Get(io.foldCauseZIO(e => Exit.failCause(failure(e)), a => Exit.succeed(success(a))))
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Continue[R1, E, A] =
    self match {
      case Effect(query) => Effect(query.mapDataSources(f))
      case Get(io)       => Get(io)
    }

  /**
   * Purely maps over the failure type of this continuation.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Continue[R, E1, A] =
    self match {
      case Effect(query) => Effect(query.mapError(f))
      case Get(io)       => Get(io.mapError(f))
    }

  /**
   * Purely maps over the failure cause of this continuation.
   */
  final def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Continue[R, E1, A] =
    self match {
      case Effect(query) => Effect(query.mapErrorCause(f))
      case Get(io)       => Get(io.mapErrorCause(f))
    }

  /**
   * Effectually maps over the success type of this continuation.
   */
  final def mapQuery[R1 <: R, E1 >: E, B](
    f: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => Effect(query.flatMap(f))
      case Get(io)       => Effect(ZQuery.fromZIONow(io).flatMap(f))
    }

  /**
   * Effectually maps over the success type of this continuation.
   */
  final def mapZIO[R1 <: R, E1 >: E, B](
    f: A => ZIO[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => Effect(query.mapZIO(f))
      case Get(io)       => Get(io.flatMap(f))
    }

  /**
   * Purely contramaps over the environment type of this continuation.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Continue[R0, E, A] =
    self match {
      case Effect(query) => Effect(query.provideSomeEnvironment(f))
      case Get(io)       => Get(io.provideSomeEnvironment(f.value))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in sequence.
   */
  final def zipWith[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => Effect(l.zipWith(r)(f))
      case (Effect(l), Get(r))    => Effect(l.zipWith(ZQuery.fromZIONow(r))(f))
      case (Get(l), Effect(r))    => Effect(ZQuery.fromZIONow(l).zipWith(r)(f))
      case (Get(l), Get(r))       => Get(l.zipWith(r)(f))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in parallel.
   */
  final def zipWithPar[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => Effect(l.zipWithPar(r)(f))
      case (Effect(l), Get(r))    => Effect(l.zipWith(ZQuery.fromZIONow(r))(f))
      case (Get(l), Effect(r))    => Effect(ZQuery.fromZIONow(l).zipWith(r)(f))
      case (Get(l), Get(r))       => Get(l.zipWith(r)(f))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, batching requests to data sources.
   */
  final def zipWithBatched[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => Effect(l.zipWithBatched(r)(f))
      case (Effect(l), Get(r))    => Effect(l.zipWith(ZQuery.fromZIONow(r))(f))
      case (Get(l), Effect(r))    => Effect(ZQuery.fromZIONow(l).zipWith(r)(f))
      case (Get(l), Get(r))       => Get(l.zipWith(r)(f))
    }
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

  final case class Effect[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A]
  final case class Get[R, E, A](io: ZIO[R, E, A])          extends Continue[R, E, A]
}
