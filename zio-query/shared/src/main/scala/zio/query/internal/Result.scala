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
import zio.query.internal.Result._
import zio.query.{DataSourceAspect, Described}
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Result[R, E, A]` is the result of running one step of a `ZQuery`. A result
 * may either by done with a value `A`, blocked on a set of requests to data
 * sources that require an environment `R`, or failed with an `E`.
 */
private[query] sealed abstract class Result[-R, +E, +A] { self =>

  /**
   * Maps the specified function over the successful value of this result.
   */
  final def map[B](f: A => B)(implicit trace: Trace): Result[R, E, B] =
    self match {
      case Blocked(br, c) => blocked(br, c.map(f))
      case Done(a)        => done(f(a))
      case e              => e.widen
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Result[R1, E, A] =
    self match {
      case Blocked(br, c) => Result.blocked(br.mapDataSources(f), c.mapDataSources(f))
      case doneOrFail     => doneOrFail.widen
    }

  /**
   * Maps the specified function over the failure cause of this result.
   */
  final def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Result[R, E1, A] =
    self match {
      case Blocked(br, c) => blocked(br, c.mapErrorCause(f))
      case Fail(e)        => Fail(f(e))
      case done           => done.widen
    }

  /**
   * Provides this result with part of its required environment.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Result[R0, E, A] =
    self match {
      case Blocked(br, c) => blocked(br.provideSomeEnvironment(f), c.provideSomeEnvironment(f))
      case doneOrFail     => doneOrFail.widen
    }

  @inline
  final def widen[R1, E1, A1]: Result[R1, E1, A1] =
    self.asInstanceOf[Result[R1, E1, A1]]
}

private[query] object Result {

  /**
   * Constructs a result that is blocked on the specified requests with the
   * specified continuation.
   */
  def blocked[R, E, A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A]): Result[R, E, A] =
    Blocked(blockedRequests, continue)

  def blockedExit[R, E, A](
    blockedRequests: BlockedRequests[R],
    continue: Continue[R, E, A]
  ): Exit[Nothing, Result[R, E, A]] =
    Exit.Success(Blocked(blockedRequests, continue))

  /**
   * Constructs a result that is done with the specified value.
   */
  def done[A](value: A): Result[Any, Nothing, A] =
    Done(value)

  def doneExit[A](value: A): Exit[Nothing, Result[Any, Nothing, A]] =
    Exit.Success(Done(value))

  /**
   * Constructs a result that is failed with the specified `Cause`.
   */
  def fail[E](cause: Cause[E]): Result[Any, E, Nothing] =
    Fail(cause)

  def failExit[E](cause: Cause[E]): Exit[Nothing, Result[Any, E, Nothing]] =
    if (cause.isFailure) Exit.Success(Fail(cause))
    else Exit.Failure(cause.asInstanceOf[Cause[Nothing]])

  /**
   * Lifts an `Exit` into a result.
   */
  def fromExit[E, A](exit: Exit[E, A]): Result[Any, E, A] =
    exit match {
      case Exit.Success(a) => Done(a)
      case Exit.Failure(e) => Fail(e)
    }

  val unit: Result[Any, Nothing, Unit] = done(())

  final case class Blocked[-R, +E, +A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A])
      extends Result[R, E, A]

  final case class Done[+A](value: A) extends Result[Any, Nothing, A]

  final case class Fail[+E](cause: Cause[E]) extends Result[Any, E, Nothing]
}
