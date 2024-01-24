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

import zio.Exit
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `CompletedRequestMap` is a universally quantified mapping from requests of
 * type `Request[E, A]` to results of type `Exit[E, A]` for all types `E` and
 * `A`. The guarantee is that for any request of type `Request[E, A]`, if there
 * is a corresponding value in the map, that value is of type `Exit[E, A]`. This
 * is used by the library to support data sources that return different result
 * types for different requests while guaranteeing that results will be of the
 * type requested.
 */
final class CompletedRequestMap private (private val map: Map[Any, Exit[Any, Any]]) { self =>

  def ++(that: CompletedRequestMap): CompletedRequestMap =
    new CompletedRequestMap(self.map ++ that.map)

  /**
   * Returns whether a result exists for the specified request.
   */
  def contains(request: Any): Boolean =
    map.contains(request)

  /**
   * Appends the specified result to the completed requests map.
   */
  def insert[E, A](request: Request[E, A], result: Exit[E, A]): CompletedRequestMap =
    new CompletedRequestMap(self.map + (request -> result))

  /**
   * Appends the specified optional result to the completed request map.
   */
  def insertOption[E, A](request: Request[E, A], result: Exit[E, Option[A]]): CompletedRequestMap =
    result match {
      case Exit.Failure(e)       => insert(request, Exit.failCause(e))
      case Exit.Success(Some(a)) => insert(request, Exit.succeed(a))
      case Exit.Success(None)    => self
    }

  /**
   * Retrieves the result of the specified request if it exists.
   */
  def lookup[E, A](request: Request[E, A]): Option[Exit[E, A]] =
    map.get(request).asInstanceOf[Option[Exit[E, A]]]

  /**
   * Collects all requests in a set.
   */
  def requests: Set[Request[_, _]] =
    map.keySet.asInstanceOf[Set[Request[_, _]]]

  override def toString: String =
    s"CompletedRequestMap(${map.mkString(", ")})"
}

object CompletedRequestMap {

  /**
   * An empty completed requests map.
   */
  val empty: CompletedRequestMap =
    new CompletedRequestMap(Map.empty)

  /**
   * Constructs a completed requests map from the specified results.
   */
  def fromIterable[E, A](iterable: Iterable[(Request[E, A], Exit[E, A])]): CompletedRequestMap =
    new CompletedRequestMap(iterable.toMap)

  /**
   * Constructs a completed requests map from the specified optional results.
   */
  def fromIterableOption[E, A](iterable: Iterable[(Request[E, A], Exit[E, Option[A]])]): CompletedRequestMap = {
    val builder = Map.newBuilder[Any, Exit[Any, Any]]
    builder.sizeHint(iterable.size)
    iterable.foreach { case (request, result) =>
      result match {
        case Exit.Failure(e)       => builder += (request -> Exit.failCause(e))
        case Exit.Success(Some(a)) => builder += (request -> Exit.succeed(a))
        case Exit.Success(None)    => ()
      }
    }
    new CompletedRequestMap(builder.result())
  }
}
