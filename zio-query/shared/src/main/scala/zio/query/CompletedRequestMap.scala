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
   * Constructs a completed requests map from an existing Iterable of tuples
   */
  def from[E, A, B](it: Iterable[(A, Exit[E, B])])(implicit ev: A <:< Request[E, B]): CompletedRequestMap =
    new CompletedRequestMap(Map.from(it))

  private[query] def fromOptional[E, A, B](
    it: Iterable[(A, Exit[E, Option[B]])]
  )(implicit ev: A <:< Request[E, B]): CompletedRequestMap = {
    val builder = Map.newBuilder[A, Exit[E, B]]
    val iter    = it.iterator
    while (iter.hasNext) {
      val (key, exit) = iter.next()
      exit match {
        case Exit.Failure(e)       => builder += ((key, Exit.failCause(e)))
        case Exit.Success(Some(a)) => builder += ((key, Exit.succeed(a)))
        case Exit.Success(None)    => ()
      }
    }
    from(builder.result())
  }

  // Only used by Scala 2.12 where the `Map.from` method doesn't exist
  private implicit class EnrichedMapOps[E, A, B](private val map: Map.type) extends AnyVal {
    def from[K, V](it: Iterable[(K, V)]): Map[K, V] =
      (Map.newBuilder[K, V] ++= it).result
  }

}
