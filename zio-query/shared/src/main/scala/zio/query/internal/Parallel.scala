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

import zio.Chunk
import zio.query.DataSource
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Parallel[R]` maintains a mapping from data sources to requests from those
 * data sources that can be executed in parallel.
 */
private[query] final class Parallel[-R](private val map: Map[DataSource[Any, Any], Chunk[BlockedRequest[Any]]]) {
  self =>

  /**
   * Combines this collection of requests that can be executed in parallel with
   * that collection of requests that can be executed in parallel to return a
   * new collection of requests that can be executed in parallel.
   */
  def ++[R1 <: R](that: Parallel[R1]): Parallel[R1] =
    new Parallel(
      self.map.foldLeft(that.map) { case (map, (k, v)) =>
        map + (k -> map.get(k).fold[Chunk[BlockedRequest[Any]]](v)(_ ++ v))
      }
    )

  /**
   * Returns whether this collection of requests is empty.
   */
  def isEmpty: Boolean =
    map.isEmpty

  /**
   * Returns a collection of the data sources that the requests in this
   * collection are from.
   */
  def keys: Iterable[DataSource[R, Any]] =
    map.keys

  /**
   * Converts this collection of requests that can be executed in parallel to a
   * batch of requests in a collection of requests that must be executed
   * sequentially.
   */
  def sequential: Sequential[R] =
    new Sequential(map.map { case (k, v) => (k, Chunk(v)) })

  /**
   * Converts this collection of requests that can be executed in parallel to an
   * `Iterable` containing mappings from data sources to requests from those
   * data sources.
   */
  def toIterable: Iterable[(DataSource[R, Any], Chunk[BlockedRequest[Any]])] =
    map
}

private[query] object Parallel {

  /**
   * Constructs a new collection of requests containing a mapping from the
   * specified data source to the specified request.
   */
  def apply[R, E, A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A]): Parallel[R] =
    new Parallel(Map(dataSource.asInstanceOf[DataSource[Any, Any]] -> Chunk(blockedRequest)))

  /**
   * The empty collection of requests.
   */
  val empty: Parallel[Any] =
    new Parallel(Map.empty)
}
