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
 * A `Sequential[R]` maintains a mapping from data sources to batches of
 * requests from those data sources that must be executed sequentially.
 */
private[query] final class Sequential[-R](
  private val map: Map[DataSource[Any, Any], Chunk[Chunk[BlockedRequest[Any]]]]
) { self =>

  /**
   * Combines this collection of batches of requests that must be executed
   * sequentially with that collection of batches of requests that must be
   * executed sequentially to return a new collection of batches of requests
   * that must be executed sequentially.
   */
  def ++[R1 <: R](that: Sequential[R1]): Sequential[R1] =
    new Sequential(
      that.map.foldLeft(self.map) { case (map, (k, v)) =>
        map + (k -> map.get(k).fold[Chunk[Chunk[BlockedRequest[Any]]]](v)(_ ++ v))
      }
    )

  /**
   * Returns whether this collection of batches of requests is empty.
   */
  def isEmpty: Boolean =
    map.isEmpty

  /**
   * Returns a collection of the data sources that the batches of requests in
   * this collection are from.
   */
  def keys: Iterable[DataSource[R, Any]] =
    map.keys

  /**
   * Converts this collection of batches requests that must be executed
   * sequentially to an `Iterable` containing mappings from data sources to
   * batches of requests from those data sources.
   */
  def toIterable: Iterable[(DataSource[R, Any], Chunk[Chunk[BlockedRequest[Any]]])] =
    map
}
