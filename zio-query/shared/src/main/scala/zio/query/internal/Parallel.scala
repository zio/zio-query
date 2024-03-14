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

import zio.query.DataSource
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.{Chunk, ChunkBuilder}

import scala.collection.mutable

/**
 * A `Parallel[R]` maintains a mapping from data sources to requests from those
 * data sources that can be executed in parallel.
 */
private[query] final class Parallel[-R](
  private val map: mutable.HashMap[DataSource[?, Any], ChunkBuilder[BlockedRequest[Any]]]
) { self =>

  /**
   * Combines this collection of requests that can be executed in parallel with
   * that collection of requests that can be executed in parallel to return a
   * new collection of requests that can be executed in parallel.
   */
  def ++=[R1 <: R](that: Parallel[R1]): Parallel[R1] = {
    that.map.foreach { case (dataSource, builder) =>
      self.map.getOrElseUpdate(dataSource, Chunk.newBuilder) ++= builder.result()
    }
    self
  }

  def addOne[R1 <: R](dataSource: DataSource[R1, Any], blockedRequest: BlockedRequest[Any]): Parallel[R1] = {
    self.map.getOrElseUpdate(dataSource, Chunk.newBuilder) += blockedRequest
    self
  }

  /**
   * Returns whether this collection of requests is empty.
   */
  def isEmpty: Boolean =
    map.isEmpty

  def head: DataSource[R, Any] =
    map.head._1.asInstanceOf[DataSource[R, Any]]

  def size: Int =
    map.size

  /**
   * Converts this collection of requests that can be executed in parallel to a
   * batch of requests in a collection of requests that must be executed
   * sequentially.
   */
  def sequential: Sequential[R] =
    new Sequential(
      map.view
        .mapValues(v => Chunk.single(v.result()))
        .toMap
        .asInstanceOf[Map[DataSource[Any, Any], Chunk[Chunk[BlockedRequest[Any]]]]]
    )
}

private[query] object Parallel {

  /**
   * Constructs a new collection of requests containing a mapping from the
   * specified data source to the specified request.
   */
  def apply[R, A](dataSource: DataSource[R, Any], blockedRequest: BlockedRequest[A]): Parallel[R] =
    empty[R].addOne(dataSource, blockedRequest)

  /**
   * The empty collection of requests.
   */
  def empty[R]: Parallel[R] =
    new Parallel(mutable.HashMap.empty)
}
