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
  private val map: mutable.HashMap[DataSource[?, ?], ChunkBuilder[BlockedRequest[Any]]]
) { self =>

  def addOne[R1 <: R](dataSource: DataSource[R1, ?], blockedRequest: BlockedRequest[Any]): Parallel[R1] = {
    self.map.getOrElseUpdate(dataSource, Chunk.newBuilder) addOne blockedRequest
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
  def sequential: Sequential[R] = {
    val builder = Map.newBuilder[DataSource[Any, Any], Chunk[Chunk[BlockedRequest[Any]]]]
    map.foreach { case (dataSource, chunkBuilder) =>
      builder += ((dataSource.asInstanceOf[DataSource[Any, Any]], Chunk.single(chunkBuilder.result())))
    }
    new Sequential(builder.result())
  }
}

private[query] object Parallel {

  /**
   * The empty collection of requests.
   */
  def empty[R]: Parallel[R] =
    new Parallel(mutable.HashMap.empty)
}
