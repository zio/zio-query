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

import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.{Chunk, Trace, ZIO}

/**
 * A `DataSourceAspect` is an aspect that can be weaved into data sources, with
 * `DataSource#@@`, and into every data source of a query, with
 * `ZQuery#mapDataSources` or `QueryAspect.fromDataSourceAspect`. You can think
 * of an aspect as a polymorphic function, capable of transforming one data
 * source into another, possibly enlarging the environment type.
 *
 * @see
 *   [[https://zio.dev/zio-query/aspects/ Aspects]]
 */
trait DataSourceAspect[-R] { self =>

  /**
   * Applies the aspect to a data source.
   */
  def apply[R1 <: R, A](dataSource: DataSource[R1, A]): DataSource[R1, A]

  /**
   * A symbolic alias for `andThen`.
   */
  final def >>>[R1 <: R](that: DataSourceAspect[R1]): DataSourceAspect[R1] =
    andThen(that)

  /**
   * Returns a new aspect that represents the sequential composition of this
   * aspect with the specified one: this aspect is applied first, and the
   * specified one to its result.
   */
  final def andThen[R1 <: R](that: DataSourceAspect[R1]): DataSourceAspect[R1] =
    new DataSourceAspect[R1] {
      def apply[R2 <: R1, A](dataSource: DataSource[R2, A]): DataSource[R2, A] =
        that(self(dataSource))
    }
}

/**
 * Constructors for data source aspects.
 */
object DataSourceAspect {

  /**
   * A data source aspect that executes requests between two effects, `before`
   * and `after`, where the result of `before` can be used by `after`.
   *
   * The effects run once per call to the data source's `runAll`, not once per
   * request, and `after` runs even if the call is interrupted. The identifier
   * of the new data source is the original identifier followed by
   * `@@ around(b)(a)`, where `b` and `a` are the two descriptions.
   */
  def around[R, A](
    before: Described[ZIO[R, Nothing, A]]
  )(after: Described[A => ZIO[R, Nothing, Any]]): DataSourceAspect[R] =
    new DataSourceAspect[R] {
      def apply[R1 <: R, A](dataSource: DataSource[R1, A]): DataSource[R1, A] =
        new DataSource[R1, A] {
          val identifier                                                                                      = s"${dataSource.identifier} @@ around(${before.description})(${after.description})"
          def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
            ZIO.acquireReleaseWith(before.value)(after.value)(_ => dataSource.runAll(requests))
        }
    }

  /**
   * A data source aspect that splits each batch of requests into batches of at
   * most `n` requests, the same as `DataSource#batchN`. If `n` is less than 1,
   * the data source dies with an `IllegalArgumentException` when it runs.
   */
  def maxBatchSize(n: Int): DataSourceAspect[Any] =
    new DataSourceAspect[Any] {
      def apply[R, A](dataSource: DataSource[R, A]): DataSource[R, A] =
        dataSource.batchN(n)
    }
}
