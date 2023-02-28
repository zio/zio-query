/*
 * Copyright 2023 John A. De Goes and the ZIO Contributors
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

import zio._

trait QueryAspect[+LowerR, -UpperR, +LowerE, -UpperE, +LowerA, -UpperA] { self =>

  def apply[R >: LowerR <: UpperR, E >: LowerE <: UpperE, A >: LowerA <: UpperA](query: ZQuery[R, E, A])(implicit
    trace: Trace
  ): ZQuery[R, E, A]

  def >>>[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    self.andThen(that)

  /**
   * Returns a new aspect that represents the sequential composition of this
   * aspect with the specified one.
   */
  def @@[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    self >>> that

  def andThen[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    new QueryAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] {
      def apply[R >: LowerR1 <: UpperR1, E >: LowerE1 <: UpperE1, A >: LowerA1 <: UpperA1](
        query: ZQuery[R, E, A]
      )(implicit trace: Trace): ZQuery[R, E, A] =
        that(self(query))
    }
}

object QueryAspect {

  /**
   * Executes the specified workflows before and after the query.
   */
  def around[R, A](
    before: ZIO[R, Nothing, A]
  )(after: ZIO[R, Nothing, Any]): QueryAspect[Nothing, R, Nothing, Any, Nothing, Any] =
    new QueryAspect[Nothing, R, Nothing, Any, Nothing, Any] {
      def apply[R1 <: R, E, B](query: ZQuery[R1, E, B])(implicit trace: Trace): ZQuery[R1, E, B] =
        ZQuery.acquireReleaseWith[R1, E, A, B](before)(_ => after)(_ => query)
    }

  def aroundDataSource[R, A](
    before: Described[ZIO[R, Nothing, A]]
  )(after: Described[A => ZIO[R, Nothing, Any]]): QueryAspect[Nothing, R, Nothing, Any, Nothing, Any] =
    fromDataSourceAspect(DataSourceAspect.around(before)(after))

  def fromDataSourceAspect[R](
    dataSourceAspect: DataSourceAspect[R]
  ): QueryAspect[Nothing, R, Nothing, Any, Nothing, Any] =
    new QueryAspect[Nothing, R, Nothing, Any, Nothing, Any] {
      def apply[R1 <: R, E, A](query: ZQuery[R1, E, A])(implicit trace: Trace): ZQuery[R1, E, A] =
        query.mapDataSources(dataSourceAspect)
    }

  def maxBatchSize(n: Int): QueryAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
    fromDataSourceAspect(DataSourceAspect.maxBatchSize(n))
}
