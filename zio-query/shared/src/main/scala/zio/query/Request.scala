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

/**
 * A `Request[E, A]` is a request from a data source for a value of type `A`
 * that may fail with an `E`. A request is plain data that says what to fetch;
 * the data source decides how. `ZQuery.fromRequest` uses the type parameters to
 * type the query, so a query for a `Request[E, A]` fails with an `E` or
 * succeeds with an `A`.
 *
 * ZIO Query compares requests with `equals` and `hashCode`: the cache of a run
 * is keyed by the request, and the results of a data source are looked up by
 * it. Define requests as case classes or case objects, so that a request equal
 * to one already made in the same run is answered from the cache.
 *
 * {{{
 * import zio._
 *
 * sealed trait UserRequest[A] extends Request[Nothing, A]
 *
 * case object GetAllIds                 extends UserRequest[List[Int]]
 * final case class GetNameById(id: Int) extends UserRequest[String]
 *
 * val userSource: DataSource[Any, UserRequest[_]] =
 *   DataSource.Batched.make[Any, UserRequest[_]]("UserSource") { requests =>
 *     ZIO.succeed(
 *       CompletedRequestMap.combine(requests.map {
 *         case GetAllIds       => CompletedRequestMap.single(GetAllIds, Exit.succeed(List(1, 2)))
 *         case GetNameById(id) => CompletedRequestMap.single(GetNameById(id), Exit.succeed(s"user-\$id"))
 *       })
 *     )
 *   }
 *
 * val ids: ZQuery[Any, Nothing, List[Int]] = ZQuery.fromRequest(GetAllIds)(userSource)
 * val name: ZQuery[Any, Nothing, String]   = ZQuery.fromRequest(GetNameById(1))(userSource)
 * // ids.zip(name).run succeeds with (List(1, 2),user-1)
 * }}}
 *
 * @see
 *   [[https://zio.dev/zio-query/creating-data-sources/ Creating Data Sources]]
 */
trait Request[E, A]
