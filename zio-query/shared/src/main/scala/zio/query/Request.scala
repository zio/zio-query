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
 * that may fail with an `E`.
 *
 * {{{
 * sealed trait UserRequest[+A] extends Request[Nothing, A]
 *
 * case object GetAllIds                 extends UserRequest[List[Int]]
 * final case class GetNameById(id: Int) extends UserRequest[String]
 *
 * }}}
 */
trait Request[+E, +A]
