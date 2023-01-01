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

import zio.Ref
import zio.query.Request
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `BlockedRequest[A]` keeps track of a request of type `A` along with a `Ref`
 * containing the result of the request, existentially hiding the result type.
 * This is used internally by the library to support data sources that return
 * different result types for different requests while guaranteeing that results
 * will be of the type requested.
 */
private[query] sealed trait BlockedRequest[+A] {
  type Failure
  type Success

  def request: Request[Failure, Success]

  def result: Ref[Option[Either[Failure, Success]]]

  override final def toString =
    s"BlockedRequest($request, $result)"
}

private[query] object BlockedRequest {

  def apply[E, A, B](request0: A, result0: Ref[Option[Either[E, B]]])(implicit
    ev: A <:< Request[E, B]
  ): BlockedRequest[A] =
    new BlockedRequest[A] {
      type Failure = E
      type Success = B

      val request = ev(request0)

      val result = result0
    }
}
