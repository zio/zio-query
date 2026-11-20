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

package zio

import zio.stacktracer.TracingImplicits.disableAutoTrace

package object query {

  /**
   * A query that requires an environment `R` and may fail with a `Throwable`.
   */
  type RQuery[-R, +A] = ZQuery[R, Throwable, A]

  /**
   * A query that requires an environment `R` and cannot fail: its error type is
   * `Nothing`.
   */
  type URQuery[-R, +A] = ZQuery[R, Nothing, A]

  /**
   * A query that requires no environment and may fail with an `E`.
   */
  type Query[+E, +A] = ZQuery[Any, E, A]

  /**
   * A query that requires no environment and cannot fail: its error type is
   * `Nothing`.
   */
  type UQuery[+A] = ZQuery[Any, Nothing, A]

  /**
   * A query that requires no environment and may fail with a `Throwable`.
   */
  type TaskQuery[+A] = ZQuery[Any, Throwable, A]
}
