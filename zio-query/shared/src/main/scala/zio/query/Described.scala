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
 * A `Described[A]` is a value of type `A` along with a string description of
 * that value. The description may be used to generate a hash associated with
 * the value, so values that are equal should have the same description and
 * values that are not equal should have different descriptions.
 */
final case class Described[+A](value: A, description: String)

object Described {

  implicit class AnySyntax[A](private val value: A) extends AnyVal {
    def ?(description: String): Described[A] =
      Described(value, description)
  }
}
