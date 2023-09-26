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

import zio._
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Cache` maintains an internal state with a mapping from requests to `Ref`s
 * that will contain the result of those requests when they are executed. This
 * is used internally by the library to provide deduplication and caching of
 * requests.
 */
trait Cache {

  /**
   * Looks up a request in the cache, failing with the unit value if the request
   * is not in the cache, succeeding with `Ref(None)` if the request is in the
   * cache but has not been executed yet, or `Ref(Some(value))` if the request
   * has been executed.
   */
  def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Ref[Option[Exit[E, A]]]]

  /**
   * Looks up a request in the cache. If the request is not in the cache returns
   * a `Left` with a `Ref` that can be set with a `Some` to complete the
   * request. If the request is in the cache returns a `Right` with a `Ref` that
   * either contains `Some` with a result if the request has been executed or
   * `None` if the request has not been executed yet.
   */
  def lookup[R, E, A, B](request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Either[Ref[Option[Exit[E, B]]], Ref[Option[Exit[E, B]]]]]

  /**
   * Inserts a request and a `Ref` that will contain the result of the request
   * when it is executed into the cache.
   */
  def put[E, A](request: Request[E, A], result: Ref[Option[Exit[E, A]]])(implicit trace: Trace): UIO[Unit]

  /**
   * Removes a request from the cache.
   */
  def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit]
}

object Cache {

  /**
   * Constructs an empty cache.
   */
  def empty(implicit trace: Trace): UIO[Cache] =
    ZIO.succeed(Cache.unsafeMake())

  private final class Default(private val state: Ref[Map[Any, Any]]) extends Cache {

    def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Ref[Option[Exit[E, A]]]] =
      state.get.map(_.get(request).asInstanceOf[Option[Ref[Option[Exit[E, A]]]]]).some.orElseFail(())

    def lookup[R, E, A, B](request: A)(implicit
      ev: A <:< Request[E, B],
      trace: Trace
    ): UIO[Either[Ref[Option[Exit[E, B]]], Ref[Option[Exit[E, B]]]]] =
      Ref.make(Option.empty[Exit[E, B]]).flatMap { ref =>
        state.modify { map =>
          map.get(request) match {
            case None      => (Left(ref), map + (request -> ref))
            case Some(ref) => (Right(ref.asInstanceOf[Ref[Option[Exit[E, B]]]]), map)
          }
        }
      }

    def put[E, A](request: Request[E, A], result: Ref[Option[Exit[E, A]]])(implicit trace: Trace): UIO[Unit] =
      state.update(_ + (request -> result))

    def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit] =
      state.update(_ - request)
  }

  private[query] def unsafeMake(): Cache =
    new Default(Ref.unsafe.make(Map.empty[Any, Any])(Unsafe.unsafe))
}
