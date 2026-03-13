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

import java.util.concurrent.ConcurrentHashMap

/**
 * A `Cache` maintains an internal state with a mapping from requests to
 * `Promise`s that will contain the result of those requests when they are
 * executed. This is used internally by the library to provide deduplication and
 * caching of requests.
 */
trait Cache {

  /**
   * Looks up a request in the cache, failing with the unit value if the request
   * is not in the cache or succeeding with a `Promise` if the request is in the
   * cache that will contain the result of the request when it is executed.
   */
  def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Promise[E, A]]

  /**
   * Looks up a request in the cache. If the request is not in the cache returns
   * a `Left` with a `Promise` that can be completed to complete the request. If
   * the request is in the cache returns a `Right` with a `Promise` that will
   * contain the result of the request when it is executed.
   */
  def lookup[E, A, B](request: A)(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Either[Promise[E, B], Promise[E, B]]]

  /**
   * Inserts a request and a `Promise` that will contain the result of the
   * request when it is executed into the cache.
   */
  def put[E, A](request: Request[E, A], result: Promise[E, A])(implicit trace: Trace): UIO[Unit]

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

  /**
   * Constructs an empty cache, sized to accommodate the specified number of
   * elements without the need for the internal data structures to be resized.
   */
  def empty(expectedNumOfElements: Int)(implicit trace: Trace): UIO[Cache] =
    ZIO.succeed(Cache.unsafeMake(expectedNumOfElements))

  /**
   * An 'InMemory' cache is one that doesn't require an effect to look up its
   * value. Prefer extending this class when implementing a cache that doesn't
   * perform any IO, such as a cache based on a Map.
   */
  abstract class InMemoryCache extends Cache {
    def getNow[E, A](request: Request[E, A]): Option[Promise[E, A]]
    def lookupNow[E, A](request: Request[E, A]): Either[Promise[E, A], Promise[E, A]]
    def putNow[E, A](request: Request[E, A], result: Promise[E, A]): Unit
    def removeNow[E, A](request: Request[E, A]): Unit

    final def get[E, A](request: Request[E, A])(implicit trace: Trace): IO[Unit, Promise[E, A]] =
      ZIO.suspendSucceed {
        getNow(request) match {
          case Some(p) => Exit.succeed(p)
          case _       => Exit.fail(())
        }
      }

    final def lookup[E, A, B](
      request: A
    )(implicit ev: A <:< Request[E, B], trace: Trace): UIO[Either[Promise[E, B], Promise[E, B]]] =
      ZIO.succeed(lookupNow(request))

    final def put[E, A](request: Request[E, A], result: Promise[E, A])(implicit trace: Trace): UIO[Unit] =
      ZIO.succeed(putNow(request, result))

    final def remove[E, A](request: Request[E, A])(implicit trace: Trace): UIO[Unit] =
      ZIO.succeed(removeNow(request))
  }

  private final class NonExpiringCache(map: ConcurrentHashMap[Request[_, _], Promise[_, _]]) extends InMemoryCache {
    private implicit val unsafe: Unsafe = Unsafe.unsafe

    def getNow[E, A](request: Request[E, A]): Option[Promise[E, A]] =
      Option(map.get(request).asInstanceOf[Promise[E, A]])

    def lookupNow[E, A](request: Request[E, A]): Either[Promise[E, A], Promise[E, A]] = {
      val newPromise = Promise.unsafe.make[E, A](FiberId.None)
      val existing   = map.putIfAbsent(request, newPromise).asInstanceOf[Promise[E, A]]
      if (existing eq null) Left(newPromise) else Right(existing)
    }

    def putNow[E, A](request: Request[E, A], result: Promise[E, A]): Unit =
      map.put(request, result)

    def removeNow[E, A](request: Request[E, A]): Unit =
      map.remove(request)
  }

  // TODO: Initialize the map with a sensible default value. Default is 16, which seems way too small for a cache
  private[query] def unsafeMake(): Cache = new NonExpiringCache(new ConcurrentHashMap())

  private[query] def unsafeMake(expectedNumOfElements: Int): Cache = {
    val initialSize = Math.ceil(expectedNumOfElements / 0.75d).toInt
    new NonExpiringCache(new ConcurrentHashMap(initialSize))
  }
}
