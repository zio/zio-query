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
import zio.{Cause, Chunk, Exit}

import scala.collection.compat._
import scala.collection.{immutable, mutable}

/**
 * A `CompletedRequestMap` is a universally quantified mapping from requests of
 * type `Request[E, A]` to results of type `Exit[E, A]` for all types `E` and
 * `A`. The guarantee is that for any request of type `Request[E, A]`, if there
 * is a corresponding value in the map, that value is of type `Exit[E, A]`. This
 * is used by the library to support data sources that return different result
 * types for different requests while guaranteeing that results will be of the
 * type requested.
 */

sealed abstract class CompletedRequestMap { self =>

  protected val map: collection.Map[Any, Exit[Any, Any]]

  /**
   * Appends the specified result to the completed requests map.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation. Prefer using one of the constructors in the companion object
   *   instead
   */
  @deprecated("Use one of the constructors in the companion object instead. See scaladoc for more info", "0.7.0")
  final def ++(that: CompletedRequestMap): CompletedRequestMap =
    new CompletedRequestMap.Immutable(immutable.HashMap.from(map) ++ that.map)

  /**
   * Returns whether a result exists for the specified request.
   */
  final def contains(request: Any): Boolean =
    map.contains(request)

  /**
   * Whether the completed requests map is empty.
   */
  final def isEmpty: Boolean =
    map.isEmpty

  /**
   * Appends the specified result to the completed requests map.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation. Prefer using one of the constructors in the companion object
   *   instead
   */
  @deprecated("Use one of the constructors in the companion object instead. See scaladoc for more info", "0.7.0")
  final def insert[E, A](request: Request[E, A], result: Exit[E, A]): CompletedRequestMap =
    new CompletedRequestMap.Immutable(immutable.HashMap.from(map).updated(request, result))

  /**
   * Appends the specified optional result to the completed request map.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation. Prefer using one of the constructors in the companion object
   *   instead
   */
  @deprecated("Use one of the constructors in the companion object instead. See scaladoc for more info", "0.7.0")
  final def insertOption[E, A](request: Request[E, A], result: Exit[E, Option[A]]): CompletedRequestMap =
    result match {
      case Exit.Failure(e)       => insert(request, Exit.failCause(e))
      case Exit.Success(Some(a)) => insert(request, Exit.succeed(a))
      case Exit.Success(None)    => self
    }

  /**
   * Retrieves the result of the specified request if it exists.
   */
  final def lookup[E, A](request: Request[E, A]): Option[Exit[E, A]] =
    map.get(request).asInstanceOf[Option[Exit[E, A]]]

  /**
   * Collects all requests in a set.
   */
  final def requests: Set[Request[_, _]] =
    map.keySet.asInstanceOf[Set[Request[_, _]]]

  final override def toString: String =
    s"CompletedRequestMap(${map.mkString(", ")})"

  final private[query] def underlying: collection.Map[Request[?, ?], Exit[Any, Any]] =
    map.asInstanceOf[collection.Map[Request[?, ?], Exit[Any, Any]]]
}

object CompletedRequestMap {

  def apply[E, A](entries: (Request[E, A], Exit[E, A])*): CompletedRequestMap =
    entries match {
      case Seq()            => empty
      case Seq((req, resp)) => single(req, resp)
      case _                => fromIterable(entries)
    }

  val empty: CompletedRequestMap =
    new Immutable(immutable.HashMap.empty)

  /**
   * Combines all completed request maps into a single one.
   *
   * This method is left-associated, meaning that if a request is present in
   * multiple maps, it will be overriden by the last map in the list.
   */
  def combine(maps: Iterable[CompletedRequestMap]): CompletedRequestMap = {
    val map = Mutable.empty(maps.foldLeft(0)(_ + _.map.size))
    maps.foreach(map.addAll)
    map
  }

  /**
   * Constructs a completed requests map that "dies" all the specified requests
   * with the specified throwable
   */
  def die[E, A](requests: Chunk[Request[E, A]], error: Throwable): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.die(error)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map that fails all the specified requests
   * with the specified error
   */
  def fail[E, A](requests: Chunk[Request[E, A]], error: E): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.fail(error)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map that fails all the specified requests
   * with the specified cause
   */
  def failCause[E, A](requests: Chunk[Request[E, A]], cause: Cause[E]): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.failCause(cause)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map from the specified results.
   */
  def fromIterable[E, A](iterable: Iterable[(Request[E, A], Exit[E, A])]): CompletedRequestMap =
    Mutable(mutable.HashMap.from(iterable))

  /**
   * Constructs a completed requests map from the specified optional results.
   */
  def fromIterableOption[E, A](iterable: Iterable[(Request[E, A], Exit[E, Option[A]])]): CompletedRequestMap = {
    val map = Mutable.empty(iterable.size)
    iterable.foreach {
      case (request, Exit.Failure(e))       => map.update(request, Exit.failCause(e))
      case (request, Exit.Success(Some(a))) => map.update(request, Exit.succeed(a))
      case (_, Exit.Success(None))          => ()
    }
    map
  }

  /**
   * Constructs a completed requests map an iterable of A and functions that map
   * each A to a request and a result
   *
   * @param f1
   *   function that maps each element of A to a request
   * @param f2
   *   function that maps each element of A to an Exit, e.g., Exit.succeed(_)
   */
  def fromIterableWith[E, A, B](
    iterable: Iterable[A]
  )(
    f1: A => Request[E, B],
    f2: A => Exit[E, B]
  ): CompletedRequestMap = {
    val map = Mutable.empty(iterable.size)
    iterable.foreach(req => map.update(f1(req), f2(req)))
    map
  }

  /**
   * Constructs a completed requests map containing a single entry
   */
  def single[E, A](request: Request[E, A], exit: Exit[E, A]): CompletedRequestMap =
    new Immutable(immutable.HashMap((request, exit)))

  /**
   * Unsafe API for constructing completed request maps.
   *
   * Constructing a [[CompletedRequestMap]] via these methods can improve
   * performance in some cases as they allow skipping the creation of
   * intermediate `Iterable[ Tuple2[_, _] ]`.
   *
   * NOTE: These methods are marked as unsafe because they do not check that the
   * requests and responses are of the same size. It is the responsibility of
   * the caller to ensure that the provided requests maps elements 1-to-1 to the
   * responses.
   */
  val unsafe: UnsafeApi = new UnsafeApi {}

  trait UnsafeApi {

    def fromExits[E, A](requests: Chunk[Request[E, A]], responses: Chunk[Exit[E, A]]): CompletedRequestMap =
      fromWith(requests, responses)(identity, identity)

    def fromSuccesses[E, A](requests: Chunk[Request[E, A]], responses: Chunk[A]): CompletedRequestMap =
      fromWith(requests, responses)(identity, Exit.succeed)

    def fromWith[E, A1, A2, B](requests: Chunk[A1], responses: Chunk[A2])(
      f1: A1 => Request[E, B],
      f2: A2 => Exit[E, B]
    ): CompletedRequestMap = {
      val size  = requests.size min responses.size
      val map   = Mutable.empty(size)
      val reqs  = requests.chunkIterator
      val resps = responses.chunkIterator
      var i     = 0
      while (i < size) {
        map.update(f1(reqs.nextAt(i)), f2(resps.nextAt(i)))
        i += 1
      }
      map
    }

  }

  final private class Immutable(override protected val map: immutable.HashMap[Any, Exit[Any, Any]])
      extends CompletedRequestMap

  final private[query] class Mutable private (
    override protected val map: mutable.HashMap[Any, Exit[Any, Any]]
  ) extends CompletedRequestMap { self =>
    import UtilsVersionSpecific._

    def addAll(that: CompletedRequestMap): Unit =
      if (!that.isEmpty) self.map.addAll(that.map)

    def update[E, A](request: Request[E, A], exit: Exit[E, A]): Unit =
      map.update(request, exit)

  }

  private[query] object Mutable {

    def apply(map: mutable.HashMap[Any, Exit[Any, Any]]): CompletedRequestMap.Mutable =
      new Mutable(map)

    def empty(size: Int): CompletedRequestMap.Mutable =
      new Mutable(UtilsVersionSpecific.newHashMap(size))

  }
}
