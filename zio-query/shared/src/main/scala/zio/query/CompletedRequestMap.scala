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
 *
 * A data source returns one from `runAll`. After the data source returns, the
 * query looks up each request it submitted with `equals`; a request with no
 * entry makes its query die with a `QueryFailure`. Build a map with the
 * constructors in [[CompletedRequestMap$]], such as `fromIterableWith`,
 * `fromIterable` or `single`.
 *
 * {{{
 * import zio._
 *
 * final case class GetUserName(id: Int) extends Request[String, String]
 *
 * val completed: CompletedRequestMap =
 *   CompletedRequestMap.fromIterableWith(List(1, 2))(
 *     id => GetUserName(id),
 *     id => if (id == 1) Exit.succeed("Ann") else Exit.fail(s"no user \$id")
 *   )
 *
 * val first  = completed.lookup(GetUserName(1)) // Some(Success(Ann))
 * val second = completed.lookup(GetUserName(2)) // Some(Failure(Fail(no user 2,StackTrace.none)))
 * val third  = completed.lookup(GetUserName(3)) // None
 * }}}
 *
 * @see
 *   [[https://zio.dev/zio-query/completed-request-map CompletedRequestMap]]
 */
sealed abstract class CompletedRequestMap { self =>

  protected val map: collection.Map[Any, Exit[Any, Any]]

  /**
   * Returns a new map with the entries of this map and that map. For a request
   * present in both, the entry of that map is kept.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation: each call copies every entry into a new map. Prefer
   *   `CompletedRequestMap.combine`.
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
   * Returns a new map with the specified result added, replacing any existing
   * result for the same request. This map is not changed.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation: each call copies every entry into a new map. Prefer using
   *   one of the constructors in the companion object instead.
   */
  @deprecated("Use one of the constructors in the companion object instead. See scaladoc for more info", "0.7.0")
  final def insert[E, A](request: Request[E, A], result: Exit[E, A]): CompletedRequestMap =
    new CompletedRequestMap.Immutable(immutable.HashMap.from(map).updated(request, result))

  /**
   * Returns a new map with the specified optional result added. A success with
   * `None` adds nothing and returns this map.
   *
   * @deprecated
   *   Usage of this method is deprecated as it leads to performance
   *   degradation: each call copies every entry into a new map. Prefer
   *   `CompletedRequestMap.fromIterableOption`.
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
   *
   * It works for maps built with `empty`, `single`, `apply` with one entry, or
   * the deprecated `++` and `insert`. It throws a `ClassCastException` for maps
   * built with the other constructors, such as `apply` with two or more
   * entries, `combine`, `fromIterable`, `fromIterableWith`, `fail` or the
   * `unsafe` ones, which include the maps returned by the
   * `DataSource.fromFunction*` data sources. This is a known bug.
   */
  final def requests: Set[Request[_, _]] =
    map.keySet.asInstanceOf[Set[Request[_, _]]]

  final override def toString: String =
    s"CompletedRequestMap(${map.mkString(", ")})"

  final private[query] def underlying: collection.Map[Request[?, ?], Exit[Any, Any]] =
    map.asInstanceOf[collection.Map[Request[?, ?], Exit[Any, Any]]]
}

/**
 * Constructors for `CompletedRequestMap` values.
 */
object CompletedRequestMap {

  /**
   * Constructs a completed requests map from the specified pairs of requests
   * and results. All pairs share the same `E` and `A`; use `combine` to merge
   * maps of different request types.
   */
  def apply[E, A](entries: (Request[E, A], Exit[E, A])*): CompletedRequestMap =
    entries match {
      case Seq()            => empty
      case Seq((req, resp)) => single(req, resp)
      case _                => fromIterable(entries)
    }

  /**
   * The completed requests map with no entries.
   */
  val empty: CompletedRequestMap =
    new Immutable(immutable.HashMap.empty)

  /**
   * Combines all completed request maps into a single one.
   *
   * If a request is present in several maps, the entry from the last of them is
   * kept.
   */
  def combine(maps: Iterable[CompletedRequestMap]): CompletedRequestMap = {
    val map = Mutable.empty(maps.foldLeft(0)(_ + _.map.size))
    maps.foreach(map.addAll)
    map
  }

  /**
   * Constructs a completed requests map that "dies" all the specified requests
   * with the specified throwable.
   */
  def die[E, A](requests: Chunk[Request[E, A]], error: Throwable): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.die(error)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map that fails all the specified requests
   * with the specified error.
   */
  def fail[E, A](requests: Chunk[Request[E, A]], error: E): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.fail(error)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map that fails all the specified requests
   * with the specified cause.
   */
  def failCause[E, A](requests: Chunk[Request[E, A]], cause: Cause[E]): CompletedRequestMap = {
    val map  = Mutable.empty(requests.size)
    val exit = Exit.failCause(cause)
    requests.foreach(map.update(_, exit))
    map
  }

  /**
   * Constructs a completed requests map from the specified results. If a
   * request appears more than once, its last result is kept.
   */
  def fromIterable[E, A](iterable: Iterable[(Request[E, A], Exit[E, A])]): CompletedRequestMap =
    Mutable(mutable.HashMap.from(iterable))

  /**
   * Constructs a completed requests map from the specified optional results. A
   * success with `None` leaves its request out of the map, so the query for
   * that request dies with a `QueryFailure`.
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
   * Constructs a completed requests map from an iterable of `A` and functions
   * that map each `A` to a request and a result, without building intermediate
   * pairs.
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
   * Constructs a completed requests map containing a single entry.
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
   * requests and responses are of the same size. They pair elements by position
   * and stop at the end of the shorter `Chunk`. It is the responsibility of the
   * caller to ensure that the provided requests maps elements 1-to-1 to the
   * responses.
   */
  val unsafe: UnsafeApi = new UnsafeApi {}

  /**
   * The constructors available through `CompletedRequestMap.unsafe`.
   */
  trait UnsafeApi {

    /**
     * Constructs a completed requests map that pairs each request with the
     * `Exit` at the same position.
     */
    def fromExits[E, A](requests: Chunk[Request[E, A]], responses: Chunk[Exit[E, A]]): CompletedRequestMap =
      fromWith(requests, responses)(identity, identity)

    /**
     * Constructs a completed requests map that pairs each request with the
     * successful value at the same position.
     */
    def fromSuccesses[E, A](requests: Chunk[Request[E, A]], responses: Chunk[A]): CompletedRequestMap =
      fromWith(requests, responses)(identity, Exit.succeed)

    /**
     * Constructs a completed requests map that pairs the elements of two
     * `Chunk`s by position, using `f1` to turn each element of `requests` into
     * a request and `f2` to turn each element of `responses` into a result.
     */
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
