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
import zio.{Chunk, Exit, Trace, ZEnvironment, ZIO}

/**
 * A `DataSource[R, A]` requires an environment `R` and is capable of executing
 * requests of type `A`. Queries send requests to a data source with
 * `ZQuery.fromRequest` or `DataSource#query`. When a query runs, ZIO Query
 * collects the requests that can be made at the same time and hands them to the
 * data source together, so it can answer them with one call to its backend.
 *
 * Data sources must implement the method `runAll` which takes a collection of
 * requests and returns an effect with a `CompletedRequestMap` containing a
 * mapping from requests to results. The type of the collection of requests is a
 * `Chunk[Chunk[A]]`. The outer `Chunk` represents batches of requests that must
 * be performed sequentially. The inner `Chunk` represents a batch of requests
 * that can be performed in parallel. This allows data sources to introspect on
 * all the requests being executed and optimize the query. Most data sources do
 * not implement `runAll` by hand: the constructors in [[DataSource$]], such as
 * `DataSource.fromFunctionBatched`, build one from a function.
 *
 * The request type `A` is normally a subtype of `Request[E, B]`:
 * `ZQuery.fromRequest` requires that evidence and uses it to give the query the
 * error type `E` and the result type `B`. A data source builds its results with
 * one of the constructors in [[CompletedRequestMap$]]. It must provide a result
 * for every request it receives, keyed by a request equal to the one it
 * received. A request without a result makes the query that needs it die with a
 * `QueryFailure` when run.
 *
 * Two data sources are equal when their identifiers are equal, and requests to
 * equal data sources that a query makes at the same time are sent in one batch.
 *
 * The following data source answers a whole batch of user ids at once. Four
 * parallel lookups, one of them a duplicate, reach it as one batch of three
 * requests:
 *
 * {{{
 * import zio._
 *
 * final case class GetUserName(id: Int) extends Request[Nothing, String]
 *
 * val userNames: DataSource[Any, GetUserName] =
 *   DataSource.fromFunctionBatchedZIO[Any, Nothing, GetUserName, String]("UserNames") { requests =>
 *     ZIO.debug(s"UserNames received \${requests.map(_.id).mkString("[", ", ", "]")}") *>
 *       ZIO.succeed(requests.map(request => s"user-\${request.id}"))
 *   }
 *
 * def getUserName(id: Int): ZQuery[Any, Nothing, String] =
 *   ZQuery.fromRequest(GetUserName(id))(userNames)
 *
 * val names: ZIO[Any, Nothing, List[String]] =
 *   ZQuery.foreachPar(List(1, 2, 1, 3))(getUserName).run
 * // prints one line, such as: UserNames received [2, 1, 3]
 * // names succeeds with List(user-1, user-2, user-1, user-3)
 * }}}
 *
 * @see
 *   [[https://zio.dev/zio-query/data-source-reference DataSource Reference]]
 */
trait DataSource[-R, -A] { self =>

  /**
   * Syntax for adding aspects.
   */
  final def @@[R1 <: R](aspect: DataSourceAspect[R1]): DataSource[R1, A] =
    aspect(self)

  /**
   * The data source's identifier. `equals`, `hashCode` and `toString` use it,
   * so two data sources with the same identifier are treated as the same data
   * source and their requests are batched together. Give each logical data
   * source its own identifier.
   */
  val identifier: String

  /**
   * Execute a collection of requests. The outer `Chunk` represents batches of
   * requests that must be performed sequentially. The inner `Chunk` represents
   * a batch of requests that can be performed in parallel. The returned map
   * must contain a result for every request received.
   */
  def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap]

  /**
   * Returns a data source that splits each batch of requests into batches of at
   * most `n` requests, which this data source receives as consecutive batches
   * in one call to `runAll`. A data source built with one of the
   * `DataSource.fromFunction*` constructors runs those batches one after
   * another. If `n` is less than 1, the data source dies with an
   * `IllegalArgumentException` when it runs. The identifier of the new data
   * source is this identifier followed by `.batchN(n)`.
   */
  def batchN(n: Int): DataSource[R, A] =
    new DataSource[R, A] {
      val identifier                                                                                     = s"${self}.batchN($n)"
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        if (n < 1)
          ZIO.die(new IllegalArgumentException("batchN: n must be at least 1"))
        else
          self.runAll(requests.foldLeft(Chunk.newBuilder[Chunk[A]])(_ addAll _.grouped(n)).result())
    }

  /**
   * Returns a new data source that executes requests of type `B` using the
   * specified function to transform `B` requests into requests that this data
   * source can execute.
   *
   * The new data source returns the results of this data source unchanged, so
   * they are keyed by the transformed requests, while the query looks up the
   * request it submitted. A query succeeds only when `f` returns a request
   * equal to its input; for any other request it dies with a `QueryFailure`.
   * This is a known bug. The identifier of the new data source is this
   * identifier followed by `.contramap(description)`.
   */
  final def contramap[B](f: Described[B => A]): DataSource[R, B] =
    new DataSource[R, B] {
      val identifier                                                                                     = s"${self.identifier}.contramap(${f.description})"
      def runAll(requests: Chunk[Chunk[B]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        self.runAll(requests.map(_.map(f.value)))
    }

  /**
   * Returns a new data source that executes requests of type `B` using the
   * specified effectual function to transform `B` requests into requests that
   * this data source can execute. The transformations of one batch run in
   * parallel, and batches are transformed one after another.
   *
   * As with `contramap`, the results are keyed by the transformed requests, so
   * a query succeeds only when `f` returns a request equal to its input; for
   * any other request it dies with a `QueryFailure`. This is a known bug.
   */
  final def contramapZIO[R1 <: R, B](f: Described[B => ZIO[R1, Nothing, A]]): DataSource[R1, B] =
    new DataSource[R1, B] {
      import scala.collection.compat.BuildFrom._ // Required for Scala 3

      val identifier                                                                                      = s"${self.identifier}.contramapZIO(${f.description})"
      def runAll(requests: Chunk[Chunk[B]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        ZIO.foreach(requests)(ZIO.foreachPar(_)(f.value)).flatMap(self.runAll)
    }

  /**
   * Returns a new data source that executes requests of type `C` using the
   * specified function to transform `C` requests into requests that either this
   * data source or that data source can execute.
   *
   * For each batch, the `Left` requests go to this data source and the `Right`
   * requests to that data source, and the two calls run in parallel.
   *
   * Two known bugs apply. Each side is called for every batch, even when no
   * request goes to it, so the query also waits for that call, and a data
   * source built with `DataSource.make` can receive an empty batch. The results
   * are keyed by the requests that `f` returns, so a query succeeds only when
   * `f` returns a request equal to its input, as when `A` and `B` are subtypes
   * of `C`; for any other request it dies with a `QueryFailure`.
   */
  final def eitherWith[R1 <: R, B, C](
    that: DataSource[R1, B]
  )(f: Described[C => Either[A, B]]): DataSource[R1, C] =
    new DataSource[R1, C] {
      val identifier                                                                                      = s"${self.identifier}.eitherWith(${that.identifier})(${f.description})"
      def runAll(requests: Chunk[Chunk[C]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        ZIO.suspendSucceed {
          val iter = requests.iterator
          val crm  = CompletedRequestMap.Mutable.empty(requests.foldLeft(0)(_ + _.size))
          ZIO
            .whileLoop(iter.hasNext) {
              val reqs     = iter.next()
              val (as, bs) = reqs.partitionMap(f.value)
              self.runAll(Chunk.single(as)) <&> that.runAll(Chunk.single(bs))
            } { case (l, r) =>
              crm.addAll(l)
              crm.addAll(r)
            }
            .as(crm)
        }
    }

  override final def equals(that: Any): Boolean =
    that match {
      case that: DataSource[_, _] => this.identifier == that.identifier
      case _                      => false
    }

  override final def hashCode: Int =
    identifier.hashCode

  /**
   * Provides this data source with its required environment. The identifier of
   * the new data source is this identifier followed by
   * `.provideSomeEnvironment(_ => description)`.
   */
  final def provideEnvironment(r: Described[ZEnvironment[R]]): DataSource[Any, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Provides this data source with part of its required environment, by
   * computing the environment it requires from an environment of type `R0`.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  ): DataSource[R0, A] =
    new DataSource[R0, A] {
      val identifier                                                                                      = s"${self.identifier}.provideSomeEnvironment(${f.description})"
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R0, Nothing, CompletedRequestMap] =
        self.runAll(requests).provideSomeEnvironment(f.value)
    }

  /**
   * Returns a query that submits a request to this data source and succeeds
   * with its result. The same as `ZQuery.fromRequest(request)(dataSource)`.
   */
  final def query[E, A1 <: A, B](request: A1)(implicit ev: A1 <:< Request[E, B], trace: Trace): ZQuery[R, E, B] =
    ZQuery.fromRequest(request)(self)

  /**
   * Returns a query that submits a `Chunk` of requests to this data source and
   * succeeds with their results in the same order. The same as
   * `ZQuery.fromRequests(requests)(dataSource)`.
   */
  final def queryAll[E, A1 <: A, B](
    requests: Chunk[A1]
  )(implicit ev: A1 <:< Request[E, B], trace: Trace): ZQuery[R, E, Chunk[B]] =
    ZQuery.fromRequests(requests)(self)

  /**
   * Returns a new data source that executes requests by sending them to this
   * data source and that data source, returning the results from the first data
   * source to complete and safely interrupting the loser. A data source that
   * completes with a map of failed results still wins; a data source whose
   * `runAll` dies loses, and the race waits for the other one.
   */
  final def race[R1 <: R, A1 <: A](that: DataSource[R1, A1]): DataSource[R1, A1] =
    new DataSource[R1, A1] {
      val identifier                                                                                       = s"${self.identifier}.race(${that.identifier})"
      def runAll(requests: Chunk[Chunk[A1]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        self.runAll(requests).race(that.runAll(requests))
    }

  override final def toString: String =
    identifier
}

/**
 * Constructors for data sources. The `fromFunction*` constructors build a data
 * source from a function of one request or of a batch of requests, and `make`
 * and `Batched.make` build one from a function that returns a
 * `CompletedRequestMap`.
 */
object DataSource {
  import UtilsVersionSpecific._

  /**
   * A data source that executes requests that can be performed in parallel in
   * batches but does not further optimize batches of requests that must be
   * performed sequentially.
   */
  trait Batched[-R, -A] extends DataSource[R, A] {

    /**
     * Executes one batch of requests that can be performed in parallel. The
     * returned map must contain a result for every request received.
     */
    def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap]

    /**
     * Calls `run` once for each non-empty batch, one batch after another. From
     * the second batch on, it leaves out requests that an earlier batch has
     * already completed, and skips a batch that has no requests left.
     */
    final def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
      requests.size match {
        case 0 => ZIO.succeed(CompletedRequestMap.empty)
        case 1 =>
          val reqs0 = requests.head
          if (reqs0.nonEmpty) run(reqs0) else ZIO.succeed(CompletedRequestMap.empty)
        case _ =>
          ZIO.suspendSucceed {
            val crm  = CompletedRequestMap.Mutable.empty(requests.foldLeft(0)(_ + _.size))
            val iter = requests.iterator
            ZIO
              .whileLoop(iter.hasNext) {
                val reqs        = iter.next()
                val newRequests = if (crm.isEmpty) reqs else reqs.filterNot(crm.contains)
                ZIO.when(newRequests.nonEmpty)(run(newRequests))
              } {
                case Some(map) => crm.addAll(map)
                case _         => ()
              }
              .as(crm)
          }
      }

  }

  /**
   * Constructors for `DataSource.Batched` data sources.
   */
  object Batched {

    /**
     * Constructs a data source from a function taking a collection of requests
     * and returning a `CompletedRequestMap`. The function is called once per
     * batch of requests that can be performed in parallel, and `name` becomes
     * the identifier.
     */
    def make[R, A](name: String)(f: Chunk[A] => ZIO[R, Nothing, CompletedRequestMap]): DataSource[R, A] =
      new DataSource.Batched[R, A] {
        val identifier: String                                                                   = name
        def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
          f(requests)
      }
  }

  /**
   * Constructs a data source from a pure function, called once for each
   * request.
   */
  def fromFunction[A, B](
    name: String
  )(f: A => B)(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    new DataSource.Batched[Any, A] {
      val identifier: String                                                                     = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] =
        ZIO.succeed(CompletedRequestMap.fromIterableWith(requests)(ev.apply, a => Exit.succeed(f(a))))
    }

  /**
   * Constructs a data source from a pure function that takes a list of requests
   * and returns a list of results of the same size. Each item in the result
   * list must correspond to the item at the same index in the request list.
   *
   * The function receives each batch of requests that can be performed in
   * parallel in one call. Results are matched to requests by position: if the
   * function returns fewer results than requests, the queries for the requests
   * without a result die with a `QueryFailure`, and extra results are ignored.
   * `name` becomes the identifier of the data source.
   *
   * {{{
   * import zio._
   *
   * final case class GetUserName(id: Int) extends Request[Nothing, String]
   *
   * val userNames: DataSource[Any, GetUserName] =
   *   DataSource.fromFunctionBatched[GetUserName, String]("UserNames") { requests =>
   *     println(s"UserNames received \${requests.size} requests")
   *     requests.map(request => s"user-\${request.id}")
   *   }
   *
   * val names: ZIO[Any, Nothing, List[String]] =
   *   ZQuery.foreachPar(List(1, 2, 3))(id => userNames.query(GetUserName(id))).run
   * // prints: UserNames received 3 requests
   * // names succeeds with List(user-1, user-2, user-3)
   * }}}
   *
   * @see
   *   [[https://zio.dev/zio-query/creating-data-sources Creating Data Sources]]
   */
  def fromFunctionBatched[A, B](
    name: String
  )(f: Chunk[A] => Chunk[B])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionBatchedZIO(name)(as => Exit.succeed(f(as)))

  /**
   * Constructs a data source from a pure function that takes a list of requests
   * and returns a list of optional results of the same size. Each item in the
   * result list must correspond to the item at the same index in the request
   * list. A `None` leaves its request without a result, and the query for it
   * dies with a `QueryFailure`.
   */
  def fromFunctionBatchedOption[A, B](
    name: String
  )(f: Chunk[A] => Chunk[Option[B]])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionBatchedOptionZIO(name)(as => Exit.succeed(f(as)))

  /**
   * Constructs a data source from an effectual function that takes a list of
   * requests and returns a list of optional results of the same size. Each item
   * in the result list must correspond to the item at the same index in the
   * request list. A `None` leaves its request without a result, and the query
   * for it dies with a `QueryFailure`. If the effect fails, every request of
   * the batch fails with the same cause.
   */
  def fromFunctionBatchedOptionZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[Option[B]]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String                                                                   = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => requests.map(a => (ev(a), Exit.failCause(e))),
            bs => requests.zipWith(bs)((a, b) => (ev(a), Exit.succeed(b)))
          )
          .map(CompletedRequestMap.fromIterableOption)
    }

  /**
   * Constructs a data source from a function `f` that takes a list of requests
   * and returns a list of results, and a function `g` that returns the request
   * a result answers. Results are matched to requests through `g`, not by
   * position, so `f` may return them in any order.
   *
   * The request that `g` returns must be equal to the request that the query
   * submitted. A request that no result maps to has no result, and the query
   * for it dies with a `QueryFailure`.
   *
   * {{{
   * import zio._
   *
   * final case class User(id: Int, name: String)
   * final case class GetUser(id: Int) extends Request[Nothing, User]
   *
   * val users: DataSource[Any, GetUser] =
   *   DataSource.fromFunctionBatchedWith[GetUser, User]("Users")(
   *     requests => requests.map(request => User(request.id, s"user-\${request.id}")).reverse,
   *     user => GetUser(user.id)
   *   )
   *
   * val result: ZIO[Any, Nothing, List[User]] =
   *   ZQuery.foreachPar(List(1, 2, 3))(id => ZQuery.fromRequest(GetUser(id))(users)).run
   * // result succeeds with List(User(1,user-1), User(2,user-2), User(3,user-3))
   * }}}
   */
  def fromFunctionBatchedWith[A, B](
    name: String
  )(f: Chunk[A] => Chunk[B], g: B => Request[Nothing, B])(implicit
    ev: A <:< Request[Nothing, B]
  ): DataSource[Any, A] =
    fromFunctionBatchedWithZIO[Any, Nothing, A, B](name)(as => Exit.succeed(f(as)), g)

  /**
   * Constructs a data source from an effectual function `f` that takes a list
   * of requests and returns a list of results, and a function `g` that returns
   * the request a result answers. Results are matched to requests through `g`,
   * not by position, so `f` may return them in any order.
   *
   * The request that `g` returns must be equal to the request that the query
   * submitted; a request that no result maps to makes its query die with a
   * `QueryFailure`. If the effect fails, every request of the batch fails with
   * the same cause.
   */
  def fromFunctionBatchedWithZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[B]], g: B => Request[E, B])(implicit
    ev: A <:< Request[E, B]
  ): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String                                                                   = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => CompletedRequestMap.failCause(ev.liftCo(requests), e),
            bs => CompletedRequestMap.unsafe.fromWith(bs, bs)(g(_), Exit.succeed)
          )

    }

  /**
   * Constructs a data source from an effectual function that takes a list of
   * requests and returns a list of results of the same size. Each item in the
   * result list must correspond to the item at the same index in the request
   * list. If the function returns fewer results than requests, the queries for
   * the requests without a result die with a `QueryFailure`. If the effect
   * fails, every request of the batch fails with the same cause.
   */
  def fromFunctionBatchedZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[B]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String                                                                   = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => CompletedRequestMap.failCause(ev.liftCo(requests), e),
            CompletedRequestMap.unsafe.fromSuccesses(ev.liftCo(requests), _)
          )
    }

  /**
   * Constructs a data source from an effectual function, called once for each
   * request. The calls for one batch run in parallel, and a failure of one call
   * fails only the query for that request.
   */
  def fromFunctionZIO[R, E, A, B](
    name: String
  )(f: A => ZIO[R, E, B])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String                                                                   = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        ZIO
          .foreachPar(requests)(a => f(a).exit)
          .map(CompletedRequestMap.unsafe.fromExits(ev.liftCo(requests), _))
    }

  /**
   * Constructs a data source from a pure function that may not provide results
   * for all requests received. A `None` leaves its request without a result,
   * and the query for it dies with a `QueryFailure`.
   */
  def fromFunctionOption[A, B](
    name: String
  )(f: A => Option[B])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionOptionZIO(name)(a => Exit.succeed(f(a)))

  /**
   * Constructs a data source from an effectual function that may not provide
   * results for all requests received. The calls for one batch run in parallel.
   * A `None` leaves its request without a result, and the query for it dies
   * with a `QueryFailure`; a failure fails only that request.
   */
  def fromFunctionOptionZIO[R, E, A, B](
    name: String
  )(f: A => ZIO[R, E, Option[B]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String                                                                   = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        ZIO
          .foreachPar(requests)(a => f(a).exit.map((ev(a), _)))
          .map(CompletedRequestMap.fromIterableOption)
    }

  /**
   * Constructs a data source from a function taking a collection of requests
   * and returning a `CompletedRequestMap`. The function is the data source's
   * `runAll`: it receives the batches that must be performed sequentially, as a
   * `Chunk[Chunk[A]]`, and `name` becomes the identifier.
   */
  def make[R, A](name: String)(f: Chunk[Chunk[A]] => ZIO[R, Nothing, CompletedRequestMap]): DataSource[R, A] =
    new DataSource[R, A] {
      val identifier: String                                                                             = name
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
    }

  /**
   * A data source that never executes requests: its `runAll` never completes,
   * so a query that sends it a request does not complete either. Combined with
   * another data source by `race`, it always loses. Useful in tests.
   */
  val never: DataSource[Any, Any] =
    new DataSource[Any, Any] {
      val identifier                                                                                         = "never"
      def runAll(requests: Chunk[Chunk[Any]])(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] =
        ZIO.never
    }

}
