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

import zio.query._
import zio.query.internal.BlockedRequests._
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.{Chunk, Exit, Promise, Trace, UIO, Unsafe, ZEnvironment, ZIO}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * `BlockedRequests` captures a collection of blocked requests as a data
 * structure. By doing this the library is able to preserve information about
 * which requests must be performed sequentially and which can be performed in
 * parallel, allowing for maximum possible batching and pipelining while
 * preserving ordering guarantees.
 */
private[query] sealed trait BlockedRequests[-R] { self =>

  /**
   * Combines this collection of blocked requests with the specified collection
   * of blocked requests, in parallel.
   */
  final def &&[R1 <: R](that: BlockedRequests[R1]): BlockedRequests[R1] =
    Both(self, that)

  /**
   * Combines this collection of blocked requests with the specified collection
   * of blocked requests, in sequence.
   */
  final def ++[R1 <: R](that: BlockedRequests[R1]): BlockedRequests[R1] =
    Then(self, that)

  /**
   * Folds over the cases of this collection of blocked requests with the
   * specified functions.
   */
  final def fold[Z](folder: Folder[R, Z]): Z = {
    sealed trait BlockedRequestsCase

    case object BothCase extends BlockedRequestsCase
    case object ThenCase extends BlockedRequestsCase

    @tailrec
    def loop(in: List[BlockedRequests[R]], out: List[Either[BlockedRequestsCase, Z]]): List[Z] =
      in match {
        case Empty :: blockedRequests =>
          loop(blockedRequests, Right(folder.emptyCase) :: out)
        case Single(dataSource, blockedRequest) :: blockedRequests =>
          loop(blockedRequests, Right(folder.singleCase(dataSource, blockedRequest)) :: out)
        case Both(left, right) :: blockedRequests =>
          loop(left :: right :: blockedRequests, Left(BothCase) :: out)
        case Then(left, right) :: blockedRequests =>
          loop(left :: right :: blockedRequests, Left(ThenCase) :: out)
        case Nil =>
          out.foldLeft[List[Z]](List.empty) {
            case (acc, Right(blockedRequests)) =>
              blockedRequests :: acc
            case (acc, Left(BothCase)) =>
              val left :: right :: blockedRequests = (acc: @unchecked)
              folder.bothCase(left, right) :: blockedRequests
            case (acc, Left(ThenCase)) =>
              val left :: right :: blockedRequests = (acc: @unchecked)
              folder.thenCase(left, right) :: blockedRequests
          }
      }

    loop(List(self), List.empty).head
  }

  /**
   * Transforms all data sources with the specified data source aspect, which
   * can change the environment type of data sources but must preserve the
   * request type of each data source.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1]): BlockedRequests[R1] =
    fold(Folder.MapDataSources(f))

  /**
   * Provides each data source with part of its required environment.
   */
  final def provideSomeEnvironment[R0](f: Described[ZEnvironment[R0] => ZEnvironment[R]]): BlockedRequests[R0] =
    fold(Folder.ProvideSomeEnvironment(f))

  /**
   * Executes all requests, submitting requests to each data source in parallel.
   */
  def run(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZQuery.currentCache.getWith { cache =>
      val flattened = BlockedRequests.flatten(self)
      ZIO.foreachDiscard(flattened) { requestsByDataSource =>
        ZIO.foreachParDiscard(requestsByDataSource.toIterable) { case (dataSource, sequential) =>
          val requests = sequential.map(_.map(_.request))

          dataSource
            .runAll(requests)
            .catchAllCause { cause =>
              ZIO.succeed {
                val exit = Exit.failCause(cause).asInstanceOf[Exit[Any, Any]]
                CompletedRequestMap.fromIterable(
                  requests.flatten.map(r => r.asInstanceOf[Request[Any, Any]] -> exit)
                )
              }
            }
            .flatMap { completedRequests =>
              ZQuery.cachingEnabled.getWith {
                val completedRequestsM = completedRequests.toMutableMap
                if (_) {
                  completePromises(dataSource, sequential) { req =>
                    // Pop the entry, and fallback to the immutable one if we already removed it
                    completedRequestsM.remove(req) orElse completedRequests.lookup(req)
                  }
                  // cache responses that were not requested but were completed by the DataSource
                  if (completedRequestsM.nonEmpty) cacheLeftovers(cache, completedRequestsM) else ZIO.unit
                } else {
                  // No need to remove entries here since we don't need to know which ones we need to put in the cache
                  ZIO.succeed(completePromises(dataSource, sequential)(completedRequestsM.get))
                }
              }
            }
        }
      }
    }
}

private[query] object BlockedRequests {

  /**
   * The empty collection of blocked requests.
   */
  val empty: BlockedRequests[Any] =
    Empty

  /**
   * Constructs a collection of blocked requests from the specified blocked
   * request and data source.
   */
  def single[R, K](dataSource: DataSource[R, K], blockedRequest: BlockedRequest[K]): BlockedRequests[R] =
    Single(dataSource, blockedRequest)

  final case class Both[-R](left: BlockedRequests[R], right: BlockedRequests[R]) extends BlockedRequests[R]

  case object Empty extends BlockedRequests[Any]

  final case class Single[-R, A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A])
      extends BlockedRequests[R]

  final case class Then[-R](left: BlockedRequests[R], right: BlockedRequests[R]) extends BlockedRequests[R]

  trait Folder[+R, Z] {
    def emptyCase: Z
    def singleCase[A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A]): Z
    def bothCase(left: Z, right: Z): Z
    def thenCase(left: Z, right: Z): Z
  }

  object Folder {

    final case class MapDataSources[R](f: DataSourceAspect[R]) extends Folder[R, BlockedRequests[R]] {
      def emptyCase: BlockedRequests[R] =
        Empty
      def singleCase[A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A]): BlockedRequests[R] =
        Single(f(dataSource), blockedRequest)
      def bothCase(left: BlockedRequests[R], right: BlockedRequests[R]): BlockedRequests[R] =
        Both(left, right)
      def thenCase(left: BlockedRequests[R], right: BlockedRequests[R]): BlockedRequests[R] =
        Then(left, right)
    }

    final case class ProvideSomeEnvironment[R0, R](f: Described[ZEnvironment[R0] => ZEnvironment[R]])
        extends Folder[R, BlockedRequests[R0]] {
      def emptyCase: BlockedRequests[R0] =
        Empty
      def singleCase[A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A]): BlockedRequests[R0] =
        Single(dataSource.provideSomeEnvironment(f), blockedRequest)
      def bothCase(left: BlockedRequests[R0], right: BlockedRequests[R0]): BlockedRequests[R0] =
        Both(left, right)
      def thenCase(left: BlockedRequests[R0], right: BlockedRequests[R0]): BlockedRequests[R0] =
        Then(left, right)
    }
  }

  /**
   * Flattens a collection of blocked requests into a collection of pipelined
   * and batched requests that can be submitted for execution.
   */
  private def flatten[R](
    blockedRequests: BlockedRequests[R]
  ): List[Sequential[R]] = {

    @tailrec
    def loop(
      blockedRequests: List[BlockedRequests[R]],
      flattened: List[Sequential[R]]
    ): List[Sequential[R]] = {
      val parallel   = Parallel.empty
      val sequential = ListBuffer.empty[BlockedRequests[R]]
      blockedRequests.foreach(step(parallel, sequential))
      val updated = merge(flattened, parallel)
      if (sequential.isEmpty) updated.reverse
      else loop(sequential.result(), updated)
    }

    loop(List(blockedRequests), List.empty)
  }

  /**
   * Takes one step in evaluating a collection of blocked requests, returning a
   * collection of blocked requests that can be performed in parallel and a list
   * of blocked requests that must be performed sequentially after those
   * requests.
   */
  private def step[R](
    parallel: Parallel[R],
    sequential: ListBuffer[BlockedRequests[R]]
  )(c: BlockedRequests[R]): Unit = {

    @tailrec
    def loop(
      blockedRequests: BlockedRequests[R],
      stack: List[BlockedRequests[R]]
    ): Unit =
      blockedRequests match {
        case Empty =>
          if (stack ne Nil) loop(stack.head, stack.tail)
        case Single(dataSource, request) =>
          parallel.addOne(dataSource, request)
          if (stack ne Nil) loop(stack.head, stack.tail)
        case Then(left, right) =>
          left match {
            case Empty      => loop(right, stack)
            case Then(l, r) => loop(Then(l, Then(r, right)), stack)
            case o =>
              if (right ne Empty) sequential.prepend(right)
              loop(o, stack)
          }
        case Both(left, right) =>
          loop(left, right :: stack)
      }

    loop(c, List.empty)
  }

  /**
   * Merges a collection of requests that must be executed sequentially with a
   * collection of requests that can be executed in parallel. If the collections
   * are both from the same single data source then the requests can be
   * pipelined while preserving ordering guarantees.
   */
  private def merge[R](sequential: List[Sequential[R]], parallel: Parallel[R]): List[Sequential[R]] =
    if (sequential.isEmpty)
      parallel.sequential :: Nil
    else if (parallel.isEmpty)
      sequential
    else if ({
      val seqHead = sequential.head
      seqHead.size == 1 && parallel.size == 1 && seqHead.head == parallel.head
    })
      (sequential.head ++ parallel.sequential) :: sequential.tail
    else
      parallel.sequential :: sequential

  private def completePromises(
    dataSource: DataSource[_, Any],
    sequential: Chunk[Chunk[BlockedRequest[Any]]]
  )(get: Request[?, ?] => Option[Exit[Any, Any]]): Unit =
    sequential.foreach {
      _.foreach { br =>
        val req = br.request
        val res = get(req) match {
          case Some(exit) => exit.asInstanceOf[Exit[br.Failure, br.Success]]
          case None       => Exit.die(QueryFailure(dataSource, req))
        }
        br.result.unsafe.done(res)(Unsafe.unsafe)
      }
    }

  private def cacheLeftovers(
    cache: Cache,
    map: mutable.HashMap[Request[_, _], Exit[Any, Any]]
  )(implicit trace: Trace): UIO[Unit] =
    cache match {
      case cache: Cache.Default =>
        ZIO.fiberIdWith { fiberId =>
          ZIO.succeedUnsafe { implicit unsafe =>
            map.foreach { case (request: Request[Any, Any], exit) =>
              cache
                .lookupUnsafe(request, fiberId)
                .merge
                .unsafe
                .done(exit)
            }
          }
        }
      case cache =>
        ZIO.foreachDiscard(map) { case (request: Request[Any, Any], exit) =>
          cache.lookup(request).flatMap(_.merge.done(exit))
        }
    }
}
