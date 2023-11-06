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

import zio.query.internal.BlockedRequests._
import zio.query.{Cache, CompletedRequestMap, DataSource, DataSourceAspect, Described, QueryFailure, ZQuery}
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.{Exit, Promise, Trace, ZEnvironment, ZIO}

import scala.annotation.tailrec

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
    ZQuery.currentCache.get.flatMap { cache =>
      ZIO.foreachDiscard(BlockedRequests.flatten(self)) { requestsByDataSource =>
        ZIO.foreachParDiscard(requestsByDataSource.toIterable) { case (dataSource, sequential) =>
          for {
            completedRequests <- dataSource.runAll(sequential.map(_.map(_.request))).catchAllCause { cause =>
                                   ZIO.succeed {
                                     sequential.map(_.map(_.request)).flatten.foldLeft(CompletedRequestMap.empty) {
                                       case (map, request) => map.insert(request)(Exit.failCause(cause))
                                     }
                                   }
                                 }
            blockedRequests = sequential.flatten
            leftovers       = completedRequests.requests -- blockedRequests.map(_.request)
            _ <- ZIO.foreachDiscard(blockedRequests) { blockedRequest =>
                   blockedRequest.result.done(
                     completedRequests
                       .lookup(blockedRequest.request)
                       .getOrElse(Exit.die(QueryFailure(dataSource, blockedRequest.request)))
                   )
                 }
            _ <- ZIO.foreachDiscard(leftovers) { request =>
                   ZIO.foreach(completedRequests.lookup(request)) { response =>
                     Promise.make[Any, Any].tap(_.done(response)).flatMap(cache.put(request, _))
                   }
                 }
          } yield ()
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
    def loop[R](
      blockedRequests: List[BlockedRequests[R]],
      flattened: List[Sequential[R]]
    ): List[Sequential[R]] = {
      val (parallel, sequential) =
        blockedRequests.foldLeft[(Parallel[R], List[BlockedRequests[R]])]((Parallel.empty, List.empty)) {
          case ((parallel, sequential), blockedRequest) =>
            val (par, seq) = step(blockedRequest)
            (parallel ++ par, sequential ++ seq)
        }
      val updated = merge(flattened, parallel)
      if (sequential.isEmpty) updated.reverse
      else loop(sequential, updated)
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
    c: BlockedRequests[R]
  ): (Parallel[R], List[BlockedRequests[R]]) = {

    @tailrec
    def loop[R](
      blockedRequests: BlockedRequests[R],
      stack: List[BlockedRequests[R]],
      parallel: Parallel[R],
      sequential: List[BlockedRequests[R]]
    ): (Parallel[R], List[BlockedRequests[R]]) =
      blockedRequests match {
        case Empty =>
          if (stack.isEmpty) (parallel, sequential)
          else loop(stack.head, stack.tail, parallel, sequential)
        case Then(left, right) =>
          left match {
            case Empty      => loop(right, stack, parallel, sequential)
            case Then(l, r) => loop(Then(l, Then(r, right)), stack, parallel, sequential)
            case Both(l, r) => loop(Both(Then(l, right), Then(r, right)), stack, parallel, sequential)
            case o          => loop(o, stack, parallel, right :: sequential)
          }
        case Both(left, right) => loop(left, right :: stack, parallel, sequential)
        case Single(dataSource, request) =>
          if (stack.isEmpty) (parallel ++ Parallel(dataSource, request), sequential)
          else loop(stack.head, stack.tail, parallel ++ Parallel(dataSource, request), sequential)
      }

    loop(c, List.empty, Parallel.empty, List.empty)
  }

  /**
   * Merges a collection of requests that must be executed sequentially with a
   * collection of requests that can be executed in parallel. If the collections
   * are both from the same single data source then the requests can be
   * pipelined while preserving ordering guarantees.
   */
  private def merge[R](sequential: List[Sequential[R]], parallel: Parallel[R]): List[Sequential[R]] =
    if (sequential.isEmpty)
      List(parallel.sequential)
    else if (parallel.isEmpty)
      sequential
    else if (sequential.head.keys.size == 1 && parallel.keys.size == 1 && sequential.head.keys == parallel.keys)
      (sequential.head ++ parallel.sequential) :: sequential.tail
    else
      parallel.sequential :: sequential
}
