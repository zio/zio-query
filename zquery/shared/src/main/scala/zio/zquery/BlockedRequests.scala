package zio.zquery

import scala.annotation.tailrec

import zio.ZIO
import zio.zquery.BlockedRequests._

/**
 * `BlockedRequests` captures a set of blocked requests as a data structure.
 * By doing this the library is able to preserve information about which
 * requests must be performed sequentially and which can be performed in
 * parallel, allowing for maximum possible batching and pipelining while
 * preserving ordering guarantees.
 */
private[zquery] sealed trait BlockedRequests[-R] { self =>

  /**
   * Combines this set of blocked requests with the specified set of blocked
   * requests, in parallel.
   */
  final def &&[R1 <: R](that: BlockedRequests[R1]): BlockedRequests[R1] =
    parallel(self, that)

  /**
   * Combines this set of blocked requests with the specified set of blocked
   * requests, in sequence.
   */
  final def ++[R1 <: R](that: BlockedRequests[R1]): BlockedRequests[R1] =
    sequential(self, that)

  /**
   * Transforms all data sources with the specified data source function, which
   * can change the environment type of data sources but must  preserve the
   * request type of each data source.
   */
  final def mapDataSources[R1](f: DataSourceFunction[R, R1]): BlockedRequests[R1] =
    self match {
      case Empty            => empty
      case Parallel(l, r)   => parallel(l.mapDataSources(f), r.mapDataSources(f))
      case Sequential(l, r) => sequential(l.mapDataSources(f), r.mapDataSources(f))
      case Single(ds, br)   => single(f(ds), br)
    }

  /**
   * Executes all requests, submitting batched requests to each data source in
   * parallel.
   */
  val run: ZIO[R, Nothing, Unit] = {
    val flattened = BlockedRequests.flatten(self)
    val pipelined = BlockedRequests.pipeline(flattened)
    ZIO.foreach_(pipelined) { requests =>
      val grouped = BlockedRequests.groupByDataSource(requests)
      ZIO.foreachPar_(grouped) {
        case (dataSource, requests) =>
          val requestsToRun = requests.map(_.map(_.request))
          for {
            completedRequests <- dataSource.runAll(requestsToRun)
            _ <- ZIO.foreach_(requests) { blockedRequest =>
                  ZIO.foreach(blockedRequest) { br =>
                    br.result.set(completedRequests.lookup(br.request))
                  }
                }
          } yield ()
      }
    }
  }
}

private[zquery] object BlockedRequests {

  /**
   * The empty set of blocked requests.
   */
  val empty: BlockedRequests[Any] =
    Empty

  /**
   * Constructs a set of blocked requests from two sets of blocked requests
   * that can be performed in parallel.
   */
  def parallel[R](left: BlockedRequests[R], right: BlockedRequests[R]): BlockedRequests[R] =
    Parallel(left, right)

  /**
   * Constructs a set of blocked requests from two sets of blocked requests
   * that must be performed sequentially.
   */
  def sequential[R](left: BlockedRequests[R], right: BlockedRequests[R]): BlockedRequests[R] =
    Sequential(left, right)

  /**
   * Constructs a set of blocked requests from the specified blocked request
   * data data source.
   */
  def single[R, K](dataSource: DataSource[R, K], blockedRequest: BlockedRequest[K]): BlockedRequests[R] =
    Single(dataSource, blockedRequest)

  case object Empty extends BlockedRequests[Any]

  final case class Single[-R, A](dataSource: DataSource[R, A], blockedRequest: BlockedRequest[A])
      extends BlockedRequests[R]

  final case class Parallel[-R](left: BlockedRequests[R], right: BlockedRequests[R]) extends BlockedRequests[R]

  final case class Sequential[-R](left: BlockedRequests[R], right: BlockedRequests[R]) extends BlockedRequests[R]

  /**
   * Flattens a collection of blocked requests to a sequence of sets of
   * blocked requests, where each set represents requests that can be performed
   * in parallel and sequential sets represent requests that must be performed
   * sequentially.
   */
  private def flatten[R](blockedRequests: BlockedRequests[R]): List[List[Single[R, _]]] = {

    @tailrec
    def loop[R](
      blockedRequests: List[BlockedRequests[R]],
      flattened: List[List[Single[R, _]]]
    ): List[List[Single[R, _]]] = {
      val (parallel, sequential) =
        blockedRequests.foldLeft((List.empty[Single[R, _]], List.empty[BlockedRequests[R]])) {
          case ((parallel, sequential), blockedRequest) =>
            val (set, seq) = step(blockedRequest)
            (parallel ++ set, sequential ++ seq)
        }
      val updated = if (parallel.nonEmpty) parallel :: flattened else flattened
      if (sequential.isEmpty) updated.reverse
      else loop(sequential, updated)
    }

    loop(List(blockedRequests), List.empty)
  }

  /**
   * Takes one step in evaluating a collection of blocked requests, returning a
   * set of blocked requests that can be performed in parallel and a list of
   * blocked requests that must be performed sequentially after those requests.
   */
  private def step[R](c: BlockedRequests[R]): (List[Single[R, _]], List[BlockedRequests[R]]) = {

    @tailrec
    def loop[R](
      blockedRequests: BlockedRequests[R],
      stack: List[BlockedRequests[R]],
      parallel: List[Single[R, _]],
      sequential: List[BlockedRequests[R]]
    ): (List[Single[R, _]], List[BlockedRequests[R]]) = blockedRequests match {
      case Empty =>
        if (stack.isEmpty) (parallel, sequential) else loop(stack.head, stack.tail, parallel, sequential)
      case Sequential(left, right) =>
        left match {
          case Empty            => loop(right, stack, parallel, sequential)
          case Sequential(l, r) => loop(Sequential(l, Sequential(r, right)), stack, parallel, sequential)
          case Parallel(l, r) =>
            loop(Parallel(Sequential(l, right), Sequential(r, right)), stack, parallel, sequential)
          case o => loop(o, stack, parallel, right :: sequential)
        }
      case Parallel(left, right) => loop(left, right :: stack, parallel, sequential)
      case o @ Single(_, _) =>
        if (stack.isEmpty) (parallel ++ List(o), sequential)
        else loop(stack.head, stack.tail, parallel ++ List(o), sequential)
    }

    loop(c, List.empty, List.empty[Single[R, _]], List.empty)
  }

  def pipeline[R](blockedRequests: List[List[Single[R, _]]]): List[List[List[Single[R, _]]]] = {

    @tailrec
    def loop(
      blockedRequests: List[List[Single[R, _]]],
      last: List[Single[R, _]],
      acc: List[List[List[Single[R, _]]]]
    ): List[List[List[Single[R, _]]]] =
      blockedRequests match {
        case h :: t =>
          val lastDataSources    = last.map(_.dataSource)
          val currentDataSources = h.map(_.dataSource)
          if (lastDataSources.size == 1 && lastDataSources == currentDataSources) {
            val combined = h :: acc.head
            loop(t, h, combined :: acc.tail)
          } else {
            loop(t, h, List(h) :: acc)
          }
        case _ => acc.reverse.map(_.reverse)
      }

    blockedRequests match {
      case h :: t => loop(t, h, List(List(h)))
      case Nil    => Nil
    }
  }

  def groupByDataSource[R](
    requests: List[List[Single[R, _]]]
  ): Map[DataSource[R, Any], List[List[BlockedRequest[Any]]]] =
    requests.foldRight(Map.empty[DataSource[R, Any], List[List[BlockedRequest[Any]]]]) { (parallel, map) =>
      val map1 = map.map { case (k, v) => (k, List.empty :: v) }
        .asInstanceOf[Map[DataSource[R, Any], List[List[BlockedRequest[Any]]]]]
      parallel.foldLeft(map1) {
        case (map, Single(dataSource, request)) =>
          val list = map.getOrElse(dataSource.asInstanceOf[DataSource[Any, Any]], List(List.empty))
          val updated = (list.head.asInstanceOf[List[BlockedRequest[Any]]] :+ request
            .asInstanceOf[BlockedRequest[Any]]) :: list.tail
          map + (dataSource.asInstanceOf[DataSource[R, Any]] -> updated
            .asInstanceOf[List[List[BlockedRequest[Any]]]])
      }
    }

}
