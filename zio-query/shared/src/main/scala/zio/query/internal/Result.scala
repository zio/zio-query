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

import zio._
import zio.query.internal.Result._
import zio.query.{DataSourceAspect, Described}
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Result[R, E, A]` is the result of running one step of a `ZQuery`. A result
 * may either by done with a value `A`, blocked on a set of requests to data
 * sources that require an environment `R`, or failed with an `E`.
 */
private[query] sealed trait Result[-R, +E, +A] { self =>

  /**
   * Folds over the successful or failed result.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Result[R, Nothing, B] =
    self match {
      case Blocked(br, c) => blocked(br, c.fold(failure, success))
      case Done(a)        => done(success(a))
      case Fail(e)        => e.failureOrCause.fold(e => done(failure(e)), c => fail(c))
    }

  /**
   * Maps the specified function over the successful value of this result.
   */
  final def map[B](f: A => B)(implicit trace: Trace): Result[R, E, B] =
    self match {
      case Blocked(br, c) => blocked(br, c.map(f))
      case Done(a)        => done(f(a))
      case Fail(e)        => fail(e)
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Result[R1, E, A] =
    self match {
      case Blocked(br, c) => Result.blocked(br.mapDataSources(f), c.mapDataSources(f))
      case Done(a)        => Result.done(a)
      case Fail(e)        => Result.fail(e)
    }

  /**
   * Maps the specified function over the failed value of this result.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Result[R, E1, A] =
    self match {
      case Blocked(br, c) => blocked(br, c.mapError(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(e.map(f))
    }

  /**
   * Maps the specified function over the failure cause of this result.
   */
  def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Result[R, E1, A] =
    self match {
      case Blocked(br, c) => blocked(br, c.mapErrorCause(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(f(e))
    }

  /**
   * Provides this result with its required environment.
   */
  final def provideEnvironment(
    r: Described[ZEnvironment[R]]
  )(implicit trace: Trace): Result[Any, E, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Provides this result with part of its required environment.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Result[R0, E, A] =
    self match {
      case Blocked(br, c) => blocked(br.provideSomeEnvironment(f), c.provideSomeEnvironment(f))
      case Done(a)        => done(a)
      case Fail(e)        => fail(e)
    }
}

private[query] object Result {

  /**
   * Constructs a result that is blocked on the specified requests with the
   * specified continuation.
   */
  def blocked[R, E, A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A]): Result[R, E, A] =
    Blocked(blockedRequests, continue)

  /**
   * Collects a collection of results into a single result. Blocked requests
   * will be batched.
   */
  def collectAllBatched[R, E, A, Collection[+Element] <: Iterable[Element]](results: Collection[Result[R, E, A]])(
    implicit
    bf: BuildFrom[Collection[Result[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Result[R, E, Collection[A]] =
    collectAllImpl(results, Continue.collectAllBatched(_: Chunk[Continue[R, E, A]])(implicitly, trace))

  /**
   * Collects a collection of results into a single result. Blocked requests and
   * their continuations will be executed in parallel.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](results: Collection[Result[R, E, A]])(implicit
    bf: BuildFrom[Collection[Result[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Result[R, E, Collection[A]] =
    collectAllImpl(results, Continue.collectAllPar(_: Chunk[Continue[R, E, A]])(implicitly, trace))

  private def collectAllImpl[R, E, A, Collection[+Element] <: Iterable[Element]](
    results: Collection[Result[R, E, A]],
    collectContinue: Chunk[Continue[R, E, A]] => Continue[R, E, Chunk[A]]
  )(implicit
    bf: BuildFrom[Collection[Result[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Result[R, E, Collection[A]] = {
    def populateArr(arr: Array[AnyRef])(values: Chunk[A], idxs: Chunk[Int]): Unit = {
      var i    = 0
      val size = idxs.size
      while (i < size) {
        arr(idxs(i)) = values(i).asInstanceOf[AnyRef]
        i += 1
      }
    }

    def collectBlocked(blocked: Chunk[BlockedRequests[R]]): BlockedRequests[R] =
      blocked.foldLeft[BlockedRequests[R]](BlockedRequests.empty)(_ && _)

    partitionResults(results) match {
      case done: PartitionedResults.AllDone[A] =>
        Result.done(bf.fromSpecific(results)(done.results))
      case res: PartitionedResults.AllBlocked[R, E, A] =>
        val blockedRequests = collectBlocked(res.blocked)
        val continue        = collectContinue(res.continue).map(bf.fromSpecific(results))
        Result.blocked(blockedRequests, continue)
      case res: PartitionedResults.Mix[R, E, A] =>
        val blockedRequests = collectBlocked(res.blocked.requests)
        val continue = collectContinue(res.blocked.continue).map { as =>
          val array     = Array.ofDim[AnyRef](results.size)
          val addValues = populateArr(array) _
          addValues(as, res.blocked.idx)
          addValues(res.done.results, res.done.idx)
          bf.fromSpecific(results)(array.asInstanceOf[Array[A]])
        }
        Result.blocked(blockedRequests, continue)
      case failed: PartitionedResults.Failed[E] =>
        Result.fail(failed.causes.foldLeft[Cause[E]](Cause.empty)(_ && _))
    }
  }

  private def partitionResults[R, E, A](results: Iterable[Result[R, E, A]]): PartitionedResults = {
    val blockedReq          = Chunk.newBuilder[BlockedRequests[R]]
    val continues           = Chunk.newBuilder[Continue[R, E, A]]
    val done                = Chunk.newBuilder[A]
    val failed              = Chunk.newBuilder[Cause[E]]
    val blockedIdx, doneIdx = new ChunkBuilder.Int

    val iterator = results.iterator
    var i        = 0
    while (iterator.hasNext) {
      iterator.next() match {
        case Blocked(br, c) => blockedReq.addOne(br); continues.addOne(c); blockedIdx.addOne(i)
        case Done(a)        => done.addOne(a); doneIdx.addOne(i)
        case Fail(e)        => failed.addOne(e)
      }
      i += 1
    }

    val fs = failed.result()
    if (fs.nonEmpty) PartitionedResults.Failed(fs)
    else {
      val bs = blockedReq.result()
      val ds = done.result()
      if (bs.isEmpty) PartitionedResults.AllDone(ds)
      else if (ds.isEmpty) PartitionedResults.AllBlocked(bs, continues.result())
      else
        PartitionedResults.Mix(
          PartitionedResults.Mix.Blocked(bs, continues.result(), blockedIdx.result()),
          PartitionedResults.Mix.Done(ds, doneIdx.result())
        )
    }
  }

  /**
   * Constructs a result that is done with the specified value.
   */
  def done[A](value: A): Result[Any, Nothing, A] =
    Done(value)

  /**
   * Constructs a result that is failed with the specified `Cause`.
   */
  def fail[E](cause: Cause[E]): Result[Any, E, Nothing] =
    Fail(cause)

  /**
   * Lifts an `Exit` into a result.
   */
  def fromExit[E, A](exit: Exit[E, A]): Result[Any, E, A] =
    exit.foldExit(Result.fail, Result.done)

  final case class Blocked[-R, +E, +A](blockedRequests: BlockedRequests[R], continue: Continue[R, E, A])
      extends Result[R, E, A]

  final case class Done[+A](value: A) extends Result[Any, Nothing, A]

  final case class Fail[+E](cause: Cause[E]) extends Result[Any, E, Nothing]

  private sealed trait PartitionedResults
  private object PartitionedResults {
    case class AllDone[A](results: Chunk[A]) extends PartitionedResults
    case class AllBlocked[R, E, A](blocked: Chunk[BlockedRequests[R]], continue: Chunk[Continue[R, E, A]])
        extends PartitionedResults
    case class Failed[E](causes: Chunk[Cause[E]])                             extends PartitionedResults
    case class Mix[R, E, A](blocked: Mix.Blocked[R, E, A], done: Mix.Done[A]) extends PartitionedResults

    object Mix {
      case class Blocked[R, E, A](
        requests: Chunk[BlockedRequests[R]],
        continue: Chunk[Continue[R, E, A]],
        idx: Chunk[Int]
      )
      case class Done[A](results: Chunk[A], idx: Chunk[Int])
    }

  }
}
