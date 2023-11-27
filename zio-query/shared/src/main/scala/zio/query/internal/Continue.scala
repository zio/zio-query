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
import zio.query._
import zio.query.internal.Continue._
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `Continue[R, E, A]` models a continuation of a blocked request that
 * requires an environment `R` and may either fail with an `E` or succeed with
 * an `A`. A continuation may either be a `Get` that merely gets the result of a
 * blocked request (potentially transforming it with pure functions) or an
 * `Effect` that may perform arbitrary effects. This is used by the library
 * internally to determine whether it is safe to pipeline two requests that must
 * be executed sequentially.
 */
private[query] sealed trait Continue[-R, +E, +A] { self =>

  /**
   * Purely folds over the failure and success types of this continuation.
   */
  final def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): Continue[R, Nothing, B] =
    self match {
      case Effect(query) => effect(query.fold(failure, success))
      case Get(io)       => get(io.fold(failure, success))
    }

  /**
   * Effectually folds over the failure and success types of this continuation.
   */
  final def foldCauseQuery[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => effect(query.foldCauseQuery(failure, success))
      case Get(io)       => effect(ZQuery.fromZIO(io).foldCauseQuery(failure, success))
    }

  /**
   * Purely maps over the success type of this continuation.
   */
  final def map[B](f: A => B)(implicit trace: Trace): Continue[R, E, B] =
    self match {
      case Effect(query) => effect(query.map(f))
      case Get(io)       => get(io.map(f))
    }

  /**
   * Transforms all data sources with the specified data source aspect.
   */
  final def mapDataSources[R1 <: R](f: DataSourceAspect[R1])(implicit trace: Trace): Continue[R1, E, A] =
    self match {
      case Effect(query) => effect(query.mapDataSources(f))
      case Get(io)       => get(io)
    }

  /**
   * Purely maps over the failure type of this continuation.
   */
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): Continue[R, E1, A] =
    self match {
      case Effect(query) => effect(query.mapError(f))
      case Get(io)       => get(io.mapError(f))
    }

  /**
   * Purely maps over the failure cause of this continuation.
   */
  final def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): Continue[R, E1, A] =
    self match {
      case Effect(query) => effect(query.mapErrorCause(f))
      case Get(io)       => get(io.mapErrorCause(f))
    }

  /**
   * Effectually maps over the success type of this continuation.
   */
  final def mapQuery[R1 <: R, E1 >: E, B](
    f: A => ZQuery[R1, E1, B]
  )(implicit trace: Trace): Continue[R1, E1, B] =
    self match {
      case Effect(query) => effect(query.flatMap(f))
      case Get(io)       => effect(ZQuery.fromZIO(io).flatMap(f))
    }

  /**
   * Purely contramaps over the environment type of this continuation.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  )(implicit trace: Trace): Continue[R0, E, A] =
    self match {
      case Effect(query) => effect(query.provideSomeEnvironment(f))
      case Get(io)       => get(io)
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in sequence.
   */
  final def zipWith[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => effect(l.zipWith(r)(f))
      case (Effect(l), Get(r))    => effect(l.zipWith(ZQuery.fromZIO(r))(f))
      case (Get(l), Effect(r))    => effect(ZQuery.fromZIO(l).zipWith(r)(f))
      case (Get(l), Get(r))       => get(l.zipWith(r)(f))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, in parallel.
   */
  final def zipWithPar[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => effect(l.zipWithPar(r)(f))
      case (Effect(l), Get(r))    => effect(l.zipWith(ZQuery.fromZIO(r))(f))
      case (Get(l), Effect(r))    => effect(ZQuery.fromZIO(l).zipWith(r)(f))
      case (Get(l), Get(r))       => get(l.zipWith(r)(f))
    }

  /**
   * Combines this continuation with that continuation using the specified
   * function, batching requests to data sources.
   */
  final def zipWithBatched[R1 <: R, E1 >: E, B, C](
    that: Continue[R1, E1, B]
  )(f: (A, B) => C)(implicit trace: Trace): Continue[R1, E1, C] =
    (self, that) match {
      case (Effect(l), Effect(r)) => effect(l.zipWithBatched(r)(f))
      case (Effect(l), Get(r))    => effect(l.zipWith(ZQuery.fromZIO(r))(f))
      case (Get(l), Effect(r))    => effect(ZQuery.fromZIO(l).zipWith(r)(f))
      case (Get(l), Get(r))       => get(l.zipWith(r)(f))
    }
}

private[query] object Continue {

  /**
   * Constructs a continuation from a request, a data source, and a `Promise`
   * that will contain the result of the request when it is executed.
   */
  def apply[R, E, A, B](request: A, dataSource: DataSource[R, A], promise: Promise[E, B])(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): Continue[R, E, B] =
    Continue.get(promise.await)

  /**
   * Collects a collection of continuation into a continuation returning a
   * collection of their results, batching requests to data sources.
   */
  def collectAllBatched[R, E, A, Collection[+Element] <: Iterable[Element]](
    continues: Collection[Continue[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[Continue[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Continue[R, E, Collection[A]] =
    collectAllImpl(continues, ZQuery.collectAllBatched(_: Chunk[ZQuery[R, E, A]]))(implicitly, trace)

  /**
   * Collects a collection of continuation into a continuation returning a
   * collection of their results, in parallel.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](
    continues: Collection[Continue[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[Continue[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Continue[R, E, Collection[A]] =
    collectAllImpl(continues, ZQuery.collectAllPar(_: Chunk[ZQuery[R, E, A]]))(implicitly, trace)

  private def collectAllImpl[R, E, A, Collection[+Element] <: Iterable[Element]](
    continues: Collection[Continue[R, E, A]],
    collectQueries: Chunk[ZQuery[R, E, A]] => ZQuery[R, E, Chunk[A]]
  )(implicit
    bf: BuildFrom[Collection[Continue[R, E, A]], A, Collection[A]],
    trace: Trace
  ): Continue[R, E, Collection[A]] = {
    def populateArr(arr: Array[AnyRef])(values: Chunk[A], idxs: Chunk[Int]): Unit = {
      var i    = 0
      val size = idxs.size
      while (i < size) {
        arr(idxs(i)) = values(i).asInstanceOf[AnyRef]
        i += 1
      }
    }

    partitionResults(continues) match {
      case res: PartitionResults.AllIos[E, A] =>
        get(ZIO.collectAll(res.ios).map(bf.fromSpecific(continues)))
      case res: PartitionResults.AllQueries[R, E, A] =>
        effect(collectQueries(res.queries).map(bf.fromSpecific(continues)))
      case res: PartitionResults.Mix[R, E, A] =>
        val query = collectQueries(res.queries.queries).flatMap { as =>
          val array     = Array.ofDim[AnyRef](continues.size)
          val addValues = populateArr(array) _
          addValues(as, res.queries.idx)
          ZQuery.fromZIO(ZIO.collectAll(res.ios.results)).map { as =>
            addValues(as, res.ios.idx)
            bf.fromSpecific(continues)(array.asInstanceOf[Array[A]])
          }
        }
        effect(query)
    }
  }

  private def partitionResults[R, E, A](results: Iterable[Continue[R, E, A]]): PartitionedResults = {
    val effects                = Chunk.newBuilder[IO[E, A]]
    val queries                = Chunk.newBuilder[ZQuery[R, E, A]]
    val effectsIdx, queriesIdx = new ChunkBuilder.Int

    val iterator = results.iterator
    var i        = 0
    while (iterator.hasNext) {
      iterator.next() match {
        case Effect(query) => queries.addOne(query); queriesIdx.addOne(i)
        case Get(io)       => effects.addOne(io); effectsIdx.addOne(i)
      }
      i += 1
    }

    val qs  = queries.result()
    val ios = effects.result()
    if (qs.isEmpty) PartitionResults.AllIos(ios)
    else if (ios.isEmpty) PartitionResults.AllQueries(qs)
    else
      PartitionResults.Mix(
        PartitionResults.Mix.Ios(ios, effectsIdx.result()),
        PartitionResults.Mix.Queries(qs, queriesIdx.result())
      )
  }

  /**
   * Constructs a continuation that may perform arbitrary effects.
   */
  def effect[R, E, A](query: ZQuery[R, E, A]): Continue[R, E, A] =
    Effect(query)

  /**
   * Constructs a continuation that merely gets the result of a blocked request
   * (potentially transforming it with pure functions).
   */
  def get[E, A](io: IO[E, A]): Continue[Any, E, A] =
    Get(io)

  final case class Effect[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A]
  final case class Get[E, A](io: IO[E, A])                 extends Continue[Any, E, A]

  private sealed trait PartitionedResults
  private object PartitionResults {
    case class AllIos[E, A](ios: Chunk[IO[E, A]])                   extends PartitionedResults
    case class AllQueries[R, E, A](queries: Chunk[ZQuery[R, E, A]]) extends PartitionedResults

    case class Mix[R, E, A](ios: Mix.Ios[E, A], queries: Mix.Queries[R, E, A]) extends PartitionedResults
    object Mix {
      case class Ios[E, A](results: Chunk[IO[E, A]], idx: Chunk[Int])
      case class Queries[R, E, A](queries: Chunk[ZQuery[R, E, A]], idx: Chunk[Int])
    }
  }
}
