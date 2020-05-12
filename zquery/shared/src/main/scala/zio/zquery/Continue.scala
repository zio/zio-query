package zio.zquery

import zio.{ CanFail, Cause, NeedsEnv }
import zio.zquery.Continue._

private[zquery] sealed trait Continue[-R, +E, +A] { self =>
  def flatMap[R1 <: R, E1 >: E, B](f: A => ZQuery[R1, E1, B]): Continue[R1, E1, B] =
    self match {
      case Done(query) => More(query.flatMap(f))
      case More(query) => More(query.flatMap(f))
    }
  final def fold[B](failure: E => B, success: A => B)(implicit ev: CanFail[E]): Continue[R, Nothing, B] =
    self match {
      case Done(query) => Done(query.fold(failure, success))
      case More(query) => More(query.fold(failure, success))
    }
  final def foldCauseM[R1 <: R, E1, B](
    failure: Cause[E] => ZQuery[R1, E1, B],
    success: A => ZQuery[R1, E1, B]
  ): Continue[R1, E1, B] =
    self match {
      case Done(query) => More(query.foldCauseM(failure, success))
      case More(query) => More(query.foldCauseM(failure, success))
    }
  def map[B](f: A => B): Continue[R, E, B] =
    self match {
      case Done(query) => Done(query.map(f))
      case More(query) => More(query.map(f))
    }
  final def mapError[E1](f: E => E1)(implicit ev: CanFail[E]): Continue[R, E1, A] =
    self match {
      case Done(query) => Done(query.mapError(f))
      case More(query) => More(query.mapError(f))
    }
  def provideSome[R0](f: Described[R0 => R])(implicit ev: NeedsEnv[R]): Continue[R0, E, A] =
    self match {
      case Done(query) => Done(query.provideSome(f))
      case More(query) => More(query.provideSome(f))
    }
  def run: ZQuery[R, E, A] =
    self match {
      case Done(query) => query
      case More(query) => query
    }
  def zipWith[R1 <: R, E1 >: E, B, C](that: Continue[R1, E1, B])(f: (A, B) => C): Continue[R1, E1, C] =
    (self, that) match {
      case (Done(query1), Done(query2)) => Done(query1.zipWith(query2)(f))
      case (Done(query1), More(query2)) => More(query1.zipWith(query2)(f))
      case (More(query1), Done(query2)) => More(query1.zipWith(query2)(f))
      case (More(query1), More(query2)) => More(query1.zipWith(query2)(f))
    }
  def zipWithPar[R1 <: R, E1 >: E, B, C](that: Continue[R1, E1, B])(f: (A, B) => C): Continue[R1, E1, C] =
    (self, that) match {
      case (Done(query1), Done(query2)) => Done(query1.zipWithPar(query2)(f))
      case (Done(query1), More(query2)) => More(query1.zipWithPar(query2)(f))
      case (More(query1), Done(query2)) => More(query1.zipWithPar(query2)(f))
      case (More(query1), More(query2)) => More(query1.zipWithPar(query2)(f))
    }
}

private[zquery] object Continue {

  final case class Done[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A]
  final case class More[R, E, A](query: ZQuery[R, E, A]) extends Continue[R, E, A]

  def done[R, E, A](query: ZQuery[R, E, A]): Continue[R, E, A] =
    Done(query)

  def more[R, E, A](query: ZQuery[R, E, A]): Continue[R, E, A] =
    More(query)
}
