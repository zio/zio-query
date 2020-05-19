package zio.query

import zio.{ Chunk, ZIO }

trait DataSourceAspect[+R] {
  type Out[+_]

  def apply[R1 >: R, A](dataSource: DataSource[R1, A]): DataSource[Out[R1], A]
}

object DataSourceAspect {

  type Aux[+R, Out0[+_]] = DataSourceAspect[R] { type Out[A] = Out0[A] }

  def around[R0, A0](before: Described[ZIO[R0, Nothing, A0]])(
    after: Described[A0 => ZIO[R0, Nothing, Any]]
  ): DataSourceAspect.Aux[Nothing, ({ type lambda[+x] = R0 with x })#lambda] =
    new DataSourceAspect[Nothing] {
      type Out[+A] = R0 with A
      def apply[R, A](dataSource: DataSource[R, A]): DataSource[R0 with R, A] =
        new DataSource[R0 with R, A] {
          val identifier = s"${dataSource.identifier} @@ around(${before.description})(${after.description})"
          def runAll(requests: Chunk[Chunk[A]]): ZIO[R0 with R, Nothing, CompletedRequestMap] =
            before.value.bracket(after.value)(_ => dataSource.runAll(requests))
        }
    }

  def provide[R](r: Described[R]): DataSourceAspect.Aux[R, ({ type lambda[+x] = Any })#lambda] =
    new DataSourceAspect[R] {
      type Out[+A] = Any
      def apply[R1 >: R, A](dataSource: DataSource[R1, A]): DataSource[Any, A] =
        dataSource.provide(r)
    }

  def provideSome[R0, R](f: Described[R0 => R]): DataSourceAspect.Aux[R, ({ type lambda[+x] = R0 })#lambda] =
    new DataSourceAspect[R] {
      type Out[+A] = R0
      def apply[R1 >: R, A](dataSource: DataSource[R1, A]): DataSource[R0, A] =
        dataSource.provideSome(f)
    }

  implicit class AndThenSyntax[R, Out0[+_]](self: DataSourceAspect.Aux[R, Out0]) {

    def >>>[Out1[+_]](
      that: DataSourceAspect.Aux[Out0[R], Out1]
    ): DataSourceAspect.Aux[R, ({ type lambda[+x] = Out1[Out0[x]] })#lambda] =
      andThen(that)

    def andThen[Out1[+_]](
      that: DataSourceAspect.Aux[Out0[R], Out1]
    ): DataSourceAspect.Aux[R, ({ type lambda[+x] = Out1[Out0[x]] })#lambda] =
      new DataSourceAspect[R] {
        type Out[+A] = Out1[Out0[A]]
        def apply[R1 >: R, A](dataSource: DataSource[R1, A]): DataSource[Out[R1], A] =
          that(self(dataSource))
      }
  }

  implicit class ComposeSyntax[R, Out0[+_], Out1[+_]](self: DataSourceAspect.Aux[Out0[R], Out1]) {

    def <<<(
      that: DataSourceAspect.Aux[R, Out0]
    ): DataSourceAspect.Aux[R, ({ type lambda[+x] = Out1[Out0[x]] })#lambda] =
      compose(that)

    def compose(
      that: DataSourceAspect.Aux[R, Out0]
    ): DataSourceAspect.Aux[R, ({ type lambda[+x] = Out1[Out0[x]] })#lambda] =
      new DataSourceAspect[R] {
        type Out[+A] = Out1[Out0[A]]
        def apply[R1 >: R, A](dataSource: DataSource[R1, A]): DataSource[Out[R1], A] =
          self(that(dataSource))
      }
  }
}
