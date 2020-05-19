package zio.query

import zio.{ Chunk, ZIO }

trait DataSourceAspect[+LowerR, -UpperR] { self =>

  def apply[R >: LowerR <: UpperR, A](dataSource: DataSource[R, A]): DataSource[R, A]

  def >>>[LowerR1 >: LowerR, UpperR1 <: UpperR](
    that: DataSourceAspect[LowerR1, UpperR1]
  ): DataSourceAspect[LowerR1, UpperR1] =
    andThen(that)

  def andThen[LowerR1 >: LowerR, UpperR1 <: UpperR](
    that: DataSourceAspect[LowerR1, UpperR1]
  ): DataSourceAspect[LowerR1, UpperR1] =
    new DataSourceAspect[LowerR1, UpperR1] {
      def apply[R >: LowerR1 <: UpperR1, A](dataSource: DataSource[R, A]): DataSource[R, A] =
        that(self(dataSource))
    }
}

object DataSourceAspect {

  /**
   * A data source aspect that executes requests between two effects, `before`
   * and `after`, where the result of `before` can be used by `after`.
   */
  def around[R0, A0](
    before: Described[ZIO[R0, Nothing, A0]]
  )(after: Described[A0 => ZIO[R0, Nothing, Any]]): DataSourceAspect[Nothing, R0] =
    new DataSourceAspect[Nothing, R0] {
      def apply[R <: R0, A](dataSource: DataSource[R, A]): DataSource[R, A] =
        new DataSource[R, A] {
          val identifier = s"${dataSource.identifier} @@ around(${before.description})(${after.description})"
          def runAll(requests: Chunk[Chunk[A]]): ZIO[R, Nothing, CompletedRequestMap] =
            before.value.bracket(after.value)(_ => runAll(requests))
        }
    }
}
