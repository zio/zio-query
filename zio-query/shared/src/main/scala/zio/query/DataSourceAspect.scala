package zio.query

/**
 * A `DataSourceAspect[R, R1]` is a universally quantified function from
 * values of type `DataSource[R, A]` to values of type `DataSource[R1, A]` for
 * all types `A`. This is used internally by the library to describe functions
 * for transforming data sources that do not change the type of requests that a
 * data source is able to execute.
 */
trait DataSourceAspect[+R, -R1] { self =>

  def apply[A](dataSource: DataSource[R, A]): DataSource[R1, A]

  /**
   * A symbolic alias for `compose`.
   */
  final def <<<[R0](that: DataSourceAspect[R0, R]): DataSourceAspect[R0, R1] =
    self compose that

  /**
   * A symbolic alias for `andThen`.
   */
  final def >>>[R2](that: DataSourceAspect[R1, R2]): DataSourceAspect[R, R2] =
    self andThen that

  /**
   * Creates a new data source aspect by applying this data source aspect
   * followed by that data source aspect.
   */
  final def andThen[R2](that: DataSourceAspect[R1, R2]): DataSourceAspect[R, R2] =
    new DataSourceAspect[R, R2] {
      def apply[A](dataSource: DataSource[R, A]): DataSource[R2, A] =
        that(self(dataSource))
    }

  /**
   * Creates a new data source aspect by applying that data source aspect
   * followed by this data source aspect.
   */
  final def compose[R0](that: DataSourceAspect[R0, R]): DataSourceAspect[R0, R1] =
    new DataSourceAspect[R0, R1] {
      def apply[A](dataSource: DataSource[R0, A]): DataSource[R1, A] =
        self(that(dataSource))
    }
}

object DataSourceAspect {

  /**
   * A data source aspect that provides a data source with its required
   * environment.
   */
  def provide[R](r: Described[R]): DataSourceAspect[R, Any] =
    provideSome(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * A data source aspect that provides a data sources with part of its
   * required environment.
   */
  def provideSome[R, R1](f: Described[R1 => R]): DataSourceAspect[R, R1] =
    new DataSourceAspect[R, R1] {
      def apply[A](dataSource: DataSource[R, A]): DataSource[R1, A] =
        dataSource.provideSome(f)
    }
}
