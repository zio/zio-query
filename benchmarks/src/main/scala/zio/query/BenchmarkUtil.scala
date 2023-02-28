package zio.query

import zio._

object BenchmarkUtil extends Runtime[Any] { self =>
  val environment = Runtime.default.environment

  val fiberRefs = Runtime.default.fiberRefs

  val runtimeFlags = Runtime.default.runtimeFlags

  def unsafeRun[E, A](query: ZQuery[Any, E, A]): A =
    Unsafe.unsafe(implicit unsafe => self.unsafe.run(query.run).getOrThrowFiberFailure())
}
