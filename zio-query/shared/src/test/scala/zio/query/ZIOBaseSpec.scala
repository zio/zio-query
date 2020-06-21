package zio.query

import zio.URIO
import zio.duration._
import zio.test._
import zio.clock.Clock
import zio.test.environment._

trait ZIOBaseSpec extends RunnableSpec[TestEnvironment, Any] {

  override def aspects: List[TestAspect[Nothing, TestEnvironment, Nothing, Any]] =
    List(TestAspect.timeout(60.seconds))

  override def runner: TestRunner[TestEnvironment, Any] =
    defaultTestRunner

  /**
   * Returns an effect that executes a given spec, producing the results of the execution.
   */
  private[zio] override def runSpec(
    spec: ZSpec[Environment, Failure]
  ): URIO[TestLogger with Clock, ExecutedSpec[Failure]] =
    runner.run(aspects.foldLeft(spec)(_ @@ _))
}
