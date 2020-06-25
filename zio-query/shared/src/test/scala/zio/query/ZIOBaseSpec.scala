package zio.query

import zio.duration._
import zio.test._
import zio.test.environment._

trait ZIOBaseSpec extends DefaultRunnableSpec {

  override def aspects: List[TestAspect[Nothing, TestEnvironment, Nothing, Any]] =
    List(TestAspect.timeout(60.seconds))
}
