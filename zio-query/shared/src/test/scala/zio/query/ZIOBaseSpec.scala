package zio.query

import zio._
import zio.test._

trait ZIOBaseSpec extends ZIOSpecDefault {
  override def aspects: Chunk[TestAspectPoly] =
    if (TestPlatform.isJVM || TestPlatform.isNative) Chunk(TestAspect.timeout(60.seconds), TestAspect.timed)
    else Chunk(TestAspect.timeout(60.seconds), TestAspect.sequential, TestAspect.timed)
}
