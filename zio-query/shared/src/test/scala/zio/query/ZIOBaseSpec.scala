package zio.query

import zio._
import zio.test._

trait ZIOBaseSpec extends ZIOSpecDefault {
  override def aspects = Chunk(TestAspect.timeout(60.seconds))
}
