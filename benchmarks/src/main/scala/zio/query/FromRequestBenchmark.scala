package zio.query

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.query.BenchmarkUtil._
import zio.{Chunk, ZIO}

import java.util.concurrent.TimeUnit

@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@Threads(1)
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class FromRequestBenchmark {

  @Param(Array("100", "1000"))
  var count: Int = 100

  @Benchmark
  def fromRequestDefault(): Long = {
    val reqs  = Chunk.fromIterable((0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)))
    val query = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def fromRequestSized(): Long = {
    val reqs  = Chunk.fromIterable((0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)))
    val query = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRunCache(query, Cache.unsafeMake(count))
  }

  @Benchmark
  def fromRequestZipRight(): Long = {
    val reqs  = Chunk.fromIterable((0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)))
    val query = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query *> query *> query)
  }

  private case class Req(i: Int) extends Request[Nothing, Int]
  private val ds = DataSource.fromFunctionBatchedZIO("Datasource") { reqs: Chunk[Req] => ZIO.succeed(reqs.map(_.i)) }
}
