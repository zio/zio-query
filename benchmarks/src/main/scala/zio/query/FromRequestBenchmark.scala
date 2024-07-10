package zio.query

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.query.BenchmarkUtil._
import zio.{Chunk, ZIO}

import java.util.concurrent.TimeUnit

@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class FromRequestBenchmark {

  @Param(Array("100", "1000"))
  var count: Int = 100

  var queries: Chunk[UQuery[Int]] = _

  @Setup(Level.Trial)
  def setup() =
    queries = Chunk.fromIterable((0 until count).map(i => ds.query(Req(i))))

  @Benchmark
  def fromRequestDefault(): Long = {
    val query = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def fromRequestSized(): Long = {
    val query = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRunCache(query, Cache.unsafeMake(count))
  }

  @Benchmark
  def fromRequests(): Long = {
    val reqs  = Chunk.fromIterable((0 until count).map(i => Req(i)))
    val query = ds.queryAll(reqs).map(_.sum.toLong)
    unsafeRunCache(query, Cache.unsafeMake(count))
  }

  @Benchmark
  def fromRequestsCached(): Long = {
    val reqs  = Chunk.fromIterable((0 until count).map(_ => Req(1)))
    val query = ds.queryAll(reqs).map(_.sum.toLong)
    unsafeRunCache(query, Cache.unsafeMake(count))
  }

  @Benchmark
  def fromRequestUncached(): Long = {
    val query = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query.uncached)
  }

  @Benchmark
  def fromRequestZipRight(): Long = {
    val query = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query *> query *> query)
  }

  @Benchmark
  def fromRequestZipRightMemoized(): Long = {
    val f = ZQuery.collectAllBatched(queries).map(_.sum.toLong).memoize
    unsafeRun(ZQuery.unwrap(f.map(q => q *> q *> q)))
  }

  private case class Req(i: Int) extends Request[Nothing, Int]
  private val ds = DataSource.fromFunctionBatchedZIO("Datasource") { (reqs: Chunk[Req]) => ZIO.succeed(reqs.map(_.i)) }
}
