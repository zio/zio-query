package zio.query

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.{Chunk, ZIO}
import zio.query.BenchmarkUtil._

import java.util.concurrent.TimeUnit

@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class CollectAllBenchmark {

  @Param(Array("100", "1000"))
  var count: Int = 100

  val parallelism: Int = 10

  @Benchmark
  def zQueryCollectAll(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAll(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllBatched(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllPar(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllPar(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllParN(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllBatched(queries).map(_.sum.toLong).withParallelism(parallelism)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryFromRequestsBatchedUnique(): Long = {
    val reqs                              = Chunk.fromIterable((0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)))
    val query: ZQuery[Any, Nothing, Long] = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryFromRequestsBatchedDuped(): Long = {
    val reqs                              = Chunk.fromIterable((0 until count).map(_ => ZQuery.fromRequest(Req(1))(ds)))
    val query: ZQuery[Any, Nothing, Long] = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query)
  }

  private case class Req(i: Int) extends Request[Nothing, Int]
  private val ds = DataSource.fromFunctionBatchedZIO("Datasource") { reqs: Chunk[Req] => ZIO.succeed(reqs.map(_.i)) }

}

/*
[info] Benchmark                                            (count)   Mode  Cnt      Score      Error  Units
[info] CollectAllBenchmark.zQueryFromRequestsBatchedDuped       100  thrpt    3  27397.424 ± 5362.120  ops/s
[info] CollectAllBenchmark.zQueryFromRequestsBatchedDuped      1000  thrpt    3   3127.118 ±  418.691  ops/s
[info] CollectAllBenchmark.zQueryFromRequestsBatchedUnique      100  thrpt    3  12770.935 ± 3246.335  ops/s
[info] CollectAllBenchmark.zQueryFromRequestsBatchedUnique     1000  thrpt    3   1116.400 ±   41.179  ops/s
 */