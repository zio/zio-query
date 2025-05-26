package zio.query

import org.openjdk.jmh.annotations.{Scope as JScope, *}
import zio.query.BenchmarkUtil.*

import java.util.concurrent.TimeUnit

@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
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
    val queries  = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query = ZQuery.collectAll(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllBatched(): Long = {
    val queries  = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllPar(): Long = {
    val queries  = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query = ZQuery.collectAllPar(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllParN(): Long = {
    val queries  = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query = ZQuery.collectAllPar(queries).map(_.sum.toLong).withParallelism(parallelism)
    unsafeRun(query)
  }
}
