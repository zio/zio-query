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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class ZQueryBenchmark {
  val cache = Cache.unsafeMake()

  val qs1 = Chunk.fill(1000)(ZQuery.succeedNow("foo").runCache(cache))
  val qs2 = Chunk.fill(1000)(ZQuery.succeed("foo").runCache(cache))

  @Benchmark
  @OperationsPerInvocation(1000)
  def zQueryRunSucceedNowBenchmark() =
    unsafeRunZIO(ZIO.collectAllDiscard(qs1))

  @Benchmark
  def zQuerySingleRunSucceedNowBenchmark() =
    unsafeRunZIO(qs1.head)

  @Benchmark
  @OperationsPerInvocation(1000)
  def zQueryRunSucceedBenchmark() =
    unsafeRunZIO(ZIO.collectAllDiscard(qs2))

  @Benchmark
  def zQuerySingleRunSucceedBenchmark() =
    unsafeRunZIO(qs2.head)
}
