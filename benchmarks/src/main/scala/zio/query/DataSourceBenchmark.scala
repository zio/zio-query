package zio.query

import cats.effect.IO
import cats.effect.unsafe.implicits._
import cats.syntax.all._
import fetch.{Fetch, fetchM}
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
class DataSourceBenchmark {

  /* Results as of 13/04/2024 (v0.7.0):
   * [info] Benchmark                                         (count)   Mode  Cnt      Score      Error  Units
   * [info] DataSourceBenchmark.fetchSumDuplicatedBenchmark       100  thrpt    3  13079.078 ± 1766.290  ops/s
   * [info] DataSourceBenchmark.fetchSumDuplicatedBenchmark      1000  thrpt    3   1750.691 ±  515.567  ops/s
   * [info] DataSourceBenchmark.fetchSumUniqueBenchmark           100  thrpt    3   1452.417 ±  301.895  ops/s
   * [info] DataSourceBenchmark.fetchSumUniqueBenchmark          1000  thrpt    3    170.926 ±   16.258  ops/s
   * [info] DataSourceBenchmark.zquerySumDuplicatedBenchmark      100  thrpt    3  50938.486 ± 2185.032  ops/s
   * [info] DataSourceBenchmark.zquerySumDuplicatedBenchmark     1000  thrpt    3   6260.981 ±  137.187  ops/s
   * [info] DataSourceBenchmark.zquerySumUniqueBenchmark          100  thrpt    3  38385.921 ± 4523.814  ops/s
   * [info] DataSourceBenchmark.zquerySumUniqueBenchmark         1000  thrpt    3   4287.090 ±  688.067  ops/s
   */

  @Param(Array("100", "1000"))
  var count: Int = 100

  @Benchmark
  def zquerySumDuplicatedBenchmark(): Long = {
    import ZQueryImpl._

    val reqs  = (0 until count).toList.map(i => ZQuery.fromRequest(Req(1))(ds))
    val query = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zquerySumUniqueBenchmark(): Long = {
    import ZQueryImpl._

    val reqs  = (0 until count).toList.map(i => ZQuery.fromRequest(Req(i))(ds))
    val query = ZQuery.collectAllBatched(reqs).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def fetchSumDuplicatedBenchmark(): Long = {
    import FetchImpl._
    import fetch.fetchM
    type FIO[A] = Fetch[IO, A]

    val reqs  = (0 until count).toList.map(i => fetchPlusOne(1))
    val query = reqs.sequence[FIO, Int].map(_.sum)
    Fetch.run(query).unsafeRunSync()
  }

  @Benchmark
  def fetchSumUniqueBenchmark(): Long = {
    import fetch.fetchM
    import FetchImpl._
    type FIO[A] = Fetch[IO, A]

    val reqs  = (0 until count).toList.map(i => fetchPlusOne(i))
    val query = reqs.sequence[FIO, Int].map(_.sum)
    Fetch.run[IO](query).unsafeRunSync()
  }

  object ZQueryImpl {
    case class Req(i: Int) extends Request[Nothing, Int]
    val ds = DataSource.fromFunctionBatchedZIO("PlusOne") { (reqs: Chunk[Req]) => ZIO.succeed(reqs.map(_.i + 1)) }
  }

  object FetchImpl {
    import cats.data.NonEmptyList
    import cats.effect._
    import fetch._

    object PlusOne extends Data[Int, Int] {
      def name = "PlusOne"

      val source: DataSource[IO, Int, Int] = new DataSource[IO, Int, Int] {
        override def data: Data[Int, Int] = PlusOne

        override def CF: Concurrent[IO] = Concurrent[IO]

        override def fetch(id: Int): IO[Option[Int]] =
          IO(Some(id + 1))

        override def batch(ids: NonEmptyList[Int]): IO[Map[Int, Int]] =
          IO(ids.toList.view.map(id => id -> (id + 1)).toMap)
      }
    }

    def fetchPlusOne(n: Int): Fetch[IO, Int] =
      Fetch(n, PlusOne.source)
  }

}
