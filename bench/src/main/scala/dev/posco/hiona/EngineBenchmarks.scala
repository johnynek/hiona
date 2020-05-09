package dev.posco.hiona.bench

import cats.effect.IO
import dev.posco.hiona._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class EngineBenchmars {

  implicit val ec = scala.concurrent.ExecutionContext.global

  implicit val ctx = IO.contextShift(ec)

  @Param(Array("1000", "10000"))
  var size: Int = 0

  var strs: Vector[(String, Int)] = Vector.empty

  val strsSrc: Event.Source[(String, Int)] =
    Event.source(
      "strs",
      Validator.pure { case (_, ts) => Timestamp(ts.toLong) }
    )

  def result[A](ev: Event[A], inputs: InputFactory[IO]): IO[Unit] =
    Engine.run(inputs, ev).compile.drain

  @Setup
  def setup(): Unit =
    strs = (1 to size).map(sz => ((sz % 37).toString, sz)).toVector

  def pipe[A]: fs2.Pipe[IO, A, A] = { strm =>
    strm
      .chunkMin(1024)
      .flatMap(fs2.Stream.chunk(_))
  }

  @Benchmark
  def mapChain(): Unit = {
    val ev = strsSrc.map(_._1).map(_.toInt).filter(_ % 2 == 0)
    //val ev = strsSrc.map(_._1.toInt).filter(_ % 2 == 0)

    val inputs = InputFactory
      .fromStream(strsSrc, fs2.Stream(strs: _*).covary[IO])
      .through(pipe)

    result(ev, inputs).unsafeRunSync()
  }
}
