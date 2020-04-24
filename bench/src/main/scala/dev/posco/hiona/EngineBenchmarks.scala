package dev.posco.hiona.bench

import cats.effect.{IO, Resource}
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

  def result[A](ev: Event[A], feeder: IO[Feeder]): IO[Unit] = {
    val output = Resource.liftF(IO { it: Iterable[A] =>
      IO(it.foreach(a => assert(a != null)))
    })
    Engine.Emitter
      .fromEvent(ev)
      .flatMap(Engine.runEmitter(Resource.liftF(feeder), _, output))
  }

  @Setup
  def setup(): Unit =
    strs = (1 to size).map(sz => ((sz % 37).toString, sz)).toVector

  @Benchmark
  def mapChain(): Unit = {
    val ev = strsSrc.map(_._1).map(_.toInt).filter(_ % 2 == 0)
    //val ev = strsSrc.map(_._1.toInt).filter(_ % 2 == 0)

    val fromIters = List(Feeder.FromIterable(strsSrc, strs))
    val feeder = Feeder.FromIterable.feederForEvent(fromIters, ev)

    result(ev, feeder).unsafeRunSync()
  }
}
