/*
 * Copyright 2022 devposco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.posco.hiona.bench

import cats.effect.IO
import dev.posco.hiona._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class EngineBenchmars {

  implicit val ec = scala.concurrent.ExecutionContext.global

  implicit val runtime = cats.effect.unsafe.IORuntime.global

  @Param(Array("1000", "10000"))
  var size: Int = 0

  var strs: Vector[(String, Int)] = Vector.empty

  val strsSrc: Event.Source[(String, Int)] =
    Event.csvSource(
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
    // val ev = strsSrc.map(_._1.toInt).filter(_ % 2 == 0)

    val inputs = InputFactory
      .fromStream(strsSrc, fs2.Stream(strs: _*).covary[IO])
      .through(pipe)

    result(ev, inputs).unsafeRunSync()
  }
}
