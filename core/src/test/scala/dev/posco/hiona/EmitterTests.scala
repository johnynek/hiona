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

package dev.posco.hiona

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import fs2.Stream
import org.scalacheck.Prop

import cats.implicits._

class EmitterTests extends munit.ScalaCheckSuite {

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        100
      ) // a bit slow, but locally, this passes with 10000

  implicit val runtime: IORuntime = IORuntime.global

  def result[E[_]: Emittable, A](
      in: InputFactory[IO],
      ev: E[A]
  ): IO[List[A]] =
    Engine.run(in, ev).map(_._2).compile.toList

  test("basic map/event processing") {
    val src = Event.csvSource[(Long, Int)](
      "numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      }
    )

    val data = List((0L, 42), (1L, 43), (2L, 44))
    val ins = InputFactory.fromStream[IO, (Long, Int)](src, Stream(data: _*))

    val res = src.map { case (_, i) => i.toString }

    result(ins, res)
      .flatMap { lst =>
        IO {
          assertEquals(lst, List("42", "43", "44"))
        }
      }
      .unsafeToFuture()
  }

  test("preLookup test") {
    val src = Event.csvSource[(Long, Int)](
      "numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      }
    )
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))
    val ins = InputFactory.fromStream[IO, (Long, Int)](src, Stream(data: _*))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.preLookup(feat)

    result(ins, res)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List((0, ((), 0)), (1, ((), 0)), (0, ((), 42)), (1, ((), 43)))
          )
        }
      }
      .unsafeToFuture()
  }

  test("postLookup test") {
    val src = Event.csvSource[(Long, Int)](
      "numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      }
    )
    val data = List((0L, 42), (1L, 43), (2L, 44), (3L, 45))
    val ins = InputFactory.fromStream[IO, (Long, Int)](src, Stream(data: _*))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = keys.postLookup(feat)

    result(ins, res)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List((0, ((), 42)), (1, ((), 43)), (0, ((), 86)), (1, ((), 88)))
          )
        }
      }
      .unsafeToFuture()
  }

  test("simple LabeledEvent test") {
    val src = Event.csvSource[(Long, Int)](
      "numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      }
    )
    val data = List((0L, 1), (1L, 2), (2L, 3), (3L, 4), (4L, 5), (5L, 6))
    val ins = InputFactory.fromStream[IO, (Long, Int)](src, Stream(data: _*))

    val label =
      Label(src.map { case (_, i) => (i % 2, i) }.sum).lookForward(Duration(3L))
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = LabeledEvent(keys, label)

    result(ins, res)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List(
              (1, ((), 4)),
              (0, ((), 6)),
              (1, ((), 9)),
              (0, ((), 12)),
              (1, ((), 9)),
              (0, ((), 12))
            )
          )
        }
      }
      .unsafeToFuture()
  }

  test("simple zipped LabeledEvent test") {
    val src = Event.csvSource[(Long, Int)](
      "numsrc",
      new Validator[(Long, Int)] {
        def validate(v: (Long, Int)) = Right(Timestamp(v._1))
      }
    )
    val data = List((0L, 1), (1L, 2), (2L, 3), (3L, 4), (4L, 5), (5L, 6))
    val ins = InputFactory.fromStream[IO, (Long, Int)](src, Stream(data: _*))

    val feat = src.map { case (_, i) => (i % 2, i) }.sum
    // exercise a lookahead where an read and write collide,
    // we should see the feature value at the end of the
    // timestamp
    val label0 = Label(feat).lookForward(Duration(2L))
    val label1 = Label(feat).lookForward(Duration(4L))
    val label = label0.zip(label1)
    val keys = src.map { case (_, i) => (i % 2, ()) }
    val res = LabeledEvent(keys, label)

    result(ins, res)
      .flatMap { lst =>
        IO {
          assertEquals(
            lst,
            List(
              (1, ((), (4, 9))),
              (0, ((), (6, 12))),
              (1, ((), (9, 9))),
              (0, ((), (12, 12))),
              (1, ((), (9, 9))),
              (0, ((), (12, 12)))
            )
          )
        }
      }
      .unsafeToFuture()
  }

  def toFactory(si: Simulator.Inputs): InputFactory[IO] =
    InputFactory.merge(si.map {
      case twinput =>
        val stream: Stream[IO, twinput.Type] =
          Stream.emits(twinput.evidence.inputs)
        InputFactory.fromStream(twinput.evidence.source, stream)
    })

  property("we match the simulator events (when timestamps are distinct)") {
    Prop.forAllNoShrink(GenEventFeature.genDefault(1000)) {
      case (inputs, twe) =>
        val event: Event[twe.Type] = twe.evidence

        val ifac: InputFactory[IO] = toFactory(inputs)
        val results: List[twe.Type] =
          Engine.run(ifac, event).map(_._2).compile.toList.unsafeRunSync()

        val simres: List[twe.Type] =
          Simulator.eventToLazyList(event, inputs).map(_._2).toList

        assertEquals(results, simres)
    }
  }
}
