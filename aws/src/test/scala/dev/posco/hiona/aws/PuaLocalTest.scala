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

package dev.posco.hiona.aws

import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.IORuntime
import io.circe.{Decoder, Encoder, Json}
import org.scalacheck.{Gen, Prop}

import cats.implicits._

class PuaLocalTest extends munit.ScalaCheckSuite {

  implicit val runtime = IORuntime.global

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        20
      ) // a bit slow, but locally, this passes with more

  def check[A: Encoder, B: Decoder](
      pua: Pua,
      dir: PuaLocal.Directory,
      as: List[A]
  )(fn: A => IO[B]): IO[Prop] =
    for {
      puaRunner <- PuaLocal.build(dir)
      fn2 = puaRunner[A, B](pua)
      _ <- as.traverse_ { a =>
        for {
          b <- fn(a)
          b2 <- fn2(a)
        } yield assertEquals(b2, b)
      }
    } yield Prop(true)

  property("we can run a composed job") {

    val pua: Pua = Pua.call("first").andThen(Pua.call("second"))

    val dir: PuaLocal.Directory =
      PuaLocal.emptyDirectory
        .addFn("first", { i: Int => IO.pure(i + 42) })
        .addFn("second", { i: Int => IO.pure(2 * i) })

    val fn: Int => IO[Int] = { i: Int => IO.pure(2 * (i + 42)) }

    Prop.forAll { ints: List[Int] => check(pua, dir, ints)(fn).unsafeRunSync() }
  }

  property("test a wordcount map/reduce example") {
    // takes a Map that tells what number this is
    // emits null
    val mapper: Pua = Pua.call("mapper")
    // takes an input telling which reducer it is
    val reducer: Pua = Pua.call("reducer")

    val mapperCount: Int = 10
    val reducerCount: Int = 5

    val mapReduce =
      Pua
        .const((0 until mapperCount).map(i => Map("mapper" -> i)))
        .andThen(mapper.parCount(mapperCount))
        .andThen(Pua.const((0 until reducerCount).map { i =>
          Map("reducer" -> i)
        }))
        .andThen(reducer.parCount(reducerCount))
        .andThen(Pua.const(()))

    val shuffleStorage: IO[Ref[IO, Map[(Int, Int), List[(String, Long)]]]] =
      Ref.of[IO, Map[(Int, Int), List[(String, Long)]]](Map.empty)

    val partitionOuts: IO[Ref[IO, Map[String, Long]]] =
      Ref.of[IO, Map[String, Long]](Map.empty)

    val genWord: Gen[String] =
      Gen
        .listOf(
          Gen.frequency(1 -> Gen.oneOf('A' to 'Z'), 10 -> Gen.oneOf('a' to 'z'))
        )
        .map(_.mkString)

    val lines: Gen[String] =
      for {
        words <- Gen.listOf(genWord)
        space <- Gen.frequency(10 -> Gen.const(" "), 1 -> Gen.const("\n"))
      } yield words.mkString(space)

    Prop.forAllNoShrink(Gen.listOfN(mapperCount, Gen.listOf(lines))) {
      inputs: List[List[String]] =>
        def words(s: String): List[String] =
          s.split("\\s+").toList.filter(_.nonEmpty)

        val localResult =
          inputs.iterator
            .flatMap(_.iterator)
            .flatMap(words)
            .toList
            .groupBy(identity)
            .map { case (k, vs) => (k, vs.size.toLong) }

        val io = (shuffleStorage, partitionOuts).mapN { (shuf, out) =>
          val dir: PuaLocal.Directory =
            PuaLocal.emptyDirectory
              .addFn(
                "mapper",
                { m: Map[String, Int] =>
                  IO.defer {
                    val task = m("mapper")
                    val lines = inputs(task)
                    def toRed(i: Any) = {
                      val r1 = i.hashCode % reducerCount
                      if (r1 < 0) r1 + reducerCount else r1
                    }
                    val myResult = lines
                      .flatMap(words)
                      .groupBy(word => (task, toRed(word)))
                      .view
                      .mapValues { words =>
                        words
                          .groupBy(identity)
                          .view
                          .mapValues(_.size.toLong)
                          .toList
                      }
                      .toList

                    shuf.update(_ ++ myResult)
                  }
                }
              )
              .addFn(
                "reducer",
                { m: Map[String, Int] =>
                  IO.defer {
                    val task = m("reducer")
                    val allLocations: List[(Int, Int)] =
                      (0 until mapperCount).map((_, task)).toList

                    val allInputs: IO[List[List[(String, Long)]]] =
                      allLocations.traverse { key =>
                        shuf.get.map(_.getOrElse(key, Nil))
                      }

                    for {
                      ll <- allInputs
                      summed =
                        ll.flatten
                          .groupBy(_._1)
                          .view
                          .mapValues(_.map(_._2).sum)
                          .toList
                      _ <- out.update(_ ++ summed)
                    } yield ()
                  }
                }
              )

          for {
            puaRunner <- PuaLocal.build(dir)
            fn2 = puaRunner[Unit, Unit](mapReduce)
            _ <- fn2(())
            results <- out.get
          } yield assertEquals(results, localResult)

        }

        io.flatten.unsafeRunSync()
    }
  }

  property("pualocal matches what we expect") {
    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir, inputs, fn) =>
        inputs.foldLeft(Prop(true)) { (p, input) =>
          val expected = fn(input)
          val computation =
            PuaLocal.build(dir).flatMap { pl =>
              val fn = pl[Json, Json](pua)
              fn(input)
            }

          p || (computation.attempt, expected.attempt)
            .mapN {
              case (Right(a), Right(b)) =>
                assertEquals(a, b)
              case (Left(_), Left(_)) =>
                assert(true)
              case (left, right) =>
                // this will fail, but we want to see what we get
                assertEquals(left, right)
            }
            .unsafeRunSync()
        }
    }
  }

  property("pualocal unoptimized matches what we expect") {
    Prop.forAllNoShrink(PuaGens.genDirectory) {
      case (pua, dir, inputs, fn) =>
        inputs.foldLeft(Prop(true)) { (p, input) =>
          val expected = fn(input)
          val computation =
            PuaLocal.buildUnoptimized(dir).flatMap { pl =>
              val fn = pl[Json, Json](pua)
              fn(input)
            }

          p || (computation.attempt, expected.attempt)
            .mapN {
              case (Right(a), Right(b)) =>
                assertEquals(a, b)
              case (Left(_), Left(_)) =>
                assert(true)
              case (left, right) =>
                // this will fail, but we want to see what we get
                assertEquals(left, right)
            }
            .unsafeRunSync()
        }
    }
  }

}
