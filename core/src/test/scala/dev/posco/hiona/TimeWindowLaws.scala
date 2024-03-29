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

import cats.{Eq, Monoid, Semigroup}
import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}

import cats.implicits._

class TimeWindowLaws extends munit.ScalaCheckSuite {
  def commutativeSemigroupLaws[A](
      gen: Gen[A]
  )(implicit sg: Semigroup[A], eqv: Eq[A]): Prop = {
    val onThree = forAll(gen, gen, gen) { (a, b, c) =>
      val prop1 = Prop(
        eqv
          .eqv(sg.combine(a, sg.combine(b, c)), sg.combine(sg.combine(a, b), c))
      ).label("associates")
      val prop2 =
        Prop(eqv.eqv(sg.combine(a, b), sg.combine(b, a))).label("commutes")

      prop1 && prop2
    }

    val combine = forAll(Gen.listOf(gen)) { items =>
      Eq[Option[A]]
        .eqv(items.reduceOption(sg.combine), sg.combineAllOption(items))
    }.label("combine law")

    onThree && combine
  }

  def genPair[W <: Duration: ValueOf, A](
      genA: Gen[A]
  ): Gen[Gen[(Timestamp, A)]] =
    // make a narrow timestamp window because we want to have windows that are nonempty
    Gen
      .choose(0L, (valueOf[W] * 4).millis)
      .flatMap { width =>
        val genTs =
          Gen.choose(Long.MinValue, Long.MaxValue - width).map(Timestamp(_))
        Gen.zip(genTs, genA)
      }

  def genOutOfWindow[W <: Duration: ValueOf](
      ts: Timestamp
  ): Gen[Option[Timestamp]] = {
    val min = valueOf[W].millis
    Gen
      .oneOf(
        Gen.choose(ts.epochMillis + min + 1L, Long.MaxValue),
        Gen.choose(Long.MinValue, ts.epochMillis - min - 1L)
      )
      .map { ts1 =>
        val res0 = Timestamp(ts1)
        if (Ordering[Timestamp].lt(ts, res0 - valueOf[W])) Some(res0)
        else if (Ordering[Timestamp].lt(res0, ts - valueOf[W])) Some(res0)
        else None
      }
  }

  def genList[W <: Duration: ValueOf, A](
      genA: Gen[A]
  ): Gen[List[(Timestamp, A)]] =
    genPair[W, A](genA)
      .flatMap(Gen.listOf(_))

  def genTimeWindow[W <: Duration: ValueOf, A: Monoid](
      genA: Gen[A]
  ): Gen[TimeWindow[W, A]] =
    genPair[W, A](genA)
      .flatMap { gen =>
        Gen.zip(gen, Gen.listOf(gen)).map {
          case (h, tail) =>
            TimeWindow.fromList[W, A](h :: tail).get
        }
      }

  property("test semigroupLaws: hour") {
    val gen: Gen[TimeWindow[Duration.hour.type, Int]] =
      genTimeWindow[Duration.hour.type, Int](Arbitrary.arbitrary[Int])

    commutativeSemigroupLaws(gen)
  }

  property("test semigroupLaws: minute") {
    val gen: Gen[TimeWindow[Duration.minute.type, Int]] =
      genTimeWindow[Duration.minute.type, Int](Arbitrary.arbitrary[Int])

    commutativeSemigroupLaws(gen)
  }

  property("test semigroupLaws: year") {
    val gen: Gen[TimeWindow[Duration.year.type, Int]] =
      genTimeWindow[Duration.year.type, Int](Arbitrary.arbitrary[Int])

    commutativeSemigroupLaws(gen)
  }

  property("combining singles is the same as making a list: year") {
    type D = Duration.year.type

    forAll(genList[D, Int](Arbitrary.arbitrary[Int])) { items =>
      val singles: List[TimeWindow[D, Int]] = items.map {
        case (a, b) => TimeWindow.single(a, b)
      }
      val res1 = TimeWindow.fromList[D, Int](items)
      val res2 = Semigroup[TimeWindow[D, Int]].combineAllOption(singles)
      assert(Eq[Option[TimeWindow[D, Int]]].eqv(res1, res2))

      val size = res1.fold(0L)(_.size)
      assert(size <= items.size.toLong)
    }
  }

  property("adding a new maximum always sets the maximum: year") {
    type D = Duration.year.type

    case class Args(window: TimeWindow[D, Int], ts: Timestamp, v: Int)
    val genArgs =
      for {
        window <- genTimeWindow[D, Int](Arbitrary.arbitrary[Int])
        ts <- TimestampGen.genTimestampFull
        v <- Gen.choose(-100, 100)
      } yield Args(window, ts, v)

    forAll(genArgs) {
      case Args(w, ts, v) =>
        val w1 = w.combine(TimeWindow.single(ts, v))
        if (Ordering[Timestamp].gteq(ts, w.maximum)) {
          assert(w1.maximum == ts)
          assert(w1 != w)
        } else if (w1 == w)
          // then we must be out of range:
          assert(Ordering[Timestamp].lt(ts + Duration.year, w.maximum))
        else assert(true)
    }
  }

  property("window width law: if t1 < t2, then t1 + duration > t2") {
    type D = Duration.year.type

    case class Pair(t1: Timestamp, t2: Timestamp)
    val genPair: Gen[Pair] =
      genTimeWindow[D, Int](Arbitrary.arbitrary[Int])
        .flatMap { window =>
          val lst = window.timestamps.toList
          Gen
            .zip(Gen.oneOf(lst), Gen.oneOf(lst))
            .map { case (t1, t2) => Pair(t1, t2) }
        }

    forAll(genPair) {
      case Pair(t1, t2) =>
        val ord = Ordering[Timestamp]
        import ord.lt

        if (lt(t1, t2))
          assert(lt(t2, t1 + Duration.year))
        else if (lt(t2, t1))
          assert(lt(t1, t2 + Duration.year))
        else
          assertEquals(t1, t2)
    }
  }

  property(
    "adding a timestamp < the maximum - window does not increase the size: year"
  ) {
    type D = Duration.year.type

    case class Args(
        window: TimeWindow[D, Int],
        outOfWindow: Option[Timestamp],
        v: Int
    )
    val genArgs =
      for {
        window <- genTimeWindow[D, Int](Arbitrary.arbitrary[Int])
        out <- genOutOfWindow[D](window.minimum)
        v <- Gen.choose(-100, 100)
      } yield Args(window, out, v)

    forAll(genArgs) {
      case Args(_, None, _) => assert(true)
      case Args(w, Some(t), v) =>
        val w1 = w.combine(TimeWindow.single(t, v))
        assert(w1.size <= w.size)
    }
  }

  test("some example cases") {
    type D = Duration.second.type

    val ts0 = Timestamp(0L)

    val t0 = TimeWindow.single[D, Int](ts0, 1)
    val t1 = TimeWindow.single[D, Int](ts0 + Duration.second, 1)
    val t2 = TimeWindow.single[D, Int](ts0 + (Duration.second * 2), 1)

    assertEquals(t2.combine(t0), t2)

    assertEquals(t0.combined, 1)
    assertEquals(t1.combine(t0).combined, 2)
  }
}
