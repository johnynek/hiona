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

import fs2.Stream
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import cats.implicits._

import TimestampGen.genTimestampRealistic

object TimeBoundGens {
  val genTimeBound: Gen[TimeBound] =
    Gen.oneOf(
      Gen.const(TimeBound.Everything),
      genTimestampRealistic.map(TimeBound.LowerBounded(_)),
      genTimestampRealistic.map(TimeBound.UpperBounded(_)),
      Gen.zip(genTimestampRealistic, genTimestampRealistic).map {
        case (l, b) => TimeBound.Bounded(l, b)
      }
    )
}

class TimeBoundLaws extends munit.ScalaCheckSuite {

  val streamGen: Gen[List[(Timestamp, Long)]] =
    Gen
      .listOf(
        Gen.zip(genTimestampRealistic, Gen.choose(Long.MinValue, Long.MaxValue))
      )
      .map(list => list.sorted)

  property("filterInput matches expectations") {
    forAll(TimeBoundGens.genTimeBound, streamGen) { (tb, list0) =>
      val strm = Stream.emits(list0)

      val fromTb: List[(Timestamp, Long)] =
        tb.filterInput[fs2.Pure, (Timestamp, Long)](_._1)(strm).compile.toList

      val manually =
        tb.exclusiveUpperBound match {
          case None     => list0
          case Some(ub) => list0.filter { case (ts, _) => ts < ub }
        }

      assertEquals(fromTb, manually)
    }
  }

  property("filterOutput matches expectations") {
    forAll(TimeBoundGens.genTimeBound, streamGen) { (tb, list0) =>
      val strm = Stream.emits(list0)

      val fromTb: List[(Timestamp, Long)] =
        tb.filterOutput[fs2.Pure, (Timestamp, Long)](_._1)(strm).compile.toList

      val manually =
        tb.inclusiveLowerBound match {
          case None     => list0
          case Some(lb) => list0.filter { case (ts, _) => lb <= ts }
        }

      assertEquals(fromTb, manually)
    }
  }
}
