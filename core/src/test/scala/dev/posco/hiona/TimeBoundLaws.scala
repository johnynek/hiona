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
