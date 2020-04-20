package dev.posco.hiona

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

object TimestampGen {
  val genTimestampFull: Gen[Timestamp] =
    Gen.choose(Long.MinValue, Long.MaxValue).map(Timestamp(_))

  // these are times from 1900 to 2100
  val genTimestampRealistic: Gen[Timestamp] =
    Gen.choose(-2208988800000L, 4102444800000L).map(Timestamp(_))

}

class TimestampLaws extends munit.ScalaCheckSuite {
  import TimestampGen._
  import DurationGen._

  property("Order[Timestamp] matches Long order") {
    forAll(genTimestampFull, genTimestampFull) { (t1, t2) =>
      assertEquals(
        java.lang.Long.compare(t1.epochMillis, t2.epochMillis),
        implicitly[Ordering[Timestamp]].compare(t1, t2)
      )
    }
  }

  property("offsetOrdering matches compareDiff") {
    forAll(genTimestampFull, genDuration, genTimestampFull, genDuration) {
      (t1, d1, t2, d2) =>
        assertEquals(
          Timestamp.compareDiff(t1, d1, t2, d2),
          Timestamp.offsetOrdering.compare((t1, d1), (t2, d2))
        )
    }
  }

  property("offsetOrdering is anti-symmetric") {
    val genPair = Gen.zip(genTimestampFull, genDuration)

    forAll(genPair, genPair) { (p1, p2) =>
      val res1 = Timestamp.offsetOrdering.compare(p1, p2)
      val res2 = Timestamp.offsetOrdering.compare(p2, p1)

      if (res1 == 0) (res2 == 0)
      else if (res1 > 0) (res2 < 0)
      else (res2 > 0)
    }
  }

  property("offsetOrdering obeys triangle inequality") {
    val genPair = Gen.zip(genTimestampFull, genDuration)

    forAll(genPair, genPair, genPair) { (p1, p2, p3) =>
      val ord = Timestamp.offsetOrdering

      if (ord.lteq(p1, p2) && ord.lteq(p2, p3)) ord.lteq(p1, p3)
      else true
    }
  }

  property("t1 - Infinity < t2 - finite") {
    forAll(genTimestampFull, genTimestampFull, genFinite) { (t1, t2, f) =>
      Timestamp.offsetOrdering.lt((t1, Duration.Infinite), (t2, f))
    }
  }

  property("t1 - f <> t2 - f == t1 <> t2") {
    forAll(genTimestampFull, genTimestampFull, genDuration) { (t1, t2, f) =>
      Timestamp.offsetOrdering.compare((t1, f), (t2, f)) ==
        Timestamp.orderingForTimestamp.compare(t1, t2)
    }
  }
  property("t - f1 <> t - f2 == f2 <> f1") {
    forAll(genTimestampFull, genDuration, genDuration) { (t, f1, f2) =>
      assertEquals(
        Timestamp.offsetOrdering.compare((t, f1), (t, f2)),
        Duration.durationOrdering.compare(f2, f1)
      )
    }
  }

  property("t1 - f2 == t2 - f2 we get with subtraction on BigInt") {
    forAll(genTimestampFull, genFinite, genTimestampFull, genFinite) {
      (t1, f1, t2, f2) =>
        val t1sub = BigInt(t1.epochMillis) - BigInt(f1.millis)
        val t2sub = BigInt(t2.epochMillis) - BigInt(f2.millis)

        assertEquals(
          Ordering[BigInt].compare(t1sub, t2sub),
          Timestamp.compareDiff(t1, f1, t2, f2)
        )
    }
  }

  property("(t + d1) + d2 == t + (d1 + d2)") {
    forAll(genTimestampFull, genDuration, genDuration) { (t, d1, d2) =>
      val d3 = d1 + d2
      if (!d3.isInfinite) {
        assertEquals((t + d1) + d2, t + d3)
      } else {
        val t2 = (t + d1) + d2
        assert(Ordering[Timestamp].lteq(t, t2))
      }
    }
  }
  property("(t + d) - d == t == (t - d) + d") {
    forAll(genTimestampFull, genDuration) { (t, d) =>
      val t1 = t + d
      if ((t1 - d) == t) {
        // this is good
        assert(true)
      } else {
        // t + d could have been clipped
        assertEquals(t + d, Timestamp.MaxValue)
      }

      val t2 = t - d
      if ((t2 + d) == t) {
        // this is good
        assert(true)
      } else {
        // t - d could have been clipped
        assertEquals(t - d, Timestamp.MinValue)
      }
    }
  }

  property("t / d = r, then d * r ~= t") {
    forAll(genTimestampFull, genDuration) { (t, d) =>
      val rD = t / d
      val diff = t % d

      if (d.isInfinite) {
        assertEquals(rD, 0L)
        assertEquals(diff, Duration.zero)
      } else {
        val tapprox =
          if (rD >= 0) (Timestamp.Zero + (d * rD))
          else (Timestamp.Zero - (d * (-rD)))

        val o = Ordering[Timestamp]
        if (tapprox != Timestamp.MaxValue && tapprox != Timestamp.MinValue) {
          val texact = tapprox + diff

          assert(o.lteq(tapprox, t))
          assert(o.gteq(tapprox + d, t))
          assertEquals(texact, t, s"rD = $rD, tapprox = $tapprox")
        }
      }
    }
  }
}
