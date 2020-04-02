package dev.posco.hiona

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

object DurationGen {
  val genFinite: Gen[Duration.Finite] =
    Gen.choose(0, Long.MaxValue - 1L).map(Duration.Finite(_))

  val genDuration: Gen[Duration] =
    Gen.frequency((10, genFinite), (1, Gen.const(Duration.Infinite)))
}

class DurationLaws extends munit.ScalaCheckSuite {
  import DurationGen._

  property("Order[Duration.Finite] matches Long order") {
    forAll(genFinite, genFinite) { (f1, f2) =>
      assertEquals(
        java.lang.Long.compare(f1.millis, f2.millis),
        Duration.durationOrdering.compare(f1, f2)
      )
    }
  }

  property("Duration + is a commutative monoid") {
    forAll(genDuration, genDuration, genDuration) { (a, b, c) =>
      (((a + b) + c) == (a + (b + c))) &&
      ((a + b) == (b + a)) &&
      ((a + Duration.Infinite) == Duration.Infinite) &&
      ((a + Duration.zero) == a)
    }
  }

  property("Duration + is homomorphic to Long +") {

    forAll(genFinite, genFinite) { (a, b) =>
      (a + b) match {
        case Duration.Infinite => true
        case Duration.Finite(c) =>
          (a.millis + b.millis) == c
      }
    }
  }

  property("isInfinite is right") {
    forAll(genDuration) {
      case i @ Duration.Infinite  => i.isInfinite
      case f @ Duration.Finite(_) => !f.isInfinite
    }
  }

  property("d * 2 == d + d") {
    forAll(genDuration)(d => assertEquals(d * 2, d + d))
  }

  property("(d * n)/n == d, or d * n is infinite") {
    forAll(genDuration, Gen.choose(1, Int.MaxValue)) { (d, n1) =>
      val n = n1 max 1
      val d1 = d * n
      val d2 = d1 / n
      if (d1.isInfinite) assert(d2.isInfinite)
      else assertEquals(d2, d)
    }
  }

  test("base units match expectations") {
    assertEquals(Duration.year.millis, (86400L * 365L + (86400L / 4L)) * 1000L)
    assertEquals(
      Duration.quarter.millis,
      (86400L * 365L + (86400L / 4L)) * 1000L / 4L
    )
    assertEquals(
      Duration.month.millis,
      (86400L * 365L + (86400L / 4L)) * 1000L / 12L
    )

    assertEquals(Duration.month * 12, Duration.year)
    assertEquals(Duration.month, Duration.year / 12)
    assertEquals(Duration.quarter * 4, Duration.year)
    assertEquals(Duration.quarter, Duration.year / 4)
    assertEquals(Duration.month * 3, Duration.quarter)
    assertEquals(Duration.month, Duration.quarter / 3)

    assertEquals(Duration.second, Duration(1000L))
    assertEquals(Duration.minute, Duration.second * 60)
    assertEquals(Duration.hour, Duration.minute * 60)
    assertEquals(Duration.day, Duration.hour * 24)
    assertEquals(Duration.week, Duration.day * 7)
  }
}
