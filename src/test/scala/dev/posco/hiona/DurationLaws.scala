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
      assertEquals(java.lang.Long.compare(f1.millis, f2.millis), Duration.durationOrdering.compare(f1, f2))
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
      case i@Duration.Infinite => i.isInfinite
      case f@Duration.Finite(_) => !f.isInfinite
    }
  }
}
