package dev.posco.hiona

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class RyuLaws extends munit.ScalaCheckSuite {

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(
        100000
      ) // a bit slow, but locally, this passes with 10000

  val genDouble =
    Gen.frequency(
      1 -> Double.NaN,
      1 -> Double.PositiveInfinity,
      1 -> Double.NegativeInfinity,
      20 -> Gen.choose(Double.MinValue, Double.MaxValue)
    )

  property("reading a ryu double string is the same as normal toString") {
    forAll(genDouble) { d =>
      val rs = ryu.RyuDouble.doubleToString(d)

      val d1 = rs.toDouble
      if (d.isNaN) assert(d1.isNaN)
      else assertEquals(d1, d)
    }
  }

  val genFloat =
    Gen.frequency(
      1 -> Float.NaN,
      1 -> Float.PositiveInfinity,
      1 -> Float.NegativeInfinity,
      20 -> Gen.choose(Float.MinValue, Float.MaxValue)
    )

  property("reading a ryu float string is the same as normal toString") {
    forAll(genFloat) { d =>
      val rs = ryu.RyuFloat.floatToString(d)

      val d1 = rs.toFloat
      if (d.isNaN) assert(d1.isNaN)
      else assertEquals(d1, d)
    }
  }
}
