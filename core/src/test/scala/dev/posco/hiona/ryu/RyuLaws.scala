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
