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

import cats.Eq
import org.scalacheck.{Arbitrary, Gen, Prop}
import shapeless._

import Prop.forAll

object Moment2Laws {

  /**
    * This should have the law that
    * s1 * (s2 * v) == (s1 * s2) * v
    *
    * and
    *
    * s * (v1 + v2) == s * v1 + s * v2
    * (s + r) * v = s * v + r * v
    *
    * 0 * v = empty
    * 1 * v = v
    */
  def laws[A: Arbitrary: Eq: DoubleModule]: Prop = {
    val mod = implicitly[DoubleModule[A]]

    implicit val doubleArb: Arbitrary[Double] =
      Arbitrary(Gen.choose(-1000.0, 1000.0))

    val law1 = forAll { (s1: Double, s2: Double, a: A) =>
      Prop(Eq[A].eqv(mod.scale(s1 * s2, a), mod.scale(s1, mod.scale(s2, a))))
    }.label("(s1 * s2) * v == s1 * (s2 * v)")

    val law2 = forAll { (s: Double, a1: A, a2: A) =>
      val left = mod.scale(s, mod.monoid.combine(a1, a2))
      val right = mod.monoid.combine(mod.scale(s, a1), mod.scale(s, a2))
      Prop(Eq[A].eqv(left, right))
    }.label("s * (v1 + v2) == s * v1 + s * v2")

    val law3 = forAll { (s: Double, r: Double, a: A) =>
      val left = mod.scale(s + r, a)
      val right = mod.monoid.combine(mod.scale(s, a), mod.scale(r, a))

      Prop(Eq[A].eqv(left, right))
    }.label("(s + r) * v == s * v + r * v")

    val law4 = forAll { v: A =>
      Prop(Eq[A].eqv(mod.scale(0.0, v), mod.monoid.empty))
    }.label("0 * v = empty")

    val law5 = forAll { v: A => Prop(Eq[A].eqv(mod.scale(1.0, v), v)) }
      .label("1 * v = v")

    law1 && law2 && law3 && law4 && law5
  }

  def lawsEq[A: Arbitrary: DoubleModule](eqv: Eq[A]): Prop =
    laws[A](implicitly, eqv, implicitly)
}

class Moment2Laws extends munit.ScalaCheckSuite {
  import Moment2Laws._
  import ShapelessEq._
  import ShapelessGen._

  implicit val arbDouble: Arbitrary[Double] =
    Arbitrary(Gen.choose(-1e5, 1e5))

  implicit val arbFloat: Arbitrary[Float] =
    Arbitrary(Gen.choose(-1e5f, 1e5f))

  implicit val eqDouble: Eq[Double] =
    new Eq[Double] {
      def eqv(a: Double, b: Double) =
        (a == b) || {
          math.abs(a - b) / math.max(a, b) < 0.001
        }
    }

  implicit val eqFloat: Eq[Float] =
    new Eq[Float] {
      def eqv(a: Float, b: Float) =
        (a == b) || {
          math.abs(a - b) / math.max(a, b) < 0.01
        }
    }

  property("DoubleModule[Double]")(laws[Double])
  property("DoubleModule[Float]")(laws[Float])
  property("DoubleModule[Float :: Double :: HNil]")(
    laws[Float :: Double :: HNil]
  )

  property("DoubleModule[Moment2]")(lawsEq[Moments2](genericEq))
}
