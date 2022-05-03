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

import cats.Defer
import org.scalacheck.{Gen, Prop}

import GenEventFeature.genMonad

class AmplificationLaws extends munit.ScalaCheckSuite {
  val genAmplification: Gen[Amplification] = {
    val consts = List(
      Amplification.Zero,
      Amplification.One,
      Amplification.ZeroOrMore,
      Amplification.ZeroOrOne
    )

    Defer[Gen]
      .fix[Amplification] { rec =>
        Gen.frequency(
          10 -> Gen.oneOf(consts),
          1 -> Gen.zip(rec, rec).map { case (a, b) => a + b },
          1 -> Gen.zip(rec, rec).map { case (a, b) => a * b }
        )
      }
  }

  val genWithBigInt: Gen[(Amplification, Int)] =
    genAmplification
      .flatMap {
        case a @ Amplification.Finite(l, h) =>
          Gen.choose(l.toInt, h.toInt).map((a, _))
        case a @ Amplification.Unbounded(l) =>
          Gen.choose(l.toInt, Int.MaxValue).map((a, _))
      }

  property("+ is commutative") {
    Prop.forAll(genAmplification, genAmplification) { (a, b) =>
      assertEquals(a + b, b + a)
    }
  }

  property("* is commutative") {
    Prop.forAll(genAmplification, genAmplification) { (a, b) =>
      assertEquals(a * b, b * a)
    }
  }

  property("+ is associative") {
    Prop.forAll(genAmplification, genAmplification, genAmplification) {
      (a, b, c) => assertEquals((a + b) + c, a + (b + c))
    }
  }

  property("* is associative") {
    Prop.forAll(genAmplification, genAmplification, genAmplification) {
      (a, b, c) => assertEquals((a * b) * c, a * (b * c))
    }
  }

  property("+/* are distributive") {
    Prop.forAll(genAmplification, genAmplification, genAmplification) {
      (a, b, c) => assertEquals((a + b) * c, (a * c) + (b * c))
    }
  }

  property("+ is sane") {
    Prop.forAll(genAmplification, genWithBigInt, Gen.choose(0, Int.MaxValue)) {
      case (a0, (a1, b), c) =>
        (a0 + a1)(b) == (a0(b) || a1(b)) &&
        (a0 + a1)(c) == (a0(c) || a1(c))
    }
  }

  property("0/1 are lawful") {
    // we wish a + Zero == a, but this isn't true:
    // if you merge ev1(1) with ev2(0), a single event could have 0 or 1 outputs
    // not just 1
    Prop.forAll(genAmplification) { a =>
      assert(a * Amplification.Zero == Amplification.Zero)
      assert(a * Amplification.One == a)
    }
  }

  property("asMostOne Laws") {
    assert(Amplification.Zero.atMostOne)
    assert(Amplification.One.atMostOne)
    assert(Amplification.ZeroOrOne.atMostOne)

    Prop.forAll(genAmplification) { a =>
      a.atMostOne == ((a == Amplification.Zero) ||
        (a == Amplification.One) ||
        (a == Amplification.ZeroOrOne))
    }
  }

  property("atMostOne won't contain 2 or more") {
    Prop.forAll(genAmplification, Gen.choose(2, Int.MaxValue)) { (a, i) =>
      if (a.atMostOne) !a(i) && (a(0) || a(1))
      else true
    }
  }

}
