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

import org.scalacheck.{Arbitrary, Gen}
import shapeless._

object ShapelessGen {
  implicit val hnilGen: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  implicit def hconsGen[A, B <: HList](implicit
      arbA: Arbitrary[A],
      arbB: => Arbitrary[B]
  ): Arbitrary[A :: B] =
    Arbitrary(for {
      a <- arbA.arbitrary
      b <- arbB.arbitrary
    } yield a :: b)

  implicit def ccons1Gen[A](implicit
      arbA: Arbitrary[A]
  ): Arbitrary[A :+: CNil] =
    Arbitrary(arbA.arbitrary.map(Inl(_)))

  implicit def cconsGen[A, B <: Coproduct](implicit
      arbA: Arbitrary[A],
      arbB: => Arbitrary[B]
  ): Arbitrary[A :+: B] =
    // TODO this is biased to the front for long coproducts
    Arbitrary(
      Gen
        .oneOf(true, false)
        .flatMap {
          case true  => arbA.arbitrary.map(Inl(_))
          case false => arbB.arbitrary.map(Inr(_))
        }
    )

  implicit def genericGen[A, B](implicit
      gen: Generic.Aux[A, B],
      genB: => Arbitrary[B]
  ): Arbitrary[A] =
    Arbitrary(Gen.lzy(genB.arbitrary).map(gen.from(_)))
}
