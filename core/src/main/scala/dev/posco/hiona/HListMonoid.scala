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

import cats.{Eq, Monoid}
import shapeless._

object ShapelessMonoid {
  implicit val hnilMonoid: Monoid[HNil] =
    new Monoid[HNil] {
      def empty = HNil
      def combine(a: HNil, b: HNil) = HNil
    }

  implicit def hconsMonoid[A, B <: HList](implicit
      ma: Monoid[A],
      mb: => Monoid[B]
  ): Monoid[A :: B] =
    new Monoid[A :: B] {
      lazy val empty = ma.empty :: mb.empty
      def combine(l: A :: B, r: A :: B) = {
        val (la :: lb) = l
        val (ra :: rb) = r
        ma.combine(la, ra) :: mb.combine(lb, rb)
      }
    }

  // This isn't safe as a default (implicit) because wrapper types would be removed
  // and could change semantics
  def genericMonoid[A, B](implicit
      gen: Generic.Aux[A, B],
      mon: => Monoid[B]
  ): Monoid[A] =
    new Monoid[A] {
      lazy val empty: A = gen.from(mon.empty)
      def combine(l: A, r: A) = gen.from(mon.combine(gen.to(l), gen.to(r)))
    }
}

object ShapelessEq {
  implicit val hnilEq: Eq[HNil] =
    new Eq[HNil] {
      def eqv(l: HNil, r: HNil) = true
    }

  implicit def hconsEq[A, B <: HList](implicit
      ma: Eq[A],
      mb: => Eq[B]
  ): Eq[A :: B] =
    new Eq[A :: B] {
      def eqv(l: A :: B, r: A :: B) = {
        val (la :: lb) = l
        val (ra :: rb) = r
        ma.eqv(la, ra) && mb.eqv(lb, rb)
      }
    }

  // This isn't safe as a default (implicit) because wrapper types would be removed
  // and could change semantics
  def genericEq[A, B](implicit gen: Generic.Aux[A, B], eqb: => Eq[B]): Eq[A] =
    new Eq[A] {
      def eqv(l: A, r: A) = eqb.eqv(gen.to(l), gen.to(r))
    }
}
