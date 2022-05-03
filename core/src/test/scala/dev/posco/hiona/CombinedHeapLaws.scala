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

import cats.{Monoid, Order}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import cats.implicits._

class CombinedHeapLaws extends munit.ScalaCheckSuite {
  def genHeap[A: Order, B: Monoid](
      items: Gen[(A, B)],
      size: Int
  ): Gen[CombinedHeap[A, B]] = {
    val recur: Gen[CombinedHeap[A, B]] = Gen.lzy(genHeap(items, size))

    val genFrom: Gen[CombinedHeap[A, B]] =
      Gen.listOfN(size, items).map(CombinedHeap.fromIterable(_))

    Gen.frequency(
      (7, genFrom),
      (1, Gen.const(CombinedHeap.empty[A, B])),
      (1, Gen.zip(recur, recur).map { case (c1, c2) => c1.combine(c2) }),
      (1, recur.map(_.remove)),
      (1, Gen.zip(recur, items).map { case (h, (a, b)) => h.add(a, b) })
    )
  }

  val genInts: Gen[CombinedHeap[Int, Int]] = Gen
    .choose(0, 10)
    .flatMap(
      genHeap(Gen.zip(Gen.choose(-1000, 1000), Gen.choose(-100, 100)), _)
    )

  val em = CombinedHeap.empty[Int, Int]

  property("empty combine x == x") {
    forAll(genInts) { h =>
      assertEquals(h.combine(em), h)
      assertEquals(em.combine(h), h)
    }
  }

  property("a combine b == b combine a") {
    forAll(genInts, genInts) { (a, b) =>
      val left = a.combine(b)
      val right = b.combine(a)

      assertEquals(left.keys.toSet, right.keys.toSet)
      assertEquals(left.unorderedFold, right.unorderedFold)
      assertEquals(left.size, right.size)
    }
  }

  property("a combine (b combine c) == (a combine b) combine c") {
    forAll(genInts, genInts, genInts) { (a, b, c) =>
      val left = a.combine(b.combine(c))
      val right = (a.combine(b)).combine(c)

      assertEquals(left.keys.toSet, right.keys.toSet)
      assertEquals(left.unorderedFold, right.unorderedFold)
      assertEquals(left.size, right.size)
    }
  }

  property("removing keys empties a CombinedHeap") {
    forAll(genInts) { h =>
      val expectedEmpty = h.keys.foldLeft(h)((h, _) => h.remove)

      assertEquals(h.size, h.keys.size.toLong)
      assertEquals(expectedEmpty, em)
    }
  }
}
