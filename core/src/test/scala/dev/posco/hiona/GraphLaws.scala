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

import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Prop}

class GraphLaws extends munit.ScalaCheckSuite {
  test("Memo.fn only calls once") {
    var called = 0

    val fn = Graph.Memo.fn { i: Int =>
      called += 1
      i * 2
    }

    assertEquals(fn(21), 42)
    assertEquals(called, 1)
    assertEquals(fn(21), 42)
    assertEquals(called, 1)
    assertEquals(fn(22), 44)
    assertEquals(called, 2)
  }

  test("Memo.rec only calls once") {
    var called = 0

    val fn = Graph.Memo.rec[Int, Int] { (i, _) =>
      called += 1
      i * 2
    }

    assertEquals(fn(21), 42)
    assertEquals(called, 1)
    assertEquals(fn(21), 42)
    assertEquals(called, 1)
    assertEquals(fn(22), 44)
    assertEquals(called, 2)
  }

  test("Memo.rec makes fib not exponentially slow") {
    val fn = Graph.Memo.rec[BigInt, BigInt] {
      case (zero, _) if zero == BigInt(0) => BigInt(1)
      case (one, _) if one == BigInt(1)   => BigInt(1)
      case (x, rec)                       => rec(x - 1) + rec(x - 2)
    }

    lazy val fibs: LazyList[BigInt] =
      LazyList(BigInt(1), BigInt(1)) #::: fibs
        .drop(1)
        .zip(fibs)
        .map {
          case (a, b) => a + b
        }

    forAll(Gen.choose(0, 100))(i => assertEquals(fn(BigInt(i)), fibs(i)))
  }

  test("Graph.fanOutCount seems right") {

    forAll(Gen.choose(2, 100)) { maxSize =>
      if (maxSize > 0) {
        val simpleGraph: Int => List[Int] = {
          case x if x >= (maxSize - 1) => Nil
          case i                       => List((i + 1) % maxSize)
        }

        val fanout = Graph.fanOutCount(Set(0))(simpleGraph)

        forAll(Gen.choose(0, maxSize - 1)) {
          case 0    => assertEquals(fanout(0), 0)
          case node => assertEquals(fanout(node), 1)
        }
      } else Prop(true)
    }
  }
}
