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

import cats.Monoid

import cats.implicits._

/** Collection of graph algorithms. */
object Graph {

  object Memo {
    def fn[A, B](fn: A => B): A => B = {
      var memo: Map[A, B] = Map.empty

      { a =>
        memo.get(a) match {
          case Some(b) => b
          case None =>
            val b = fn(a)
            memo = memo.updated(a, b)
            b
        }
      }
    }

    def rec[A, B](fn: (A, A => B) => B): A => B = {
      var memo: Map[A, B] = Map.empty

      lazy val res: A => B = { a =>
        memo.get(a) match {
          case Some(b) => b
          case None =>
            val b = fn(a, res)
            memo = memo.updated(a, b)
            b
        }
      }

      res
    }
  }

  // this fails for non-dags because there is an infinite loop
  def fanOutCount[A](seed: Set[A])(depends: A => List[A]): A => Int = {

    // reflexive transitive closure
    val reachable: A => Set[A] =
      Memo.rec[A, Set[A]] { (node, rec) =>
        depends(node) match {
          case Nil      => Set.empty[A] + node
          case nonEmpty => nonEmpty.iterator.map(rec(_)).reduce(_ | _) + node
        }
      }

    val allNodes: Set[A] = seed.flatMap(reachable)

    val childrenOf: Map[A, Set[A]] =
      Monoid.combineAll(for {
        a <- allNodes.iterator
        b <- depends(a).iterator
      } yield Map(b -> Set(a)))

    { a: A =>
      childrenOf.get(a) match {
        case None    => 0
        case Some(s) => s.size
      }
    }
  }
}
