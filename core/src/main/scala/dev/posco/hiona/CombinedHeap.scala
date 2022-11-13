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
import cats.Order
import cats.kernel.CommutativeMonoid
import scala.annotation.tailrec

/**
  * A CombinedHeap is a pairing heap that also
  * caches the commutative combination of all
  * values so we can do fast queries of combine.
  *
  * This is based on cats-collection PairingHeap (which Oscar Boykin wrote)
  *
  * See:
  * https://en.wikipedia.org/wiki/Pairing_heap
  * in particular:
  * https://en.wikipedia.org/wiki/Pairing_heap#Summary_of_running_times
  *
  * Additionally, it supports an efficient O(1) combine operation
  */
sealed abstract class CombinedHeap[A, B] {

  import CombinedHeap._

  /**
    * insert an item into the heap
    * O(1)
    */
  final def add(a: A, b: B)(implicit
      order: Order[A],
      mon: Monoid[B]
  ): CombinedHeap[A, B] =
    if (isEmpty) CombinedHeap(a, b)
    else {
      val tree = this.asInstanceOf[Tree[A, B]]
      import tree._

      if (order.lt(min, a)) {
        val that = Tree(a, b, Nil)
        val sum = mon.combine(combined, b)
        Tree(min, sum, that :: tree.subtrees)
      } else {
        // we can avoid allocating Tree on this branch
        val sum = mon.combine(b, combined)
        Tree(a, sum, tree :: Nil)
      }
    }

  /*
   * Add a collection of items in. This is O(N) if as is size N
   */
  def addAll(
      as: Iterable[(A, B)]
  )(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B] = {
    val ait = as.iterator
    var heap = this
    while (ait.hasNext) {
      val (a, b) = ait.next()
      heap = heap.add(a, b)
    }
    heap
  }

  /**
    * Returns min value on the heap, if it exists
    *
    * O(1)
    */
  def minimumOption: Option[A]

  /**
    * Return all values combined in no guaranteed order
    * for this to be safe, the monoid should be commutative
    */
  final def unorderedFold(implicit m: Monoid[B]): B =
    if (isEmpty) m.empty
    else {
      val tree = this.asInstanceOf[Tree[A, B]]
      tree.combined
    }

  /**
    * Returns the size of the heap.
    * O(1)
    */
  def size: Long

  /**
    * Verifies if the heap is empty.
    * O(1)
    */
  def isEmpty: Boolean

  /**
    * Return true if this is not empty
    * O(1)
    */
  def nonEmpty: Boolean = !isEmpty

  /** Merge two heaps, this is O(1) work */
  def combine(
      that: CombinedHeap[A, B]
  )(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B]

  /**
    * Remove the min element from the heap (the root) and return it along with the updated heap.
    * Order O(log n)
    */
  def pop(implicit
      order: Order[A],
      mon: Monoid[B]
  ): Option[(A, B, CombinedHeap[A, B])] =
    this match {
      case Tree(a, b, subtrees) => Some((a, b, combineAll(subtrees)))
      case _                    => None
    }

  def keys: LazyList[A] =
    this match {
      case Tree(key, _, rest) => key #:: rest.to(LazyList).flatMap(_.keys)
      case _                  => LazyList.empty
    }

  /**
    * if not empty, remove the min, else return empty
    * this is thought to be O(log N) (but not proven to be so)
    */
  def remove(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B] =
    if (isEmpty) this
    else {
      val thisTree = this.asInstanceOf[Tree[A, B]]
      combineAll(thisTree.subtrees)
    }

  def removeLessThan(
      a: A
  )(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B] =
    if (nonEmpty) {
      val thisTree = this.asInstanceOf[Tree[A, B]]
      if (order.lteqv(a, thisTree.min))
        // we are done:
        this
      else
        // min < a, so we need to remove it and recurse:
        combineAllIter(thisTree.subtrees.iterator.map(_.removeLessThan(a)), Nil)
    } else
      this

  def subtrees: List[CombinedHeap[A, B]]
}

object CombinedHeap {

  def empty[A, B]: CombinedHeap[A, B] = Leaf()

  def apply[A, B](a: A, b: B): CombinedHeap[A, B] = Tree(a, b, Nil)

  /** This is thought to be O(log N) where N is the size of the final heap */
  def combineAll[A: Order, B: Monoid](
      it: Iterable[CombinedHeap[A, B]]
  ): CombinedHeap[A, B] =
    combineAllIter(it.iterator, Nil)

  @tailrec
  private def combineLoop[A: Order, B: Monoid](
      ts: List[CombinedHeap[A, B]],
      acc: CombinedHeap[A, B]
  ): CombinedHeap[A, B] =
    ts match {
      case Nil       => acc
      case h :: tail => combineLoop(tail, h.combine(acc))
    }

  @tailrec
  private def combineAllIter[A: Order, B: Monoid](
      iter: Iterator[CombinedHeap[A, B]],
      pairs: List[CombinedHeap[A, B]]
  ): CombinedHeap[A, B] =
    if (iter.isEmpty)
      combineLoop(pairs, empty[A, B])
    else {
      val p0 = iter.next()
      if (iter.isEmpty) combineLoop(pairs, p0)
      else {
        val p1 = iter.next()
        val pair =
          p0.combine(p1) // this is where the name pairing heap comes from
        combineAllIter(iter, pair :: pairs)
      }
    }

  /** build a heap from a list of items, O(N) */
  def fromIterable[A, B](
      as: Iterable[(A, B)]
  )(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B] = {
    val iter = as.iterator
    var heap = empty[A, B]
    while (iter.hasNext) {
      val pair = iter.next()
      heap = heap.add(pair._1, pair._2)
    }
    heap
  }

  /**
    * this is useful for finding the k maximum values in O(N) times for N items
    * same as as.toList.sorted.reverse.take(count), but O(N log(count)) vs O(N log N)
    * for a full sort. When N is very large, this can be a very large savings
    */
  def takeLargest[A, B](as: Iterable[(A, B)], count: Int)(implicit
      order: Order[A],
      mon: Monoid[B]
  ): CombinedHeap[A, B] =
    if (count <= 0) empty
    else {
      var heap = empty[A, B]
      val iter = as.iterator
      while (iter.hasNext) {
        val (a, b) = iter.next()
        heap =
          if (heap.size < count) heap.add(a, b)
          else if (order.lt(heap.asInstanceOf[Tree[A, B]].min, a))
            heap.remove.add(a, b)
          else heap
      }

      heap
    }

  final case class Tree[A, B](min: A, combined: B, subtrees: List[Tree[A, B]])
      extends CombinedHeap[A, B] {
    override val size = {
      @tailrec
      def loop(ts: List[Tree[A, B]], acc: Long): Long =
        ts match {
          case Nil       => acc
          case h :: tail => loop(tail, acc + h.size)
        }
      loop(subtrees, 1L)
    }

    override def isEmpty: Boolean = false

    override def minimumOption: Option[A] = Some(min)

    override def combine(
        that: CombinedHeap[A, B]
    )(implicit order: Order[A], mon: Monoid[B]): CombinedHeap[A, B] =
      if (that.isEmpty) this
      else {
        val thatTree = that.asInstanceOf[Tree[A, B]]
        if (order.lt(min, thatTree.min)) {
          val sum = mon.combine(combined, thatTree.combined)
          Tree(min, sum, thatTree :: subtrees)
        } else {
          val sum = mon.combine(thatTree.combined, combined)
          Tree(thatTree.min, sum, this :: thatTree.subtrees)
        }
      }
  }

  final private case object Leaf extends CombinedHeap[Nothing, Nothing] {
    def apply[A, B](): CombinedHeap[A, B] =
      this.asInstanceOf[CombinedHeap[A, B]]

    def unapply[A, B](heap: CombinedHeap[A, B]): Boolean = heap.isEmpty

    override def subtrees: List[CombinedHeap[Nothing, Nothing]] = Nil

    override def size: Long = 0L

    override def isEmpty: Boolean = true

    override def minimumOption: Option[Nothing] = None

    override def combine(that: CombinedHeap[Nothing, Nothing])(implicit
        order: Order[Nothing],
        mon: Monoid[Nothing]
    ): CombinedHeap[Nothing, Nothing] = that
  }

  implicit def catsCollectionCombinedHeapCommutativeMonoid[A: Order, B: Monoid]
      : CommutativeMonoid[CombinedHeap[A, B]] =
    new CommutativeMonoid[CombinedHeap[A, B]] {
      def empty = CombinedHeap.empty[A, B]
      def combine(a: CombinedHeap[A, B], b: CombinedHeap[A, B]) = a.combine(b)
    }
}
