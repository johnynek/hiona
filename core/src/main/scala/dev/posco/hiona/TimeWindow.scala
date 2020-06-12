package dev.posco.hiona

import cats.collections.PairingHeap
import cats.kernel.CommutativeSemigroup
import cats.{Eq, Order}

import cats.implicits._

/**
  * This keeps a window of events at most Duration apart
  *
  * so any two timestamps in here, if t1 < t2 then t1 + duration > t2
  * another way to say it: |t1 - t2| < duration
  */
sealed abstract class TimeWindow[W <: Duration, A] {
  final def getWindow(implicit v: ValueOf[W]): W = v.value

  def combine(
      that: TimeWindow[W, A]
  )(implicit v: ValueOf[W], ord: Order[A]): TimeWindow[W, A]

  def minimum: (Timestamp, A)
  def maximum: (Timestamp, A)
  def size: Long

  def toList(implicit ord: Order[A]): List[(Timestamp, A)]

  def +(
      tsA: (Timestamp, A)
  )(implicit v: ValueOf[W], ord: Order[A]): TimeWindow[W, A] =
    combine(TimeWindow.single(tsA))

  /**
    * make some aggregate value of the entire window. The B needs to have a commutative monoid
    * should be commutative since we don't promise the ordering
    * if you do need the order, use toList and apply your function
    * to the result there
    *
    * An example would be to take the average over a window, the average value is
    * commutative, so we can apply that here
    */
  def unorderedFoldMap[B: CommutativeSemigroup](fn: ((Timestamp, A)) => B): B
}

object TimeWindow {
  private case class Items[W <: Duration, A](
      toHeap: PairingHeap[(Timestamp, A)],
      maximum: (Timestamp, A)
  ) extends TimeWindow[W, A] {
    def minimum: (Timestamp, A) =
      // the heap is never empty by construction
      toHeap.minimumOption.get

    def combine(
        that: TimeWindow[W, A]
    )(implicit v: ValueOf[W], ord: Order[A]): TimeWindow[W, A] = {
      val newMax =
        if (Order[Timestamp].lt(that.maximum._1, maximum._1)) maximum
        else that.maximum

      val Items(thatHeap, _) = that
      var resHeap = thatHeap.combine(toHeap)

      // inclusive minimimum time
      val newMinTime = newMax._1 - v.value
      while (Order[Timestamp].lt(resHeap.minimumOption.get._1, newMinTime))
        resHeap = resHeap.remove

      Items(resHeap, newMax)
    }

    def size = toHeap.size

    def toList(implicit ord: Order[A]): List[(Timestamp, A)] = toHeap.toList

    def unorderedFoldMap[B: CommutativeSemigroup](
        fn: ((Timestamp, A)) => B
    ): B =
      toHeap
        .unorderedFoldMap[Option[B]](fn.andThen(Some(_)))
        .getOrElse(fn(maximum))
  }

  def single[W <: Duration, A](tsA: (Timestamp, A)): TimeWindow[W, A] =
    Items(PairingHeap(tsA), tsA)

  def fromList[W <: Duration, A: Order](
      items: List[(Timestamp, A)]
  )(implicit v: ValueOf[W]): Option[TimeWindow[W, A]] =
    items match {
      case Nil => None
      case nonempty @ (_ :: _) =>
        val maxV @ (maxTime, _) = nonempty.iterator.maxBy(_._1)
        // must be >= minTime
        val minTime = maxTime - v.value
        val heap = items.foldLeft(PairingHeap.empty[(Timestamp, A)]) {
          case (heap, pair @ (ts, _)) =>
            if (Order[Timestamp].gteqv(ts, minTime)) heap.add(pair)
            else heap
        }

        Some(Items(heap, maxV))
    }

  implicit def semigroupForTimeWindow[W <: Duration, A](implicit
      v: ValueOf[W],
      ord: Order[A]
  ): CommutativeSemigroup[TimeWindow[W, A]] =
    new CommutativeSemigroup[TimeWindow[W, A]] {
      def combine(
          left: TimeWindow[W, A],
          right: TimeWindow[W, A]
      ): TimeWindow[W, A] =
        left.combine(right)
    }

  implicit def eqForTimeWindow[W <: Duration: ValueOf, A: Order]
      : Eq[TimeWindow[W, A]] =
    new Eq[TimeWindow[W, A]] {
      val eqList: Eq[List[(Timestamp, A)]] = Eq[List[(Timestamp, A)]]
      def eqv(left: TimeWindow[W, A], right: TimeWindow[W, A]): Boolean =
        (left eq right) || {
          (left.size == right.size) &&
          eqList.eqv(left.toList, right.toList)
        }
    }
}
