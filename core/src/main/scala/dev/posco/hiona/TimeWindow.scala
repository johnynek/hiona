package dev.posco.hiona

import cats.kernel.CommutativeSemigroup
import cats.{Eq, Monoid, Order}

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
  )(implicit v: ValueOf[W], ord: Monoid[A]): TimeWindow[W, A]

  def minimum: Timestamp
  def maximum: Timestamp
  def size: Long

  def +(
      tsA: (Timestamp, A)
  )(implicit v: ValueOf[W], ord: Monoid[A]): TimeWindow[W, A] =
    combine(TimeWindow.single(tsA._1, tsA._2))

  def combined(implicit mon: Monoid[A]): A

  def timestamps: Iterable[Timestamp]
}

object TimeWindow {
  private case class Items[W <: Duration, A](
      toHeap: CombinedHeap[Timestamp, A],
      maximum: Timestamp
  ) extends TimeWindow[W, A] {
    def minimum: Timestamp =
      // the heap is never empty by construction
      toHeap.minimumOption.get

    def combined(implicit m: Monoid[A]) = toHeap.unorderedFold

    def combine(
        that: TimeWindow[W, A]
    )(implicit v: ValueOf[W], mon: Monoid[A]): TimeWindow[W, A] = {
      val newMax =
        if (that.maximum < maximum) maximum
        else that.maximum

      // inclusive minimimum time
      val newMinTime = newMax - v.value
      if (maximum < newMinTime) that
      else if (that.maximum < newMinTime) this
      else {
        val Items(thatHeap, _) = that
        val resHeap = thatHeap
          .removeLessThan(newMinTime)
          .combine(toHeap.removeLessThan(newMinTime))
        Items(resHeap, newMax)
      }
    }

    def size = toHeap.size
    def timestamps: Iterable[Timestamp] = toHeap.keys
  }

  def single[W <: Duration, A](ts: Timestamp, a: A): TimeWindow[W, A] =
    Items(CombinedHeap(ts, a), ts)

  def fromList[W <: Duration, A: Monoid](
      items: List[(Timestamp, A)]
  )(implicit v: ValueOf[W]): Option[TimeWindow[W, A]] =
    items match {
      case Nil => None
      case nonempty @ (_ :: _) =>
        val maxTime = nonempty.iterator.map(_._1).max
        // must be >= minTime
        val minTime = maxTime - v.value
        val heap = items.foldLeft(CombinedHeap.empty[Timestamp, A]) {
          case (heap, (ts, a)) =>
            if (Order[Timestamp].gteqv(ts, minTime)) heap.add(ts, a)
            else heap
        }

        Some(Items(heap, maxTime))
    }

  implicit def semigroupForTimeWindow[W <: Duration, A](implicit
      v: ValueOf[W],
      ord: Monoid[A]
  ): CommutativeSemigroup[TimeWindow[W, A]] =
    new CommutativeSemigroup[TimeWindow[W, A]] {
      def combine(
          left: TimeWindow[W, A],
          right: TimeWindow[W, A]
      ): TimeWindow[W, A] =
        left.combine(right)
    }

  implicit def eqTimeWindow[W <: Duration, A: Eq: Monoid]
      : Eq[TimeWindow[W, A]] =
    new Eq[TimeWindow[W, A]] {
      def eqv(a: TimeWindow[W, A], b: TimeWindow[W, A]) =
        (a.minimum == b.minimum) &&
          (a.maximum == b.maximum) &&
          (a.size == b.size) &&
          (Eq[A].eqv(a.combined, b.combined))
    }
}
