package dev.posco.hiona

import cats.Order

final case class Timestamp(epochMillis: Long) {
  def +(that: Duration): Timestamp =
    that match {
      case Duration.Infinite => Timestamp.MaxValue
      case Duration.Finite(thatMillis) =>
        val m1 = epochMillis + thatMillis
        if (m1 >= epochMillis) Timestamp(m1)
        else Timestamp.MaxValue
    }
  def -(that: Duration): Timestamp =
    that match {
      case Duration.Infinite => Timestamp.MinValue
      case Duration.Finite(thatMillis) =>
        val m1 = epochMillis - thatMillis
        if (m1 <= epochMillis) Timestamp(m1)
        else Timestamp.MinValue
    }

  def /(that: Duration): Long =
    that match {
      case Duration.Infinite => 0L
      case Duration.Finite(thatMillis) =>
        val d = epochMillis / thatMillis
        if (epochMillis >= 0) d
        else (d - 1L)
    }

  /**
    * what is the remainder in millis
    * of this duration.
    */
  def %(that: Duration): Duration =
    that match {
      case Duration.Infinite => Duration.zero
      case Duration.Finite(thatMillis) =>
        if (thatMillis == 0L) Duration.Infinite
        else {
          val rem = epochMillis % thatMillis
          val remPos =
            if (rem < 0) rem + thatMillis
            else rem
          Duration.Finite(remPos)
        }
    }

  def unixDayOfWeek: Int =
    ((this % Duration.week).millis / Duration.day.millis).toInt

  def unixHourOfDay: Int =
    ((this % Duration.day).millis / Duration.hour.millis).toInt

  def unixMinuteOfDay: Int =
    ((this % Duration.day).millis / Duration.minute.millis).toInt
}

object Timestamp {

  val MinValue: Timestamp = Timestamp(Long.MinValue)
  val Zero: Timestamp = Timestamp(0L)
  val MaxValue: Timestamp = Timestamp(Long.MaxValue)

  implicit val orderingForTimestamp: Ordering[Timestamp] =
    new Ordering[Timestamp] {
      def compare(left: Timestamp, right: Timestamp): Int =
        java.lang.Long.compare(left.epochMillis, right.epochMillis)
    }

  implicit val orderForTimestamp: Order[Timestamp] =
    Order.fromOrdering(orderingForTimestamp)

  def compareDiff(
      leftT: Timestamp,
      leftD: Duration,
      rightT: Timestamp,
      rightD: Duration
  ): Int =
    if (leftD == rightD) Timestamp.orderingForTimestamp.compare(leftT, rightT)
    else if (leftD.isInfinite)
      // right can't be infinite, or they would be the same, left is first
      -1
    else if (rightD.isInfinite)
      // left can't be infinite, or they would be the same, right is first
      1
    else {
      // they are both different, but not infinite, be careful with underflow
      val leftOff = leftD.millis
      val rightOff = rightD.millis

      val left0 = leftT.epochMillis
      val right0 = rightT.epochMillis

      val left1 = left0 - leftOff
      val right1 = right0 - rightOff

      if (left1 <= left0)
        // left didn't underflow
        if (right1 <= right0)
          // neither underflowed
          java.lang.Long.compare(left1, right1)
        else
          // right underflowed, so it must be smaller
          1
      else
      // left underflowed
      if (right1 <= right0)
        // right not underflowed, so it must be larger
        -1
      else
        // both underflowed, so we can just compare them directly
        java.lang.Long.compare(left1, right1)
    }

  // order by timestamp - duration
  val offsetOrdering: Ordering[(Timestamp, Duration)] =
    new Ordering[(Timestamp, Duration)] {
      def compare(left: (Timestamp, Duration), right: (Timestamp, Duration)) =
        compareDiff(left._1, left._2, right._1, right._2)
    }
}
