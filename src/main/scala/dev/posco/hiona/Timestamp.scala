package dev.posco.hiona

final case class Timestamp(epochMillis: Long)

object Timestamp {
  import Duration.Finite

  implicit val orderingForTimestamp: Ordering[Timestamp] =
    new Ordering[Timestamp] {
      def compare(left: Timestamp, right: Timestamp): Int =
        java.lang.Long.compare(left.epochMillis, right.epochMillis)
    }

  def compareDiff(
      leftT: Timestamp,
      leftD: Duration,
      rightT: Timestamp,
      rightD: Duration
  ): Int =
    if (leftD == rightD) Timestamp.orderingForTimestamp.compare(leftT, rightT)
    else if (leftD.isInfinite) {
      // right can't be infinite, or they would be the same, left is first
      -1
    } else if (rightD.isInfinite) {
      // left can't be infinite, or they would be the same, right is first
      1
    } else {
      // they are both different, but not infinite, be careful with underflow
      val Finite(leftOff) = leftD
      val Finite(rightOff) = rightD

      val left0 = leftT.epochMillis
      val right0 = rightT.epochMillis

      val left1 = left0 - leftOff
      val right1 = right0 - rightOff

      if (left1 <= left0) {
        // left didn't underflow
        if (right1 <= right0) {
          // neither underflowed
          java.lang.Long.compare(left1, right1)
        } else {
          // right underflowed, so it must be smaller
          1
        }
      } else {
        // left underflowed
        if (right1 <= right0) {
          // right not underflowed, so it must be larger
          -1
        } else {
          // both underflowed, so we can just compare them directly
          java.lang.Long.compare(left1, right1)
        }
      }
    }

  // order by timestamp - duration
  val offsetOrdering: Ordering[(Timestamp, Duration)] =
    new Ordering[(Timestamp, Duration)] {
      def compare(left: (Timestamp, Duration), right: (Timestamp, Duration)) =
        compareDiff(left._1, left._2, right._1, right._2)
    }
}
