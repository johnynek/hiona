package dev.posco.hiona

sealed abstract class Duration(val isInfinite: Boolean) {
  def +(that: Duration): Duration
  def millis: Long
}
object Duration {

  def apply(ms: Long): Duration =
    Finite(ms)

  case object Infinite extends Duration(true) {
    def +(that: Duration): Duration = this

    def millis = Long.MaxValue
  }
  case class Finite(millis: Long) extends Duration(false) {
    require(millis >= 0, s"$millis should be >= 0")

    def +(that: Duration): Duration =
      that match {
        case Infinite => Infinite
        case Finite(m1) =>
          val res = millis + m1
          if (res >= millis) Finite(res)
          else {
            // overflow. Could throw here, or return Infinite.
            // for now, let's say infinite
            Infinite
          }
      }
  }

  implicit val durationOrdering: Ordering[Duration] =
    new Ordering[Duration] {
      def compare(left: Duration, right: Duration) =
        if (left == right) 0
        else if (left.isInfinite) 1
        else if (right.isInfinite) -1
        else java.lang.Long.compare(left.millis, right.millis)
    }

  val Zero: Finite = Finite(0)
  def zero: Duration = Zero

  def min(cnt: Int): Duration =
    if (cnt == 0) zero
    else Finite(cnt.toLong * 60L * 1000L)
}
