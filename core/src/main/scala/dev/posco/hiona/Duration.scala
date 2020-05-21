package dev.posco.hiona

sealed abstract class Duration(val isInfinite: Boolean) {
  def +(that: Duration): Duration
  def millis: Long

  def *(that: Long): Duration
  def /(that: Long): Duration
}

object Duration {

  def apply(ms: Long): Duration =
    Finite(ms)

  case object Infinite extends Duration(true) {
    def +(that: Duration): Duration = this

    def millis = Long.MaxValue

    def *(that: Long): Duration = {
      require(that >= 0, "we cannot multiply by a negative number")
      if (that == 0) zero
      else this
    }

    def /(that: Long): Duration = {
      require(that > 0, "can only divide by a number > 0")
      this
    }
  }

  case class Finite(millis: Long) extends Duration(false) {
    require(millis >= 0, s"$millis should be >= 0")

    def +(that: Duration): Duration =
      that match {
        case Infinite => Infinite
        case Finite(m1) =>
          val res = millis + m1
          if (res >= millis) Finite(res)
          else
            // overflow. Could throw here, or return Infinite.
            // for now, let's say infinite
            Infinite
      }

    def *(that: Long): Duration = {
      require(that >= 0L, "we cannot multiply by a negative number")
      if (that == 0L) zero
      else if (that == 1L) this
      else {
        val thatL = that.toLong
        val m1 = millis * thatL
        if (m1 <= millis) Infinite // overflow
        else {
          val m2 = m1 / thatL
          if (m2 == millis) Finite(m1)
          else Infinite // overflow
        }
      }
    }

    def /(that: Long): Duration =
      if (that <= 0L)
        sys.error(s"can only divide a Duration by > 0, found: $this / $that")
      else if (that == 1L) this
      else {
        val m1 = millis / that.toLong
        if (m1 == 0L) zero
        else Finite(m1)
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
  val zero: Duration = Zero

  // some constants we can access at the type-level: second.type, minute.type, etc...
  val millisecond: Duration = Duration(1L)
  val second: Duration = millisecond * 1000
  val minute: Duration = second * 60
  val hour: Duration = minute * 60
  val day: Duration = hour * 24
  val week: Duration = day * 7
  // the following are approximate
  // this is 365.25 days, which is also an integer number of milliseconds
  // which is divisible by 12
  val year: Duration = (day * 365) + (hour * 6)
  val quarter: Duration = year / 4
  val month: Duration = quarter / 3

  def minutes(cnt: Long): Duration =
    minute * cnt
}
