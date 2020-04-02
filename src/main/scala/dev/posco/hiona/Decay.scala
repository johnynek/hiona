package dev.posco.hiona

import cats.Monoid

case class Decay[H <: Duration](scaledTime: Double, value: Double) {
  def timestampDouble(implicit v: ValueOf[H]): Double =
    scaledTime * v.value.millis

  def timestamp(implicit v: ValueOf[H]): Timestamp =
    Timestamp(timestampDouble.toLong)

  def combine(that: Decay[H]): Decay[H] =
    if (scaledTime > that.scaledTime) that.combine(this)
    else {
      // we know scaledTime <= that.scaledTime
      val decayThis = math.exp(scaledTime - that.scaledTime) * value
      if (decayThis == 0.0) that
      else Decay[H](that.scaledTime, that.value + decayThis)
    }
}
object Decay {
  def build[H <: Duration: ValueOf, N: Numeric](
      time: Timestamp,
      n: N
  ): Decay[H] =
    Decay(
      time.epochMillis.toDouble / valueOf[H].millis,
      implicitly[Numeric[N]].toDouble(n)
    )

  implicit def monoidForDecay[H <: Duration]: Monoid[Decay[H]] =
    new Monoid[Decay[H]] {
      val empty = Decay[H](Double.NegativeInfinity, 0.0)
      def combine(l: Decay[H], r: Decay[H]): Decay[H] = l.combine(r)
    }
}
