package dev.posco.hiona

import cats.Monoid

case class Decay[H <: Decay.DecayRate](scaledTime: Double, value: Double) {
  def timestampDouble(implicit v: ValueOf[H]): Double =
    scaledTime * v.value.toMillis

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
  sealed abstract class DecayRate(val toMillis: Long)
  case object Hour extends DecayRate(60L * 60L * 1000L)
  case object Day extends DecayRate(24L * 60L * 60L * 1000L)
  case object Week extends DecayRate(7L * 24L * 60L * 60L * 1000L)
  case object Month extends DecayRate(31557600L * 1000L / 12L)
  case object Quarter extends DecayRate(31557600L * 1000L / 4L)
  case object Year extends DecayRate(31557600L * 1000L)

  def build[H <: DecayRate: ValueOf, N: Numeric](time: Timestamp, n: N): Decay[H] =
    Decay(time.epochMillis.toDouble / valueOf[H].toMillis, implicitly[Numeric[N]].toDouble(n))

  implicit def monoidForDecay[H <: DecayRate]: Monoid[Decay[H]] =
    new Monoid[Decay[H]] {
      val empty = Decay[H](Double.NegativeInfinity, 0.0)
      def combine(l: Decay[H], r: Decay[H]): Decay[H] = l.combine(r)
    }
}

