package dev.posco.hiona

import cats.Monoid

case class Decay[H <: Duration, V](scaledTime: Double, value: V) {
  def timestampDouble(implicit v: ValueOf[H]): Double =
    scaledTime * v.value.millis

  def timestamp(implicit v: ValueOf[H]): Timestamp =
    Timestamp(timestampDouble.toLong)

  /**
    * What would the value be at the given timestamp assuming
    * no other values
    */
  def atTimestamp(
      t: Timestamp
  )(implicit v: ValueOf[H], dmod: DoubleModule[V]): V = {
    val stime = Decay.scaledTime[H](t)
    val negDelta = scaledTime - stime
    val scaleFactor = math.exp(negDelta)
    if (scaleFactor == 0.0) dmod.monoid.empty
    else dmod.scale(scaleFactor, value)
  }

  def combine(that: Decay[H, V])(implicit dmod: DoubleModule[V]): Decay[H, V] =
    if (scaledTime > that.scaledTime) that.combine(this)
    else {
      // we know scaledTime <= that.scaledTime
      // so we know that 0 <= scaleFactor <= 1
      val scaleFactor = math.exp(scaledTime - that.scaledTime)
      if (scaleFactor == 0.0) that
      else {
        val v1 =
          if (scaleFactor != 1.0)
            dmod.monoid.combine(dmod.scale(scaleFactor, value), that.value)
          else dmod.monoid.combine(value, that.value)
        Decay[H, V](that.scaledTime, v1)
      }
    }
}
object Decay {

  def scaledTime[H <: Duration: ValueOf](ts: Timestamp): Double =
    ts.epochMillis.toDouble / valueOf[H].millis

  def toFloat[H <: Duration: ValueOf, N: Numeric](
      time: Timestamp,
      value: N
  ): Decay[H, Float] =
    Decay(
      scaledTime(time),
      Numeric[N].toFloat(value)
    )

  def toDouble[H <: Duration: ValueOf, N: Numeric](
      time: Timestamp,
      value: N
  ): Decay[H, Double] =
    Decay(
      scaledTime(time),
      Numeric[N].toDouble(value)
    )

  def fromTimestamped[H <: Duration: ValueOf, V](
      time: Timestamp,
      value: V
  ): Decay[H, V] =
    Decay(
      time.epochMillis.toDouble / valueOf[H].millis,
      value
    )

  implicit def monoidForDecay[H <: Duration, V: DoubleModule]
      : Monoid[Decay[H, V]] =
    new Monoid[Decay[H, V]] {
      val empty = Decay[H, V](
        Double.NegativeInfinity,
        implicitly[DoubleModule[V]].monoid.empty
      )
      def combine(l: Decay[H, V], r: Decay[H, V]): Decay[H, V] = l.combine(r)
    }
}
