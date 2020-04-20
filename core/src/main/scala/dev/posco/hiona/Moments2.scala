package dev.posco.hiona

import cats.Monoid

/**
  * The first two moments computed in a stable way.
  */
final case class Moments2(count: Double, mean: Double, mom2: Double) {
  def +(that: Moments2): Moments2 = {
    val c1 = count + that.count

    if (c1 == 0.0) Moments2.zero
    else {
      val mean1 = Moments2.combineMean(count, mean, that.count, that.mean)

      val delta = that.mean - mean
      val mom21 = mom2 + that.mom2 +
        math.pow(delta, 2) * count * that.count / c1

      Moments2(c1, mean1, mom21)
    }
  }

  /**
    * (v - mean) / stddev
    */
  @inline def zscore(v: Double): Double = {
    val diff = v - mean
    if (diff == 0.0) 0.0
    else diff / stddev
  }

  /**
    * population variance (not sample)
    */
  @inline def variance: Double = mom2 / count

  /**
    * population stddev (not sample)
    */
  @inline def stddev: Double = math.sqrt(variance)
}

object Moments2 {

  val zero: Moments2 = Moments2(0.0, 0.0, 0.0)

  def value(d: Double): Moments2 =
    Moments2(1.0, d, 0.0)

  def numeric[N: Numeric](n: N): Moments2 =
    value(implicitly[Numeric[N]].toDouble(n))

  implicit val moments2Monoid: Monoid[Moments2] =
    new Monoid[Moments2] {
      def empty = zero
      def combine(a: Moments2, b: Moments2): Moments2 = a + b
    }

  implicit val moments2DoubleModule: DoubleModule[Moments2] =
    new DoubleModule[Moments2] {
      def monoid = moments2Monoid
      def scale(s: Double, v: Moments2): Moments2 =
        if (s == 0.0) zero
        else if (s == 1.0) v
        else {
          // this scales down the count, but preserves the mean and mom2
          val c1 = s * v.count
          // if (a + b) = 1, then a * v + b * v == v
          val mom21 = s * v.mom2
          Moments2(c1, v.mean, mom21)
        }
    }

  /**
    * Stolen from Algebird:
    * https://github.com/twitter/algebird/blob/d8071fd11a81512f4b577a5529a796dd83660a14/algebird-core/src/main/scala/com/twitter/algebird/MomentsGroup.scala#L96
    *
    * but we are modifying count to be a Double so we can do
    * decaying Moments2
    *
    * Given two streams of doubles (n, an) and (k, ak) of form (count,
    * mean), calculates the mean of the combined stream.
    *
    * Uses a more stable online algorithm which should be suitable for
    * large numbers of records similar to:
    * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    */
  def combineMean(n: Double, an: Double, k: Double, ak: Double): Double =
    if (n < k) combineMean(k, ak, n, an)
    else {
      val nk = n + k
      if (nk == 0.0) 0.0
      else if (nk == n) an
      else {
        val scaling = k / nk
        // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
        val StabilityConst = 0.1
        if (scaling < StabilityConst) (an + (ak - an) * scaling)
        else (n * an + k * ak) / nk
      }
    }
}
