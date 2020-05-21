package dev.posco.hiona

import cats.Monoid

import cats.implicits._

import shapeless._

trait DoubleModule[@specialized(Float, Double) V] {
  def monoid: Monoid[V]

  /**
    * This should have the law that
    * s1 * (s2 * v) == (s1 * s2) * v
    *
    * and
    *
    * s * (v1 + v2) == s * v1 + s * v2
    * (s + r) * v = s * v + r * v
    *
    * 0 * v = empty
    * 1 * v = v
    */
  def scale(s: Double, v: V): V
}

object DoubleModule extends Priority1DoubleModule {
  implicit val floatDModule: DoubleModule[Float] =
    new DoubleModule[Float] {
      val monoid = Monoid[Float]
      def scale(s: Double, v: Float) = (s * v.toDouble).toFloat
    }

  implicit val doubleDModule: DoubleModule[Double] =
    new DoubleModule[Double] {
      val monoid = Monoid[Double]
      def scale(s: Double, v: Double) = s * v
    }

  implicit val bigDecimalDModule: DoubleModule[BigDecimal] =
    new DoubleModule[BigDecimal] {
      val monoid = Monoid[BigDecimal]
      def scale(s: Double, v: BigDecimal) = v * s
    }

  implicit val unitDModule: DoubleModule[Unit] =
    new DoubleModule[Unit] {
      val monoid = Monoid[Unit]
      def scale(s: Double, v: Unit) = ()
    }

  def genericModule[A, B](implicit
      gen: Generic.Aux[A, B],
      modB: => DoubleModule[B]
  ): DoubleModule[A] =
    new DoubleModule[A] {
      lazy val monoid = ShapelessMonoid.genericMonoid[A, B](gen, modB.monoid)
      lazy val zeroA = monoid.empty

      def scale(s: Double, v: A): A =
        if (s == 0.0) zeroA
        else if (s == 1.0) v
        else {
          val b = gen.to(v)
          val b1 = modB.scale(s, b)
          gen.from(b1)
        }
    }

  implicit def tuple2Module[A: DoubleModule, B: DoubleModule]
      : DoubleModule[(A, B)] =
    genericModule

  implicit def tuple3Module[A: DoubleModule, B: DoubleModule, C: DoubleModule]
      : DoubleModule[(A, B, C)] =
    genericModule
}

sealed trait Priority1DoubleModule {

  implicit val hnilDModule: DoubleModule[HNil] =
    new DoubleModule[HNil] {
      val monoid = ShapelessMonoid.hnilMonoid
      def scale(s: Double, v: HNil) = HNil
    }

  implicit def hconsModule[A, B <: HList](implicit
      modA: DoubleModule[A],
      lazyModB: => DoubleModule[B]
  ): DoubleModule[A :: B] =
    new DoubleModule[A :: B] {
      lazy val modB = lazyModB

      lazy val monoid =
        ShapelessMonoid.hconsMonoid[A, B](modA.monoid, modB.monoid)
      lazy val zero = monoid.empty

      def scale(s: Double, v: A :: B) =
        if (s == 0.0) zero
        else if (s == 1.0) v
        else {
          val (va :: vb) = v
          modA.scale(s, va) :: modB.scale(s, vb)
        }
    }

}
