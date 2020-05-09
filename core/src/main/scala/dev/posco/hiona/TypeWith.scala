package dev.posco.hiona

import cats.arrow.FunctionK
import cats.Semigroupal

sealed abstract class TypeWith[Ev[_]] {
  type Type
  def evidence: Ev[Type]

  def mapK[F[_]](fn: FunctionK[Ev, F]): TypeWith.Aux[F, Type] =
    TypeWith[F, Type](fn(evidence))

  def product(
      that: TypeWith[Ev]
  )(implicit ev: Semigroupal[Ev]): TypeWith.Aux[Ev, (Type, that.Type)] =
    TypeWith[Ev, (Type, that.Type)](ev.product(evidence, that.evidence))
}

object TypeWith {
  type Aux[Ev[_], A] = TypeWith[Ev] { type Type = A }

  def apply[Ev[_], A](implicit ev: Ev[A]): Aux[Ev, A] =
    new TypeWith[Ev] {
      type Type = A
      def evidence = ev
      override def equals(that: Any) =
        that match {
          case tw: TypeWith[_] => ev == tw.evidence
          case _               => false
        }
      override def hashCode: Int = ev.hashCode
    }
}
