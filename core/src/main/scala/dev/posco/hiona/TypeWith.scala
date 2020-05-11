package dev.posco.hiona

import cats.arrow.FunctionK
import cats.{Functor, Semigroupal}

sealed abstract class TypeWith[Ev[_]] {
  type Type
  def evidence: Ev[Type]

  def mapK[F[_]](fn: FunctionK[Ev, F]): TypeWith.Aux[F, Type] =
    TypeWith[F, Type](fn(evidence))

  def mapK2[F[_]: Functor, G[_]](
      fn: FunctionK[Ev, Lambda[x => F[G[x]]]]
  ): F[TypeWith.Aux[G, Type]] =
    Functor[F].map(fn(evidence))(TypeWith[G, Type](_))

  def product(
      that: TypeWith[Ev]
  )(implicit ev: Semigroupal[Ev]): TypeWith.Aux[Ev, (Type, that.Type)] =
    TypeWith[Ev, (Type, that.Type)](ev.product(evidence, that.evidence))

  override def toString = s"TypeWith($evidence)"

  override def equals(that: Any) =
    that match {
      case tw: TypeWith[_] => evidence == tw.evidence
      case _               => false
    }
  override def hashCode: Int = evidence.hashCode
}

object TypeWith {
  type Aux[Ev[_], A] = TypeWith[Ev] { type Type = A }

  def apply[Ev[_], A](implicit ev: Ev[A]): Aux[Ev, A] =
    new TypeWith[Ev] {
      type Type = A
      def evidence = ev
    }
}

sealed abstract class TypeWith2[Ev[_, _]] {
  type Type1
  type Type2
  def evidence: Ev[Type1, Type2]

  override def toString = s"TypeWith2($evidence)"

  override def equals(that: Any) =
    that match {
      case tw: TypeWith2[_] => evidence == tw.evidence
      case _                => false
    }
  override def hashCode: Int = evidence.hashCode
}

object TypeWith2 {
  type Aux[Ev[_, _], A, B] = TypeWith2[Ev] { type Type1 = A; type Type2 = B }

  def apply[Ev[_, _], A, B](implicit ev: Ev[A, B]): Aux[Ev, A, B] =
    new TypeWith2[Ev] {
      type Type1 = A
      type Type2 = B
      def evidence = ev
    }
}
