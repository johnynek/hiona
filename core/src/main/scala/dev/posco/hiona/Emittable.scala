package dev.posco.hiona

sealed trait Emittable[F[_]] {
  def toEither[A](f: F[A]): Either[LabeledEvent[A], Event[A]]
  def sourcesAndOffsetsOf[A](
      f: F[A]
  ): Map[String, (Set[Event.Source[_]], Set[Duration])]
}

object Emittable {
  def apply[F[_]](implicit f: Emittable[F]): Emittable[F] = f

  implicit val eventIsEmittable: Emittable[Event] =
    new Emittable[Event] {
      def toEither[A](f: Event[A]): Either[LabeledEvent[A], Event[A]] =
        Right(f)
      def sourcesAndOffsetsOf[A](
          f: Event[A]
      ): Map[String, (Set[Event.Source[_]], Set[Duration])] = {
        val inputs = Event.sourcesOf(f)
        inputs.map { case (k, vs) => (k, (vs, Set(Duration.zero))) }
      }
    }

  implicit val labeledEventIsEmittable: Emittable[LabeledEvent] =
    new Emittable[LabeledEvent] {
      def toEither[A](f: LabeledEvent[A]): Either[LabeledEvent[A], Event[A]] =
        Left(f)
      def sourcesAndOffsetsOf[A](
          f: LabeledEvent[A]
      ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
        LabeledEvent.sourcesAndOffsetsOf(f)
    }

  implicit val valueIsEmittable: Emittable[Value] =
    new Emittable[Value] {
      def toEither[A](f: Value[A]): Either[LabeledEvent[A], Event[A]] =
        f.emittable.toEither(f.value)

      def sourcesAndOffsetsOf[A](
          f: Value[A]
      ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
        f.emittable.sourcesAndOffsetsOf(f.value)
    }

  /**
    * This is used to represent either LabledEvent or Event
    * when we have a value but want to forget which
    */
  sealed abstract class Value[A] {
    type Outer[_]
    type Inner

    val cast: A =:= Inner
    val value: Outer[A]
    val emittable: Emittable[Outer]

    override val hashCode = (value, emittable).hashCode
    override def equals(that: Any): Boolean =
      that match {
        case v: Value[_] => (value == v.value) && (emittable == v.emittable)
        case _           => false
      }
  }

  object Value {
    type Aux[F[_], A] = Value[A] { type Outer[x] = F[x]; type Inner = A }
    def apply[F[_], A](fa: F[A])(implicit em: Emittable[F]): Value.Aux[F, A] =
      new Value[A] {
        type Outer[x] = F[x]
        type Inner = A
        val cast = implicitly[A =:= A]
        val value = fa
        val emittable = em
      }
  }

}
