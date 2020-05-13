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
}
