package dev.posco.hiona

import cats.effect.IO

import Engine.Emitter

sealed trait Emittable[F[_]] {
  def from[A](f: F[A]): IO[Emitter[A]]
  def sourcesAndOffsetsOf[A](
      f: F[A]
  ): Map[String, (Set[Event.Source[_]], Set[Duration])]
}

object Emittable {
  def apply[F[_]](implicit f: Emittable[F]): Emittable[F] = f

  implicit val eventIsEmittable: Emittable[Event] =
    new Emittable[Event] {
      def from[A](ev: Event[A]) = Emitter.fromEvent(ev)
      def sourcesAndOffsetsOf[A](
          f: Event[A]
      ): Map[String, (Set[Event.Source[_]], Set[Duration])] = {
        val inputs = Event.sourcesOf(f)
        inputs.map { case (k, vs) => (k, (vs, Set(Duration.zero))) }
      }
    }

  implicit val labeledEventIsEmittable: Emittable[LabeledEvent] =
    new Emittable[LabeledEvent] {
      def from[A](ev: LabeledEvent[A]) = Emitter.fromLabeledEvent(ev)
      def sourcesAndOffsetsOf[A](
          f: LabeledEvent[A]
      ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
        LabeledEvent.sourcesAndOffsetsOf(f)
    }
}
