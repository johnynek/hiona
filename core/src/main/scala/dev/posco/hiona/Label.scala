package dev.posco.hiona

import cats.Monoid

import cats.implicits._

/**
  * A Label is something we want to predict, generally about the future.
  * It is time varying, it is basically a future value of a Feature
  */
sealed abstract class Label[K, V] {
  final def lookForward(duration: Duration): Label[K, V] =
    Label.LookForward(this, duration)

  final def zip[W](that: Label[K, W]): Label[K, (V, W)] =
    Label.Zipped(this, that, implicitly[(V, W) =:= (V, W)])

  final def map[W](fn: V => W): Label[K, W] =
    mapWithKey(Feature.ValueMap(fn))

  final def mapWithKey[W](fn: (K, V) => W): Label[K, W] =
    Label.Mapped(this, fn)
}

object Label {
  def apply[K, V](feature: Feature[K, V]): Label[K, V] =
    FromFeature(feature)

  case class FromFeature[K, V](feature: Feature[K, V]) extends Label[K, V]
  case class LookForward[K, V](label: Label[K, V], duration: Duration)
      extends Label[K, V]
  case class Mapped[K, V, W](initial: Label[K, V], fn: (K, V) => W)
      extends Label[K, W]
  case class Zipped[K, X, Y, Z](
      left: Label[K, X],
      right: Label[K, Y],
      cast: (X, Y) =:= Z
  ) extends Label[K, Z]

  def sourcesAndOffsetsOf[K, V](
      label: Label[K, V],
      offset: Duration
  ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
    label match {
      case FromFeature(f) =>
        Feature
          .sourcesOf(f)
          .iterator
          .map { case (n, s) => (n, (s, Set(offset))) }
          .toMap
      case LookForward(l, off1) => sourcesAndOffsetsOf(l, offset + off1)
      case Mapped(l, _)         => sourcesAndOffsetsOf(l, offset)
      case Zipped(l, r, _) =>
        Monoid[Map[String, (Set[Event.Source[_]], Set[Duration])]]
          .combine(
            sourcesAndOffsetsOf(l, offset),
            sourcesAndOffsetsOf(r, offset)
          )
    }

  def lookupsOf[K, V](label: Label[K, V]): Set[Event.Lookup[_, _, _]] =
    label match {
      case FromFeature(f)    => Feature.lookupsOf(f)
      case LookForward(l, _) => lookupsOf(l)
      case Mapped(l, _)      => lookupsOf(l)
      case Zipped(l, r, _)   => lookupsOf(l) | lookupsOf(r)
    }
}

sealed abstract class LabeledEvent[A] {
  final def map[B](fn: A => B): LabeledEvent[B] =
    LabeledEvent.Mapped(this, fn)

  // we can discard rows if we want to filter before emitting data
  final def filter(fn: A => Boolean): LabeledEvent[A] =
    LabeledEvent.Filtered(this, fn)
}

object LabeledEvent {

  def apply[K, V, W](
      event: Event[(K, V)],
      label: Label[K, W]
  ): LabeledEvent[(K, (V, W))] =
    WithLabel(event, label, implicitly[(K, (V, W)) =:= (K, (V, W))])

  def sourcesAndOffsetsOf[A](
      ev: LabeledEvent[A]
  ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
    ev match {
      case WithLabel(ev, label, _) =>
        Monoid[Map[String, (Set[Event.Source[_]], Set[Duration])]]
          .combine(
            Event
              .sourcesOf(ev)
              .iterator
              .map { case (n, s) => (n, (s, Set(Duration.zero))) }
              .toMap,
            Label.sourcesAndOffsetsOf(label, Duration.zero)
          )
      case Mapped(l, _)   => sourcesAndOffsetsOf(l)
      case Filtered(l, _) => sourcesAndOffsetsOf(l)
    }

  def sourcesOf[A](
      ev: LabeledEvent[A]
  ): Map[String, Set[Event.Source[_]]] =
    sourcesAndOffsetsOf(ev).iterator.map { case (k, (v, _)) => (k, v) }.toMap

  def lookupsOf[A](le: LabeledEvent[A]): Set[Event.Lookup[_, _, _]] =
    le match {
      case WithLabel(ev, label, _) =>
        Event.lookupsOf(ev) | Label.lookupsOf(label)
      case Mapped(l, _)   => lookupsOf(l)
      case Filtered(l, _) => lookupsOf(l)
    }

  case class WithLabel[K, X, Y, Z](
      event: Event[(K, X)],
      label: Label[K, Y],
      cast: (K, (X, Y)) =:= Z
  ) extends LabeledEvent[Z]
  case class Mapped[A, B](labeled: LabeledEvent[A], fn: A => B)
      extends LabeledEvent[B]
  case class Filtered[A](labeled: LabeledEvent[A], fn: A => Boolean)
      extends LabeledEvent[A]
}
