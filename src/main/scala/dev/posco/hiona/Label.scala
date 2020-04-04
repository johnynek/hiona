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
    Label.Zipped(this, that)

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
  case class Zipped[K, V, W](left: Label[K, V], right: Label[K, W])
      extends Label[K, (V, W)]

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
      case Zipped(l, r) =>
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
      case Zipped(l, r)      => lookupsOf(l) | lookupsOf(r)
    }
}

// Labeled events always have a subject K, and then features + label: V
// we can't change what LabeledEvents subjects are
sealed abstract class LabeledEvent[K, V] {
  final def map[W](fn: V => W): LabeledEvent[K, W] =
    LabeledEvent.Mapped(this, fn)

  // we can discard rows if we want to filter before emitting data
  final def filter(fn: (K, V) => Boolean): LabeledEvent[K, V] =
    LabeledEvent.Filtered(this, fn)
}

object LabeledEvent {

  def apply[K, V, W](
      event: Event[(K, V)],
      label: Label[K, W]
  ): LabeledEvent[K, (V, W)] =
    WithLabel(event, label)

  def sourcesAndOffsetsOf[A, B](
      ev: LabeledEvent[A, B]
  ): Map[String, (Set[Event.Source[_]], Set[Duration])] =
    ev match {
      case WithLabel(ev, label) =>
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

  def sourcesOf[A, B](
      ev: LabeledEvent[A, B]
  ): Map[String, Set[Event.Source[_]]] =
    sourcesAndOffsetsOf(ev).iterator.map { case (k, (v, _)) => (k, v) }.toMap

  def lookupsOf[K, V](le: LabeledEvent[K, V]): Set[Event.Lookup[_, _, _]] =
    le match {
      case WithLabel(ev, label) =>
        Event.lookupsOf(ev) | Label.lookupsOf(label)
      case Mapped(l, _)   => lookupsOf(l)
      case Filtered(l, _) => lookupsOf(l)
    }

  case class WithLabel[K, V, W](event: Event[(K, V)], label: Label[K, W])
      extends LabeledEvent[K, (V, W)]
  case class Mapped[K, V, W](labeled: LabeledEvent[K, V], fn: V => W)
      extends LabeledEvent[K, W]
  case class Filtered[K, V](labeled: LabeledEvent[K, V], fn: (K, V) => Boolean)
      extends LabeledEvent[K, V]
}
