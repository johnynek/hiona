package dev.posco.hiona

import Hiona.{Event, Feature, Duration}

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
  case class LookForward[K, V](label: Label[K, V], duration: Duration) extends Label[K, V]
  case class Mapped[K, V, W](initial: Label[K, V], fn: (K, V) => W) extends Label[K, W]
  case class Zipped[K, V, W](left: Label[K, V], right: Label[K, W]) extends Label[K, (V, W)]
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

  def apply[K, V, W](event: Event[(K, V)], label: Label[K, W]): LabeledEvent[K, (V, W)] =
    WithLabel(event, label)

  case class WithLabel[K, V, W](event: Event[(K, V)], label: Label[K, W]) extends LabeledEvent[K, (V, W)]
  case class Mapped[K, V, W](labeled: LabeledEvent[K, V], fn: V => W) extends LabeledEvent[K, W]
  case class Filtered[K, V](labeled: LabeledEvent[K, V], fn: (K, V) => Boolean) extends LabeledEvent[K, V]
}
