package dev.posco.hiona

import cats.Monoid
import cats.implicits._

sealed abstract class Feature[K, V] {

  final def zip[W](that: Feature[K, W]): Feature[K, (V, W)] =
    Feature.Zipped(this, that)

  final def map[W](fn: V => W): Feature[K, W] =
    mapWithKey(Feature.ValueMap(fn))

  final def mapWithKey[W](fn: (K, V) => W): Feature[K, W] =
    Feature.Mapped(this, fn)
}

object Feature {
  case class ValueMap[V, W](fn: V => W) extends Function2[Any, V, W] {
    def apply(k: Any, v: V): W = fn(v)
  }
  case class ConstFn[A](result: A) extends Function[Any, A] {
    def apply(a: Any) = result
  }

  def const[K, V](v: V): Feature[K, V] =
    Event.empty[(K, Unit)].sum.map(ConstFn(v))

  def sourcesOf[K, V](f: Feature[K, V]): Map[String, Set[Event.Source[_]]] =
    f match {
      case Summed(ev, _) => Event.sourcesOf(ev)
      case Latest(ev, _) => Event.sourcesOf(ev)
      case Mapped(f, _)  => sourcesOf(f)
      case Zipped(left, right) =>
        Monoid[Map[String, Set[Event.Source[_]]]]
          .combine(sourcesOf(left), sourcesOf(right))
    }

  def lookupsOf[K, V](f: Feature[K, V]): Set[Event.Lookup[_, _, _]] =
    f match {
      case Summed(ev, _) => Event.lookupsOf(ev)
      case Latest(ev, _) => Event.lookupsOf(ev)
      case Mapped(f, _)  => lookupsOf(f)
      case Zipped(l, r)  => lookupsOf(l) | lookupsOf(r)
    }

  case class Summed[K, V](event: Event[(K, V)], monoid: Monoid[V])
      extends Feature[K, V]
  case class Latest[K, V](event: Event[(K, V)], within: Duration)
      extends Feature[K, Option[V]]
  case class Mapped[K, V, W](initial: Feature[K, V], fn: (K, V) => W)
      extends Feature[K, W]
  case class Zipped[K, V, W](left: Feature[K, V], right: Feature[K, W])
      extends Feature[K, (V, W)]
}
