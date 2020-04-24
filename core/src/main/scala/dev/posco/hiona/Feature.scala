package dev.posco.hiona

import cats.Monoid
import cats.implicits._

sealed abstract class Feature[K, V] {

  final def zip[W](that: Feature[K, W]): Feature[K, (V, W)] =
    Feature.Zipped(this, that, implicitly[(V, W) =:= (V, W)])

  final def map[W](fn: V => W): Feature[K, W] =
    mapWithKey(Feature.ValueMap(fn))

  final def mapWithKey[W](fn: (K, V) => W): Feature[K, W] =
    mapWithKeyTime(Feature.Ignore3(fn))

  final def mapWithKeyTime[W](fn: (K, V, Timestamp) => W): Feature[K, W] =
    Feature.Mapped(this, fn)

  /**
    * get an event each time this feature changes value.
    */
  final def triggers: Event[(K, Unit)] =
    Feature.triggersOf(this)

  /**
    * get an event stream of changes: the tuple has the value before
    * and after an event
    */
  final def changes: Event[(K, (V, V))] =
    triggers
      .preLookup(this)
      .postLookup(this)
      .mapValues(Feature.ChangesFn[V]())
}

object Feature {
  case class ValueMap[V, W](fn: V => W) extends Function2[Any, V, W] {
    def apply(k: Any, v: V): W = fn(v)
  }
  case class ConstFn[A](result: A) extends Function[Any, A] {
    def apply(a: Any) = result
  }
  case class Ignore3[K, V, W](fn: (K, V) => W) extends Function3[K, V, Any, W] {
    def apply(k: K, v: V, a: Any): W = fn(k, v)
  }
  case class ChangesFn[A]() extends Function1[((Unit, A), A), (A, A)] {
    def apply(in: ((Unit, A), A)) = (in._1._2, in._2)
  }

  def const[K, V](v: V): Feature[K, V] =
    Event.empty[(K, Unit)].sum.map(ConstFn(v))

  def sourcesOf[K, V](f: Feature[K, V]): Map[String, Set[Event.Source[_]]] =
    f match {
      case Summed(ev, _)    => Event.sourcesOf(ev)
      case Latest(ev, _, _) => Event.sourcesOf(ev)
      case Mapped(f, _)     => sourcesOf(f)
      case Zipped(left, right, _) =>
        Monoid[Map[String, Set[Event.Source[_]]]]
          .combine(sourcesOf(left), sourcesOf(right))
    }

  def triggersOf[K, V](f: Feature[K, V]): Event[(K, Unit)] = {
    def loop[W](f: Feature[K, W]): List[Event[(K, Unit)]] =
      f match {
        case Summed(ev, _)    => ev.triggers :: Nil
        case Latest(ev, _, _) => ev.triggers :: Nil
        case Mapped(f, _)     => loop(f)
        case Zipped(l, r, _)  => loop(l) ::: loop(r)
      }

    loop(f).distinct.reduce(_ ++ _)
  }

  def lookupsOf[K, V](f: Feature[K, V]): Set[Event.Lookup[_, _, _]] =
    f match {
      case Summed(ev, _)    => Event.lookupsOf(ev)
      case Latest(ev, _, _) => Event.lookupsOf(ev)
      case Mapped(f, _)     => lookupsOf(f)
      case Zipped(l, r, _)  => lookupsOf(l) | lookupsOf(r)
    }

  case class Summed[K, V](event: Event[(K, V)], monoid: Monoid[V])
      extends Feature[K, V]

  case class Latest[K, W, V](
      event: Event[(K, W)],
      within: Duration,
      cast: Option[W] =:= V
  ) extends Feature[K, V]

  case class Mapped[K, V, W](initial: Feature[K, V], fn: (K, V, Timestamp) => W)
      extends Feature[K, W]

  case class Zipped[K, W, X, Y](
      left: Feature[K, W],
      right: Feature[K, X],
      cast: (W, X) =:= Y
  ) extends Feature[K, Y]
}
