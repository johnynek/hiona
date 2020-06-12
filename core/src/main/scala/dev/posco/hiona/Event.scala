package dev.posco.hiona

import cats.Monoid
import cats.implicits._

/**
  * Represents the stream of events of value A which also have an associated Timestamp.
  *
  * This will correspond to a log of things that happen at a specific time. They can
  * be source events, transformed in some way, or joined with features via lookupBefore/lookupAfter
  */
sealed abstract class Event[+A] {
  final def map[B](fn: A => B): Event[B] =
    Event.Mapped(this, fn)

  final def filter(fn: A => Boolean): Event[A] =
    Event.Filtered(this, fn, implicitly[A =:= A])

  final def concatMap[B](fn: A => Iterable[B]): Event[B] =
    Event.ConcatMapped(this, fn)

  /**
    * For each event, attach the timestamp it occurs at
    */
  final def withTime: Event[(A, Timestamp)] =
    Event.WithTime(this, implicitly[(A, Timestamp) =:= (A, Timestamp)])

  /**
    * Merge two event streams together
    */
  final def ++[A1 >: A](that: Event[A1]): Event[A1] =
    Event.Concat(this, that)

  /**
    * convert this event value to a tuple with the current value
    * as the key. Useful for doing pre/postLookup
    */
  final def asKeys: Event[(A, Unit)] =
    map(Event.ToKey())

  /**
    * convenience function for applying resuable transformations
    * to inputs:
    *
    * pipe
    *   .through(dedup)
    *   .through(attachCounts)
    *
    * where dedup and attachCounts can be some library functions
    * you keep to apply to many different inputs.
    */
  final def through[B](fn: Event.Pipe[A, B]): Event[B] =
    fn(this)

  /** attach a key from the event
    */
  final def keyBy[K](fn: A => K): Event[(K, A)] =
    map(Event.KeyBy(fn))

}

object Event {
  type Keyed[A, B] = Event[(A, B)]
  type Pipe[-A, +B] = Function1[Event[A], Event[B]]

  sealed trait NonSource[+A] extends Event[A] {
    type Prior
    def previous: Event[Prior]
  }

  /**
    * @param name a unique name for a given source. To run, this name has to be connected to an input path
    * @param validator the validator to check each input value and extract the timestamp
    */
  def source[A: Row](name: String, validator: Validator[A]): Event.Source[A] =
    Source(name, implicitly[Row[A]], validator)

  def empty[A]: Event[A] = Empty

  /**
    * this are methods that use the type parameter A in an invariant way, which
    * cannot be methods since we do want the type to be covariant on Event
    */
  implicit class InvariantEventMethods[A](private val self: Event[A])
      extends AnyVal {

    /** shortcut for keyBy(fn).latest
      */
    def latestBy[K](fn: A => K): Feature[K, Option[A]] =
      self.keyBy(fn).latest

    /** shortcut for keyBy(fn).sum
      */
    def sumBy[K](fn: A => K)(implicit m: Monoid[A]): Feature[K, A] =
      self.keyBy(fn).sum
  }

  /**
    * These are extension methods for events of tuples, which are common.
    * Many of the most useful methods are here (e.g. .sum, .latest, .max and lookupBefore/lookupAfter
    */
  implicit class KeyedEvent[K, V](private val ev: Event[(K, V)])
      extends AnyVal {

    /**
      * When Feature `that` depends on Event `ev`, the event will trigger a change to the feature.
      * preLookup gives you the feature value before the event at timestamp
      * has been processed
      */
    final def preLookup[W](that: Feature[K, W]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, LookupOrder.Before)

    /**
      * When Feature `that` depends on Event `ev`, the event will trigger a change to the feature.
      * postLookup gives you the feature value after the event at timestamp
      * has been processed
      */
    final def postLookup[W](that: Feature[K, W]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, LookupOrder.After)

    final def valueWithTime: Event[(K, (V, Timestamp))] =
      Event.ValueWithTime(
        ev,
        implicitly[(K, (V, Timestamp)) =:= (K, (V, Timestamp))]
      )

    final def sum(implicit m: Monoid[V]): Feature[K, V] =
      Feature.Summed(ev, m)

    final def latest: Feature[K, Option[V]] =
      Feature.Latest(ev, Duration.Infinite, implicitly[Option[V] =:= Option[V]])

    final def latestWithin(
        dur: Duration
    ): Feature[K, Option[V]] =
      Feature.Latest(ev, dur, implicitly[Option[V] =:= Option[V]])

    /**
      * Build a window, typically you would derive some statistic from the window,
      * e.g. maximum value over 10 minutes, or the max - min, sum, median, etc...
      */
    final def window[W <: Duration](implicit
        v: ValueOf[W],
        o: Ordering[V]
    ): Feature[K, Option[TimeWindow[W, V]]] = {
      implicit val catsOrd = cats.Order.fromOrdering(o)
      ev.valueWithTime.mapValues {
        case (v, ts) => Option(TimeWindow.single[W, V]((ts, v)))
      }.sum
    }

    /**
      * Compute a sum over a specific window of time.
      */
    final def windowSum(
        dur: Duration
    )(implicit o: Ordering[V], m: Monoid[V]): Feature[K, V] = {
      implicit val catsOrd = cats.Order.fromOrdering(o)
      window[dur.type].map {
        case None => m.empty
        case Some(win) =>
          m.combineAll(win.toList.iterator.map { case (_, v) => v })
      }
    }

    /**
      * Does a sum from "now" until W in the future
      */
    final def sumForward(
        dur: Duration
    )(implicit o: Ordering[V], m: Monoid[V]): Label[K, V] =
      Label(windowSum(dur)).lookForward(dur)

    final def mapValues[W](fn: V => W): Event[(K, W)] =
      ev.map(Event.MapValuesFn(fn, implicitly[(K, V) <:< (K, V)]))

    final def max(implicit ord: Ordering[V]): Feature[K, Option[V]] = {
      val ev1: Event[(K, Option[V])] = ev.mapValues(Event.ToSome())
      ev1.sum(MaxMonoid(ord))
    }

    final def min(implicit ord: Ordering[V]): Feature[K, Option[V]] = {
      val ev1: Event[(K, Option[V])] = ev.mapValues(Event.ToSome())
      ev1.sum(MaxMonoid(ord.reverse))
    }

    /**
      * discard a key, this is used when looking up a feature on one aspect
      * of an event before applying it to another: e.g.
      * we might lookup the total volume of trades on one symbol, but then
      * use it as a feature for another symbol.
      */
    final def values: Event[V] =
      Event.Mapped(ev, Event.Second[V]())

    /**
      * discard any trailing lookups and just get the events
      * that would trigger a subsequent lookup
      *
      * Useful discard the values without
      * rekeying the event stream.
      */
    final def triggers: Event[(K, Unit)] =
      Event.triggersOf(ev)
  }

  case object Empty extends Event[Nothing]
  case class Source[A](name: String, row: Row[A], validator: Validator[A])
      extends Event[A]

  object Source {
    def equiv[A, B](srcA: Source[A], srcB: Source[B]): Option[A =:= B] =
      if (srcA == srcB)
        // since Source is invariant, and depends on invariant typeclass
        // we assume this implies the types are equal
        Some(implicitly[A =:= A].asInstanceOf[A =:= B])
      else None
  }

  case class Concat[A](left: Event[A], right: Event[A]) extends Event[A]

  /**
    * List all the sources required by a given Event. Used by the engine to plan and run
    * not useful for end-users making features.
    */
  def sourcesOf[A](ev: Event[A]): Map[String, Set[Source[_]]] =
    ev match {
      case src @ Source(_, _, _) => Map(src.name -> Set(src))
      case Concat(left, right) =>
        Monoid[Map[String, Set[Source[_]]]]
          .combine(sourcesOf(left), sourcesOf(right))
      case Empty => Map.empty
      case Lookup(ev, f, _) =>
        Monoid[Map[String, Set[Source[_]]]]
          .combine(sourcesOf(ev), Feature.sourcesOf(f))
      case ns: NonSource[_] => sourcesOf(ns.previous)
    }

  def lookupsOf[A](ev: Event[A]): Set[Lookup[_, _, _]] =
    ev match {
      case Source(_, _, _) | Empty => Set.empty
      case Concat(left, right) =>
        lookupsOf(left) | lookupsOf(right)
      case l @ Lookup(ev, f, _) =>
        (lookupsOf(ev) | Feature.lookupsOf(f)) + l
      case ns: NonSource[_] => lookupsOf(ns.previous)
    }

  private[hiona] def triggersList[A](
      ev: Event[(A, Any)]
  ): List[Event[(A, Any)]] = {
    def loop(ev: Event[(A, Any)]): List[Event[(A, Any)]] =
      ev match {
        case Empty                 => Nil
        case src @ Source(_, _, _) => src :: Nil
        case Concat(left, right) =>
          loop(left) ::: loop(right)
        case Lookup(ev, _, _)                     => loop(ev)
        case Mapped(ev, MapValuesFn(_, evidence)) =>
          // if we only change the values, we can
          // skip this and possibly remove more lookups
          val tupleEv: Event[(A, Any)] = evidence.liftCo[Event](ev)
          loop(tupleEv)
        case v @ ValueWithTime(_, _) =>
          // We can remove the value with time because it doesn't change the key
          // this is complex to get the compiler to see the types line up
          def go[K, V, W <: (A, Any)](
              vt: ValueWithTime[K, V, W]
          ): List[Event[(A, Any)]] = {
            val ev0: Event[(K, V)] = vt.event
            val ev1: Event[(K, (V, Timestamp))] =
              // we add a mapValues, but this will be removed by looping:
              ev0.mapValues(v => (v, Timestamp.MinValue))

            // now  we can convert (K, (V, Timestamp)) to W
            val ev2: Event[W] = vt.cast.liftCo[Event](ev1)
            // widen using covariance
            val ev3: Event[(A, Any)] = ev2
            // now loop without the ValueWithTime on there
            loop(ev3)
          }
          go(v)
        case ns: NonSource[_] =>
          ns :: Nil
      }

    loop(ev)
  }

  private[hiona] def unitValues[A](
      ls: List[Event[(A, Any)]]
  ): Event[(A, Unit)] = {
    def keys[K](ev: Event[(K, Any)]): Event[(K, Unit)] =
      ev.mapValues(Feature.ConstFn(()))

    ls.distinct
      .reduceOption(_ ++ _)
      .map(keys)
      .getOrElse(Empty)
  }

  /**
    * return an event that just signals when the keys change
    */
  def triggersOf[A](ev: Event[(A, Any)]): Event[(A, Unit)] =
    unitValues(triggersList(ev))

  def amplificationOf[A](ev: Event[A]): Amplification =
    ev match {
      case Empty           => Amplification.Zero
      case Source(_, _, _) => Amplification.One
      case Concat(left, right) =>
        amplificationOf(left) + amplificationOf(right)
      case Lookup(ev, _, _) => amplificationOf(ev)
      case ConcatMapped(ev, _) =>
        amplificationOf(ev) * Amplification.ZeroOrMore
      case Filtered(ev, _, _) =>
        amplificationOf(ev) * Amplification.ZeroOrOne
      case ns: NonSource[_] =>
        // otherwise it is 1 in 1 out
        amplificationOf(ns.previous)
    }

  case class Mapped[A, B](init: Event[A], fn: A => B) extends NonSource[B] {
    type Prior = A
    def previous: Event[A] = init
  }
  case class Filtered[A, B](init: Event[A], fn: A => Boolean, cast: A =:= B)
      extends NonSource[B] {
    type Prior = A
    def previous: Event[A] = init
  }
  case class ConcatMapped[A, B](init: Event[A], fn: A => Iterable[B])
      extends NonSource[B] {
    type Prior = A
    def previous: Event[A] = init
  }
  case class WithTime[A, B](init: Event[A], cast: (A, Timestamp) =:= B)
      extends NonSource[B] {
    type Prior = A
    def previous: Event[A] = init
  }

  case class Lookup[K, V, W](
      event: Event[(K, V)],
      feature: Feature[K, W],
      order: LookupOrder
  ) extends Event[(K, (V, W))]

  case class ValueWithTime[K, V, W](
      event: Event[(K, V)],
      cast: (K, (V, Timestamp)) =:= W
  ) extends NonSource[W] {
    type Prior = (K, V)
    def previous: Event[(K, V)] = event
  }

  case class Second[A]() extends Function[(Any, A), A] {
    def apply(kv: (Any, A)): A = kv._2
  }

  case class ToSome[A]() extends Function[A, Some[A]] {
    def apply(a: A): Some[A] = Some(a)
  }

  case class MapValuesFn[A0, A, B, C](fn: B => C, ev: A0 <:< (A, B))
      extends Function[A0, (A, C)] {
    def apply(a0: A0): (A, C) = {
      val ab = ev(a0)
      (ab._1, fn(ab._2))
    }
  }

  case class ToKey[A]() extends Function[A, (A, Unit)] {
    def apply(a: A): (A, Unit) = (a, ())
  }

  case class KeyBy[A, B](fn: A => B) extends Function[A, (B, A)] {
    def apply(a: A): (B, A) = (fn(a), a)
  }

}

/**
  * A type to prevent "boolean blindness" to specify before or after ordering
  */
sealed abstract class LookupOrder(val isBefore: Boolean) {
  def isAfter: Boolean = !isBefore
}
object LookupOrder {
  case object Before extends LookupOrder(true)
  case object After extends LookupOrder(false)
}

case class MaxMonoid[A](ord: Ordering[A]) extends Monoid[Option[A]] {
  def empty: Option[Nothing] = None
  def combine(left: Option[A], right: Option[A]): Option[A] =
    (left, right) match {
      case (None, r) => r
      case (l, None) => l
      case (sl @ Some(l), sr @ Some(r)) =>
        if (ord.compare(l, r) > 0) sl
        else sr
    }
}
