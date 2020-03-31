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
    Event.Filtered(this, fn)

  final def concatMap[B](fn: A => Iterable[B]): Event[B] =
    Event.ConcatMapped(this, fn)

  /**
   * For each event, attach the timestamp it occurs at
   */
  final def withTime: Event[(A, Timestamp)] =
    Event.WithTime(this)

  /**
   * Merge two event streams together
   */
  final def ++[A1 >: A](that: Event[A1]): Event[A1] =
    Event.Concat(this, that)
}

object Event {
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
   * These are extension methods for events of tuples, which are common.
   * Many of the most useful methods are here (e.g. .sum, .latest, .max and lookupBefore/lookupAfter
   */
  implicit class KeyedEvent[K, V](val ev: Event[(K, V)]) extends AnyVal {
    final def lookupAfter[W](that: Feature[K, W]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, LookupOrder.After)

    final def lookupBefore[W](that: Feature[K, W]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, LookupOrder.Before)

    final def valueWithTime: Event[(K, (V, Timestamp))] =
      Event.ValueWithTime(ev)

    final def sum(implicit m: Monoid[V]): Feature[K, V] =
      Feature.Summed(ev, m)

    final def latest(within: Duration): Feature[K, Option[V]] =
      Feature.Latest(ev)

    final def mapValues[W](fn: V => W): Event[(K, W)] =
      ev.map(Event.MapValuesFn(fn))

    final def max(implicit ord: Ordering[V]): Feature[K, Option[V]] = {
      val ev1: Event[(K, Option[V])] = ev.mapValues(Event.ToSome())
      ev1.sum(MaxMonoid(ord))
    }

    // discard a key, this is used when looking up a feature on one aspect
    // of an event before applying it to another: e.g.
    // we might lookup the total volume of trades on one symbol, but then
    // use it as a feature for another symbol.
    final def values: Event[V] =
      Event.Mapped(ev, Event.Second[V]())
  }

  case object Empty extends Event[Nothing]
  case class Source[A](name: String, row: Row[A], validator: Validator[A]) extends Event[A]
  case class Concat[A](left: Event[A], right: Event[A]) extends Event[A]


  /**
   * List all the sources required by a given Event. Used by the engine to plan and run
   * not useful for end-users making features.
   */
  def sourcesOf[A](ev: Event[A]): Map[String, Set[Source[_]]] =
    ev match {
      case src@Source(_, _, _) => Map(src.name -> Set(src))
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
      case l@Lookup(ev, f, _) =>
        (lookupsOf(ev) | Feature.lookupsOf(f)) + l
      case ns: NonSource[_] => lookupsOf(ns.previous)
    }

  case class Mapped[A, B](init: Event[A], fn: A => B) extends NonSource[B] {
    type Prior = A
    def previous = init
  }
  case class Filtered[A](init: Event[A], fn: A => Boolean) extends NonSource[A] {
    type Prior = A
    def previous = init
  }
  case class ConcatMapped[A, B](init: Event[A], fn: A => Iterable[B]) extends NonSource[B] {
    type Prior = A
    def previous = init
  }
  case class WithTime[A](init: Event[A]) extends NonSource[(A, Timestamp)] {
    type Prior = A
    def previous = init
  }

  case class Lookup[K, V, W](
    event: Event[(K, V)],
    feature: Feature[K, W],
    order: LookupOrder) extends NonSource[(K, (V, W))] {
    type Prior = (K, V)
    def previous = event
  }

  case class ValueWithTime[K, V](event: Event[(K, V)]) extends NonSource[(K, (V, Timestamp))] {
    type Prior = (K, V)
    def previous = event
  }

  case class Second[A]() extends Function[(Any, A), A] {
    def apply(kv: (Any, A)) = kv._2
  }

  case class ToSome[A]() extends Function[A, Some[A]] {
    def apply(a: A) = Some(a)
  }

  case class MapValuesFn[A, B, C](fn: B => C) extends Function[(A, B), (A, C)] {
    def apply(ab: (A, B)) = (ab._1, fn(ab._2))
  }

}

/**
 * A type to prevent "boolean blindness" to specify before or after ordering
 */
sealed abstract class LookupOrder(val isBefore: Boolean)
object LookupOrder {
  case object Before extends LookupOrder(true)
  case object After extends LookupOrder(false)
}

case class MaxMonoid[A](ord: Ordering[A]) extends Monoid[Option[A]] {
  def empty = None
  def combine(left: Option[A], right: Option[A]): Option[A] =
    (left, right) match {
      case (None, r) => r
      case (l, None) => l
      case (sl@Some(l), sr@Some(r)) =>
        if (ord.compare(l, r) > 0) sl
        else sr
    }
}
