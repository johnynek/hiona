package dev.posco.hiona

import cats.Monoid
import cats.data.NonEmptyList

import scala.concurrent.duration.Duration
import cats.implicits._

/**
 *
 * Build an in-memory runner that requires sorted-by time input events
 * and then plays the events one-by-one updating the state of the features
 * to emit the final events
 */

object Hiona {
  final case class Timestamp(epochMillis: Long)

  object Timestamp {
    implicit val orderingForTimestamp: Ordering[Timestamp] =
      new Ordering[Timestamp] {
        def compare(left: Timestamp, right: Timestamp): Int =
          java.lang.Long.compare(left.epochMillis, right.epochMillis)
      }
  }

  trait Validator[A] {
    def validate(a: A): Either[Validator.Error, Timestamp]
  }

  object Validator {
    sealed trait Error extends Exception

    case class MissingTimestamp[A](from: A) extends Error {
      override def getMessage = s"value $from has a missing timestamp"
    }
  }

  sealed abstract class Event[+A] {
    final def map[B](fn: A => B): Event[B] =
      Event.Mapped(this, fn)

    final def filter(fn: A => Boolean): Event[A] =
      Event.Filtered(this, fn)

    final def concatMap[B](fn: A => Iterable[B]): Event[B] =
      Event.ConcatMapped(this, fn)

    final def withTime: Event[(A, Timestamp)] =
      Event.WithTime(this)

    final def ++[A1 >: A](that: Event[A1]): Event[A1] =
      Event.Concat(this, that)
  }

  object Event {
    sealed trait NonSource[+A] extends Event[A] {
      type Prior
      def previous: Event[Prior]
    }

    def source[A: Row: Validator](name: String): Event[A] =
      Source(name, implicitly[Row[A]], implicitly[Validator[A]])

    def empty[A]: Event[A] = Empty

    case object Empty extends Event[Nothing]
    case class Source[A](name: String, row: Row[A], validator: Validator[A]) extends Event[A]
    case class Concat[A](left: Event[A], right: Event[A]) extends Event[A]


    def sourcesOf[A](ev: Event[A]): Map[String, NonEmptyList[Source[_]]] =
      ev match {
        case src@Source(_, _, _) => Map(src.name -> NonEmptyList(src, Nil))
        case Concat(left, right) =>
          Monoid[Map[String, NonEmptyList[Source[_]]]]
            .combine(sourcesOf(left), sourcesOf(right))
        case Empty => Map.empty
        case ns: NonSource[_] => sourcesOf(ns.previous)
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
  }

  sealed abstract class LookupOrder(val isBefore: Boolean)
  object LookupOrder {
    case object Before extends LookupOrder(true)
    case object After extends LookupOrder(false)
  }

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

    final def values: Event[V] =
      Event.Mapped(ev, Event.Second[V]())
  }

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

    case class Summed[K, V](event: Event[(K, V)], monoid: Monoid[V]) extends Feature[K, V]
    case class Latest[K, V](event: Event[(K, V)]) extends Feature[K, Option[V]]
    case class Mapped[K, V, W](initial: Feature[K, V], fn: (K, V) => W) extends Feature[K, W]
    case class Zipped[K, V, W](left: Feature[K, V], right: Feature[K, W]) extends Feature[K, (V, W)]
  }
}
