package dev.posco.hiona


/**
 *
 * Build an in-memory runner that requires sorted-by time input events
 * and then plays the events one-by-one updating the state of the features
 * to emit the final events
 */

object Hiona {
  // read/write CSV/TSV
  sealed trait Row[A] {
    def columns: Int
    def writeToStrings(a: A, offset: Int, dest: Array[String]): Unit
    def fromStrings(s: Seq[String]): Option[A]
    def zip[B](that: Row[B]): Row[(A, B)] = ???
    def imap[B](toFn: A => Option[B])(fromFn: B => A): Row[B] = ???
  }

  object Row {
    // we know that at least one of columns is non-empty for all values
    sealed trait NonEmptyRow[A] extends Row[A]

    implicit case object UnitRow extends Row[Unit] {
      val someUnit: Some[Unit] = Some(())

      def columns = 0
      def writeToStrings(a: Unit, offset: Int, dest: Array[String]) = ()
      def fromStrings(s: Seq[String]): Option[Unit] = someUnit
    }
  }

  sealed trait Timestamp
  sealed trait Duration
  sealed trait Monoid[A] {
    def empty: A
    def combine(left: A, right: A): A
  }

  object Monoid {
    implicit case object UnitMonoid extends Monoid[Unit] {
      def empty = ()
      def combine(left: Unit, right: Unit) = empty
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
  }

  object Event {
    sealed trait NonSource[A] extends Event[A] {
      type Prior
      def previous: Event[Prior]
    }

    def source[A: Row](name: String): Event[A] =
      Source(name, implicitly[Row[A]])

    def empty[A]: Event[A] = Empty

    case object Empty extends Event[Nothing]
    case class Source[A](name: String, row: Row[A]) extends Event[A]

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
      rowV: Row[V],
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
    final def lookupAfter[W](that: Feature[K, W])(implicit rv: Row[V]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, rv, LookupOrder.After)

    final def lookupBefore[W](that: Feature[K, W])(implicit rv: Row[V]): Event[(K, (V, W))] =
      Event.Lookup(ev, that, rv, LookupOrder.Before)

    final def valueWithTime: Event[(K, (V, Timestamp))] =
      Event.ValueWithTime(ev)

    final def sum(implicit k: Row[K], v: Row[V], m: Monoid[V]): Feature[K, V] =
      Feature.Summed(k, v, ev, m)

    // TODO: Row[Option[V]] is subtle. Probably want some notion of non-nullable Rows
    final def latest(within: Duration)(implicit k: Row[K], v: Row[Option[V]]): Feature[K, Option[V]] =
      Feature.Latest(k, v, ev)

    final def values: Event[V] =
      Event.Mapped(ev, Event.Second[V]())
  }

  sealed abstract class Feature[K, V] {
    def keyRow: Row[K]
    def valueRow: Row[V]

    final def zip[W](that: Feature[K, W]): Feature[K, (V, W)] =
      Feature.Zipped(this, that)

    final def map[W: Row](fn: V => W): Feature[K, W] =
      mapWithKey(Feature.ValueMap(fn))

    final def mapWithKey[W: Row](fn: (K, V) => W): Feature[K, W] =
      Feature.Mapped(implicitly[Row[W]], this, fn)
  }

  object Feature {
    case class ValueMap[V, W](fn: V => W) extends Function2[Any, V, W] {
      def apply(k: Any, v: V): W = fn(v)
    }
    case class ConstFn[A](result: A) extends Function[Any, A] {
      def apply(a: Any) = result
    }

    def const[K, V](v: V)(implicit rk: Row[K], rv: Row[V]): Feature[K, V] =
      Event.empty[(K, Unit)].sum.map(ConstFn(v))

    case class Summed[K, V](keyRow: Row[K], valueRow: Row[V], event: Event[(K, V)], monoid: Monoid[V]) extends Feature[K, V]
    case class Latest[K, V](keyRow: Row[K], valueRow: Row[Option[V]], event: Event[(K, V)]) extends Feature[K, Option[V]]

    case class Mapped[K, V, W](valueRow: Row[W], initial: Feature[K, V], fn: (K, V) => W) extends Feature[K, W] {
      def keyRow = initial.keyRow
    }

    case class Zipped[K, V, W](left: Feature[K, V], right: Feature[K, W]) extends Feature[K, (V, W)] {
      def keyRow = left.keyRow
      val valueRow = left.valueRow.zip(right.valueRow)
    }
  }
}
