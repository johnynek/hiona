package dev.posco.hiona

import cats.{Monoid, Parallel}
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Resource}
import cats.effect.concurrent.Ref
import java.io.PrintWriter
import java.nio.file.Path

import cats.implicits._

import Hiona._

object Engine {
  def run[A: Row](
    inputs: Map[String, Path],
    event: Event[A],
    output: Path)(implicit ctx: ContextShift[IO]): IO[Unit] = {

    Emitter.fromEvent(event).flatMap { emitter =>

      val outRes = Row.writerRes(output)
      val feedRes = Feeder.fromInputs(inputs, event)

      outRes
        .product(feedRes)
        .use { case (writer, feeder) =>

          val batchSize = 1000

          def loop(batch: Seq[Point], seq: Long, out: Iterable[A] => IO[Unit]): IO[Unit] = {
            if (batch.isEmpty) IO.unit
            else {
              val stepWrite =
                for {
                  pair <- emitter.feedAll(batch, seq)
                  (items, nextSeq) = pair
                  _ <- out(items)
                } yield nextSeq

              // we can in parallel read the next batch and do this write
              Parallel.parProduct(feeder.nextBatch(batchSize), stepWrite)
                .flatMap { case (b, nextSeq) =>
                  loop(b, nextSeq, out)
                }
            }
          }

          for {
            batch <- feeder.nextBatch(batchSize)
            _ <- loop(batch, 0L, writer)
          } yield ()
        }
    }
  }

  sealed abstract class Emitter[+O] {
    // TODO:
    // we can index nodes in a graph by some arbitrary order, so we
    // have a 1:1 mapping of Int to Event[_]. Then we can index sources
    // into an array to get a bit better performance here
    // since we can add all the sources into an identically indexed array
    def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[O]]

    final def feedPoint(p: Point, seq: Long): IO[List[O]] =
      p match {
        case Point.Sourced(src, v, ts) => feed(src, v, ts, seq)
      }

    final def feedAll(batch: Iterable[Point], seq: Long): IO[(List[O], Long)] =
      IO.suspend {
        val iter = batch.iterator
        def loop(seq: Long, acc: List[O]): IO[(List[O], Long)] = {
          if (iter.hasNext) {
            feedPoint(iter.next(), seq)
              .flatMap { outs =>
                loop(seq + 1L, outs reverse_::: acc)
              }
          }
          else IO.pure((acc.reverse, seq))
        }

        loop(seq, Nil)
      }
  }

  object Emitter {
    import Impl._

    def fromEvent[A](ev: Event[A]): IO[Emitter[A]] =
      ev match {
        case Event.Empty => IO.pure(EmptyEmitter)
        case src@Event.Source(_, _, _) => IO.pure(SourceEmitter(src))
        case Event.Concat(left, right) =>
          (fromEvent(left), fromEvent(right), Ref.of[IO, (Long, List[A])]((Long.MinValue, Nil)))
            .mapN(ConcatEmitter(_, _, _))
        case Event.WithTime(ev) =>
          fromEvent(ev).map(WithTimeEmitter(_))
        case Event.Mapped(ev, fn) =>
          fromEvent(ev).map(ConcatMapEmitter(_, MapToConcat(fn)))
        case f: Event.Filtered[a] =>
          def go[B1](ev: Event[B1], fn: B1 => Boolean): IO[Emitter[B1]] =
            fromEvent(ev).map(ConcatMapEmitter(_, FilterToConcat(fn)))

          go[a](f.previous, f.fn)
        case Event.ConcatMapped(ev, fn) =>
          fromEvent(ev).map(ConcatMapEmitter(_, fn))
        case Event.ValueWithTime(ev) =>
          fromEvent(ev).map(ValueWithTimeEmitter(_))
        case Event.Lookup(ev, feat, order) =>
          (fromEvent(ev), FeatureState.fromFeature(feat))
            .mapN(LookupEmitter(_, _, order))
      }
  }

  sealed abstract class Reader[K, V] {
    def apply(k: K): V
  }

  object Reader {
    case class FromMap[K, V](toMap: Map[K, V], default: V) extends Reader[K, V] {

      def apply(k: K) =
        toMap.get(k) match {
          case Some(v) => v
          case None => default
        }
    }

    case class FromMapOption[K, V](toMap: Map[K, V]) extends Reader[K, Option[V]] {
      def apply(k: K) = toMap.get(k)
    }

    case class Mapped[K, V, W](r: Reader[K, V], fn: (K, V) => W) extends Reader[K, W] {
      def apply(k: K) = {
        val v = r(k)
        fn(k, v)
      }
    }
    case class Zipped[K, V, W](left: Reader[K, V], right: Reader[K, W]) extends Reader[K, (V, W)] {
      def apply(k: K) = (left(k), right(k))
    }
  }

  sealed abstract class FeatureState[K, V] {
    // consume these events and return the full state before and after these events
    def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[(Reader[K, V], Reader[K, V])]
  }

  object FeatureState {
    import Impl._

    def fromFeature[K, V](f: Feature[K, V]): IO[FeatureState[K, V]] =
      f match {
        case Feature.Summed(ev, monoid) =>
          (Emitter.fromEvent(ev),
            Ref.of[IO, (Long, Map[K, V], Map[K, V])](
              (Long.MinValue, Map.empty, Map.empty)))
            .mapN(SummedFS(_, _, monoid))

        case f@Feature.Latest(_) =>
          def go[B](l: Feature.Latest[K, B]): IO[FeatureState[K, Option[B]]] =
            (Emitter.fromEvent(l.event),
              Ref.of[IO, (Long, Map[K, B], Map[K, B])](
                (Long.MinValue, Map.empty, Map.empty)))
              .mapN(LatestFS[K, B](_, _))

          go(f)
        case Feature.Mapped(feat, fn) =>
          fromFeature(feat).map(MappedFS(_, fn))
        case Feature.Zipped(left, right) =>
          (fromFeature(left), fromFeature(right)).mapN(ZippedFS(_, _))
      }
  }


  private object Impl {
    case class MapToConcat[A, B](fn: A => B) extends Function1[A, Iterable[B]] {
      def apply(a: A): Iterable[B] = fn(a) :: Nil
    }

    case class FilterToConcat[A](fn: A => Boolean) extends Function1[A, Iterable[A]] {
      def apply(a: A): Iterable[A] = if (fn(a)) a :: Nil else Nil
    }

    case object EmptyEmitter extends Emitter[Nothing] {
      val ioNil: IO[List[Nothing]] = IO.pure(Nil)

      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[Nothing]] =
        ioNil
    }

    case class SourceEmitter[A](ev: Event.Source[A]) extends Emitter[A] {
      val ioNil: IO[List[A]] = IO.pure(Nil)

      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[A]] =
        if (from.name == ev.name) {
          // Assume previous checks means I == A
          IO(item.asInstanceOf[A] :: Nil)
        }
        else ioNil
    }

    case class ConcatEmitter[A](left: Emitter[A], right: Emitter[A], ref: Ref[IO, (Long, List[A])]) extends Emitter[A] {

      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[A]] = {
        val run = (left.feed(from, item, ts, seq), right.feed(from, item, ts, seq)).mapN(_ ::: _)

        for {
          ((seq0, as), set) <-ref.access
          res <- if (seq0 == seq) IO.pure(as) else run.flatMap { res => set((seq, res)).as(res) }
        } yield res
      }
    }

    case class WithTimeEmitter[A](prev: Emitter[A]) extends Emitter[(A, Timestamp)] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[(A, Timestamp)]] =
        prev.feed(from, item, ts, seq).map(_.map { a => (a, ts) })
    }

    case class ValueWithTimeEmitter[A, B](prev: Emitter[(A, B)]) extends Emitter[(A, (B, Timestamp))] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[(A, (B, Timestamp))]] =
        prev.feed(from, item, ts, seq).map(_.map { case (a, b) => (a, (b, ts)) })
    }

    case class ConcatMapEmitter[A, B](prev: Emitter[A], fn: A => Iterable[B]) extends Emitter[B] {
      // we could cache here, but maybe not needed
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[B]] =
        prev.feed(from, item, ts, seq).map(_.flatMap(fn))
    }

    case class LookupEmitter[K, V, W](ev: Emitter[(K, V)], fs: FeatureState[K, W], order: LookupOrder) extends Emitter[(K, (V, W))] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long): IO[List[(K, (V, W))]] =
        (ev.feed(from, item, ts, seq), fs.feed(from, item, ts, seq))
          .mapN { case (evs, (before, after)) =>
            val reader = if (order.isBefore) before else after

            evs.map { case (k, v) =>
              (k, (v, reader(k)))
            }
          }
    }

    case class SummedFS[K, V](
      emitter: Emitter[(K, V)],
      state: Ref[IO, (Long, Map[K, V], Map[K, V])],
      monoid: Monoid[V]) extends FeatureState[K, V] {

      private val empty = monoid.empty

      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long) =
        state
          .access
          .flatMap { case ((seq0, before, after), set) =>
            if (seq0 < seq) {
              // we need to process this
              emitter.feed(from, item, ts, seq)
                .flatMap { kvs =>
                  val after1 = kvs.foldLeft(after) { case (state, (k, v)) =>
                    state.get(k) match {
                      case Some(v0) => state.updated(k, monoid.combine(v0, v))
                      case None => state.updated(k, v)
                    }
                  }

                  set((seq, after, after1))
                    .flatMap {
                      case true =>
                        IO.pure((Reader.FromMap(after, empty), Reader.FromMap(after1, empty)))
                      case false =>
                        // loop:
                        feed(from, item, ts, seq)
                    }
                }
            }
            else {
              // we have already processed this event
              IO.pure((Reader.FromMap(before, empty), Reader.FromMap(after, empty)))
            }
          }

    }

    case class LatestFS[K, V](
      emitter: Emitter[(K, V)],
      state: Ref[IO, (Long, Map[K, V], Map[K, V])]) extends FeatureState[K, Option[V]] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long) =
        state
          .access
          .flatMap { case ((seq0, before, after), set) =>
            if (seq0 < seq) {
              // we need to process this
              emitter.feed(from, item, ts, seq)
                .flatMap { kvs =>
                  val after1 = after ++ kvs

                  set((seq, after, after1))
                    .flatMap {
                      case true =>
                        IO.pure((Reader.FromMapOption(after), Reader.FromMapOption(after1)))
                      case false =>
                        // loop:
                        feed(from, item, ts, seq)
                    }
                }
            }
            else {
              // we have already processed this event
              IO.pure((Reader.FromMapOption(before), Reader.FromMapOption(after)))
            }
          }
    }

    case class MappedFS[K, V, W](fs: FeatureState[K, V], fn: (K, V) => W) extends FeatureState[K, W] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long) =
        fs.feed(from, item, ts, seq)
          .map { case (before, after) =>
            (Reader.Mapped(before, fn), Reader.Mapped(after, fn))
          }
    }
    case class ZippedFS[K, V, W](left: FeatureState[K, V], right: FeatureState[K, W]) extends FeatureState[K, (V, W)] {
      def feed[I](from: Event.Source[I], item: I, ts: Timestamp, seq: Long) =
        (left.feed(from, item, ts, seq), right.feed(from, item, ts, seq))
          .mapN { case ((bv, av), (bw, aw)) =>
            (Reader.Zipped(bv, bw), Reader.Zipped(av, aw))
          }
    }
  }
}
