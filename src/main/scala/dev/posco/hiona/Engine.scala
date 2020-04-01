package dev.posco.hiona

import cats.{Monoid, Parallel}
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Resource}
import cats.effect.concurrent.Ref
import java.io.PrintWriter
import java.nio.file.Path

import cats.implicits._

/**
  * This is a collection of tools to run Events and LabeledEvents into
  * a List of values, and some helper functions to make it easy
  * to write those events out
  */
object Engine {

  private def runEmitter[A: Row](
      feedRes: Resource[IO, Feeder],
      emitter: Emitter[A],
      output: Path
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    Row
      .writerRes(output)
      .product(feedRes)
      .use {
        case (writer, feeder) =>
          Emitter.run(feeder, batchSize = 1000, emitter)(writer)
      }

  def run[A: Row](inputs: Map[String, Path], event: Event[A], output: Path)(
      implicit ctx: ContextShift[IO]
  ): IO[Unit] =
    Emitter
      .fromEvent(event)
      .flatMap(runEmitter(Feeder.fromInputs(inputs, event), _, output))

  def runLabeled[A: Row, B: Row](
      inputs: Map[String, Path],
      labeled: LabeledEvent[A, B],
      output: Path
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    Emitter
      .fromLabeledEvent(labeled)
      .flatMap(runEmitter(Feeder.fromInputsLabels(inputs, labeled), _, output))

  /**
    * An emitter is a something that can process an input event,
    * from any of the Event.Sources, and spit out 0 or more events
    * in response. These will typically be written out to a file
    * or stored in some way, but that is out of scope
    * for an Emitter.
    */
  sealed abstract class Emitter[+O] {

    /**
      * This is the list of source names and Duration offsets this
      * emitter cares about. As an optimization, we can just ignore
      * any event from a source/offset not in this set.
      */
    def consumes: Set[(String, Duration)]
    // TODO:
    // we can index nodes in a graph by some arbitrary order, so we
    // have a 1:1 mapping of Int to Event[_]. Then we can index sources
    // into an array to get a bit better performance here
    // since we can add all the sources into an identically indexed array
    def feed(p: Point, seq: Long): IO[List[O]]

    final def feedAll(batch: Iterable[Point], seq: Long): IO[(List[O], Long)] =
      IO.suspend {
        val iter = batch.iterator
        def loop(seq: Long, acc: List[O]): IO[(List[O], Long)] =
          if (iter.hasNext) {
            feed(iter.next(), seq)
              .flatMap(outs => loop(seq + 1L, outs reverse_::: acc))
          } else IO.pure((acc.reverse, seq))

        loop(seq, Nil)
      }
  }

  object Emitter {

    def run[A](feeder: Feeder, batchSize: Int, emitter: Emitter[A])(
        writer: Iterable[A] => IO[Unit]
    )(implicit ctx: ContextShift[IO]): IO[Unit] = {

      def loop(
          batch: Seq[Point],
          seq: Long,
          writes: Option[Iterable[A]]
      ): IO[Unit] = {
        val wRes = writes.fold(IO.unit)(writer)
        if (batch.isEmpty) wRes
        else {
          // we can in parallel read the next batch and do this write
          Parallel
            .parProduct(
              Parallel.parProduct(
                feeder.nextBatch(batchSize),
                emitter.feedAll(batch, seq)
              ),
              wRes
            )
            .flatMap {
              case ((b, (items, nextSeq)), _) =>
                loop(b, nextSeq, Some(items))
            }
        }
      }

      for {
        batch <- feeder.nextBatch(batchSize)
        _ <- loop(batch, 0L, None)
      } yield ()
    }

    // A writer useful for testing
    def listWriter[A]: IO[(Iterable[A] => IO[Unit], IO[List[A]])] =
      Ref
        .of[IO, List[A]](Nil)
        .map {
          case ref =>
            val write = { (it: Iterable[A]) =>
              ref.update { lst =>
                it.foldLeft(lst)((tail, head) => head :: tail)
              }
            }

            val read = ref.get.map(_.reverse)

            (write, read)
        }

    def runToList[A](feeder: Feeder, batchSize: Int, emitter: Emitter[A])(
        implicit ctx: ContextShift[IO]
    ): IO[List[A]] =
      listWriter[A].flatMap {
        case (write, read) =>
          run(feeder, batchSize, emitter)(write)
            .flatMap(_ => read)
      }

    def fromEvent[A](ev: Event[A]): IO[Emitter[A]] =
      Impl.fromEvent(ev, Duration.Zero)

    def fromLabeledEvent[K, V](le: LabeledEvent[K, V]): IO[Emitter[(K, V)]] =
      le match {
        case LabeledEvent.WithLabel(ev, label) =>
          (fromEvent(ev), FeatureState.fromLabel(label))
            .mapN(Impl.LookupEmitter(_, _, LookupOrder.After))
        case m @ LabeledEvent.Mapped(_, _) =>
          def go[K, V, W](
              m: LabeledEvent.Mapped[K, V, W]
          ): IO[Emitter[(K, W)]] =
            fromLabeledEvent(m.labeled).map(
              Impl.ConcatMapEmitter(
                _,
                Impl.MapToConcat(Event.MapValuesFn[K, V, W](m.fn))
              )
            )

          go(m)
        case LabeledEvent.Filtered(ev, fn) =>
          fromLabeledEvent(ev).map(
            Impl.ConcatMapEmitter(_, Impl.FilterToConcat(Impl.Fn2To1(fn)))
          )
      }
  }

  sealed abstract class Reader[K, V] {
    def apply(k: K): V
  }

  object Reader {
    case class FromMap[K, V](toMap: Map[K, V], default: V)
        extends Reader[K, V] {

      def apply(k: K) =
        toMap.get(k) match {
          case Some(v) => v
          case None    => default
        }
    }

    case class FromMapOption[K, V](toMap: Map[K, V])
        extends Reader[K, Option[V]] {
      def apply(k: K) = toMap.get(k)
    }

    case class Mapped[K, V, W](r: Reader[K, V], fn: (K, V) => W)
        extends Reader[K, W] {
      def apply(k: K) = {
        val v = r(k)
        fn(k, v)
      }
    }
    case class Zipped[K, V, W](left: Reader[K, V], right: Reader[K, W])
        extends Reader[K, (V, W)] {
      def apply(k: K) = (left(k), right(k))
    }
  }

  sealed abstract class FeatureState[K, V] {
    def consumes: Set[(String, Duration)]
    // consume these events and return the full state before and after these events
    def feed(point: Point, seq: Long): IO[(Reader[K, V], Reader[K, V])]
  }

  object FeatureState {
    def fromFeature[K, V](f: Feature[K, V]): IO[FeatureState[K, V]] =
      Impl.fromFeature(f, Duration.Zero)

    def fromLabel[K, V](l: Label[K, V]): IO[FeatureState[K, V]] =
      Impl.fromLabel(l, Duration.Zero)
  }

  private object Impl {
    def fromEvent[A](ev: Event[A], offset: Duration): IO[Emitter[A]] =
      ev match {
        case Event.Empty                 => IO.pure(EmptyEmitter)
        case src @ Event.Source(_, _, _) => IO.pure(SourceEmitter(src, offset))
        case Event.Concat(left, right) =>
          (
            fromEvent(left, offset),
            fromEvent(right, offset),
            Ref.of[IO, (Long, List[A])]((Long.MinValue, Nil))
          ).mapN(ConcatEmitter(_, _, _))
        case Event.WithTime(ev) =>
          fromEvent(ev, offset).map(WithTimeEmitter(_))
        case Event.Mapped(ev, fn) =>
          fromEvent(ev, offset).map(ConcatMapEmitter(_, MapToConcat(fn)))
        case f: Event.Filtered[a] =>
          def go[B1](ev: Event[B1], fn: B1 => Boolean): IO[Emitter[B1]] =
            fromEvent(ev, offset).map(ConcatMapEmitter(_, FilterToConcat(fn)))

          go[a](f.previous, f.fn)
        case Event.ConcatMapped(ev, fn) =>
          fromEvent(ev, offset).map(ConcatMapEmitter(_, fn))
        case Event.ValueWithTime(ev) =>
          fromEvent(ev, offset).map(ValueWithTimeEmitter(_))
        case Event.Lookup(ev, feat, order) =>
          (fromEvent(ev, offset), fromFeature(feat, offset))
            .mapN(LookupEmitter(_, _, order))
      }

    def fromFeature[K, V](
        f: Feature[K, V],
        offset: Duration
    ): IO[FeatureState[K, V]] =
      f match {
        case Feature.Summed(ev, monoid) =>
          (
            fromEvent(ev, offset),
            Ref.of[IO, (Long, Map[K, V], Map[K, V])](
              (Long.MinValue, Map.empty, Map.empty)
            )
          ).mapN(SummedFS(_, _, monoid))

        case f @ Feature.Latest(_, _) =>
          // TODO make use of the window duration to prune old events from the state
          def go[B](l: Feature.Latest[K, B]): IO[FeatureState[K, Option[B]]] =
            (
              fromEvent(l.event, offset),
              Ref.of[IO, (Long, Map[K, B], Map[K, B])](
                (Long.MinValue, Map.empty, Map.empty)
              )
            ).mapN(LatestFS[K, B](_, _))

          go(f)
        case Feature.Mapped(feat, fn) =>
          fromFeature(feat, offset).map(MappedFS(_, fn))
        case Feature.Zipped(left, right) =>
          (fromFeature(left, offset), fromFeature(right, offset))
            .mapN(ZippedFS(_, _))
      }

    def fromLabel[K, V](
        l: Label[K, V],
        offset: Duration
    ): IO[FeatureState[K, V]] =
      l match {
        case Label.FromFeature(f) => fromFeature(f, offset)
        case Label.LookForward(l, offset1) =>
          fromLabel(l, offset + offset1)
        case Label.Mapped(feat, fn) =>
          fromLabel(feat, offset).map(MappedFS(_, fn))
        case Label.Zipped(left, right) =>
          (fromLabel(left, offset), fromLabel(right, offset))
            .mapN(ZippedFS(_, _))
      }

    case class MapToConcat[A, B](fn: A => B) extends Function1[A, Iterable[B]] {
      def apply(a: A): Iterable[B] = fn(a) :: Nil
    }

    case class FilterToConcat[A](fn: A => Boolean)
        extends Function1[A, Iterable[A]] {
      def apply(a: A): Iterable[A] = if (fn(a)) a :: Nil else Nil
    }

    case class Fn2To1[A, B, C](fn: (A, B) => C) extends Function1[(A, B), C] {
      def apply(ab: (A, B)) = fn(ab._1, ab._2)
    }

    val ioNil: IO[List[Nothing]] = IO.pure(Nil)

    case object EmptyEmitter extends Emitter[Nothing] {
      val consumes: Set[(String, Duration)] = Set.empty

      def feed(point: Point, seq: Long): IO[List[Nothing]] =
        ioNil
    }

    case class SourceEmitter[A](ev: Event.Source[A], offset: Duration)
        extends Emitter[A] {
      val consumes = Set((ev.name, offset))

      def feed(point: Point, seq: Long): IO[List[A]] =
        if (point.offset == offset && point.name == ev.name) {
          // Assume previous checks means I == A
          val Point.Sourced(_, item, _, _) = point
          IO(item.asInstanceOf[A] :: Nil)
        } else ioNil
    }

    case class ConcatEmitter[A](
        left: Emitter[A],
        right: Emitter[A],
        ref: Ref[IO, (Long, List[A])]
    ) extends Emitter[A] {
      val consumes = left.consumes | right.consumes

      def feed(point: Point, seq: Long): IO[List[A]] = {
        val nmOff = (point.name, point.offset)
        val leftRes = if (left.consumes(nmOff)) left.feed(point, seq) else ioNil
        val rightRes =
          if (right.consumes(nmOff)) right.feed(point, seq) else ioNil

        val run = (leftRes, rightRes).mapN(_ ::: _)

        for {
          ((seq0, as), set) <- ref.access
          res <- if (seq0 == seq) IO.pure(as)
          else run.flatMap(res => set((seq, res)).as(res))
        } yield res
      }
    }

    case class WithTimeEmitter[A](prev: Emitter[A])
        extends Emitter[(A, Timestamp)] {
      def consumes = prev.consumes

      def feed(point: Point, seq: Long): IO[List[(A, Timestamp)]] =
        prev.feed(point, seq).map(_.map(a => (a, point.ts)))
    }

    case class ValueWithTimeEmitter[A, B](prev: Emitter[(A, B)])
        extends Emitter[(A, (B, Timestamp))] {
      def consumes = prev.consumes
      def feed(point: Point, seq: Long): IO[List[(A, (B, Timestamp))]] =
        prev.feed(point, seq).map(_.map { case (a, b) => (a, (b, point.ts)) })
    }

    case class ConcatMapEmitter[A, B](prev: Emitter[A], fn: A => Iterable[B])
        extends Emitter[B] {
      def consumes = prev.consumes
      // we could cache here, but maybe not needed
      def feed(point: Point, seq: Long): IO[List[B]] =
        prev.feed(point, seq).map(_.flatMap(fn))
    }

    case class LookupEmitter[K, V, W](
        ev: Emitter[(K, V)],
        fs: FeatureState[K, W],
        order: LookupOrder
    ) extends Emitter[(K, (V, W))] {
      val consumes = ev.consumes | fs.consumes

      def feed(point: Point, seq: Long): IO[List[(K, (V, W))]] = {
        val nameOff = (point.name, point.offset)
        val evRes = if (ev.consumes(nameOff)) ev.feed(point, seq) else ioNil
        (evRes, fs.feed(point, seq))
          .mapN {
            case (evs, (before, after)) =>
              val reader = if (order.isBefore) before else after

              evs.map {
                case (k, v) =>
                  (k, (v, reader(k)))
              }
          }
      }
    }

    case class SummedFS[K, V](
        emitter: Emitter[(K, V)],
        state: Ref[IO, (Long, Map[K, V], Map[K, V])],
        monoid: Monoid[V]
    ) extends FeatureState[K, V] {

      private val empty = monoid.empty
      def consumes = emitter.consumes

      def feed(point: Point, seq: Long) =
        state.access
          .flatMap {
            case ((seq0, before, after), set) =>
              if (seq0 < seq) {
                // we need to process this
                val emitRes =
                  if (emitter.consumes((point.name, point.offset)))
                    emitter.feed(point, seq)
                  else ioNil
                emitRes
                  .flatMap { kvs =>
                    val after1 = kvs.foldLeft(after) {
                      case (state, (k, v)) =>
                        state.get(k) match {
                          case Some(v0) =>
                            state.updated(k, monoid.combine(v0, v))
                          case None => state.updated(k, v)
                        }
                    }

                    set((seq, after, after1))
                      .flatMap {
                        case true =>
                          IO.pure(
                            (
                              Reader.FromMap(after, empty),
                              Reader.FromMap(after1, empty)
                            )
                          )
                        case false =>
                          // loop:
                          feed(point, seq)
                      }
                  }
              } else {
                // we have already processed this event
                IO.pure(
                  (Reader.FromMap(before, empty), Reader.FromMap(after, empty))
                )
              }
          }

    }

    case class LatestFS[K, V](
        emitter: Emitter[(K, V)],
        state: Ref[IO, (Long, Map[K, V], Map[K, V])]
    ) extends FeatureState[K, Option[V]] {
      def consumes = emitter.consumes
      def feed(point: Point, seq: Long) =
        state.access
          .flatMap {
            case ((seq0, before, after), set) =>
              if (seq0 < seq) {
                // we need to process this
                val emitRes =
                  if (emitter.consumes((point.name, point.offset)))
                    emitter.feed(point, seq)
                  else ioNil
                emitRes
                  .flatMap { kvs =>
                    val after1 = after ++ kvs

                    set((seq, after, after1))
                      .flatMap {
                        case true =>
                          IO.pure(
                            (
                              Reader.FromMapOption(after),
                              Reader.FromMapOption(after1)
                            )
                          )
                        case false =>
                          // loop:
                          feed(point, seq)
                      }
                  }
              } else {
                // we have already processed this event
                IO.pure(
                  (Reader.FromMapOption(before), Reader.FromMapOption(after))
                )
              }
          }
    }

    case class MappedFS[K, V, W](fs: FeatureState[K, V], fn: (K, V) => W)
        extends FeatureState[K, W] {
      def consumes = fs.consumes
      def feed(point: Point, seq: Long) =
        fs.feed(point, seq)
          .map {
            case (before, after) =>
              (Reader.Mapped(before, fn), Reader.Mapped(after, fn))
          }
    }
    case class ZippedFS[K, V, W](
        left: FeatureState[K, V],
        right: FeatureState[K, W]
    ) extends FeatureState[K, (V, W)] {
      val consumes = left.consumes | right.consumes

      def feed(point: Point, seq: Long) =
        (left.feed(point, seq), right.feed(point, seq))
          .mapN {
            case ((bv, av), (bw, aw)) =>
              (Reader.Zipped(bv, bw), Reader.Zipped(av, aw))
          }
    }
  }
}
