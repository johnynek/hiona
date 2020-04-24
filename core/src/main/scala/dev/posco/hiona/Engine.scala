package dev.posco.hiona

import cats.{Monoid, Parallel}
import cats.effect.{ContextShift, IO, Resource}
import cats.effect.concurrent.Ref
import java.nio.file.Path

import cats.implicits._

/**
  * This is a collection of tools to run Events and LabeledEvents into
  * a List of values, and some helper functions to make it easy
  * to write those events out
  */
object Engine {

  def runEmitter[A](
      feedRes: Resource[IO, Feeder],
      emitter: Emitter[A],
      outputRes: Resource[IO, Iterable[A] => IO[Unit]]
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    outputRes
      .product(feedRes)
      .use {
        case (writer, feeder) =>
          Emitter.run(feeder, batchSize = 1000, emitter)(writer)
      }

  def run[A: Row](
      inputs: Iterable[(String, Path)],
      event: Event[A],
      output: Path
  )(
      implicit ctx: ContextShift[IO]
  ): IO[Unit] =
    Emitter
      .fromEvent(event)
      .flatMap(
        runEmitter(Feeder.fromInputs(inputs, event), _, Row.writerRes(output))
      )

  def runLabeled[A: Row](
      inputs: Iterable[(String, Path)],
      labeled: LabeledEvent[A],
      output: Path
  )(implicit ctx: ContextShift[IO]): IO[Unit] =
    Emitter
      .fromLabeledEvent(labeled)
      .flatMap(
        runEmitter(
          Feeder.fromInputsLabels(inputs, labeled),
          _,
          Row.writerRes(output)
        )
      )

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
      Impl
        .newState(Impl.Node.fanOutFn(ev))
        .flatMap(state => state.fromEventDur(ev, Duration.Zero))

    def fromLabeledEvent[A](le: LabeledEvent[A]): IO[Emitter[A]] =
      le match {
        case LabeledEvent.WithLabel(ev, label, cast) =>
          Impl
            .newState(Impl.Node.fanOutFn(le))
            .flatMap { state =>
              (
                state.fromEventDur(ev, Duration.Zero),
                state.fromLabel(label, Duration.Zero)
              ).mapN((e, l) =>
                cast.substituteCo[Emitter](
                  Impl.LookupEmitter(e, l, LookupOrder.After)
                )
              )
            }
        case m @ LabeledEvent.Mapped(_, _) =>
          def go[A0](
              m: LabeledEvent.Mapped[A0, A]
          ): IO[Emitter[A]] =
            fromLabeledEvent(m.labeled).map(
              // TODO we could do fusion
              // here, we know there is no fanout
              Impl.ConcatMapEmitter(
                _,
                Impl.MapToConcat(
                  m.fn
                )
              )
            )

          go(m)
        case LabeledEvent.Filtered(ev, fn) =>
          // TODO we could do fusion
          // here, we know there is no fanout
          fromLabeledEvent(ev).map(
            Impl.ConcatMapEmitter(_, Impl.FilterToConcat(fn))
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
      Impl
        .newState(Impl.Node.fanOutFn(f))
        .flatMap(state => state.fromFeatureDur(f, Duration.Zero))

    def fromLabel[K, V](l: Label[K, V]): IO[FeatureState[K, V]] =
      Impl
        .newState(Impl.Node.fanOutFn(l))
        .flatMap(state => state.fromLabel(l, Duration.Zero))
  }

  private object Impl {
    import scala.collection.mutable.{Map => MMap}

    // this is only safe if two keys that are equal
    // have the same complete types
    final class Cache1[K[_], V[_]](map: MMap[K[_], V[_]]) {
      def apply[A](k: K[A])(iov: => IO[V[A]]): IO[V[A]] =
        IO.suspend {
          map.get(k) match {
            case Some(v) => IO.pure(v.asInstanceOf[V[A]])
            case None    => iov.flatMap(v => IO(map.update(k, v)).as(v))
          }
        }
    }

    object Cache1 {
      def build[K[_], V[_]]: IO[Cache1[K, V]] =
        // has to be in IO because we allocate mutable memory
        IO(new Cache1[K, V](MMap.empty))
    }

    // this is only safe if two keys that are equal
    // have the same complete types
    final class Cache2[K[_, _], V[_, _]](map: MMap[K[_, _], V[_, _]]) {
      def apply[A, B](k: K[A, B])(iov: => IO[V[A, B]]): IO[V[A, B]] =
        IO.suspend {
          map.get(k) match {
            case Some(v) => IO.pure(v.asInstanceOf[V[A, B]])
            case None    => iov.flatMap(v => IO(map.update(k, v)).as(v))
          }
        }
    }

    object Cache2 {
      def build[K[_, _], V[_, _]]: IO[Cache2[K, V]] =
        // has to be in IO because we allocate mutable memory
        IO(new Cache2[K, V](MMap.empty))
    }

    type EvDur[A] = (Event[A], Duration)
    type FeatDur[K, V] = (Feature[K, V], Duration)
    type ECache = Cache1[EvDur, Emitter]
    type FCache = Cache2[FeatDur, FeatureState]
    val emptyECache: IO[ECache] = Cache1.build
    val emptyFCache: IO[FCache] = Cache2.build

    sealed trait Node
    object Node {
      case class ENode[A](ev: Event[A], dur: Duration) extends Node
      case class LENode[A](le: LabeledEvent[A], dur: Duration) extends Node
      case class FNode[K, V](f: Feature[K, V], dur: Duration) extends Node
      case class LNode[K, V](l: Label[K, V], dur: Duration) extends Node

      def dependsOn(n: Node): List[Node] =
        n match {
          case ENode(Event.Source(_, _, _) | Event.Empty, _) => Nil
          case ENode(Event.Concat(a, b), dur) =>
            ENode(a, dur) :: ENode(b, dur) :: Nil
          case ENode(Event.Lookup(e, f, _), dur) =>
            ENode(e, dur) :: FNode(f, dur) :: Nil
          case ENode(ns: Event.NonSource[_], dur) =>
            ENode(ns.previous, dur) :: Nil
          case LENode(LabeledEvent.WithLabel(e, l, _), dur) =>
            ENode(e, dur) :: LNode(l, dur) :: Nil
          case LENode(LabeledEvent.Mapped(e, _), dur)   => LENode(e, dur) :: Nil
          case LENode(LabeledEvent.Filtered(e, _), dur) => LENode(e, dur) :: Nil
          case FNode(Feature.Summed(ev, _), dur)        => ENode(ev, dur) :: Nil
          case FNode(Feature.Latest(ev, _, _), dur)     => ENode(ev, dur) :: Nil
          case FNode(Feature.Mapped(f, _), dur)         => FNode(f, dur) :: Nil
          case FNode(Feature.Zipped(l, r, _), dur) =>
            FNode(l, dur) :: FNode(r, dur) :: Nil
          case LNode(Label.FromFeature(f), dur)     => FNode(f, dur) :: Nil
          case LNode(Label.LookForward(l, d0), dur) => LNode(l, d0 + dur) :: Nil
          case LNode(Label.Mapped(l, _), dur)       => LNode(l, dur) :: Nil
          case LNode(Label.Zipped(l, r, _), dur) =>
            LNode(l, dur) :: LNode(r, dur) :: Nil
        }

      def fanOutFn[A](ev: Event[A]): Node => Int =
        Graph.fanOutCount(Set[Node](ENode(ev, Duration.Zero)))(dependsOn(_))

      def fanOutFn[A](ev: LabeledEvent[A]): Node => Int =
        Graph.fanOutCount(Set[Node](LENode(ev, Duration.Zero)))(dependsOn(_))

      def fanOutFn[A, B](le: Label[A, B]): Node => Int =
        Graph.fanOutCount(Set[Node](LNode(le, Duration.Zero)))(dependsOn(_))

      def fanOutFn[A, B](f: Feature[A, B]): Node => Int =
        Graph.fanOutCount(Set[Node](FNode(f, Duration.Zero)))(dependsOn(_))
    }

    def newState(fo: Node => Int): IO[State] =
      (emptyECache, emptyFCache).mapN(new State(_, _, fo))

    class State(ecache: ECache, fcache: FCache, fanOut: Node => Int) {

      def fromEventDur[A](
          ev: Event[A],
          offset: Duration
      ): IO[Emitter[A]] =
        ecache((ev, offset)) {
          val uncached: IO[Emitter[A]] = ev match {
            case Event.Empty => IO.pure(EmptyEmitter)
            case src @ Event.Source(_, _, _) =>
              IO.pure(SourceEmitter(src, offset))
            case Event.Concat(left, right) =>
              (
                fromEventDur(left, offset),
                fromEventDur(right, offset)
              ).mapN(ConcatEmitter(_, _))
            case Event.WithTime(ev) =>
              fromEventDur(ev, offset).map(WithTimeEmitter(_))
            case Event.Mapped(ev1 @ Event.Mapped(ev0, fn0), fn1)
                if fanOut(Node.ENode(ev1, offset)) < 2 =>
              // ev1 is only used here, we can compose:
              val composed = Event.Mapped(ev0, AndThen.build(fn0, fn1))
              fromEventDur(composed, offset)
            case Event.ConcatMapped(ev1 @ Event.ConcatMapped(ev0, fn0), fn1)
                if fanOut(Node.ENode(ev1, offset)) < 2 =>
              // ev1 is only used here, we can compose:
              val composed = Event.ConcatMapped(ev0, AndThenIt.build(fn0, fn1))
              fromEventDur(composed, offset)
            case Event.Filtered(ev1 @ Event.Filtered(ev0, fn0, c0), fn1, c1)
                if fanOut(Node.ENode(ev1, offset)) < 2 =>
              // ev1 is only used here, we can compose:
              val composed =
                Event.Filtered(ev0, AndFn.build(fn0, fn1), c0.andThen(c1))
              fromEventDur(composed, offset)
            case Event.Mapped(ev, fn) =>
              // rewrite this node
              val rewrite = Event.ConcatMapped(ev, MapToConcat(fn))
              fromEventDur(rewrite, offset)
            case Event.Filtered(ev, fn, cast) =>
              // rewrite this node
              val rewrite = Event.ConcatMapped(ev, FilterToConcat(fn))
              type T[Z] = IO[Emitter[Z]]

              cast.substituteCo[T](fromEventDur(rewrite, offset))
            case Event.ConcatMapped(ev, fn) =>
              fromEventDur(ev, offset).map(
                ConcatMapEmitter(_, fn)
              )
            case Event.ValueWithTime(ev) =>
              fromEventDur(ev, offset).map(
                ValueWithTimeEmitter(_)
              )
            case Event.Lookup(ev, feat, order) =>
              (
                fromEventDur(ev, offset),
                fromFeatureDur(feat, offset)
              ).mapN(LookupEmitter(_, _, order))
          }

          if (fanOut(Node.ENode(ev, offset)) > 1) {
            (uncached, Ref.of[IO, (Long, List[A])]((Long.MinValue, Nil)))
              .mapN(CacheEmitter[A](_, _))
          } else uncached
        }

      def fromFeatureDur[K, V](
          f: Feature[K, V],
          offset: Duration
      ): IO[FeatureState[K, V]] =
        fcache((f, offset)) {
          f match {
            case Feature.Summed(ev, monoid) =>
              (
                fromEventDur(ev, offset),
                Ref.of[IO, (Long, Map[K, V], Map[K, V])](
                  (Long.MinValue, Map.empty, Map.empty)
                )
              ).mapN(SummedFS(_, _, monoid))

            case f @ Feature.Latest(_, _, _) =>
              // TODO make use of the window duration to prune old events from the state
              // we know that C == Option[B]
              type Z[A] = FeatureState[K, A]
              def go[B, C](l: Feature.Latest[K, B, C]): IO[FeatureState[K, C]] =
                (
                  fromEventDur(l.event, offset),
                  Ref.of[IO, (Long, Map[K, B], Map[K, B])](
                    (Long.MinValue, Map.empty, Map.empty)
                  )
                ).mapN((e, r) => l.cast.substituteCo[Z](LatestFS[K, B](e, r)))

              go(f)

            case Feature.Mapped(feat, fn) =>
              fromFeatureDur(feat, offset).map(MappedFS(_, fn))

            case Feature.Zipped(left, right, cast) =>
              // we know there exists W, X such that V =:= (W, X)
              // scala intoduces unknowns for W and X
              type Z[A] = FeatureState[K, A]
              (
                fromFeatureDur(left, offset), // W
                fromFeatureDur(right, offset) // X
              ).mapN((l, r) => cast.substituteCo[Z](ZippedFS(l, r)))
            // cast goes from F[(W, X)] to F[V] which is what I want
          }
        }

      def fromLabel[K, V](
          l: Label[K, V],
          offset: Duration
      ): IO[FeatureState[K, V]] =
        l match {
          case Label.FromFeature(f) => fromFeatureDur(f, offset)
          case Label.LookForward(l, offset1) =>
            fromLabel(l, offset + offset1)
          case Label.Mapped(feat, fn) =>
            fromLabel(feat, offset).map(
              MappedFS(_, Feature.Ignore3(fn))
            )
          case Label.Zipped(left, right, cast) =>
            type FS[A] = FeatureState[K, A]
            (
              fromLabel(left, offset),
              fromLabel(right, offset)
            ).mapN((l, r) => cast.substituteCo[FS](ZippedFS(l, r)))
        }
    }

    case class AndThen[A, B, C](fn1: A => B, fn2: B => C)
        extends Function1[A, C] {
      def apply(a: A): C = fn2(fn1(a))
    }

    object AndThen {
      def build[A, B, C](fn1: A => B, fn2: B => C): A => C =
        fn1 match {
          case AndThen(x, y) => build(x, build(y, fn2))
          case notAndThen    => AndThen(notAndThen, fn2)
        }
    }

    case class AndThenIt[A, B, C](fn1: A => Iterable[B], fn2: B => Iterable[C])
        extends Function1[A, Iterable[C]] {
      def apply(a: A): Iterable[C] = fn1(a).flatMap(fn2)
    }

    object AndThenIt {
      def build[A, B, C](
          fn1: A => Iterable[B],
          fn2: B => Iterable[C]
      ): A => Iterable[C] =
        fn1 match {
          case AndThenIt(x, y) =>
            // we want a deep chain to be: fn(a).flatMap(x => f2(x).flatMap(y => ...
            // so that we short circuit if any are empty
            build(x, build(y, fn2))
          case notAndThen => AndThenIt(notAndThen, fn2)
        }
    }

    case class AndFn[A](fn1: A => Boolean, fn2: A => Boolean)
        extends Function1[A, Boolean] {
      def apply(a: A): Boolean = fn1(a) && fn2(a)
    }

    object AndFn {
      def build[A](fn1: A => Boolean, fn2: A => Boolean): A => Boolean =
        fn1 match {
          case AndFn(x, y) => build(x, build(y, fn2))
          case notAnd      => AndFn(notAnd, fn2)
        }
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

    case class CacheEmitter[A](prev: Emitter[A], ref: Ref[IO, (Long, List[A])])
        extends Emitter[A] {

      def consumes = prev.consumes

      def feed(point: Point, seq: Long): IO[List[A]] =
        for {
          ((seq0, as), set) <- ref.access
          res <- if (seq0 == seq) IO.pure(as)
          else {
            for {
              res <- prev.feed(point, seq)
              success <- set((seq, res))
              res1 <- if (success) IO.pure(res) else feed(point, seq)
            } yield res1
          }
        } yield res
    }

    case class ConcatEmitter[A](
        left: Emitter[A],
        right: Emitter[A]
    ) extends Emitter[A] {
      val consumes = left.consumes | right.consumes

      def feed(point: Point, seq: Long): IO[List[A]] = {
        val nmOff = (point.name, point.offset)
        val leftRes = if (left.consumes(nmOff)) left.feed(point, seq) else ioNil
        val rightRes =
          if (right.consumes(nmOff)) right.feed(point, seq) else ioNil

        (leftRes, rightRes).mapN(_ ::: _)
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

    case class Apply3[A, B, C, D](fn: (A, B, C) => D, c: C)
        extends Function2[A, B, D] {
      def apply(a: A, b: B) = fn(a, b, c)
    }
    case class MappedFS[K, V, W](
        fs: FeatureState[K, V],
        fn: (K, V, Timestamp) => W
    ) extends FeatureState[K, W] {
      def consumes = fs.consumes
      def feed(point: Point, seq: Long) =
        fs.feed(point, seq)
          .map {
            case (before, after) =>
              val ap = Apply3(fn, point.ts)
              (Reader.Mapped(before, ap), Reader.Mapped(after, ap))
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
