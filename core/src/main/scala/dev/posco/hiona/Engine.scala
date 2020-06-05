package dev.posco.hiona

import cats.Monoid
import cats.effect.{IO, LiftIO}
import cats.effect.concurrent.Ref
import fs2.{Chunk, Pull, Stream}

import cats.implicits._

/**
  * This is a collection of tools to run Events and LabeledEvents into
  * a List of values, and some helper functions to make it easy
  * to write those events out
  */
object Engine {

  def run[F[_]: LiftIO, E[_]: Emittable, A](
      inputFactory: InputFactory[F],
      ev: E[A]
  ): Stream[F, (Timestamp, A)] = {
    val inputs = inputFactory.allInputs(ev)

    val ioEmit = Emitter.from(ev)
    Stream
      .eval(LiftIO[F].liftIO(ioEmit))
      .flatMap(emit => runStream[F, A](inputs, emit))
  }

  def runStream[F[_]: LiftIO, A](
      inputs: Stream[F, Point],
      emit: Emitter[A]
  ): Stream[F, (Timestamp, A)] = {
    def feed(
        inputs: Stream[F, Point],
        seq: Long
    ): Pull[F, (Timestamp, A), Unit] =
      inputs.pull.uncons
        .flatMap {
          case Some((points, rest)) =>
            val batch = emit.feedAll(points, seq)
            val batchF = LiftIO[F].liftIO(batch)
            for {
              (as, s1) <- Pull.eval(batchF)
              _ <- Pull.output(Chunk.seq(as))
              next <- feed(rest, s1)
            } yield next
          case None =>
            Pull.done
        }

    feed(inputs, 0L).stream
  }

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
    def consumes: Set[Point.Key]
    // TODO:
    // we can index nodes in a graph by some arbitrary order, so we
    // have a 1:1 mapping of Int to Event[_]. Then we can index sources
    // into an array to get a bit better performance here
    // since we can add all the sources into an identically indexed array
    def feed(p: Point, seq: Long): IO[List[O]]

    final def feedAll(
        batch: Chunk[Point],
        seq: Long
    ): IO[(List[(Timestamp, O)], Long)] = {
      def loop(
          idx: Int,
          seq: Long,
          acc: List[(Timestamp, O)]
      ): IO[(List[(Timestamp, O)], Long)] =
        if (idx < batch.size) {
          val point = batch(idx)
          val ts = point.ts
          feed(point, seq)
            .flatMap(outs =>
              loop(idx + 1, seq + 1L, outs.map((ts, _)) reverse_::: acc)
            )
        } else IO.pure((acc.reverse, seq))

      loop(0, seq, Nil)
    }
  }

  object Emitter {

    def from[F[_], A](ev: F[A])(implicit em: Emittable[F]): IO[Emitter[A]] =
      em.toEither(ev) match {
        case Right(ev) => fromEvent(ev)
        case Left(lev) => fromLabeledEvent(lev)
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
              Impl
                .MapLikeEmitter(
                  _,
                  Impl.ConcatToMapLike(
                    Impl.MapToConcat(
                      m.fn
                    )
                  )
                )
                .contract1
            )

          go(m)
        case LabeledEvent.Filtered(ev, fn) =>
          fromLabeledEvent(ev).map(
            Impl
              .MapLikeEmitter(_, Impl.ConcatToMapLike(Impl.FilterToConcat(fn)))
              .contract1
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
    def consumes: Set[Point.Key]
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
            case Event.Filtered(ev1 @ Event.Filtered(ev0, fn0, c0), fn1, c1)
                if fanOut(Node.ENode(ev1, offset)) < 2 =>
              // ev1 is only used here, we can compose:
              val composed =
                Event.Filtered(ev0, AndFn.build(fn0, fn1), c0.andThen(c1))
              fromEventDur(composed, offset)
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
            case Event.Lookup(ev, feat, order) =>
              (
                fromEventDur(ev, offset),
                fromFeatureDur(feat, offset)
              ).mapN(LookupEmitter(_, _, order))
            case ns: Event.NonSource[a] =>
              val nsem: IO[Emitter[a]] =
                ns match {
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
                    fromEventDur(ev, offset).map { prev =>
                      val fn1 = ConcatToMapLike(fn)
                      MapLikeEmitter(prev, fn1)
                    }
                  case Event.WithTime(ev, cast) =>
                    fromEventDur(ev, offset).map { prev =>
                      cast.substituteCo[Emitter](
                        MapLikeEmitter(prev, WithTimeFn())
                      )
                    }
                  case vwt: Event.ValueWithTime[k, v, w] =>
                    fromEventDur(vwt.event, offset)
                      .map { prev: Emitter[(k, v)] =>
                        vwt.cast.substituteCo[Emitter](
                          MapLikeEmitter(prev, ValueWithTimeFn[k, v]())
                        )
                      }
                }

              nsem.map {
                case ml: MapLikeEmitter[_, a] =>
                  // there is no reason to have to adjacent MapLikeEmitters
                  // since there is no cache or effect between them
                  ml.contract1
                case other => other
              }
          }

          if (fanOut(Node.ENode(ev, offset)) > 1)
            (uncached, Ref.of[IO, (Long, List[A])]((Long.MinValue, Nil)))
              .mapN(CacheEmitter[A](_, _))
          else uncached
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

    case class AndThenCtx[Ctx, A, B, C](fn1: (Ctx, A) => B, fn2: (Ctx, B) => C)
        extends Function2[Ctx, A, C] {
      def apply(ctx: Ctx, a: A): C = {
        val b = fn1(ctx, a)
        fn2(ctx, b)
      }
    }

    object AndThenCtx {
      def build[Ctx, A, B, C](
          fn1: (Ctx, A) => B,
          fn2: (Ctx, B) => C
      ): (Ctx, A) => C =
        fn1 match {
          case AndThenCtx(x, y) => build(x, build(y, fn2))
          case notAnd           => AndThenCtx(notAnd, fn2)
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

    case class ConcatToMapLike[A, B](fn: A => Iterable[B])
        extends Function2[Timestamp, List[A], List[B]] {
      def apply(ts: Timestamp, as: List[A]) = as.flatMap(fn)
    }

    case class WithTimeFn[A]()
        extends Function2[Timestamp, List[A], List[(A, Timestamp)]] {
      def apply(ts: Timestamp, as: List[A]) = {
        val build = List.newBuilder[(A, Timestamp)]
        var lst = as
        while (lst != Nil) {
          build += ((lst.head, ts))
          lst = lst.tail
        }
        build.result()
      }
    }

    case class ValueWithTimeFn[A, B]()
        extends Function2[Timestamp, List[(A, B)], List[(A, (B, Timestamp))]] {
      def apply(ts: Timestamp, abs: List[(A, B)]) = {
        val build = List.newBuilder[(A, (B, Timestamp))]
        var lst = abs
        while (lst != Nil) {
          val ab = lst.head
          build += ((ab._1, (ab._2, ts)))
          lst = lst.tail
        }
        build.result()
      }
    }

    val ioNil: IO[List[Nothing]] = IO.pure(Nil)

    case object EmptyEmitter extends Emitter[Nothing] {
      val consumes = Set.empty

      def feed(point: Point, seq: Long): IO[List[Nothing]] =
        ioNil
    }

    case class SourceEmitter[A](ev: Event.Source[A], offset: Duration)
        extends Emitter[A] {
      val key = Point.Key(ev.name, offset)
      val consumes = Set(key)

      def feed(point: Point, seq: Long): IO[List[A]] =
        if (point.key == key) {
          // Assume previous checks means I == A
          val Point.Sourced(_, item, _, _) = point
          IO(item.asInstanceOf[A] :: Nil)
        } else ioNil
    }

    case class CacheEmitter[A](prev: Emitter[A], ref: Ref[IO, (Long, List[A])])
        extends Emitter[A] {

      val consumes = prev.consumes

      def feed(point: Point, seq: Long): IO[List[A]] =
        for {
          ((seq0, as), set) <- ref.access
          res <-
            if (seq0 == seq) IO.pure(as)
            else
              for {
                res <- prev.feed(point, seq)
                success <- set((seq, res))
                res1 <- if (success) IO.pure(res) else feed(point, seq)
              } yield res1
        } yield res
    }

    case class ConcatEmitter[A](
        left: Emitter[A],
        right: Emitter[A]
    ) extends Emitter[A] {
      val consumes = left.consumes | right.consumes

      def feed(point: Point, seq: Long): IO[List[A]] = {
        val leftRes =
          if (left.consumes(point.key)) left.feed(point, seq) else ioNil
        val rightRes =
          if (right.consumes(point.key)) right.feed(point, seq) else ioNil

        (leftRes, rightRes).mapN(_ ::: _)
      }
    }

    case class MapLikeEmitter[A, B](
        prev: Emitter[A],
        fn: (Timestamp, List[A]) => List[B]
    ) extends Emitter[B] {
      val consumes = prev.consumes
      def feed(point: Point, seq: Long): IO[List[B]] =
        prev.feed(point, seq).map(fn(point.ts, _))

      def contract1: Emitter[B] =
        prev match {
          case MapLikeEmitter(p2, fn2) =>
            MapLikeEmitter(p2, AndThenCtx.build(fn2, fn))
          case _ => this
        }
    }

    case class LookupEmitter[K, V, W](
        ev: Emitter[(K, V)],
        fs: FeatureState[K, W],
        order: LookupOrder
    ) extends Emitter[(K, (V, W))] {
      val consumes = ev.consumes | fs.consumes

      def feed(point: Point, seq: Long): IO[List[(K, (V, W))]] = {
        val evRes = if (ev.consumes(point.key)) ev.feed(point, seq) else ioNil
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
      val consumes = emitter.consumes

      def feed(point: Point, seq: Long) =
        state.access
          .flatMap {
            case ((seq0, before, after), set) =>
              if (seq0 < seq) {
                // we need to process this
                val emitRes =
                  if (emitter.consumes(point.key))
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
              } else
                // we have already processed this event
                IO.pure(
                  (Reader.FromMap(before, empty), Reader.FromMap(after, empty))
                )
          }

    }

    case class LatestFS[K, V](
        emitter: Emitter[(K, V)],
        state: Ref[IO, (Long, Map[K, V], Map[K, V])]
    ) extends FeatureState[K, Option[V]] {
      val consumes = emitter.consumes
      def feed(point: Point, seq: Long) =
        state.access
          .flatMap {
            case ((seq0, before, after), set) =>
              if (seq0 < seq) {
                // we need to process this
                val emitRes =
                  if (emitter.consumes(point.key))
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
              } else
                // we have already processed this event
                IO.pure(
                  (Reader.FromMapOption(before), Reader.FromMapOption(after))
                )
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
      val consumes = fs.consumes
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
