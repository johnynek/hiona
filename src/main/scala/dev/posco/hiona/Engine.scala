package dev.posco.hiona

import cats.Parallel
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

    val ioFeeder = Feeder.fromInputs(inputs, event) match {
      case Left(err) => err.toError
      case Right(feeder) => IO.pure(feeder)
    }

    (ioFeeder, Emitter.fromEvent(event)).mapN { (feeder, emitter) =>

      val batchSize = 1000

      def write(res: List[A], out: PrintWriter): IO[Unit] = ???

      def loop(batch: Array[Point], seq: Long, out: PrintWriter): IO[Unit] = {
        if (batch.isEmpty) IO.unit
        else {
          val stepWrite =
            for {
              pair <- emitter.feedAll(batch, seq)
              (items, nextSeq) = pair
              _ <- write(items, out)
            } yield nextSeq

          // we can in parallel read the next batch and do this write
          Parallel.parProduct(feeder.nextBatch(batchSize), stepWrite)
            .flatMap { case (b, nextSeq) =>
              loop(b, nextSeq, out)
            }
        }
      }

      val out: Resource[IO, PrintWriter] = ???

      out.use { pw =>
        for {
          batch <- feeder.nextBatch(batchSize)
          _ <- loop(batch, 0L, pw)
        } yield ()
      }
    }
    .flatten
  }

  sealed abstract class Point
  object Point {
    case class Sourced[A](src: Event.Source[A], value: A, ts: Timestamp) extends Point
  }

  sealed abstract class Feeder {
    def nextBatch(n: Int): IO[Array[Point]]
  }

  object Feeder {
    sealed abstract class Error extends Exception {
      def toError[A]: IO[A] = IO.raiseError(this)
    }

    case class DuplicateEventSources(dups: NonEmptyList[Event.Source[_]]) extends Error {
      override def getMessage(): String = toString
    }

    case class MismatchInputs(missingPaths: Set[String], extraPaths: Set[String]) extends Error {
      override def getMessage(): String = toString
    }

    sealed abstract class Src {
      type A
      def eventSource: Event.Source[A]

      override def hashCode = eventSource.hashCode
      override def equals(that: Any) =
        that match {
          case s: Src => eventSource == s.eventSource
          case _ => false
        }

    }

    object Src {
      def apply[A1](src: Event.Source[A1]): Src { type A = A1 } =
        new Src {
          type A = A1
          def eventSource = src
        }
    }


    private case class SourceMapFeeder(table: Map[String, (Path, Src)]) extends Feeder {
      def nextBatch(n: Int): IO[Array[Point]] = ???
    }

    def fromInputs(paths: Map[String, Path], ev: Event[Any]): Either[Error, Feeder] = {
      val srcs = Event.sourcesOf(ev)
      val badSrcs = srcs.filter { case (_, nel) => nel.tail.nonEmpty }

      if (badSrcs.nonEmpty) {
        Left(DuplicateEventSources(badSrcs.iterator.map(_._2).reduce(_.concatNel(_))))
      }
      else {
        val srcMap: Map[String, Event.Source[_]] =
          srcs
            .iterator
            .map { case (n, singleton) => (n, singleton.head) }
            .toMap

        // we need exactly the same names

        val missing = srcMap.keySet -- paths.keySet
        val extra = paths.keySet -- srcMap.keySet
        if (missing.nonEmpty || extra.nonEmpty) {
          Left(MismatchInputs(missing, extra))
        }
        else {
          // the keyset is exactly the same:
          val table = paths
            .iterator
            .map { case (name, path) => (name, (path, Src(srcMap(name)))) }
            .toMap[String, (Path, Src)]

          Right(SourceMapFeeder(table))
        }
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

    final def feedAll(batch: Array[Point], seq: Long): IO[(List[O], Long)] = {
      def loop(idx: Int, seq: Long, acc: List[O]): IO[(List[O], Long)] =
        if (idx < batch.length) {
          feedPoint(batch(idx), seq)
            .flatMap { outs =>
              loop(idx + 1, seq + 1L, outs ::: acc)
            }
        }
        else IO.pure((acc.reverse, seq))

      loop(0, seq, Nil)
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
        case Event.Lookup(_, _, _) =>
          ???
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
  }
}
