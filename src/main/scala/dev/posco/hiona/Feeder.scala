package dev.posco.hiona

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import java.io.{BufferedReader, FileReader}
import java.nio.file.Path
import java.util.{Comparator, PriorityQueue}
import net.tixxit.delimited.{DelimitedError, DelimitedParser, DelimitedFormat, Row => DRow}

import Hiona.{Event, Validator}

import cats.implicits._

sealed abstract class Point {
  def ts: Timestamp
  def offset: Duration
  def name: String
}

object Point {
  case class Sourced[A](src: Event.Source[A], value: A, ts: Timestamp, offset: Duration) extends Point {
    def name: String = src.name
  }
}

sealed abstract class Feeder {
  // return null for missing, throws when things go bad
  protected def unsafeNext(): Point

  def next: IO[Option[Point]] = IO(Option(unsafeNext()))

  def nextBatch(n: Int): IO[List[Point]] =
    IO {
      val bldr = List.newBuilder[Point]
      var idx = 0
      while (idx < n) {
        val point = unsafeNext()
        if (point != null) bldr += point
        else {
          idx = n
        }
        idx += 1
      }

      bldr.result()
    }
}

object Feeder {
  sealed abstract class Error(msg: String) extends Exception(msg) {
    def toError[A]: IO[A] = IO.raiseError(this)
  }

  case class DuplicateEventSources(dups: NonEmptyList[Event.Source[_]]) extends Error(s"duplicate sources: $dups")

  case class MismatchInputs(
    missingPaths: Set[String],
    extraPaths: Set[String]) extends Error(s"mismatch inputs: missing=$missingPaths, extra = $extraPaths")

  private case class IteratorFeeder[A](event: Event.Source[A], offset: Duration, iter: Iterator[Either[DelimitedError, DRow]]) extends Feeder {
    protected def unsafeNext(): Point =
      if (iter.hasNext) {
        iter.next() match {
          case Right(drow) =>
            val a = event.row.unsafeFromStrings(0, drow)
            event.validator.validate(a) match {
              case Right(ts) => Point.Sourced(event, a, ts, offset)
              case Left(err) => throw err
            }

          case Left(err) => throw err
        }
      }
      else null
  }

  private case class DecodedIteratorFeeder[A](event: Event.Source[A], offset: Duration, iter: Iterator[A]) extends Feeder {
    protected def unsafeNext(): Point =
      if (iter.hasNext) {
        val a = iter.next()
        event.validator.validate(a) match {
          case Right(ts) => Point.Sourced(event, a, ts, offset)
          case Left(err) => throw err
        }
      }
      else null
  }

  private case class MultiPointFeeder(
    queue: PriorityQueue[(Point, Feeder)]) extends Feeder {

    protected def unsafeNext(): Point =
      queue.poll match {
        case null => null
        case (point, feeder) =>
          // get the next point out of the feeder and push it back in.
          val nextPoint = feeder.unsafeNext()
          if (nextPoint != null) {
            queue.add((nextPoint, feeder))
          }

          point
      }
  }

  // this allocates mutable state so it has to be in IO
  def multiFeeder(it: Iterable[Feeder]): IO[Feeder] = {
    val cmp = new Comparator[(Point, Feeder)] {
      val ordT = Timestamp.orderingForTimestamp

      def compare(left: (Point, Feeder), right: (Point, Feeder)) = {
        val lpoint = left._1
        val rpoint = right._1
        val res = Timestamp.compareDiff(lpoint.ts, lpoint.offset, rpoint.ts, rpoint.offset)
        if (res == 0) lpoint.name.compare(rpoint.name)
        else res
      }
    }

    it
      .toList
      .traverse { feeder =>
        // read the first timestamp from each feeder
        // since we go in order
        feeder.next.map { pointOpt =>
          pointOpt
            .toList
            .map { point =>
              (point, feeder)
            }
        }
      }
      .flatMap { init0 =>
        val init = init0.flatten
        val size = init.size
        IO {
          val queue = new PriorityQueue[(Point, Feeder)](size, cmp)
          // now insert all the items into the queue by checking the first items in each feeder
          init.foreach(queue.add(_))

          MultiPointFeeder(queue)
        }
      }
  }

  def fromPath[A](path: Path, src: Event.Source[A], offset: Duration): Resource[IO, Feeder] = {
    val resBR = Resource.make(IO {
      new BufferedReader(new FileReader(path.toFile))
    }) { br => IO(br.close()) }

    resBR.flatMap { br =>
      Resource.liftF(IO {
        // todo, this should be more principled:
        val it = DelimitedParser(DelimitedFormat.CSV).parseReader(br)
        if (it.hasNext) {
          // skip the header
          it.next()
        }
        IteratorFeeder(src, offset, it)
      })
    }
  }

  def iterableFeeder[A](src: Event.Source[A], offset: Duration, items: Iterable[A]): IO[Feeder] =
    // accessing a mutable value must be done inside IO
    IO(DecodedIteratorFeeder(src, offset, items.iterator))

  def fromInputs(paths: Map[String, Path], ev: Event[Any]): Resource[IO, Feeder] = {
    val srcs = Event.sourcesOf(ev)
    val badSrcs = srcs.filter { case (_, nel) => nel.size > 1 }

    if (badSrcs.nonEmpty) {
      val bads = badSrcs.iterator.map(_._2).reduce(_ | _)
      val badNel = NonEmptyList.fromListUnsafe(bads.toList.sortBy(_.name))
      Resource.liftF(
        IO.raiseError(DuplicateEventSources(badNel))
      )
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
        Resource.liftF(IO.raiseError(MismatchInputs(missing, extra)))
      }
      else {
        // the keyset is exactly the same:
        paths
          .toList
          .traverse {
            case (name, path) =>
              fromPath(path, srcMap(name), Duration.Zero)
          }
          .flatMap { feeds => Resource.liftF(multiFeeder(feeds)) }
      }
    }
  }

  def fromInputsLabels[A, B](paths: Map[String, Path], ev: LabeledEvent[A, B]): Resource[IO, Feeder] = {
    val srcs = LabeledEvent.sourcesAndOffsetsOf(ev)
    val badSrcs = srcs.filter { case (_, (srcs, _)) => srcs.size > 1 }

    if (badSrcs.nonEmpty) {
      val bads = badSrcs.iterator.map { case (_, (srcs, _)) => srcs }.reduce(_ | _)
      val badNel = NonEmptyList.fromListUnsafe(bads.toList.sortBy(_.name))
      Resource.liftF(
        IO.raiseError(DuplicateEventSources(badNel))
      )
    }
    else {
      val srcMap: Map[String, Event.Source[_]] =
        srcs
          .iterator
          .map { case (n, (singleton, _)) => (n, singleton.head) }
          .toMap

      // we need exactly the same names

      val missing = srcMap.keySet -- paths.keySet
      val extra = paths.keySet -- srcMap.keySet
      if (missing.nonEmpty || extra.nonEmpty) {
        Resource.liftF(IO.raiseError(MismatchInputs(missing, extra)))
      }
      else {
        // the keyset is exactly the same:
        paths
          .toList
          .traverse {
            case (name, path) =>
              val src = srcMap(name)
              val offsets = srcs(name)._2.toList.sorted
              offsets.traverse { offset =>
                fromPath(path, src, offset)
              }
          }
          .flatMap { feeds => Resource.liftF(multiFeeder(feeds.flatten)) }
      }
    }
  }
}

