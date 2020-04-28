package dev.posco.hiona

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.file.Path
import java.util.PriorityQueue
import java.util.zip.GZIPInputStream
import net.tixxit.delimited.{
  DelimitedError,
  DelimitedParser,
  DelimitedFormat,
  Row => DRow
}

import cats.implicits._

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

  case class DuplicateEventSources(dups: NonEmptyList[Event.Source[_]])
      extends Error(s"duplicate sources: $dups")

  case class MismatchInputs(missingPaths: Set[String], extraPaths: Set[String])
      extends Error(
        s"mismatch inputs: missing=$missingPaths, extra = $extraPaths"
      )

  sealed abstract class FromIterable {
    type A
    val event: Event.Source[A]
    val iterable: Iterable[A]
  }

  object FromIterable {
    def apply[A0](
        ev: Event.Source[A0],
        iter: Iterable[A0]
    ): FromIterable { type A = A0 } =
      new FromIterable {
        type A = A0
        val event = ev
        val iterable = iter
      }

    def feederFor(
        its: Iterable[FromIterable],
        feeds: Map[String, (Set[Event.Source[_]], Set[Duration])]
    ): IO[Feeder] =
      its.toList
        .traverse { fromIt =>
          val (srcs, durs) = feeds(fromIt.event.name)

          assert(srcs == Set(fromIt.event))

          durs.toList.sorted.traverse { dur =>
            Feeder.iterableFeeder[fromIt.A](fromIt.event, dur, fromIt.iterable)
          }
        }
        .flatMap(llf => Feeder.multiFeeder(llf.flatten))

    def feederForEvent[A](
        its: Iterable[FromIterable],
        ev: Event[A]
    ): IO[Feeder] =
      feederFor(its, Event.sourcesOf(ev).map {
        case (k, v) => (k, (v, Set(Duration.Zero)))
      })

    def feederForLabeledEvent[A](
        its: Iterable[FromIterable],
        ev: LabeledEvent[A]
    ): IO[Feeder] =
      feederFor(its, LabeledEvent.sourcesAndOffsetsOf(ev))
  }

  private case class IteratorFeeder[A](
      event: Event.Source[A],
      offset: Duration,
      iter: Iterator[Either[DelimitedError, DRow]],
      strictTime: Boolean
  ) extends Feeder {
    private var highWater: Timestamp = null
    private val key = Point.Key(event.name, offset)

    protected def unsafeNext(): Point =
      if (iter.hasNext) {
        iter.next() match {
          case Right(drow) =>
            val a = event.row.unsafeFromStrings(0, drow)
            event.validator.validate(a) match {
              case Right(ts) =>
                if (strictTime && (highWater != null)) {
                  if (Ordering[Timestamp].gt(highWater, ts)) {
                    throw new Exception(
                      s"out of order timestamp: highwater = $highWater, current = $ts"
                    )
                  }
                }
                highWater = ts
                Point.Sourced(event, a, ts, key)
              case Left(err) =>
                throw err
            }

          case Left(err) => throw err
        }
      } else null
  }

  private case class DecodedIteratorFeeder[A](
      event: Event.Source[A],
      offset: Duration,
      iter: Iterator[A],
      strictTime: Boolean
  ) extends Feeder {
    private var highWater: Timestamp = null
    private val key: Point.Key = Point.Key(event.name, offset)

    protected def unsafeNext(): Point =
      if (iter.hasNext) {
        val a = iter.next()
        event.validator.validate(a) match {
          case Right(ts) =>
            if (strictTime && (highWater != null)) {
              if (Ordering[Timestamp].gt(highWater, ts)) {
                throw new Exception(
                  s"out of order timestamp: highwater = $highWater, current = $ts"
                )
              }
            }
            highWater = ts
            Point.Sourced(event, a, ts, key)
          case Left(err) => throw err
        }
      } else null
  }

  private case class MultiPointFeeder(queue: PriorityQueue[(Point, Feeder)])
      extends Feeder {

    protected def unsafeNext(): Point =
      queue.poll match {
        case null            => null
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
  def multiFeeder(it: Iterable[Feeder]): IO[Feeder] =
    if (it.size == 1) IO.pure(it.head)
    else {
      val cmp = Point.orderingForPoint.on[(Point, Feeder)](_._1)

      it.toList
        .traverse { feeder =>
          // read the first timestamp from each feeder
          // since we go in order
          feeder.next.map { pointOpt =>
            pointOpt.toList
              .map(point => (point, feeder))
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

  private def inputStreamFor(p: Path): Resource[IO, InputStream] =
    Resource.make(IO {
      val fis = new FileInputStream(p.toFile)
      val bis = new BufferedInputStream(fis)
      if (p.toString.endsWith(".gz")) {
        new GZIPInputStream(bis)
      } else bis
    })(is => IO(is.close()))

  def fromPath[A](
      path: Path,
      src: Event.Source[A],
      offset: Duration,
      strictTime: Boolean = true
  ): Resource[IO, Feeder] =
    fromInputStream(inputStreamFor(path), src, offset, strictTime)

  def fromInputStream[A](
      is: Resource[IO, InputStream],
      src: Event.Source[A],
      offset: Duration,
      strictTime: Boolean = true
  ): Resource[IO, Feeder] =
    is.flatMap { is =>
      Resource.liftF(
        IO {
          // todo, this should be more principled:
          val it = DelimitedParser(DelimitedFormat.CSV).parseInputStream(is)
          if (it.hasNext) {
            // skip the header
            it.next()
          }
          IteratorFeeder(src, offset, it, strictTime)
        }
      )
    }

  def iterableFeeder[A](
      src: Event.Source[A],
      offset: Duration,
      items: Iterable[A],
      strictTime: Boolean = true
  ): IO[Feeder] =
    // accessing a mutable value must be done inside IO
    IO(DecodedIteratorFeeder(src, offset, items.iterator, strictTime))

  def toMap[K, V](
      items: Iterable[(K, V)]
  ): Either[Iterable[(K, V)], Map[K, V]] = {
    val map = items.groupBy(_._1)
    val badKeys = map.filter { case (_, kvs) => kvs.iterator.take(2).size == 2 }
    if (badKeys.isEmpty) Right(items.toMap)
    else {
      val badItems = items.filter { case (k, _) => badKeys.contains(k) }
      Left(badItems)
    }
  }

  def keyMatch[K, V1, V2](
      m1: Map[K, V1],
      m2: Map[K, V2]
  ): Either[(Set[K], Set[K]), Map[K, (V1, V2)]] =
    if (m1.keySet == m2.keySet) Right(m1.map {
      case (k, v1) => (k, (v1, m2(k)))
    })
    else {
      Left((m1.keySet -- m2.keySet, m2.keySet -- m1.keySet))
    }

  def fromInputs(
      paths: Iterable[(String, Path)],
      ev: Event[Any]
  ): Resource[IO, Feeder] =
    fromInputsFn(paths, ev)((nm, path) => fromPath(path, nm, Duration.Zero))

  def fromInputsFn[A](
      paths: Iterable[(String, A)],
      ev: Event[Any]
  )(fn: (Event.Source[_], A) => Resource[IO, Feeder]): Resource[IO, Feeder] = {
    val srcs = Event.sourcesOf(ev)
    val badSrcs = srcs.filter { case (_, nel) => nel.size > 1 }

    if (badSrcs.nonEmpty) {
      val bads = badSrcs.iterator.map(_._2).reduce(_ | _)
      val badNel = NonEmptyList.fromListUnsafe(bads.toList.sortBy(_.name))
      Resource.liftF(
        IO.raiseError(DuplicateEventSources(badNel))
      )
    } else {
      val srcMap: Map[String, Event.Source[_]] =
        srcs.iterator.map { case (n, singleton) => (n, singleton.head) }.toMap

      // we need exactly the same names
      keyMatch(srcMap, paths.groupBy(_._1)) match {
        case Left((missing, extra)) =>
          Resource.liftF(IO.raiseError(MismatchInputs(missing, extra)))
        case Right(matched) =>
          // the keyset is exactly the same:
          matched.toList
            .sortBy(_._1)
            .flatMap {
              case (_, (src, paths)) =>
                paths.map { case (_, p) => (src, p) }
            }
            .traverse {
              case (src, path) =>
                fn(src, path)
            }
            .flatMap(feeds => Resource.liftF(multiFeeder(feeds)))
      }
    }
  }

  def fromInputsLabels[A](
      paths: Iterable[(String, Path)],
      ev: LabeledEvent[A]
  ): Resource[IO, Feeder] =
    fromInputsLabelsFn[Path](paths, ev) { (src, path, dur) =>
      fromPath(path, src, dur)
    }

  def fromInputsLabelsFn[A](
      paths: Iterable[(String, A)],
      ev: LabeledEvent[_]
  )(
      fn: (Event.Source[_], A, Duration) => Resource[IO, Feeder]
  ): Resource[IO, Feeder] = {
    val srcs = LabeledEvent.sourcesAndOffsetsOf(ev)
    val badSrcs = srcs.filter { case (_, (srcs, _)) => srcs.size > 1 }

    if (badSrcs.nonEmpty) {
      val bads =
        badSrcs.iterator.map { case (_, (srcs, _)) => srcs }.reduce(_ | _)
      val badNel = NonEmptyList.fromListUnsafe(bads.toList.sortBy(_.name))
      Resource.liftF(
        IO.raiseError(DuplicateEventSources(badNel))
      )
    } else {
      val srcMap: Map[String, (Event.Source[_], List[Duration])] =
        srcs.iterator.map {
          case (n, (singleton, offs)) =>
            (n, (singleton.head, offs.toList.sorted))
        }.toMap

      // we need exactly the same names
      keyMatch(srcMap, paths.groupBy(_._1)) match {
        case Left((missing, extra)) =>
          Resource.liftF(IO.raiseError(MismatchInputs(missing, extra)))
        case Right(matched) =>
          // the keyset is exactly the same:
          matched.toList
            .sortBy(_._1)
            .flatMap {
              case (_, (so, paths)) =>
                paths.map { case (_, p) => (so, p) }
            }
            .traverse {
              case ((src, offsets), path) =>
                offsets.traverse(offset => fn(src, path, offset))
            }
            .flatMap(feeds => Resource.liftF(multiFeeder(feeds.flatten)))
      }
    }
  }
}
