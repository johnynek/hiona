package dev.posco.hiona

import cats.arrow.FunctionK
import cats.collections.Heap
import cats.effect.{Blocker, Bracket, ContextShift, IO, LiftIO, Resource, Sync}
import fs2.{Pipe, Pull, Stream}
import java.nio.file.Path
import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.util.zip.GZIPInputStream

import cats.implicits._

object Fs2Tools {

  /**
    * This gives a stream of a single item that you can flatMap on to other streams to
    * acquire and release a resource
    */
  def fromResource[F[_], A](
      r: Resource[F, A]
  )(implicit b: Bracket[F, Throwable]): Stream[F, A] =
    Stream.bracket(r.allocated)(_._2).map(_._1)

  def liftResource[F[_]: LiftIO: Sync, A, B](
      res: Resource[IO, A => IO[B]]
  ): Resource[F, A => F[B]] = {
    val lift = LiftIO[F]

    res
      .mapK(new FunctionK[IO, F] {
        def apply[T](f: IO[T]): F[T] = lift.liftIO(f)
      })
      .map(fn => fn.andThen(lift.liftIO))
  }

  /**
    * Write a stream out with a given resource function and echo the results back
    * into the stream
    */
  def tapStream[F[_], A](
      input: Stream[F, A],
      res: Resource[F, Iterable[A] => F[Unit]]
  )(implicit b: Bracket[F, Throwable]): Stream[F, A] = {
    val fn = fromResource(res)

    fn.flatMap { writeFn =>
      input.chunks
        .evalMap(chunk => writeFn(chunk.toList).as(chunk))
        .flatMap(Stream.chunk(_))
    }
  }

  /**
    * Like write stream, but returns an empty stream that only represents the effect
    */
  def sinkStream[F[_], A](
      stream: Stream[F, A],
      res: Resource[F, Iterator[A] => F[Unit]],
      chunkSize: Int = 1024
  )(implicit b: Bracket[F, Throwable]): Stream[F, fs2.INothing] = {
    val fn = fromResource(res)

    fn.flatMap { writeFn =>
      stream.chunkMin(chunkSize).evalMap(chunk => writeFn(chunk.iterator)).drain
    }
  }

  def sortMerge[F[_], A: Ordering](
      streams: List[Stream[F, A]]
  ): Stream[F, A] = {
    implicit val ord: cats.Order[Stream.StepLeg[F, A]] =
      new cats.Order[Stream.StepLeg[F, A]] {
        val ordA = implicitly[Ordering[A]]

        def compare(
            left: Stream.StepLeg[F, A],
            right: Stream.StepLeg[F, A]
        ): Int =
          if (left.head.isEmpty) {
            // prefer to step so we don't skip items
            if (right.head.isEmpty) 0 else -1
          } else if (right.head.isEmpty) {
            // we need to step so we don't misorder items
            1
          } else {
            // neither are empty just compare the head
            ordA.compare(left.head(0), right.head(0))
          }
      }

    def finish(finalLeg: Stream.StepLeg[F, A]): Pull[F, A, Unit] =
      for {
        _ <- Pull.output(finalLeg.head)
        _ <- finalLeg.stream.pull.echo
      } yield ()

    // invariant: the heap has at least two items
    def go(heap: Heap[Stream.StepLeg[F, A]]): Pull[F, A, Unit] =
      heap.pop match {
        case Some((sl, rest)) =>
          if (sl.head.nonEmpty) {
            for {
              _ <- Pull.output1(sl.head(0))
              nextSl = sl.setHead(sl.head.drop(1))
              nextHeap = rest.add(nextSl)
              // note: nextHeap.size == heap.size
              _ <- go(nextHeap)
            } yield ()
          } else {
            // this chunk is done
            sl.stepLeg
              .flatMap {
                case Some(nextSl) =>
                  val nextHeap = rest.add(nextSl)
                  // note: nextHeap.size == heap.size
                  go(nextHeap)
                case None =>
                  // this leg is exhausted
                  if (rest.size == 1) {
                    // if there is only one stream left, just
                    // emit everything from this stream without
                    // pulling one at a time

                    finish(rest.minimumOption.get)
                  } else {
                    // rest has size heap.size - 1 but > 1
                    go(rest)
                  }
              }
          }

        case None =>
          sys.error("invariant violation, expected at least 2 on each call")
      }

    streams match {
      case Nil       => Stream.empty
      case h :: Nil  => h
      case twoOrMore =>
        // we have at least two streams, but some may be empty
        twoOrMore
          .traverse(_.pull.stepLeg)
          .map(_.flatten)
          .flatMap {
            case Nil        => Pull.done
            case one :: Nil => finish(one)
            case twoOrMore  =>
              // maintain the invariant that there are at least two items in the heap
              val heap = Heap.fromIterable(twoOrMore)
              go(heap)
          }
          .stream
    }
  }

  /**
    * this pulls the next A from the stream while in parallel applying B
    */
  def scanF[F[_], A, B](init: B)(fn: (B, A) => F[B]): Pipe[F, A, B] = {
    def loop(in: Stream[F, A], init: B): Pull[F, B, Unit] =
      in.pull.uncons1
        .flatMap {
          case Some((a, rest)) =>
            val fb = fn(init, a)
            Pull
              .eval(fb)
              .flatMap(b => Pull.output1(b) >> loop(rest, b))
          case None => Pull.done
        }

    { in: Stream[F, A] => Stream(init) ++ loop(in, init).stream }
  }

  def fromPath[F[_]: Sync: ContextShift](
      path: Path,
      chunkSize: Int,
      blocker: Blocker
  ): Stream[F, Byte] =
    fs2.io.readInputStream(
      openFile(path),
      chunkSize,
      blocker,
      closeAfterUse = true
    )

  /**
    * if the file ends with .gz, then use GZipInputStream to decode
    */
  def openFile[F[_]: Sync](path: Path): F[InputStream] =
    Sync[F].delay {
      val fis = new FileInputStream(path.toFile)
      val bis = new BufferedInputStream(fis)
      if (path.toString.endsWith(".gz")) {
        new GZIPInputStream(bis)
      } else bis
    }
}
