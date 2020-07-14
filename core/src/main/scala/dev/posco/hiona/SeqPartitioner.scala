package dev.posco.hiona

import cats.effect.{IO, Resource}
import fs2.{Pull, Stream}

import cats.implicits._

/**
  * A SeqPartitioner partitions a stream linearly,
  * element by element we check to see if it goes into
  * the current partition or if it needs to be closed
  */
object SeqPartitioner {

  trait Writer[-A] {
    def write(item: A): IO[Boolean]
    def close: IO[Unit]
  }

  object Writer {
    def filtered[A](
        writeRes: Resource[IO, Iterator[A] => IO[Unit]]
    )(fn: A => Boolean): IO[Writer[A]] =
      writeRes.allocated
        .map {
          case (writeFn, closeRes) =>
            new Writer[A] {
              def write(item: A) =
                if (fn(item)) writeFn(Iterator.single(item)).as(true)
                else IO.pure(false)

              def close = closeRes
            }
        }
  }

  trait Partitioner[A] {
    def next(a: A): IO[Writer[A]]
    def close: IO[Unit]

    final def pipe: fs2.Pipe[IO, A, Nothing] = { input: Stream[IO, A] =>
      def run(
          input: Stream[IO, A],
          writer: Option[Writer[A]]
      ): Pull[IO, Nothing, Unit] =
        input.pull.uncons
          .flatMap {
            case Some((chunk, tail)) =>
              def writeNew(a: A): IO[Option[Writer[A]]] =
                for {
                  writer <- next(a)
                  good <- writer.write(a).onError(_ => close)
                  _ <-
                    if (!good)
                      (writer.close *> IO.raiseError(
                        new IllegalStateException(
                          s"item: $a built $writer which would not accept the value"
                        )
                      ))
                    else IO.unit
                } yield Some(writer)

              val writeChunk = chunk.foldM(writer) {
                case (None, a) => writeNew(a)
                case (s @ Some(w), a) =>
                  w.write(a)
                    .onError(_ => w.close)
                    .flatMap {
                      case true  => IO.pure(s)
                      case false => w.close *> writeNew(a)
                    }
              }

              for {
                nextW <- Pull.eval(writeChunk)
                _ <- run(tail, nextW)
              } yield ()
            case None =>
              writer.fold(Pull.done.covary[IO]) { w =>
                Pull.eval(w.close) >> Pull.done
              }
          }

      run(input, None).stream
    }
  }
}
