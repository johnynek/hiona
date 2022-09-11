/*
 * Copyright 2022 devposco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.posco.hiona

import cats.arrow.FunctionK
import cats.collections.Heap
import cats.effect.{IO, LiftIO, MonadCancel, Resource, Sync}
import fs2.{Pipe, Pull, Stream}
import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.file.Path
import java.util.zip.GZIPInputStream

import cats.implicits._

object Fs2Tools {

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
  )(implicit F: MonadCancel[F, _]): Stream[F, A] =
    Stream
      .resource(res)
      .flatMap { writeFn =>
        input.chunks
          .evalMapChunk(chunk => writeFn(chunk.toList).as(chunk))
          .flatMap(Stream.chunk(_))
      }

  /** Like write stream, but returns an empty stream that only represents the effect */
  def sinkStream[F[_], A](
      res: Resource[F, Iterator[A] => F[Unit]],
      chunkSize: Int = 1024
  )(implicit F: MonadCancel[F, _]): Pipe[F, A, fs2.INothing] = {
    stream: Stream[F, A] =>
      Stream
        .resource(res)
        .flatMap { writeFn =>
          stream
            .chunkMin(chunkSize)
            .evalMapChunk(chunk => writeFn(chunk.iterator))
            .drain
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
          if (left.head.isEmpty)
            // prefer to step so we don't skip items
            if (right.head.isEmpty) 0 else -1
          else if (right.head.isEmpty)
            // we need to step so we don't misorder items
            1
          else
            // neither are empty just compare the head
            ordA.compare(left.head(0), right.head(0))
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
          if (sl.head.nonEmpty)
            for {
              _ <- Pull.output1(sl.head(0))
              nextSl = sl.setHead(sl.head.drop(1))
              nextHeap = rest.add(nextSl)
              // note: nextHeap.size == heap.size
              _ <- go(nextHeap)
            } yield ()
          else
            // this chunk is done
            sl.stepLeg
              .flatMap {
                case Some(nextSl) =>
                  val nextHeap = rest.add(nextSl)
                  // note: nextHeap.size == heap.size
                  go(nextHeap)
                case None =>
                  // this leg is exhausted
                  if (rest.size == 1)
                    // if there is only one stream left, just
                    // emit everything from this stream without
                    // pulling one at a time
                    finish(rest.minimumOption.get)
                  else
                    // rest has size heap.size - 1 but > 1
                    go(rest)
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

  def fromPath[F[_]: Sync](path: Path, chunkSize: Int): Stream[F, Byte] =
    fs2.io.readInputStream(
      openFile(path),
      chunkSize,
      closeAfterUse = true
    )

  /** if the file ends with .gz, then use GZipInputStream to decode */
  def openFile[F[_]: Sync](path: Path): F[InputStream] =
    Sync[F].delay {
      val fis = new FileInputStream(path.toFile)
      val bis = new BufferedInputStream(fis)
      if (path.toString.endsWith(".gz"))
        new GZIPInputStream(bis)
      else bis
    }
}
