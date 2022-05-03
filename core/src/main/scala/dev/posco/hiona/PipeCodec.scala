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

import cats.effect.{Blocker, ContextShift, IO, Resource, Sync}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.Codec
import java.io.PrintWriter
import java.nio.file.Path

/**
  * Typeclass to define how to decode from sources and encode to sinks
  * @tparam A
  */
trait PipeCodec[A] {
  def decode[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A]

  def encode(output: Path): Resource[IO, Iterator[A] => IO[Unit]]

  def encode(pw: PrintWriter): IO[Iterator[A] => IO[Unit]]
}

final class CsvCodec[A: Row](skipHeader: Boolean) extends PipeCodec[A] {
  override def decode[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A] =
    fs2.text.utf8Decode andThen
      Row.decodeFromCSV[F, A](implicitly[Row[A]], skipHeader)

  override def encode(pw: PrintWriter): IO[Iterator[A] => IO[Unit]] = Row.writer(pw)

  override def encode(output: Path): Resource[IO, Iterator[A] => IO[Unit]] =
    Row
      .fileWriter(output)
      .flatMap(pw => Resource.liftF(encode(pw)))
}

final class JsonCodec[A: Codec]() extends PipeCodec[A] {
  override def decode[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A] =
    fs2.text.utf8Decode andThen
      io.circe.fs2.stringArrayParser andThen
      io.circe.fs2.decoder[F, A]

  override def encode(output: Path): Resource[IO, Iterator[A] => IO[Unit]] =
    Row
      .fileWriter(output)
      .flatMap(pw => Resource.liftF(encode(pw)))

  override def encode(pw: PrintWriter): IO[Iterator[A] => IO[Unit]] =
    IO { (iter: Iterator[A]) =>
      IO {
        val codec = Codec[A]
        iter.foreach { a =>
          val jsonText = io.circe.Printer.noSpaces.print(codec(a))
          pw.print(jsonText)
        }
      }
    }
}

object PipeCodec {
  def csv[A: Row](skipHeader: Boolean = true): PipeCodec[A] =
    new CsvCodec[A](skipHeader)

  def json[A: Codec]: PipeCodec[A] = new JsonCodec[A]()

  def stream[F[_]: Sync: ContextShift, A: PipeCodec](
      path: Path,
      blocker: Blocker
  ): Stream[F, A] =
    Fs2Tools
      .fromPath[F](path, 1 << 16, blocker)
      .through(implicitly[PipeCodec[A]].decode)
}
