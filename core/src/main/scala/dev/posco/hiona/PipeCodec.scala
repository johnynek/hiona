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
  def pipe[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A]

  def writer(output: Path): Resource[IO, Iterator[A] => IO[Unit]]

  def writer(pw: PrintWriter): IO[Iterator[A] => IO[Unit]]
}

final class CsvCodec[A: Row](skipHeader: Boolean) extends PipeCodec[A] {
  override def pipe[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A] =
    fs2.text.utf8Decode andThen
      Row.decodeFromCSV[F, A](implicitly[Row[A]], skipHeader)

  def writer(pw: PrintWriter): IO[Iterator[A] => IO[Unit]] = Row.writer(pw)

  def writer(output: Path): Resource[IO, Iterator[A] => IO[Unit]] =
    Row
      .fileWriter(output)
      .flatMap(pw => Resource.liftF(writer(pw)))
}

final class JsonCodec[A: Codec]() extends PipeCodec[A] {
  override def pipe[F[_]: RaiseThrowable: Sync]: Pipe[F, Byte, A] =
    fs2.text.utf8Decode andThen
      io.circe.fs2.stringArrayParser andThen
      io.circe.fs2.decoder[F, A]

  override def writer(output: Path): Resource[IO, Iterator[A] => IO[Unit]] = {
    Row
      .fileWriter(output)
      .flatMap(pw => Resource.liftF(writer(pw)))
  }

  def writer(pw: PrintWriter): IO[Iterator[A] => IO[Unit]] =
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

  def json[A: Codec](): PipeCodec[A] = new JsonCodec[A]()

  def stream[F[_]: Sync: ContextShift, A: PipeCodec](
      path: Path,
      blocker: Blocker
  ): Stream[F, A] =
    Fs2Tools
      .fromPath[F](path, 1 << 16, blocker)
      .through(implicitly[PipeCodec[A]].pipe)
}
