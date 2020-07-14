package dev.posco.hiona.aws

import alex.mojaki.s3upload.StreamTransferManager
import cats.effect.{Blocker, ContextShift, IO, Resource, Sync}
import cats.effect.concurrent.Ref
import com.amazonaws.services.s3
import com.amazonaws.services.s3.model.GetObjectRequest
import dev.posco.hiona.Row
import fs2.Stream
import java.io.{BufferedInputStream, InputStream, OutputStream}
import java.nio.file.Path
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

//import cats.implicits._

final class AWSIO(s3client: s3.AmazonS3) {

  def openInputStream[F[_]: Sync](s3Addr: S3Addr): F[InputStream] =
    Sync[F].delay {
      val is = s3client.getObject(s3Addr.bucket, s3Addr.key).getObjectContent()
      val bis = new BufferedInputStream(is)
      if (s3Addr.key.endsWith(".gz")) new GZIPInputStream(bis)
      else bis
    }

  def read[F[_]: Sync](s3Addr: S3Addr): Resource[F, InputStream] =
    Resource.make(openInputStream(s3Addr))(is => Sync[F].delay(is.close()))

  def readStream[F[_]: Sync: ContextShift](
      s3Addr: S3Addr,
      chunkSize: Int,
      blocker: Blocker
  ): Stream[F, Byte] =
    fs2.io.readInputStream(
      openInputStream(s3Addr),
      chunkSize,
      blocker,
      closeAfterUse = true
    )

  def readCsv[F[_]: Sync: ContextShift, A: Row](
      s3Addr: S3Addr,
      chunkSize: Int,
      skipHeader: Boolean,
      blocker: Blocker
  ): Stream[F, A] =
    readStream[F](s3Addr, chunkSize, blocker)
      .through(fs2.text.utf8Decode)
      .through(Row.decodeFromCSV[F, A](implicitly[Row[A]], skipHeader))

  def putPath(s3Addr: S3Addr, path: Path, blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): IO[Unit] =
    blocker.blockOn(IO {
      s3client.putObject(s3Addr.bucket, s3Addr.key, path.toFile)

      ()
    })

  def download(s3Addr: S3Addr, path: Path, blocker: Blocker)(implicit
      ctx: ContextShift[IO]
  ): IO[Unit] =
    blocker.blockOn(IO {
      val req = new GetObjectRequest(s3Addr.bucket, s3Addr.key)

      s3client.getObject(req, path.toFile)
      ()
    })

  private def onFail[A, B](
      makeA: IO[A],
      close: (Option[Throwable], A) => IO[Unit],
      fn: A => Resource[IO, B => IO[Unit]]
  ): Resource[IO, B => IO[Unit]] =
    for {
      err <- Resource.liftF(Ref.of[IO, Option[Throwable]](None))
      a <- Resource.make(makeA)(a => err.get.flatMap(ot => close(ot, a)))
      action0 <- fn(a)
    } yield { b: B =>
      action0(b)
        .redeemWith(e => err.set(Some(e)) *> IO.raiseError(e), IO.pure(_))
    }

  /**
    * create a writer that stages everything locally in a single
    * file, then uploads at the end
    */
  def tempWriter[A](
      output: S3Addr,
      row: Row[A],
      blocker: Blocker
  )(implicit ctx: ContextShift[IO]): Resource[IO, Iterator[A] => IO[Unit]] = {
    val suffix =
      if (output.key.endsWith(".gz")) "csv.gz" else "csv"
    for {
      path <- Row.tempPath("output", suffix)
      fn <- onFail[Unit, Iterator[A]](
        IO.unit,
        {
          case (None, _)    => putPath(output, path, blocker)
          case (Some(_), _) => IO.unit
        },
        _ => Row.writerRes(path)(row)
      )
    } yield fn
  }

  /**
    * use the code from:
    * https://github.com/alexmojaki/s3-stream-upload
    * to write without buffering in memory
    */
  def multiPartOutput[A](
      s3Addr: S3Addr,
      row: Row[A]
  ): Resource[IO, Iterator[A] => IO[Unit]] = {
    val mos = IO {
      val m = new StreamTransferManager(s3Addr.bucket, s3Addr.key, s3client)
        .numStreams(1)
        .numUploadThreads(3)
        .queueCapacity(3) // how many parts can wait
        .partSize(10) // 10 MB chunks

      val os = m.getMultiPartOutputStreams.get(0)
      val s =
        if (s3Addr.key.endsWith(".gz")) new GZIPOutputStream(os)
        else os

      (m, s)
    }

    val close: (Option[Throwable], (StreamTransferManager, OutputStream)) => IO[
      Unit
    ] = {
      case (None, (m, _)) =>
        IO {
          m.complete()
        }
      case (Some(err), (m, _)) =>
        IO {
          m.abort(err)
          m.complete()
        }
    }

    val makeWriter: (
        (StreamTransferManager, OutputStream)
    ) => Resource[IO, Iterator[A] => IO[Unit]] = {
      case (_, os) =>
        for {
          pw <- Row.toPrintWriter(os)
          wfn <- Resource.liftF(Row.writer(pw)(row))
        } yield wfn
    }

    onFail(mos, close, makeWriter)
  }
}

object AWSIO {

  val awsS3: Resource[IO, s3.AmazonS3] =
    Resource.make(IO {
      s3.AmazonS3ClientBuilder.defaultClient()
    })(awsS3 => IO(awsS3.shutdown()))

  val resource: Resource[IO, AWSIO] =
    awsS3.map(new AWSIO(_))
}
