package dev.posco.hiona.aws

import cats.effect.{Blocker, ContextShift, Sync}
import java.nio.file.Path
import com.amazonaws.services.s3
import org.apache.commons.codec.binary.Base32

import cats.implicits._

object ContentStore {

  def hashToS3Addr(root: S3Addr, hash: fs2.Chunk[Byte]): S3Addr = {
    val b32 = new Base32()
    val str = b32.encodeToString(hash.toArray)
    // 32 x 32 = 1024 top level directories
    val prefix = str.take(2)
    val rest = str.drop(2).takeWhile(_ != '=')
    root / prefix / rest
  }

  def put[F[_]: Sync: ContextShift](
      s3client: s3.AmazonS3,
      root: S3Addr,
      p: Path,
      blocker: Blocker
  ): F[S3Addr] = {
    val F = Sync[F]

    val getSize: F[Long] =
      blocker.delay[F, Long](p.toFile.length())

    val target: F[S3Addr] =
      fs2.io.file
        .readAll(p, blocker, 1 << 16)
        .through(fs2.hash.sha256[F])
        .compile
        .to(fs2.Chunk)
        .map(hashToS3Addr(root, _))

    def remoteSize(s3Addr: S3Addr): F[Option[Long]] =
      blocker.delay[F, Option[Long]] {
        try Some(
          s3client
            .getObjectMetadata(s3Addr.bucket, s3Addr.key)
            .getContentLength()
        )
        catch {
          case ex: s3.model.AmazonS3Exception if ex.getStatusCode() == 404 =>
            None
        }
      }

    def writeTo(p: Path, s3a: S3Addr): F[Unit] =
      blocker.delay[F, Unit] {
        s3client.putObject(s3a.bucket, s3a.key, p.toFile())
        ()
      }

    for {
      sz <- getSize
      tname <- target
      rsz <- remoteSize(tname)
      _ <- if (Some(sz) == rsz) F.unit else writeTo(p, tname)
    } yield tname
  }
}
