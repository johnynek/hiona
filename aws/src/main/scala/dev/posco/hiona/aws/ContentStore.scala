package dev.posco.hiona.aws

import cats.effect.{Blocker, ContextShift, Sync}
import java.nio.file.Path
import com.amazonaws.services.s3
import org.apache.commons.codec.binary.Base32
import org.slf4j.LoggerFactory

import cats.implicits._

object ContentStore {
  private[this] val logger = LoggerFactory.getLogger(getClass)

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
      Sync[F].delay(logger.info("computing hash for {}", p)) *>
        fs2.io.file
          .readAll(p, blocker, 1 << 16)
          .through(fs2.hash.sha256[F])
          .compile
          .to(fs2.Chunk)
          .map(hashToS3Addr(root, _))

    def remoteSize(s3Addr: S3Addr): F[Option[Long]] =
      blocker.delay[F, Option[Long]] {
        logger.info("fetching remote size of {}", s3Addr)
        try {
          val size = s3client
            .getObjectMetadata(s3Addr.bucket, s3Addr.key)
            .getContentLength()

          logger.info("size of {} is {}", s3Addr, size)
          Some(size)
        } catch {
          case ex: s3.model.AmazonS3Exception if ex.getStatusCode() == 404 =>
            logger.info("{} not present", s3Addr)
            None
        }
      }

    def writeTo(p: Path, s3a: S3Addr, size: Long): F[Unit] =
      blocker.delay[F, Unit] {
        logger.info("start upload of {} to {}", p, s3a)
        val start = System.nanoTime()
        s3client.putObject(s3a.bucket, s3a.key, p.toFile())
        val diff = System.nanoTime() - start
        val seconds = diff.toDouble / 1e9
        val mbPerSec = size.toDouble / 1e6 / seconds
        logger.info(
          "done uploading {} to {} in {} seconds ({} MB/sec)",
          p,
          s3a,
          seconds,
          mbPerSec
        )
        ()
      }

    for {
      sz <- getSize
      tname <- target
      rsz <- remoteSize(tname)
      _ = logger.info("size of {} is {} bytes", p, sz)
      _ <- if (Some(sz) == rsz) F.unit else writeTo(p, tname, sz)
    } yield tname
  }
}
