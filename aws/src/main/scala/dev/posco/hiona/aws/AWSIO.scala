package dev.posco.hiona.aws

import cats.effect.{IO, Resource}
import java.io.InputStream
import java.nio.file.Path
import software.amazon.awssdk.services.s3

final class AWSIO(s3client: s3.S3Client) {

  def read(s3Addr: S3Addr): Resource[IO, InputStream] =
    Resource.make(IO {
      s3client.getObject { bldr =>
        bldr.bucket(s3Addr.bucket).key(s3Addr.key);
        ()
      }
    })(is => IO(is.close()))

  def putPath(s3Addr: S3Addr, path: Path): IO[Unit] =
    IO {
      s3client.putObject({ bldr =>
        bldr.bucket(s3Addr.bucket).key(s3Addr.key);
        ()
      }, path)

      ()
    }
}
