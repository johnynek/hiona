package dev.posco.hiona.aws

import cats.data.Validated
import com.monovore.decline.Argument

final case class S3Addr(bucket: String, key: String) {
  def /(that: String): S3Addr =
    if (key.endsWith("/")) S3Addr(bucket, key + that)
    else S3Addr(bucket, s"$key/$that")
}

object S3Addr {
  def parse(str: String): Either[String, S3Addr] =
    if (str.startsWith("s3://") || str.startsWith("S3://")) {
      val str1 = str.drop(5)
      val slashIdx = str1.indexOf('/')
      if (slashIdx < 0) {
        Left(s"expected / in $str")
      } else {
        Right(S3Addr(str1.take(slashIdx), str1.substring(slashIdx + 1)))
      }
    } else {
      Left(s"expected $str to begin with s3://")
    }

  implicit val s3AddrArgument: Argument[S3Addr] =
    new Argument[S3Addr] {
      def defaultMetavar = "s3uri"
      def read(s: String) =
        parse(s) match {
          case Right(s3) => Validated.valid(s3)
          case Left(err) => Validated.invalidNel(err)
        }
    }
}
