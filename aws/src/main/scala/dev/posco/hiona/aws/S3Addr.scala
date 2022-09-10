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

package dev.posco.hiona.aws

import cats.data.Validated
import com.monovore.decline.Argument

final case class S3Addr(bucket: String, key: String) {
  def /(that: String): S3Addr =
    if (key.endsWith("/")) S3Addr(bucket, key + that)
    else S3Addr(bucket, s"$key/$that")

  override def toString = s"s3://$bucket/$key"
}

object S3Addr {
  def parse(str: String): Either[String, S3Addr] =
    if (str.startsWith("s3://") || str.startsWith("S3://")) {
      val str1 = str.drop(5)
      val slashIdx = str1.indexOf('/')
      if (slashIdx < 0)
        Left(s"expected / in $str")
      else
        Right(S3Addr(str1.take(slashIdx), str1.substring(slashIdx + 1)))
    } else
      Left(s"expected $str to begin with s3://")

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
