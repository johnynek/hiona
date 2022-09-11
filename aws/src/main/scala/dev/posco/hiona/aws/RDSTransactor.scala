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

import cats.effect.Async
import doobie._

import cats.implicits._

object RDSTransactor {
  case class HostPort(host: String, port: Int)
  object HostPort {
    import io.circe._
    import io.circe.generic.semiauto._

    implicit val hostPortDecoder: Decoder[HostPort] = deriveDecoder[HostPort]
  }

  case class AuthInfo(username: String, password: String)
  object AuthInfo {
    import io.circe._
    import io.circe.generic.semiauto._

    implicit val authInfoDecoder: Decoder[AuthInfo] = deriveDecoder[AuthInfo]
  }

  case class DatabaseName(asString: String)

  // TODO, this should return a Resource
  def build[F[_]: Async](
      region: String,
      secretName: String,
      dbName: DatabaseName,
      hostPort: Option[HostPort] = None
  ): F[Transactor[F]] =
    Secrets
      .makeClient[F](region)
      .use(client => Secrets.getJsonSecret(client, secretName))
      .flatMap { jvalue =>
        Async[F].delay {
          val HostPort(h, p) = hostPort match {
            case None =>
              jvalue.as[HostPort].fold(throw _, identity)
            case Some(hp) => hp
          }

          val AuthInfo(uname, password) =
            jvalue.as[AuthInfo].fold(throw _, identity)
          (h, p, uname, password)
        }
      }
      .map {
        case (h, p, uname, password) =>
          val db = dbName.asString
          Transactor.fromDriverManager[F](
            classOf[org.postgresql.Driver].getName, // driver classname
            s"jdbc:postgresql://$h:$p/$db", // connect URL (driver-specific)
            uname,
            password
          )
      }
}
