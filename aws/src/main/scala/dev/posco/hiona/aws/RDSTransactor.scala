package dev.posco.hiona.aws

import cats.effect.{Async, Blocker, ContextShift}
import doobie._

import cats.implicits._

object RDSTransactor {
  case class HostPort(host: String, port: Int)
  object HostPort {
    import io.circe._, io.circe.generic.semiauto._

    implicit val hostPortDecoder: Decoder[HostPort] = deriveDecoder[HostPort]
  }

  case class AuthInfo(username: String, password: String)
  object AuthInfo {
    import io.circe._, io.circe.generic.semiauto._

    implicit val authInfoDecoder: Decoder[AuthInfo] = deriveDecoder[AuthInfo]
  }

  case class DatabaseName(asString: String)

  // TODO, this should return a Resource
  def build[F[_]: Async](
      region: String,
      secretName: String,
      dbName: DatabaseName,
      blocker: Blocker,
      hostPort: Option[HostPort] = None
  )(implicit ctx: ContextShift[F]): F[Transactor[F]] =
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
            password,
            blocker
          )
      }
}
